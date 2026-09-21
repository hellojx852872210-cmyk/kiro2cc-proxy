// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 真实 provider + loopback HTTP mock（无真实上游）。

use super::*;
use crate::kiro::error::RateLimitError;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::MultiTokenManager;
use crate::model::config::Config;
use crate::model::rpm::RpmTracker;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::post;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

fn valid_cred(token: &str) -> KiroCredentials {
    KiroCredentials {
        access_token: Some(token.to_string()),
        refresh_token: Some("r".repeat(150)),
        expires_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339()),
        ..Default::default()
    }
}

fn provider_with(
    creds: Vec<KiroCredentials>,
    config: Config,
    api: String,
    mcp: String,
) -> KiroProvider {
    config.validate().unwrap();
    let tm = MultiTokenManager::new(config, creds, None, None, false).unwrap();
    KiroProvider::new(Arc::new(tm)).with_test_urls(api, mcp)
}

async fn spawn_router(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let url = format!("http://{addr}");
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    url
}

#[tokio::test]
async fn mock_429_retry_after_stops_this_call_and_returns_typed_error() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                let mut headers = HeaderMap::new();
                headers.insert(header::RETRY_AFTER, "37".parse().unwrap());
                (StatusCode::TOO_MANY_REQUESTS, headers, "throttled")
            }
        }),
    );
    let base = spawn_router(app).await;
    let api = format!("{base}/generateAssistantResponse");
    let mcp = format!("{base}/mcp");
    let provider = provider_with(vec![valid_cred("t1")], Config::default(), api, mcp);

    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("expected typed 429"),
    };
    let rl = err.downcast_ref::<RateLimitError>().expect("typed 429");
    assert_eq!(rl.retry_after_secs(), 37);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "有效 Retry-After 不得换桶重试"
    );
}

#[tokio::test]
async fn mock_hold_stream_global2_account1_blocks_third_same_account() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(8);
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let mut rx = rx.clone();
            let hits2 = hits2.clone();
            let ready_tx = ready_tx.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                let _ = ready_tx.try_send(());
                let (body_tx, body_rx) =
                    tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
                tokio::spawn(async move {
                    let _ = rx.changed().await;
                    let _ = body_tx.send(Ok(Bytes::from("done"))).await;
                });
                let stream = futures::stream::unfold(body_rx, |mut rx| async move {
                    rx.recv().await.map(|item| (item, rx))
                });
                axum::response::Response::builder()
                    .status(200)
                    .header("content-type", "application/octet-stream")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
        }),
    );
    let base = spawn_router(app).await;
    let api = format!("{base}/generateAssistantResponse");
    let mcp = format!("{base}/mcp");
    let cred_a = valid_cred("a");
    let cred_b = valid_cred("b");
    let provider = Arc::new(
        provider_with(vec![cred_a, cred_b], Config::default(), api, mcp)
            .with_test_admission(2, 1, 500, 10),
    );

    let p1 = provider.clone();
    let h1 = tokio::spawn(async move { p1.call_api_stream("{}", false, false, &[1]).await });
    ready_rx.recv().await.expect("first stream headers");

    let p2 = provider.clone();
    let r2 = p2.call_api_stream("{}", false, false, &[1]).await;
    let err = match r2 {
        Err(e) => e,
        Ok(_) => panic!("同账号第二路应 typed local busy"),
    };
    assert!(
        err.downcast_ref::<RateLimitError>().is_some(),
        "同账号 account=1 应 local busy: {err}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    let p3 = provider.clone();
    let h3 = tokio::spawn(async move { p3.call_api_stream("{}", false, false, &[2]).await });
    ready_rx.recv().await.expect("other account headers");
    assert_eq!(hits.load(Ordering::SeqCst), 2);

    let _ = tx.send(true);
    assert!(h1.await.unwrap().is_ok());
    assert!(h3.await.unwrap().is_ok());
}

#[tokio::test]
async fn mock_rpm_limit_8_from_16_via_real_send() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let base = spawn_router(app).await;
    let api = format!("{base}/generateAssistantResponse");
    let mcp = format!("{base}/mcp");
    let mut config = Config::default();
    config.max_rpm_per_credential = 8;
    let rpm = Arc::new(RpmTracker::new());
    let provider = Arc::new(
        provider_with(vec![valid_cred("t1")], config, api, mcp).with_rpm_tracker(rpm.clone()),
    );

    let mut joins = Vec::new();
    for _ in 0..16 {
        let p = provider.clone();
        joins.push(tokio::spawn(async move {
            p.call_api("{}", false, false, &[]).await
        }));
    }
    let mut ok = 0;
    let mut limited = 0;
    for j in joins {
        match j.await.unwrap() {
            Ok(_) => ok += 1,
            Err(e) => {
                assert!(e.downcast_ref::<RateLimitError>().is_some(), "{e}");
                limited += 1;
            }
        }
    }
    assert_eq!(ok, 8);
    assert_eq!(limited, 8);
    assert_eq!(hits.load(Ordering::SeqCst), 8);
    assert_eq!(rpm.credential_rpm(1), 8);
}

#[tokio::test]
async fn mock_eof_releases_slot_for_next_request() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                "ok"
            }
        }),
    );
    let base = spawn_router(app).await;
    let api = format!("{base}/generateAssistantResponse");
    let mcp = format!("{base}/mcp");
    let provider = provider_with(vec![valid_cred("t1")], Config::default(), api, mcp)
        .with_test_admission(1, 1, 2000, 4);

    let (resp, _) = provider
        .call_api_stream("{}", false, false, &[])
        .await
        .unwrap();
    let mut stream = resp.bytes_stream();
    while stream.next().await.is_some() {}
    drop(stream);

    let (resp2, _) = provider
        .call_api_stream("{}", false, false, &[])
        .await
        .unwrap();
    drop(resp2);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mock_mcp_429_is_typed_not_200() {
    let app = Router::new().route(
        "/mcp",
        post(|| async {
            let mut headers = HeaderMap::new();
            headers.insert(header::RETRY_AFTER, "9".parse().unwrap());
            (StatusCode::TOO_MANY_REQUESTS, headers, "slow")
        }),
    );
    let base = spawn_router(app).await;
    let provider = provider_with(
        vec![valid_cred("t1")],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let err = match provider.call_mcp("{}", &[]).await {
        Err(e) => e,
        Ok(_) => panic!("expected typed mcp 429"),
    };
    let rl = err.downcast_ref::<RateLimitError>().expect("typed mcp 429");
    assert_eq!(rl.retry_after_secs(), 9);
}

#[tokio::test]
async fn mock_new_request_all_endpoints_throttled_is_typed_429() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let base = spawn_router(app).await;
    let registry = Arc::new(EndpointBucketRegistry::new());
    for name in EndpointName::ALL {
        registry.throttle(1, name, Duration::from_secs(30));
    }
    let provider = provider_with(
        vec![valid_cred("t1")],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_endpoint_registry(registry);
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("全桶冷却应 typed 429 而非 502"),
    };
    assert!(err.downcast_ref::<RateLimitError>().is_some(), "got {err}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mock_ten_candidates_ninth_limited_still_reaches_tenth() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let base = spawn_router(app).await;
    let mut config = Config::default();
    config.max_rpm_per_credential = 1;
    let rpm = Arc::new(RpmTracker::new());
    for id in 1..=9 {
        assert!(rpm.try_reserve_credential(id, 1).is_ok());
    }
    let creds: Vec<_> = (1..=10)
        .map(|i| {
            let mut c = valid_cred(&format!("t{i}"));
            c.id = Some(i);
            c
        })
        .collect();
    let provider = provider_with(
        creds,
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_rpm_tracker(rpm);
    let (_resp, id) = provider
        .call_api("{}", false, false, &[])
        .await
        .expect("第10个空闲账号应被选中");
    assert_eq!(id, 10);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mock_earliest_recovery_not_last_candidate() {
    let registry = Arc::new(EndpointBucketRegistry::new());
    for name in EndpointName::ALL {
        registry.throttle(1, name, Duration::from_secs(60));
        registry.throttle(2, name, Duration::from_secs(2));
    }
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(|| async { StatusCode::OK }),
    );
    let base = spawn_router(app).await;
    let provider = provider_with(
        vec![a, b],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_endpoint_registry(registry);
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("两端点全冷却应 429"),
    };
    let secs = err
        .downcast_ref::<RateLimitError>()
        .unwrap()
        .retry_after_secs();
    assert!(secs <= 3, "应取最早恢复而非 60s, got {secs}");
}

#[tokio::test]
async fn mock_admission_deadline_blocks_hanging_refresh_without_model_send() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let base = spawn_router(app).await;
    let hang = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((_stream, _)) = listener.accept().await {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        format!("http://{addr}/token")
    };
    let cred = KiroCredentials {
        auth_method: Some("external_idp".to_string()),
        refresh_token: Some("short-refresh-token".to_string()),
        client_id: Some("client-id".to_string()),
        token_endpoint: Some(hang),
        access_token: Some("old".to_string()),
        expires_at: Some((Utc::now() - ChronoDuration::hours(1)).to_rfc3339()),
        ..Default::default()
    };
    let provider = provider_with(
        vec![cred],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_test_admission(50, 20, 200, 10);
    let started = std::time::Instant::now();
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("短 deadline 应失败"),
    };
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        err.downcast_ref::<RateLimitError>().is_some(),
        "无历史错误应 local busy: {err}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0, "不得发模型请求");
}

#[tokio::test]
async fn mock_drop_held_mcp_releases_global1_slot() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(4);
    let mcp_hits = Arc::new(AtomicUsize::new(0));
    let api_hits = Arc::new(AtomicUsize::new(0));
    let mcp_hits2 = mcp_hits.clone();
    let api_hits2 = api_hits.clone();
    let app = Router::new()
        .route(
            "/generateAssistantResponse",
            post(move || {
                let api_hits2 = api_hits2.clone();
                async move {
                    api_hits2.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        )
        .route(
            "/mcp",
            post({
                let rx = rx.clone();
                let ready_tx = ready_tx.clone();
                move || {
                    let mut rx = rx.clone();
                    let mcp_hits2 = mcp_hits2.clone();
                    let ready_tx = ready_tx.clone();
                    async move {
                        mcp_hits2.fetch_add(1, Ordering::SeqCst);
                        let _ = ready_tx.try_send(());
                        let (body_tx, body_rx) =
                            tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
                        tokio::spawn(async move {
                            let _ = rx.changed().await;
                            let _ = body_tx.send(Ok(Bytes::from("{}"))).await;
                        });
                        let stream = futures::stream::unfold(body_rx, |mut rx| async move {
                            rx.recv().await.map(|item| (item, rx))
                        });
                        axum::response::Response::builder()
                            .status(200)
                            .body(Body::from_stream(stream))
                            .unwrap()
                    }
                }
            }),
        );
    let base = spawn_router(app).await;
    let provider = Arc::new(
        provider_with(
            vec![valid_cred("t1")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(1, 1, 2000, 4),
    );
    let p = provider.clone();
    let h = tokio::spawn(async move { p.call_mcp("{}", &[]).await });
    ready_rx.recv().await.unwrap();
    h.abort();
    let _ = h.await;
    let _ = tx.send(true);
    let _ = provider
        .call_api("{}", false, false, &[])
        .await
        .expect("MCP drop 后 global1 槽应可用");
    assert_eq!(api_hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mock_same_account_ide_429_without_header_falls_back_to_runtime() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hosts = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let hits2 = hits.clone();
    let hosts2 = hosts.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move |hdrs: HeaderMap| {
            let hits2 = hits2.clone();
            let hosts2 = hosts2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                let host = hdrs
                    .get(header::HOST)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                hosts2.lock().unwrap().push(host.clone());
                if host.starts_with("runtime.") {
                    StatusCode::OK
                } else {
                    StatusCode::TOO_MANY_REQUESTS
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let rpm = Arc::new(RpmTracker::new());
    let provider = provider_with(
        vec![valid_cred("t1")],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_rpm_tracker(rpm.clone());

    let started = std::time::Instant::now();
    let result = provider.call_api("{}", false, false, &[]).await;
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "同号端点 fallback 不应被短 deadline 误杀"
    );
    let (_resp, id) = result.expect("IDE 无头 429 后应落到 Runtime 200");
    assert_eq!(id, 1);
    assert_eq!(hits.load(Ordering::SeqCst), 2, "应发送两次");
    let seen = hosts.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "hosts={seen:?}");
    assert!(
        seen[0].starts_with("q."),
        "第一次应为 Ide host, got {}",
        seen[0]
    );
    assert!(
        seen[1].starts_with("runtime."),
        "第二次应为 Runtime host, got {}",
        seen[1]
    );
    assert_ne!(seen[0], seen[1]);
    assert_eq!(rpm.credential_rpm(1), 2, "每次真正 send 记一次 RPM");
}

fn expired_idp_cred(endpoint: String) -> KiroCredentials {
    KiroCredentials {
        auth_method: Some("external_idp".to_string()),
        refresh_token: Some("old-refresh-token".to_string()),
        client_id: Some("client-id".to_string()),
        token_endpoint: Some(endpoint),
        access_token: Some("old-access".to_string()),
        expires_at: Some((Utc::now() - ChronoDuration::hours(1)).to_rfc3339()),
        ..Default::default()
    }
}

fn provider_with_creds_file(
    creds: Vec<KiroCredentials>,
    config: Config,
    api: String,
    mcp: String,
    creds_path: std::path::PathBuf,
) -> KiroProvider {
    config.validate().unwrap();
    let json = serde_json::to_string_pretty(&creds).unwrap();
    std::fs::write(&creds_path, json).unwrap();
    let tm = MultiTokenManager::new(config, creds, None, Some(creds_path), true).unwrap();
    KiroProvider::new(Arc::new(tm)).with_test_urls(api, mcp)
}

async fn spawn_delayed_oauth(
    delay: Duration,
    hits: Arc<AtomicUsize>,
    started: Option<tokio::sync::mpsc::Sender<()>>,
) -> String {
    let app = Router::new().route(
        "/token",
        post(move || {
            let hits = hits.clone();
            let started = started.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = started {
                    let _ = tx.try_send(());
                }
                tokio::time::sleep(delay).await;
                axum::Json(serde_json::json!({
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 3600
                }))
            }
        }),
    );
    let base = spawn_router(app).await;
    format!("{base}/token")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_token_prep_timeout_keeps_rotated_refresh_without_model_send() {
    let model_hits = Arc::new(AtomicUsize::new(0));
    let model_hits2 = model_hits.clone();
    let model_app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let model_hits2 = model_hits2.clone();
            async move {
                model_hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let model_base = spawn_router(model_app).await;
    let oauth_hits = Arc::new(AtomicUsize::new(0));
    let oauth = spawn_delayed_oauth(Duration::from_millis(400), oauth_hits.clone(), None).await;
    let dir = std::env::temp_dir().join(format!("k2cc-r11-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let creds_path = dir.join("credentials.json");
    let provider = provider_with_creds_file(
        vec![expired_idp_cred(oauth)],
        Config::default(),
        format!("{model_base}/generateAssistantResponse"),
        format!("{model_base}/mcp"),
        creds_path.clone(),
    )
    .with_test_admission(50, 20, 200, 10);

    let started = std::time::Instant::now();
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("短 deadline 应及时 429"),
    };
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(err.downcast_ref::<RateLimitError>().is_some(), "{err}");
    assert_eq!(model_hits.load(Ordering::SeqCst), 0, "超时不得发模型请求");

    let (access, refresh) = wait_rotated_secrets(provider.token_manager(), 1).await;
    assert_eq!(access.as_deref(), Some("new-access"));
    assert_eq!(refresh.as_deref(), Some("new-refresh"));
    let disk_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let disk_refresh = loop {
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&creds_path).unwrap()).unwrap();
        let got = on_disk
            .as_array()
            .and_then(|a| a.first())
            .and_then(|c| c.get("refreshToken"))
            .and_then(|v| v.as_str())
            .map(str::to_string);
        if got.as_deref() == Some("new-refresh") {
            break got;
        }
        assert!(
            std::time::Instant::now() < disk_deadline,
            "文件尚未落到 new-refresh: {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(disk_refresh.as_deref(), Some("new-refresh"));
    assert_eq!(oauth_hits.load(Ordering::SeqCst), 1);

    let _ = provider
        .call_api("{}", false, false, &[])
        .await
        .expect("后续请求应使用已轮换 token 成功");
    assert_eq!(model_hits.load(Ordering::SeqCst), 1);
    assert_eq!(oauth_hits.load(Ordering::SeqCst), 1, "不得再刷新");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_token_prep_client_cancel_still_saves_rotation() {
    let model_hits = Arc::new(AtomicUsize::new(0));
    let model_hits2 = model_hits.clone();
    let model_app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let model_hits2 = model_hits2.clone();
            async move {
                model_hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let model_base = spawn_router(model_app).await;
    let oauth_hits = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(1);
    let oauth = spawn_delayed_oauth(
        Duration::from_millis(400),
        oauth_hits.clone(),
        Some(started_tx),
    )
    .await;
    let dir = std::env::temp_dir().join(format!("k2cc-r11-cancel-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let creds_path = dir.join("credentials.json");
    let provider = Arc::new(
        provider_with_creds_file(
            vec![expired_idp_cred(oauth)],
            Config::default(),
            format!("{model_base}/generateAssistantResponse"),
            format!("{model_base}/mcp"),
            creds_path.clone(),
        )
        .with_test_admission(50, 20, 5000, 10),
    );
    let p = provider.clone();
    let h = tokio::spawn(async move { p.call_api("{}", false, false, &[]).await });
    started_rx.recv().await.expect("OAuth 已收到旧 token");
    h.abort();
    let _ = h.await;
    assert_eq!(model_hits.load(Ordering::SeqCst), 0);
    let (_, refresh) = wait_rotated_secrets(provider.token_manager(), 1).await;
    assert_eq!(refresh.as_deref(), Some("new-refresh"));
    let on_disk: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&creds_path).unwrap()).unwrap();
    assert_eq!(on_disk[0]["refreshToken"].as_str(), Some("new-refresh"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_token_prep_slots_bound_same_account_single_refresh() {
    let model_hits = Arc::new(AtomicUsize::new(0));
    let model_hits2 = model_hits.clone();
    let model_app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let model_hits2 = model_hits2.clone();
            async move {
                model_hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let model_base = spawn_router(model_app).await;
    let oauth_hits = Arc::new(AtomicUsize::new(0));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(1);
    let (hold_tx, hold_rx) = watch::channel(false);
    let oauth = spawn_held_oauth(oauth_hits.clone(), started_tx, hold_rx).await;
    let dir = std::env::temp_dir().join(format!("k2cc-r11-slots-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let creds_path = dir.join("credentials.json");
    let provider = Arc::new(
        provider_with_creds_file(
            vec![expired_idp_cred(oauth)],
            Config::default(),
            format!("{model_base}/generateAssistantResponse"),
            format!("{model_base}/mcp"),
            creds_path,
        )
        .with_test_admission(50, 20, 150, 2),
    );
    let mut joins = Vec::new();
    for _ in 0..10 {
        let p = provider.clone();
        joins.push(tokio::spawn(async move {
            p.call_api("{}", false, false, &[]).await
        }));
    }
    started_rx.recv().await.expect("OAuth 仍在途");
    for j in joins {
        let res = j.await.unwrap();
        assert!(res.is_err(), "短期限应全部失败");
    }
    assert_eq!(model_hits.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider.token_prep_available(),
        0,
        "第一波 timeout 后准备槽仍应被在途 OAuth 占满"
    );
    assert_eq!(oauth_hits.load(Ordering::SeqCst), 1);

    let mut wave2 = Vec::new();
    for _ in 0..10 {
        let p = provider.clone();
        wave2.push(tokio::spawn(async move {
            p.call_api("{}", false, false, &[]).await
        }));
    }
    for j in wave2 {
        let res = j.await.unwrap();
        assert!(res.is_err(), "准备槽满时应 local busy");
        let err = res.unwrap_err();
        assert!(
            err.downcast_ref::<RateLimitError>().is_some(),
            "第二波应为 typed local busy: {err}"
        );
    }
    assert_eq!(
        oauth_hits.load(Ordering::SeqCst),
        1,
        "第二波不得新增后台准备/刷新"
    );
    assert_eq!(model_hits.load(Ordering::SeqCst), 0);
    assert_eq!(provider.token_prep_available(), 0);

    let _ = hold_tx.send(true);
    let (_, refresh) = wait_rotated_secrets(provider.token_manager(), 1).await;
    assert_eq!(refresh.as_deref(), Some("new-refresh"));
    assert_eq!(
        oauth_hits.load(Ordering::SeqCst),
        1,
        "同账号 refresh_lock 二次检查只应刷新一次"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while provider.token_prep_available() != 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "准备槽完成后应回收, left={}",
            provider.token_prep_available()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn spawn_held_oauth(
    hits: Arc<AtomicUsize>,
    started: tokio::sync::mpsc::Sender<()>,
    hold: watch::Receiver<bool>,
) -> String {
    let app = Router::new().route(
        "/token",
        post(move || {
            let hits = hits.clone();
            let started = started.clone();
            let mut hold = hold.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = started.try_send(());
                while !*hold.borrow() {
                    if hold.changed().await.is_err() {
                        break;
                    }
                }
                axum::Json(serde_json::json!({
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 3600
                }))
            }
        }),
    );
    let base = spawn_router(app).await;
    format!("{base}/token")
}

async fn wait_rotated_secrets(tm: &MultiTokenManager, id: u64) -> (Option<String>, Option<String>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let secrets = tm.credential_secrets_for_test(id).expect("账号仍在内存");
        if secrets.1.as_deref() == Some("new-refresh") {
            return secrets;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "后台轮换超时仍为 {:?}",
            secrets
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn mock_finalize_keeps_rpm_rate_over_token_busy() {
    let model_hits = Arc::new(AtomicUsize::new(0));
    let model_hits2 = model_hits.clone();
    let model_app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let model_hits2 = model_hits2.clone();
            async move {
                model_hits2.fetch_add(1, Ordering::SeqCst);
                StatusCode::OK
            }
        }),
    );
    let model_base = spawn_router(model_app).await;
    let hang = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((_stream, _)) = listener.accept().await {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        format!("http://{addr}/token")
    };
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = expired_idp_cred(hang);
    b.id = Some(2);
    b.priority = 2;
    let mut config = Config::default();
    config.max_rpm_per_credential = 1;
    let rpm = Arc::new(RpmTracker::new());
    assert!(rpm.try_reserve_credential(1, 1).is_ok());
    let provider = provider_with(
        vec![a, b],
        config,
        format!("{model_base}/generateAssistantResponse"),
        format!("{model_base}/mcp"),
    )
    .with_rpm_tracker(rpm)
    .with_test_admission(50, 20, 200, 10);
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("应失败"),
    };
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() >= 30,
        "token busy 不得把 RPM 历史改成 1s, got {}",
        rl.retry_after_secs()
    );
    assert_eq!(model_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mock_finalize_keeps_5xx_over_token_busy() {
    let hang = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((_stream, _)) = listener.accept().await {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        format!("http://{addr}/token")
    };
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = expired_idp_cred(hang);
    b.id = Some(2);
    b.priority = 2;
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let base = spawn_router(app).await;
    let provider = provider_with(
        vec![a, b],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_test_admission(50, 20, 400, 10);
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("应失败"),
    };
    let s = err.to_string();
    assert!(
        s.contains("500") || s.contains("Internal") || s.contains("失败"),
        "历史 5xx 不得被 token busy 改成 429: {s}"
    );
    assert!(err.downcast_ref::<RateLimitError>().is_none(), "{s}");
}

#[tokio::test]
async fn mock_bound_quota_402_beats_prior_429() {
    use axum::response::IntoResponse;
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let n2 = n2.clone();
            async move {
                let i = n2.fetch_add(1, Ordering::SeqCst);
                if i == 0 {
                    StatusCode::TOO_MANY_REQUESTS.into_response()
                } else {
                    (StatusCode::from_u16(402).unwrap(), "MONTHLY_REQUEST_COUNT").into_response()
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let provider = provider_with(
        vec![a, b],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let err = match provider.call_api("{}", false, false, &[1]).await {
        Err(e) => e,
        Ok(_) => panic!("bound A 全额耗尽应 402"),
    };
    let s = err.to_string();
    assert!(
        s.contains("QUOTA_EXHAUSTED_ALL"),
        "池外 B 健康时 bound A 402 不得被历史 429 盖住: {s}"
    );
}

#[tokio::test]
async fn mock_subset_quota_preserves_original_scope_rpm() {
    use crate::kiro::error::RateLimitKind;
    let hits = Arc::new(AtomicUsize::new(0));
    let received = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let received = received.clone();
            async move {
                received.fetch_add(1, Ordering::SeqCst);
                (StatusCode::PAYMENT_REQUIRED, "MONTHLY_REQUEST_COUNT")
            }
        }),
    );
    let base = spawn_router(app).await;
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let config: Config = serde_json::from_value(serde_json::json!({
        "maxRpmPerCredential": 2
    }))
    .unwrap();
    let rpm = Arc::new(RpmTracker::new());
    for _ in 0..2 {
        rpm.try_reserve_credential(1, 2).unwrap();
    }
    let provider = provider_with(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_rpm_tracker(rpm.clone());
    let err = match provider.call_api("{}", false, false, &[1, 2]).await {
        Err(err) => err,
        Ok(_) => panic!("A RPM满、B额度耗尽应暂时限流"),
    };
    let limit = err
        .downcast_ref::<RateLimitError>()
        .expect("子集额度标记不得覆盖原范围RPM429");
    assert_eq!(limit.kind, RateLimitKind::Rpm);
    assert!(!err.to_string().contains(QUOTA_EXHAUSTED_ALL_MARKER));
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "只能发送给B一次，不能越过A的RPM门"
    );
    assert_eq!(rpm.credential_rpm(1), 2);
}

#[test]
fn subset_quota_marker_without_rate_is_not_an_original_scope_402() {
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let provider = provider_with(
        vec![a, b],
        Config::default(),
        "http://127.0.0.1:9/generateAssistantResponse".into(),
        "http://127.0.0.1:9/mcp".into(),
    );
    provider.token_manager.report_quota_exhausted(2);
    let subset = provider.token_manager.describe_unavailable(None, &[2]);
    assert!(subset.contains(QUOTA_EXHAUSTED_ALL_MARKER));
    let err = provider.finalize_outcome(None, &[1, 2], &None, &Some(anyhow::anyhow!(subset)), None);
    assert!(
        !err.to_string().contains(QUOTA_EXHAUSTED_ALL_MARKER),
        "不能从历史子集推断原范围全耗尽"
    );
}

#[tokio::test]
async fn mock_hard_a_soft_b_still_uses_b_runtime() {
    let hosts = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let hits = Arc::new(AtomicUsize::new(0));
    let hosts2 = hosts.clone();
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move |hdrs: HeaderMap| {
            let hosts2 = hosts2.clone();
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                let host = hdrs
                    .get(header::HOST)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                hosts2.lock().unwrap().push(host.clone());
                if host.starts_with("runtime.") {
                    StatusCode::OK
                } else {
                    StatusCode::TOO_MANY_REQUESTS
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let mut config = Config::default();
    // A 打满 RPM；B 仍需 Ide+Runtime 两次发送，故上限 >1。
    config.max_rpm_per_credential = 5;
    let rpm = Arc::new(RpmTracker::new());
    for _ in 0..5 {
        assert!(rpm.try_reserve_credential(1, 5).is_ok());
    }
    let provider = provider_with(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_rpm_tracker(rpm.clone());
    let (_resp, id) = provider
        .call_api("{}", false, false, &[1, 2])
        .await
        .expect("B Runtime 应 200");
    assert_eq!(id, 2);
    assert_eq!(hits.load(Ordering::SeqCst), 2, "A 不发包，B 两端点共 2 次");
    let seen = hosts.lock().unwrap().clone();
    assert!(seen[0].starts_with("q."), "{seen:?}");
    assert!(seen[1].starts_with("runtime."), "{seen:?}");
    assert_eq!(rpm.credential_rpm(1), 5, "A 预留后不再 send");
    assert_eq!(rpm.credential_rpm(2), 2);
}

#[tokio::test]
async fn mock_429_valid_retry_after_does_not_wait_body_eof() {
    let (hold_tx, hold_rx) = watch::channel(false);
    let app = Router::new().route(
        "/generateAssistantResponse",
        post({
            let hold_rx = hold_rx.clone();
            move || {
                let mut hold_rx = hold_rx.clone();
                async move {
                    let (body_tx, body_rx) =
                        tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
                    tokio::spawn(async move {
                        while !*hold_rx.borrow() {
                            if hold_rx.changed().await.is_err() {
                                break;
                            }
                        }
                        let _ = body_tx.send(Ok(Bytes::from("slow-body"))).await;
                    });
                    let stream = futures::stream::unfold(body_rx, |mut rx| async move {
                        rx.recv().await.map(|item| (item, rx))
                    });
                    let mut headers = HeaderMap::new();
                    headers.insert(header::RETRY_AFTER, "37".parse().unwrap());
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        headers,
                        Body::from_stream(stream),
                    )
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let provider = provider_with(
        vec![valid_cred("t1")],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_test_admission(1, 1, 5000, 4);
    let started = std::time::Instant::now();
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("应 typed 429"),
    };
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "不得等 body EOF: {elapsed:?}"
    );
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert_eq!(rl.retry_after_secs(), 37);
    let started2 = std::time::Instant::now();
    let err2 = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("槽应已释放并仍限流"),
    };
    assert!(started2.elapsed() < Duration::from_secs(2));
    assert!(err2.downcast_ref::<RateLimitError>().is_some());
    let _ = hold_tx.send(true);
}

#[tokio::test]
async fn mock_mcp_429_valid_retry_after_does_not_wait_body_eof() {
    let (hold_tx, hold_rx) = watch::channel(false);
    let app = Router::new().route(
        "/mcp",
        post({
            let hold_rx = hold_rx.clone();
            move || {
                let mut hold_rx = hold_rx.clone();
                async move {
                    let (body_tx, body_rx) =
                        tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
                    tokio::spawn(async move {
                        while !*hold_rx.borrow() {
                            if hold_rx.changed().await.is_err() {
                                break;
                            }
                        }
                        let _ = body_tx.send(Ok(Bytes::from("slow-mcp"))).await;
                    });
                    let stream = futures::stream::unfold(body_rx, |mut rx| async move {
                        rx.recv().await.map(|item| (item, rx))
                    });
                    let mut headers = HeaderMap::new();
                    headers.insert(header::RETRY_AFTER, "37".parse().unwrap());
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        headers,
                        Body::from_stream(stream),
                    )
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let provider = provider_with(
        vec![valid_cred("t1")],
        Config::default(),
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    )
    .with_test_admission(1, 1, 5000, 4);
    let started = std::time::Instant::now();
    let err = match provider.call_mcp("{}", &[]).await {
        Err(e) => e,
        Ok(_) => panic!("应 typed 429"),
    };
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        err.downcast_ref::<RateLimitError>()
            .expect("typed")
            .retry_after_secs(),
        37
    );
    let started2 = std::time::Instant::now();
    let _ = provider.call_mcp("{}", &[]).await;
    assert!(
        started2.elapsed() < Duration::from_secs(2),
        "MCP 槽应已释放"
    );
    let _ = hold_tx.send(true);
}
