// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 新现网策略（默认 5 / 可选退避开启 / send≤3）的 mock 覆盖。
//! 旧回归在 provider_tests 里显式关闭退避。

use super::*;
use crate::kiro::error::{RateLimitError, RateLimitKind};
use crate::kiro::gate::AdmitBlocked;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::MultiTokenManager;
use crate::model::concurrency::ConcurrencySettings;
use crate::model::config::Config;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::post;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::watch;

fn valid_cred(token: &str) -> KiroCredentials {
    KiroCredentials {
        access_token: Some(token.to_string()),
        refresh_token: Some("r".repeat(150)),
        expires_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339()),
        ..Default::default()
    }
}

fn live_provider(
    creds: Vec<KiroCredentials>,
    mut config: Config,
    api: String,
    mcp: String,
) -> KiroProvider {
    if config.concurrency.is_none() {
        config.concurrency = Some(ConcurrencySettings::default());
    }
    config.validate().unwrap();
    let settings = config.effective_concurrency_settings();
    let tm = MultiTokenManager::new(config, creds, None, None, false).unwrap();
    KiroProvider::new(Arc::new(tm))
        .with_test_urls(api, mcp)
        .with_concurrency_gate(Arc::new(crate::kiro::gate::ConcurrencyGate::new(settings)))
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

fn hold_app(
    hits: Arc<AtomicUsize>,
    rx: watch::Receiver<bool>,
    ready_tx: tokio::sync::mpsc::Sender<()>,
) -> Router {
    Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let mut rx = rx.clone();
            let hits = hits.clone();
            let ready_tx = ready_tx.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
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
    )
}

#[tokio::test]
async fn default_cap_five_blocks_sixth_same_account() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(16);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 5, 400, 20),
    );
    let mut joins = Vec::new();
    for _ in 0..5 {
        let p = provider.clone();
        joins.push(tokio::spawn(async move {
            p.call_api_stream("{}", false, false, &[]).await
        }));
        ready_rx.recv().await.expect("held stream");
    }
    assert_eq!(provider.concurrency_gate().inflight(1), 5);
    let sixth = provider.call_api_stream("{}", false, false, &[]).await;
    assert!(sixth.is_err(), "第 6 路应被默认 5 挡住");
    let _ = tx.send(true);
    for j in joins {
        assert!(j.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn zero_cap_allows_more_than_twenty_same_account() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(32);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": { "maxInflightPerCredential": 0, "globalCooldownEnabled": false }
    }))
    .unwrap();
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a")],
            config,
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 0, 5000, 80),
    );
    let mut joins = Vec::new();
    for _ in 0..21 {
        let p = provider.clone();
        joins.push(tokio::spawn(async move {
            p.call_api_stream("{}", false, false, &[]).await
        }));
        ready_rx.recv().await.expect("held stream");
    }
    assert_eq!(hits.load(Ordering::SeqCst), 21);
    assert_eq!(provider.concurrency_gate().inflight(1), 21);
    let _ = tx.send(true);
    for j in joins {
        assert!(j.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn global_fifty_still_caps_when_account_unlimited() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(64);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a"), valid_cred("b")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 0, 800, 80),
    );
    let mut joins = Vec::new();
    for i in 0..50 {
        let p = provider.clone();
        let bound = if i % 2 == 0 { 1u64 } else { 2 };
        joins.push(tokio::spawn(async move {
            p.call_api_stream("{}", false, false, &[bound]).await
        }));
        ready_rx.recv().await.expect("held stream");
    }
    assert_eq!(hits.load(Ordering::SeqCst), 50);
    let extra = provider.call_api_stream("{}", false, false, &[1]).await;
    assert!(extra.is_err(), "global 50 仍应挡住第 51 路");
    let _ = tx.send(true);
    for j in joins {
        let _ = j.await;
    }
}

#[tokio::test]
async fn account_a_full_uses_b() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(8);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let provider = Arc::new(
        live_provider(
            vec![a, b],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 1, 2000, 10),
    );
    let p1 = provider.clone();
    let h1 = tokio::spawn(async move { p1.call_api_stream("{}", false, false, &[1]).await });
    ready_rx.recv().await.expect("A held");
    let p2 = provider.clone();
    let h2 = tokio::spawn(async move { p2.call_api_stream("{}", false, false, &[1, 2]).await });
    ready_rx.recv().await.expect("B used");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let _ = tx.send(true);
    assert!(h1.await.unwrap().is_ok());
    assert!(h2.await.unwrap().is_ok());
}

#[tokio::test]
async fn hot_update_zero_allows_over_five_without_resetting_inflight() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(16);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 5, 2000, 20),
    );
    let mut joins = Vec::new();
    for _ in 0..5 {
        let p = provider.clone();
        joins.push(tokio::spawn(async move {
            p.call_api_stream("{}", false, false, &[]).await
        }));
        ready_rx.recv().await.expect("held");
    }
    assert_eq!(provider.concurrency_gate().inflight(1), 5);
    let mut s = provider.concurrency_gate().settings();
    s.max_inflight_per_credential = 0;
    provider.concurrency_gate().update_settings(s);
    assert_eq!(provider.concurrency_gate().inflight(1), 5);
    let p6 = provider.clone();
    let h6 = tokio::spawn(async move { p6.call_api_stream("{}", false, false, &[]).await });
    ready_rx.recv().await.expect("6th after hot 0");
    assert_eq!(provider.concurrency_gate().inflight(1), 6);
    let _ = tx.send(true);
    assert!(h6.await.unwrap().is_ok());
    for j in joins {
        assert!(j.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn release_wakes_waiter() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(4);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 1, 2000, 8),
    );
    let p1 = provider.clone();
    let h1 = tokio::spawn(async move { p1.call_api_stream("{}", false, false, &[]).await });
    ready_rx.recv().await.expect("first");
    let p2 = provider.clone();
    let h2 = tokio::spawn(async move { p2.call_api_stream("{}", false, false, &[]).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = tx.send(true);
    assert!(h1.await.unwrap().is_ok());
    ready_rx.recv().await.expect("woken second");
    assert!(h2.await.unwrap().is_ok());
}

#[tokio::test]
async fn cooldown_has_zero_inflight_and_timeout_keeps_retry_after() {
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": { "globalFirstPauseSecs": 15, "globalCooldownEnabled": true },
        "admissionTimeoutMs": 80
    }))
    .unwrap();
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
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    provider.concurrency_gate().on_global_429();
    assert_eq!(provider.concurrency_gate().inflight(1), 0);
    assert!(matches!(
        provider.concurrency_gate().try_admit(1),
        Err(AdmitBlocked::Global(_))
    ));
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("冷却中应 429"),
    };
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() >= 10,
        "不得用 1 秒掩盖 15 秒冷却, got {}",
        rl.retry_after_secs()
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(provider.concurrency_gate().inflight(1), 0);
}

#[tokio::test]
async fn early_429_updates_gate_without_body() {
    let (hold_tx, hold_rx) = watch::channel(false);
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post({
            let hold_rx = hold_rx.clone();
            move || {
                let mut hold_rx = hold_rx.clone();
                let hits2 = hits2.clone();
                async move {
                    hits2.fetch_add(1, Ordering::SeqCst);
                    let (body_tx, body_rx) =
                        tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
                    tokio::spawn(async move {
                        while !*hold_rx.borrow() {
                            if hold_rx.changed().await.is_err() {
                                break;
                            }
                        }
                        let _ = body_tx.send(Ok(Bytes::from("slow"))).await;
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
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": { "globalFirstPauseSecs": 5 }
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let err = match provider.call_api("{}", false, false, &[]).await {
        Err(e) => e,
        Ok(_) => panic!("应 429"),
    };
    assert_eq!(
        err.downcast_ref::<RateLimitError>()
            .expect("typed")
            .retry_after_secs(),
        37
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(provider.concurrency_gate().global_blocked_until().is_some());
    let _ = hold_tx.send(true);
}

#[tokio::test]
async fn capacity_same_bucket_at_most_two_all_sends_at_most_three() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                (StatusCode::TOO_MANY_REQUESTS, "INSUFFICIENT_MODEL_CAPACITY")
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 0,
            "backoffMaxMs": 0
        }
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let _ = provider.call_api("{}", false, false, &[]).await;
    let n = hits.load(Ordering::SeqCst);
    assert!(n <= 2, "无头容量同号同端点最多 2, got {n}");
}

#[tokio::test]
async fn mixed_errors_actual_sends_at_most_three() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                let n = hits2.fetch_add(1, Ordering::SeqCst);
                match n {
                    0 => (StatusCode::TOO_MANY_REQUESTS, "INSUFFICIENT_MODEL_CAPACITY"),
                    1 => (StatusCode::INTERNAL_SERVER_ERROR, "boom"),
                    _ => (StatusCode::BAD_GATEWAY, "nope"),
                }
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": { "globalCooldownEnabled": false, "backoffBaseMs": 0 }
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let _ = provider.call_api("{}", false, false, &[]).await;
    assert_eq!(hits.load(Ordering::SeqCst), 3, "所有错误混合实际 send≤3");
}

#[test]
fn config_nested_alias_defaults() {
    let empty: Config = serde_json::from_str("{}").unwrap();
    assert_eq!(
        empty
            .effective_concurrency_settings()
            .max_inflight_per_credential,
        5
    );
    let alias: Config = serde_json::from_str(r#"{"maxConcurrentPerCredential":7}"#).unwrap();
    assert_eq!(
        alias
            .effective_concurrency_settings()
            .max_inflight_per_credential,
        7
    );
    let nested: Config = serde_json::from_str(
        r#"{"concurrency":{"maxInflightPerCredential":0},"maxConcurrentPerCredential":20}"#,
    )
    .unwrap();
    assert_eq!(
        nested
            .effective_concurrency_settings()
            .max_inflight_per_credential,
        0
    );
}

#[tokio::test]
async fn earliest_known_until_uses_short_account_not_last() {
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
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 200,
            "suspendedBackoffMs": 10000
        },
        "admissionTimeoutMs": 3000
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let provider = live_provider(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    provider.concurrency_gate().on_account_throttle(1, false);
    provider.concurrency_gate().on_account_throttle(2, true);
    let started = std::time::Instant::now();
    let ok = provider.call_api("{}", false, false, &[1, 2]).await;
    assert!(ok.is_ok(), "A 200ms 到期后应发出: {ok:?}");
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn current_constraint_sees_global_5_to_15() {
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": true,
            "globalFirstPauseSecs": 5,
            "globalSecondPauseSecs": 15
        },
        "admissionTimeoutMs": 80
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        "http://127.0.0.1:9/generateAssistantResponse".into(),
        "http://127.0.0.1:9/mcp".into(),
    );
    provider.concurrency_gate().on_global_429();
    provider.concurrency_gate().on_global_429();
    let err = provider
        .call_api("{}", false, false, &[])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() >= 10,
        "5→15 终态应回 15 附近, got {}",
        rl.retry_after_secs()
    );
}

#[tokio::test]
async fn account_window_longer_than_global_is_reported() {
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": true,
            "globalFirstPauseSecs": 5,
            "suspendedBackoffMs": 15000
        },
        "admissionTimeoutMs": 80
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        "http://127.0.0.1:9/generateAssistantResponse".into(),
        "http://127.0.0.1:9/mcp".into(),
    );
    provider.concurrency_gate().on_global_429();
    provider.concurrency_gate().on_account_throttle(1, true);
    let err = provider
        .call_api("{}", false, false, &[])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() >= 10,
        "账号窗长于 global 应报账号截止, got {}",
        rl.retry_after_secs()
    );
}

#[tokio::test]
async fn full_candidate_does_not_inherit_other_long_window() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(4);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "maxInflightPerCredential": 1,
            "suspendedBackoffMs": 10000
        },
        "admissionTimeoutMs": 200
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let provider = Arc::new(live_provider(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    ));
    let p1 = provider.clone();
    let h1 = tokio::spawn(async move { p1.call_api_stream("{}", false, false, &[1]).await });
    ready_rx.recv().await.expect("A held");
    provider.concurrency_gate().on_account_throttle(2, true);
    let err = provider
        .call_api("{}", false, false, &[1, 2])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() <= 2,
        "A 仅 Full 不得套用 B 的 10s, got {}",
        rl.retry_after_secs()
    );
    let _ = tx.send(true);
    let _ = h1.await;
}

#[tokio::test]
async fn capacity_balanced_stays_same_account_and_host() {
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
                hosts2.lock().unwrap().push(host);
                (StatusCode::TOO_MANY_REQUESTS, "INSUFFICIENT_MODEL_CAPACITY")
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "loadBalancingMode": "balanced",
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 0,
            "backoffMaxMs": 0
        },
        "admissionTimeoutMs": 5000
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let provider = live_provider(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let _ = provider.call_api("{}", false, false, &[1, 2]).await;
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(n, 2, "容量同号同端点恰好 2, got {n}");
    let seen = hosts.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1], "Host 不得换端点: {seen:?}");
    assert_eq!(provider.token_manager().rotation_bias_for_test(1), 0);
    assert_eq!(provider.token_manager().rotation_bias_for_test(2), 0);
}

#[tokio::test]
async fn mcp_capacity_at_most_two() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/mcp",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                (StatusCode::TOO_MANY_REQUESTS, "INSUFFICIENT_MODEL_CAPACITY")
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 0
        },
        "admissionTimeoutMs": 5000
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a"), valid_cred("b")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let _ = provider.call_mcp("{}", &[]).await;
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(provider.token_manager().rotation_bias_for_test(1), 0);
}

#[tokio::test]
async fn suspended_uses_custom_backoff_not_streak() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits2 = hits.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move || {
            let hits2 = hits2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                (StatusCode::TOO_MANY_REQUESTS, "TEMPORARILY_SUSPENDED")
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 500,
            "suspendedBackoffMs": 2500
        },
        "admissionTimeoutMs": 200
    }))
    .unwrap();
    let provider = live_provider(
        vec![valid_cred("a")],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let err = provider
        .call_api("{}", false, false, &[])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert!(
        rl.retry_after_secs() >= 2,
        "suspended 应走 2500ms 而非 500ms streak, got {}",
        rl.retry_after_secs()
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let until = provider.concurrency_gate().account_blocked_until(1);
    assert!(until.is_some());
}

#[tokio::test]
async fn account_full_waiters_do_not_spin_on_global_bounce() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(4);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let provider = Arc::new(
        live_provider(
            vec![valid_cred("a")],
            Config::default(),
            format!("{base}/generateAssistantResponse"),
            format!("{base}/mcp"),
        )
        .with_test_admission(50, 1, 2000, 20),
    );
    let p0 = provider.clone();
    let h0 = tokio::spawn(async move { p0.call_api_stream("{}", false, false, &[1]).await });
    ready_rx.recv().await.expect("held");
    let before = provider.acquire_attempts();
    let p1 = provider.clone();
    let h1 = tokio::spawn(async move { p1.call_api_stream("{}", false, false, &[1]).await });
    let p2 = provider.clone();
    let h2 = tokio::spawn(async move { p2.call_api_stream("{}", false, false, &[1]).await });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let extra = provider.acquire_attempts().saturating_sub(before);
    assert!(
        extra < 30,
        "global 有余量时账号满等待者不得互唤醒空转, attempts={extra}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let _ = tx.send(true);
    assert!(h0.await.unwrap().is_ok());
    let _ = h1.await;
    let _ = h2.await;
}

#[tokio::test]
async fn capacity_pin_wait_does_not_use_other_healthy_account() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hosts = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let ids_seen = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let hits2 = hits.clone();
    let hosts2 = hosts.clone();
    let ids2 = ids_seen.clone();
    let app = Router::new().route(
        "/generateAssistantResponse",
        post(move |hdrs: HeaderMap| {
            let hits2 = hits2.clone();
            let hosts2 = hosts2.clone();
            let ids2 = ids2.clone();
            async move {
                hits2.fetch_add(1, Ordering::SeqCst);
                let actor = match hdrs
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                {
                    Some("Bearer a") => 1,
                    Some("Bearer b") => 2,
                    _ => 0,
                };
                ids2.lock().unwrap().push(actor);
                let host = hdrs
                    .get(header::HOST)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                hosts2.lock().unwrap().push(host);
                (StatusCode::TOO_MANY_REQUESTS, "INSUFFICIENT_MODEL_CAPACITY")
            }
        }),
    );
    let base = spawn_router(app).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": false,
            "backoffBaseMs": 1500,
            "backoffMaxMs": 1500
        },
        "admissionTimeoutMs": 5000
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    a.priority = 1;
    let mut b = valid_cred("b");
    b.id = Some(2);
    b.priority = 2;
    let provider = live_provider(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    );
    let started = std::time::Instant::now();
    let _ = provider.call_api("{}", false, false, &[1, 2]).await;
    let n = hits.load(Ordering::SeqCst);
    assert_eq!(n, 2, "应对 A 打满 2 枪, got {n}");
    let seen = hosts.lock().unwrap().clone();
    assert_eq!(seen[0], seen[1], "不得换端点/号: {seen:?}");
    let extra = provider.acquire_attempts();
    assert!(extra < 40, "pin 后不得拿 B 立即可用空转, attempts={extra}");
    assert!(
        started.elapsed() >= Duration::from_millis(1400),
        "必须真正经过pin账号退避等待"
    );
    assert_eq!(provider.token_manager().rotation_bias_for_test(2), 0);
    assert_eq!(
        *ids_seen.lock().unwrap(),
        vec![1, 1],
        "健康B不是本次容量重试候选"
    );
}

#[tokio::test]
async fn global_full_untimed_not_covered_by_other_backoff() {
    let (tx, rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(4);
    let hits = Arc::new(AtomicUsize::new(0));
    let base = spawn_router(hold_app(hits.clone(), rx, ready_tx)).await;
    let config: Config = serde_json::from_value(serde_json::json!({
        "maxConcurrentRequests": 1,
        "concurrency": {
            "globalCooldownEnabled": false,
            "maxInflightPerCredential": 5,
            "suspendedBackoffMs": 15000
        },
        "admissionTimeoutMs": 200
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let provider = Arc::new(live_provider(
        vec![a, b],
        config,
        format!("{base}/generateAssistantResponse"),
        format!("{base}/mcp"),
    ));
    let p0 = provider.clone();
    let h0 = tokio::spawn(async move { p0.call_api_stream("{}", false, false, &[1]).await });
    ready_rx.recv().await.expect("global held");
    provider.concurrency_gate().on_account_throttle(2, true);
    let err = provider
        .call_api("{}", false, false, &[1, 2])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert_eq!(rl.kind, RateLimitKind::LocalBusy);
    assert!(
        rl.retry_after_secs() <= 2,
        "A 只等 global 流结束不得套 B 15s, got {}",
        rl.retry_after_secs()
    );
    let _ = tx.send(true);
    let _ = h0.await;
}

#[tokio::test]
async fn global_cooldown_not_masked_by_local_busy() {
    let config: Config = serde_json::from_value(serde_json::json!({
        "concurrency": {
            "globalCooldownEnabled": true,
            "globalFirstPauseSecs": 15,
            "suspendedBackoffMs": 15000
        },
        "admissionTimeoutMs": 80
    }))
    .unwrap();
    let mut a = valid_cred("a");
    a.id = Some(1);
    let mut b = valid_cred("b");
    b.id = Some(2);
    let provider = live_provider(
        vec![a, b],
        config,
        "http://127.0.0.1:9/generateAssistantResponse".into(),
        "http://127.0.0.1:9/mcp".into(),
    );
    provider.concurrency_gate().on_global_429();
    provider.concurrency_gate().on_account_throttle(2, true);
    let err = provider
        .call_api("{}", false, false, &[1, 2])
        .await
        .unwrap_err();
    let rl = err.downcast_ref::<RateLimitError>().expect("typed");
    assert_ne!(rl.kind, RateLimitKind::LocalBusy);
    assert!(
        rl.retry_after_secs() >= 10,
        "覆盖全池的 global 冷却不能被 1s LocalBusy 盖短, got {}",
        rl.retry_after_secs()
    );
}
