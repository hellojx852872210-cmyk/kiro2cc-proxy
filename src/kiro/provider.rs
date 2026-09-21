// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多账号故障转移和重试

use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HOST, HeaderMap, HeaderValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::admission::{AdmissionGate, AdmissionTicket, try_acquire_pair};
use crate::kiro::endpoint::{
    BUCKET_THROTTLE_DURATION, Endpoint, EndpointBucketRegistry, EndpointName,
};
use crate::kiro::error::{RateLimitError, parse_retry_after_from_headers};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::{KiroCredentials, fallback_profile_arn_value};
use crate::kiro::response::LeasedResponse;
use crate::kiro::token_manager::{CallContext, MultiTokenManager, QUOTA_EXHAUSTED_ALL_MARKER};
use crate::model::config::TlsBackend;
use crate::model::failure_log::FailureLogStore;
use crate::model::rpm::RpmTracker;
use crate::model::throttle_log::ThrottleLogStore;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

/// 每个账号的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;

/// 最大并发请求数（同时发往上游的请求上限）
#[allow(dead_code)]
const MAX_CONCURRENT_REQUESTS: usize = 50;

/// 单账号最大并发请求数
#[allow(dead_code)]
const MAX_CONCURRENT_PER_CREDENTIAL: usize = 20;

/// 所有上游 API 请求统一使用的 HTTP 总超时（秒）。
///
/// 历史上曾按请求类型分档：压缩请求 1000s（历史修复 commit 9338888，大上下文
/// 非流式 502），普通请求 180s（commit d669fb6，意图是让网络异常时报错更快）。
/// 但 `reqwest::Client::timeout` 是覆盖「发请求到读完整个响应体」的总超时，
/// 流式响应的持续生成阶段同样受它约束——100K+ 输入的长生成请求实测可超过 180s，
/// 会被该超时在中途掐断（表现为 `error decoding response body` / 502），
/// 生产日志已实证（三次流式失败 + 一次非流式 502 耗时全部 ≈180s）。
/// 故统一回 1000s：上游正常生成耗时不受影响（超时只是上限），
/// 真正挂死的连接由 TCP keepalive 与客户端自身重试兜底。
const UPSTREAM_TIMEOUT_SECS: u64 = 1000;

/// Kiro API Provider
///
/// 核心组件，负责与 Kiro API 通信
/// 支持多账号故障转移和重试机制
pub struct KiroProvider {
    token_manager: Arc<MultiTokenManager>,
    /// 全局代理配置（用于账号无自定义代理时的回退）
    global_proxy: Option<ProxyConfig>,
    /// Client 缓存：key = (effective proxy config, use_long_timeout)，value = reqwest::Client
    /// 不同代理配置的账号使用不同的 Client，共享相同代理的账号复用 Client。
    /// 历史上按请求类型分两档超时（压缩/流式 1000s vs 普通 180s），现已统一为
    /// UPSTREAM_TIMEOUT_SECS=1000s，但缓存结构保留 bool 维度以兼容既有调用点
    /// （见 `client_for`）。
    client_cache: Mutex<HashMap<(Option<ProxyConfig>, bool), Client>>,
    /// TLS 后端配置
    tls_backend: TlsBackend,
    /// 并发控制信号量，限制同时发往上游的请求数
    concurrency_limit: Arc<Semaphore>,
    /// 单账号并发信号量：限制每个账号的同时请求数
    credential_semaphores: Mutex<HashMap<u64, Arc<tokio::sync::Semaphore>>>,
    per_credential_limit: usize,
    /// 未交付响应的 provider 调用上限
    admission: AdmissionGate,
    /// Token 准备（含刷新）独立有界槽，上限复用 maxAdmissionWaiters。
    /// 与准入票分离：请求超时/取消只停止等待，不 abort 已发出的 OAuth 轮换。
    token_prep: Arc<Semaphore>,
    #[cfg(test)]
    test_api_url: Option<String>,
    #[cfg(test)]
    test_mcp_url: Option<String>,
    /// RPM 追踪器（可选，用于记录账号维度的 RPM）
    rpm_tracker: Option<Arc<RpmTracker>>,
    /// 限流日志存储（可选）
    throttle_log_store: Option<Arc<ThrottleLogStore>>,
    /// 失败日志存储（可选）
    failure_log_store: Option<Arc<FailureLogStore>>,
    /// 端点级 429 状态注册表（多端点 LB 使用）
    endpoint_registry: Arc<EndpointBucketRegistry>,
}

#[allow(dead_code)]
impl KiroProvider {
    /// 创建新的 KiroProvider 实例
    pub fn new(token_manager: Arc<MultiTokenManager>) -> Self {
        Self::with_proxy(token_manager, None)
    }

    /// 创建带代理配置的 KiroProvider 实例
    pub fn with_proxy(token_manager: Arc<MultiTokenManager>, proxy: Option<ProxyConfig>) -> Self {
        let cfg = token_manager.config();
        if let Err(e) = cfg.validate() {
            panic!("配置非法: {e}");
        }
        let tls_backend = cfg.tls_backend;
        let global_limit = cfg.max_concurrent_requests;
        let per_credential_limit = cfg.max_concurrent_per_credential;
        let waiters = cfg.max_admission_waiters;
        let admission =
            AdmissionGate::new(waiters, Duration::from_millis(cfg.admission_timeout_ms));
        let token_prep = Arc::new(Semaphore::new(waiters));
        // 预热：为全局代理配置构建普通超时 Client（长超时 Client 按需懒创建）
        let initial_client = build_client(proxy.as_ref(), UPSTREAM_TIMEOUT_SECS, tls_backend)
            .expect("创建 HTTP 客户端失败");
        let mut cache = HashMap::new();
        cache.insert((proxy.clone(), false), initial_client);

        Self {
            token_manager,
            global_proxy: proxy,
            client_cache: Mutex::new(cache),
            tls_backend,
            concurrency_limit: Arc::new(Semaphore::new(global_limit)),
            credential_semaphores: Mutex::new(HashMap::new()),
            per_credential_limit,
            admission,
            token_prep,
            #[cfg(test)]
            test_api_url: None,
            #[cfg(test)]
            test_mcp_url: None,
            rpm_tracker: None,
            throttle_log_store: None,
            failure_log_store: None,
            endpoint_registry: Arc::new(EndpointBucketRegistry::new()),
        }
    }

    #[cfg(test)]
    pub fn with_test_admission(
        mut self,
        global: usize,
        per_credential: usize,
        timeout_ms: u64,
        waiters: usize,
    ) -> Self {
        self.concurrency_limit = Arc::new(Semaphore::new(global.max(1)));
        self.per_credential_limit = per_credential.max(1);
        self.credential_semaphores = Mutex::new(HashMap::new());
        let waiters = waiters.max(1);
        self.admission = AdmissionGate::new(waiters, Duration::from_millis(timeout_ms.max(1)));
        self.token_prep = Arc::new(Semaphore::new(waiters));
        self
    }

    #[cfg(test)]
    pub fn with_test_urls(
        mut self,
        api_url: impl Into<String>,
        mcp_url: impl Into<String>,
    ) -> Self {
        self.test_api_url = Some(api_url.into());
        self.test_mcp_url = Some(mcp_url.into());
        self
    }

    /// 注入外部 endpoint_registry（多 provider 共享桶状态时使用）
    pub fn with_endpoint_registry(mut self, registry: Arc<EndpointBucketRegistry>) -> Self {
        self.endpoint_registry = registry;
        self
    }

    /// 设置 RPM 追踪器
    pub fn with_rpm_tracker(mut self, tracker: Arc<RpmTracker>) -> Self {
        self.rpm_tracker = Some(tracker);
        self
    }

    /// 设置限流日志存储
    pub fn with_throttle_log_store(mut self, store: Arc<ThrottleLogStore>) -> Self {
        self.throttle_log_store = Some(store);
        self
    }

    /// 设置失败日志存储
    pub fn with_failure_log_store(mut self, store: Arc<FailureLogStore>) -> Self {
        self.failure_log_store = Some(store);
        self
    }

    /// 根据账号的代理配置获取（或创建并缓存）对应的 reqwest::Client
    ///
    /// 历史上按请求类型分两档超时（参数 `use_long_timeout` 区分），现统一为
    /// `UPSTREAM_TIMEOUT_SECS`。参数保留以兼容既有调用点，两档构建的 Client
    /// 超时一致，仅缓存 key 隔离。
    fn client_for(
        &self,
        credentials: &KiroCredentials,
        use_long_timeout: bool,
    ) -> anyhow::Result<Client> {
        #[cfg(test)]
        if self.test_api_url.is_some() || self.test_mcp_url.is_some() {
            return Ok(Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(UPSTREAM_TIMEOUT_SECS))
                .build()?);
        }
        let effective = credentials.effective_proxy(self.global_proxy.as_ref());
        let key = (effective.clone(), use_long_timeout);
        let mut cache = self.client_cache.lock();
        if let Some(client) = cache.get(&key) {
            return Ok(client.clone());
        }
        let timeout_secs = UPSTREAM_TIMEOUT_SECS;
        tracing::debug!(
            "[CLIENT] 创建新 Client：use_long_timeout={} timeout_secs={}",
            use_long_timeout,
            timeout_secs
        );
        let client = build_client(effective.as_ref(), timeout_secs, self.tls_backend)?;
        cache.insert(key, client.clone());
        Ok(client)
    }

    /// 获取指定账号的并发信号量（懒初始化）
    fn semaphore_for(&self, credential_id: u64) -> Arc<tokio::sync::Semaphore> {
        let limit = self.per_credential_limit;
        let mut map = self.credential_semaphores.lock();
        map.entry(credential_id)
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(limit)))
            .clone()
    }

    /// 获取 token_manager 的引用
    pub fn token_manager(&self) -> &MultiTokenManager {
        &self.token_manager
    }

    /// 获取 API 基础 URL（使用 config 级 api_region + 默认 Ide 端点）
    pub fn base_url(&self) -> String {
        let region = self.token_manager.config().effective_api_region();
        let endpoint = Endpoint::by_name(EndpointName::Ide, region);
        format!("https://{}/generateAssistantResponse", endpoint.host)
    }

    /// 获取 MCP API URL（使用 config 级 api_region，MCP 端点独立于多端点 LB）
    pub fn mcp_url(&self) -> String {
        format!(
            "https://q.{}.amazonaws.com/mcp",
            self.token_manager.config().effective_api_region()
        )
    }

    /// 获取 API 基础域名（使用 config 级 api_region + 默认 Ide 端点）
    pub fn base_domain(&self) -> String {
        Endpoint::by_name(
            EndpointName::Ide,
            self.token_manager.config().effective_api_region(),
        )
        .host
    }

    /// 获取账号级 API 基础 URL（按指定 endpoint）
    fn base_url_for(&self, _credentials: &KiroCredentials, endpoint: &Endpoint) -> String {
        #[cfg(test)]
        if let Some(url) = &self.test_api_url {
            return url.clone();
        }
        format!("https://{}/generateAssistantResponse", endpoint.host)
    }

    /// 获取账号级 MCP API URL（MCP 端点不走多端点 LB）
    fn mcp_url_for(&self, credentials: &KiroCredentials) -> String {
        #[cfg(test)]
        if let Some(url) = &self.test_mcp_url {
            return url.clone();
        }
        format!(
            "https://q.{}.amazonaws.com/mcp",
            credentials.effective_api_region(self.token_manager.config())
        )
    }

    /// 获取账号级 API 基础域名（按指定 endpoint）
    fn base_domain_for(&self, _credentials: &KiroCredentials, endpoint: &Endpoint) -> String {
        endpoint.host.clone()
    }

    /// 从请求体中提取模型信息
    ///
    /// 尝试解析 JSON 请求体，提取 conversationState.currentMessage.userInputMessage.modelId
    fn extract_model_from_request(request_body: &str) -> Option<String> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(request_body).ok()?;

        // 尝试提取 conversationState.currentMessage.userInputMessage.modelId
        json.get("conversationState")?
            .get("currentMessage")?
            .get("userInputMessage")?
            .get("modelId")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// 从请求体中提取 agentTaskType
    ///
    /// 提取 conversationState.agentTaskType，用于设置 x-amzn-kiro-agent-mode 请求头
    fn extract_agent_task_type_from_request(request_body: &str) -> &'static str {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(request_body) else {
            return "vibe";
        };
        match json
            .get("conversationState")
            .and_then(|s| s.get("agentTaskType"))
            .and_then(|v| v.as_str())
        {
            Some("spectask") => "spectask",
            _ => "vibe",
        }
    }

    /// 提取 conversationState.agentContinuationId，用于 sticky cache 路由
    fn extract_continuation_id_from_request(request_body: &str) -> Option<String> {
        let json: serde_json::Value = serde_json::from_str(request_body).ok()?;
        json.get("conversationState")?
            .get("agentContinuationId")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// 构建请求头
    ///
    /// # Arguments
    /// * `ctx` - API 调用上下文，包含账号和 token
    /// * `request_body` - 请求体，用于提取 agentTaskType
    /// * `endpoint` - 当前选中的上游端点（决定 HOST 头与可选的 x-amz-target）
    fn build_headers(
        &self,
        ctx: &CallContext,
        request_body: &str,
        attempt: usize,
        endpoint: &Endpoint,
    ) -> anyhow::Result<HeaderMap> {
        let config = self.token_manager.config();

        let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

        let kiro_version = &config.kiro_version;
        let os_name = &config.system_version;
        let node_version = &config.node_version;

        let x_amz_user_agent = format!("aws-sdk-js/1.0.27 KiroIDE-{}-{}", kiro_version, machine_id);

        let user_agent = format!(
            "aws-sdk-js/1.0.27 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererstreaming#1.0.27 m/E KiroIDE-{}-{}",
            os_name, node_version, kiro_version, machine_id
        );

        let agent_mode = Self::extract_agent_task_type_from_request(request_body);

        let mut headers = HeaderMap::new();

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "x-amzn-codewhisperer-optout",
            HeaderValue::from_static("true"),
        );
        headers.insert(
            "x-amzn-kiro-agent-mode",
            HeaderValue::from_static(agent_mode),
        );
        headers.insert(
            "x-amz-user-agent",
            HeaderValue::from_str(&x_amz_user_agent).unwrap(),
        );
        headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_str(&user_agent).unwrap(),
        );
        headers.insert(
            HOST,
            HeaderValue::from_str(&self.base_domain_for(&ctx.credentials, endpoint)).unwrap(),
        );
        headers.insert(
            "amz-sdk-invocation-id",
            HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
        );
        headers.insert(
            "amz-sdk-request",
            HeaderValue::from_str(&format!("attempt={}; max=3", attempt + 1)).unwrap(),
        );
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", ctx.token)).unwrap(),
        );

        // codewhisperer / amazonq 端点需要 x-amz-target 头路由到对应后端服务
        if let Some(target) = endpoint.amz_target {
            headers.insert("x-amz-target", HeaderValue::from_static(target));
        }

        if ctx
            .credentials
            .auth_method
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case("external_idp"))
        {
            headers.insert("TokenType", HeaderValue::from_static("EXTERNAL_IDP"));
        }

        Ok(headers)
    }

    /// 构建 MCP 请求头
    fn build_mcp_headers(&self, ctx: &CallContext, attempt: usize) -> anyhow::Result<HeaderMap> {
        let config = self.token_manager.config();

        let machine_id = machine_id::generate_from_credentials(&ctx.credentials, config);

        let kiro_version = &config.kiro_version;
        let os_name = &config.system_version;
        let node_version = &config.node_version;

        let x_amz_user_agent = format!("aws-sdk-js/1.0.27 KiroIDE-{}-{}", kiro_version, machine_id);

        let user_agent = format!(
            "aws-sdk-js/1.0.27 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererstreaming#1.0.27 m/E KiroIDE-{}-{}",
            os_name, node_version, kiro_version, machine_id
        );

        let mut headers = HeaderMap::new();

        // 按照严格顺序添加请求头
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        headers.insert(
            "x-amz-user-agent",
            HeaderValue::from_str(&x_amz_user_agent).unwrap(),
        );
        headers.insert("user-agent", HeaderValue::from_str(&user_agent).unwrap());
        // MCP 端点独立于多端点 LB，沿用 q.{region}.amazonaws.com
        headers.insert(
            "host",
            HeaderValue::from_str(&format!(
                "q.{}.amazonaws.com",
                ctx.credentials
                    .effective_api_region(self.token_manager.config())
            ))
            .unwrap(),
        );
        headers.insert(
            "amz-sdk-invocation-id",
            HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap(),
        );
        headers.insert(
            "amz-sdk-request",
            HeaderValue::from_str(&format!("attempt={}; max=3", attempt + 1)).unwrap(),
        );
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", ctx.token)).unwrap(),
        );
        Ok(headers)
    }

    /// 发送非流式 API 请求
    ///
    /// 支持多账号故障转移：
    /// - 400 Bad Request: 直接返回错误，不计入账号失败
    /// - 401/403: 视为账号/权限问题，计入失败次数并允许故障转移
    /// - 402 MONTHLY_REQUEST_COUNT: 视为额度用尽，禁用账号并切换
    /// - 429/5xx/网络等瞬态错误: 重试但不禁用或切换账号（避免误把所有账号锁死）
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的请求体字符串
    /// * `is_compact` - 是否为 Claude Code `/compact` 压缩请求
    ///   （超时统一为 `UPSTREAM_TIMEOUT_SECS`，历史分档已移除）
    ///
    /// # Returns
    /// 返回原始的 HTTP Response，不做解析
    pub async fn call_api(
        &self,
        request_body: &str,
        is_compact: bool,
        thinking_adaptive_requested: bool,
        bound_ids: &[u64],
    ) -> anyhow::Result<(LeasedResponse, u64)> {
        self.call_api_with_retry(
            request_body,
            false,
            is_compact,
            thinking_adaptive_requested,
            bound_ids,
        )
        .await
    }

    /// 发送流式 API 请求
    ///
    /// 支持多账号故障转移：
    /// - 400 Bad Request: 直接返回错误，不计入账号失败
    /// - 401/403: 视为账号/权限问题，计入失败次数并允许故障转移
    /// - 402 MONTHLY_REQUEST_COUNT: 视为额度用尽，禁用账号并切换
    /// - 429/5xx/网络等瞬态错误: 重试但不禁用或切换账号（避免误把所有账号锁死）
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的请求体字符串
    /// * `is_compact` - 是否为 Claude Code `/compact` 压缩请求
    ///   （超时统一为 `UPSTREAM_TIMEOUT_SECS`，历史分档已移除）
    ///
    /// # Returns
    /// 返回原始的 HTTP Response，调用方负责处理流式数据
    pub async fn call_api_stream(
        &self,
        request_body: &str,
        is_compact: bool,
        thinking_adaptive_requested: bool,
        bound_ids: &[u64],
    ) -> anyhow::Result<(LeasedResponse, u64)> {
        self.call_api_with_retry(
            request_body,
            true,
            is_compact,
            thinking_adaptive_requested,
            bound_ids,
        )
        .await
    }

    /// 发送 MCP API 请求
    ///
    /// 用于 WebSearch 等工具调用。MCP 调用不涉及 `/compact` 压缩语义，
    /// 与普通请求共用统一超时（`UPSTREAM_TIMEOUT_SECS`）。
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的 MCP 请求体字符串
    ///
    /// # Returns
    /// 返回原始的 HTTP Response
    pub async fn call_mcp(
        &self,
        request_body: &str,
        bound_ids: &[u64],
    ) -> anyhow::Result<(LeasedResponse, u64)> {
        self.call_mcp_with_retry(request_body, bound_ids).await
    }

    /// 内部方法：带重试逻辑的 MCP API 调用
    async fn call_mcp_with_retry(
        &self,
        request_body: &str,
        bound_ids: &[u64],
    ) -> anyhow::Result<(LeasedResponse, u64)> {
        let ticket = self.admission.try_enter().map_err(anyhow::Error::from)?;
        let effective_pool = if bound_ids.is_empty() {
            self.token_manager.total_count()
        } else {
            bound_ids.len()
        };
        let max_retries = (effective_pool * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut last_rate_limit: Option<RateLimitError> = None;

        let continuation_id = Self::extract_continuation_id_from_request(request_body);
        let mut throttled_in_request: Vec<u64> = Vec::new();
        let mut sends = 0usize;
        let mut scans = 0usize;
        let max_scans = (effective_pool.max(1) * 4).max(max_retries + 1);

        loop {
            if sends >= max_retries || scans >= max_scans {
                break;
            }
            scans += 1;
            if let Some(err) =
                self.deadline_exit(&ticket, None, bound_ids, &last_rate_limit, &last_error)
            {
                return Err(err);
            }

            let ctx = match self
                .acquire_ctx_within(
                    &ticket,
                    None,
                    bound_ids,
                    continuation_id.as_deref(),
                    &throttled_in_request,
                )
                .await
            {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    self.note_token_error(
                        e,
                        &mut last_error,
                        &mut last_rate_limit,
                        &mut throttled_in_request,
                    );
                    continue;
                }
                Err(busy) => {
                    return Err(self.finalize_outcome(
                        None,
                        bound_ids,
                        &last_rate_limit,
                        &last_error,
                        Some(busy),
                    ));
                }
            };
            if ticket.expired() {
                return Err(self.finalize_outcome(
                    None,
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }

            if throttled_in_request.contains(&ctx.id) {
                return Err(self.finalize_outcome(
                    None,
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }
            if self.endpoint_registry.is_mcp_throttled(ctx.id) {
                let rl = self
                    .endpoint_registry
                    .mcp_ready_at(ctx.id)
                    .map(|u| RateLimitError::at(crate::kiro::error::RateLimitKind::Upstream, u))
                    .unwrap_or_else(|| RateLimitError::upstream(Some(Duration::from_secs(1))));
                RateLimitError::keep_earliest_real(&mut last_rate_limit, rl);
                self.mark_throttled(
                    &mut throttled_in_request,
                    ctx.id,
                    continuation_id.as_deref(),
                );
                continue;
            }

            let Some((global_permit, cred_permit)) =
                try_acquire_pair(&self.concurrency_limit, &self.semaphore_for(ctx.id))
            else {
                self.mark_throttled(
                    &mut throttled_in_request,
                    ctx.id,
                    continuation_id.as_deref(),
                );
                continue;
            };

            let url = self.mcp_url_for(&ctx.credentials);
            let headers = match self.build_mcp_headers(&ctx, sends) {
                Ok(h) => h,
                Err(e) => {
                    drop((global_permit, cred_permit));
                    last_error = Some(e);
                    continue;
                }
            };
            let client = match self.client_for(&ctx.credentials, false) {
                Ok(c) => c,
                Err(e) => {
                    drop((global_permit, cred_permit));
                    last_error = Some(e);
                    continue;
                }
            };
            let effective_mcp_body = Self::rewrite_profile_arn(request_body, &ctx.credentials);

            if let Err(rl) = self.reserve_rpm(ctx.id) {
                drop((global_permit, cred_permit));
                RateLimitError::keep_earliest_real(&mut last_rate_limit, rl);
                self.mark_throttled(
                    &mut throttled_in_request,
                    ctx.id,
                    continuation_id.as_deref(),
                );
                continue;
            }
            if ticket.expired() {
                drop((global_permit, cred_permit));
                return Err(self.finalize_outcome(
                    None,
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }
            sends += 1;

            let response = match client
                .post(&url)
                .headers(headers)
                .body(effective_mcp_body)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!("MCP 请求发送失败（尝试 {}/{}）: {}", sends, max_retries, e);
                    last_error = Some(e.into());
                    drop((global_permit, cred_permit));
                    self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                        .await;
                    continue;
                }
            };

            let status = response.status();
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok((
                    LeasedResponse::new(response, Some(global_permit), Some(cred_permit)),
                    ctx.id,
                ));
            }

            let retry_after = parse_retry_after_from_headers(response.headers());
            if status.as_u16() == 429
                && let Some(d) = retry_after
                && let Some(until) = Instant::now().checked_add(d)
            {
                self.token_manager.report_throttled(ctx.id);
                self.token_manager.report_throttled_for_rotation(ctx.id);
                if let Some(cid) = continuation_id.as_deref() {
                    self.token_manager.report_sticky_throttled(cid, ctx.id);
                }
                if let Some(ref store) = self.throttle_log_store {
                    store.record(ctx.id, "mcp", 429, "(unread body)", None);
                }
                self.endpoint_registry.throttle_mcp_until(ctx.id, until);
                tracing::warn!(
                    "MCP 上游 429 带有效 Retry-After，未读取错误 body，credential={} until={:?}",
                    ctx.id,
                    until
                );
                drop(response);
                drop((global_permit, cred_permit));
                return Err(
                    RateLimitError::at(crate::kiro::error::RateLimitKind::Upstream, until).into(),
                );
            }
            let body = response.text().await.unwrap_or_default();
            drop((global_permit, cred_permit));

            if status.as_u16() == 402 && Self::is_monthly_request_limit(&body) {
                self.token_manager.report_quota_exhausted(ctx.id);
                let desc = self.token_manager.describe_unavailable(None, bound_ids);
                if desc.contains(QUOTA_EXHAUSTED_ALL_MARKER) {
                    anyhow::bail!("{desc}");
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            if status.as_u16() == 400 {
                if Self::is_profile_arn_required_error(&body) {
                    tracing::warn!(
                        "MCP 请求失败（账号缺少 profileArn，尝试 {}/{}）: {} {}",
                        sends,
                        max_retries,
                        status,
                        body
                    );
                    let has_available = self.token_manager.report_profile_arn_missing(ctx.id);
                    if let Some(ref store) = self.failure_log_store {
                        store.record(ctx.id, "mcp", status.as_u16(), &body);
                    }
                    if !has_available {
                        anyhow::bail!("MCP 请求失败（所有账号已用尽）: {} {}", status, body);
                    }
                    last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                    continue;
                }
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            if matches!(status.as_u16(), 401 | 403) {
                let has_available = self.token_manager.report_failure(ctx.id);
                if let Some(ref store) = self.failure_log_store {
                    store.record(ctx.id, "mcp", status.as_u16(), &body);
                }
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有账号已用尽）: {} {}", status, body);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                continue;
            }

            if status.as_u16() == 429 {
                tracing::warn!(
                    "MCP 请求失败（上游限流，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                self.token_manager.report_throttled(ctx.id);
                self.token_manager.report_throttled_for_rotation(ctx.id);
                if let Some(cid) = continuation_id.as_deref() {
                    self.token_manager.report_sticky_throttled(cid, ctx.id);
                }
                Self::push_unique(&mut throttled_in_request, ctx.id);
                if let Some(ref store) = self.throttle_log_store {
                    store.record(ctx.id, "mcp", status.as_u16(), &body, None);
                }
                if let Some(d) = retry_after
                    && let Some(until) = Instant::now().checked_add(d)
                {
                    self.endpoint_registry.throttle_mcp_until(ctx.id, until);
                    return Err(RateLimitError::at(
                        crate::kiro::error::RateLimitKind::Upstream,
                        until,
                    )
                    .into());
                }
                let until = Instant::now() + BUCKET_THROTTLE_DURATION;
                self.endpoint_registry.throttle_mcp_until(ctx.id, until);
                RateLimitError::keep_earliest_real(
                    &mut last_rate_limit,
                    RateLimitError::at(crate::kiro::error::RateLimitKind::Upstream, until),
                );
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                self.sleep_retry(&ticket, sends.saturating_sub(1), true)
                    .await;
                continue;
            }

            if status.as_u16() == 408 || status.is_server_error() {
                tracing::warn!(
                    "MCP 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
                self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                    .await;
                continue;
            }

            if status.is_client_error() {
                anyhow::bail!("MCP 请求失败: {} {}", status, body);
            }

            last_error = Some(anyhow::anyhow!("MCP 请求失败: {} {}", status, body));
            self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                .await;
        }

        Err(self.finish_error(
            None,
            bound_ids,
            last_rate_limit,
            last_error,
            "MCP 请求失败：已达到最大重试次数",
        ))
    }

    /// 内部方法：带重试逻辑的 API 调用
    ///
    /// 重试策略：
    /// 选择下一个可用端点
    ///
    /// 跳过被 `endpoint_registry` 标记为封禁的桶；按账号 `effective_endpoints` 顺序 +
    /// `attempt` 偏移返回第一个可用端点。所有端点被封禁返回 `None`（外层应升级为
    /// `AllEndpointsThrottled` 错误，不静默切换账号）。
    ///
    /// 端点切换与账号切换**正交**：本函数仅在单账号内切端点，不消耗外层切账号配额。
    pub(crate) fn select_endpoint(
        &self,
        credentials: &KiroCredentials,
        attempt: usize,
    ) -> Option<Endpoint> {
        let region = credentials.effective_api_region(self.token_manager.config());
        let endpoints = credentials.effective_endpoints(region);
        if endpoints.is_empty() {
            return None;
        }
        let len = endpoints.len();
        // 起点 attempt 偏移（attempt % len），轮询保证单账号内多次 attempt 走不同端点
        let start = attempt % len;
        for i in 0..len {
            let candidate = &endpoints[(start + i) % len];
            if !self
                .endpoint_registry
                .is_throttled(credentials.id.unwrap_or(0), candidate.name)
            {
                return Some(candidate.clone());
            }
        }
        None
    }

    /// - 每个账号最多重试 MAX_RETRIES_PER_CREDENTIAL 次
    /// - 总重试次数 = min(可用池大小 × 每账号重试次数, MAX_TOTAL_RETRIES)
    /// - 硬上限 9 次，避免无限重试
    /// - 当可用池 ≤ 1 时，仅重试 3 次并使用更长退避间隔
    /// - 单账号内 3 次 attempts 之间切换多端点（不消耗切账号配额）
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
        is_compact: bool,
        thinking_adaptive_requested: bool,
        bound_ids: &[u64],
    ) -> anyhow::Result<(LeasedResponse, u64)> {
        let ticket = self.admission.try_enter().map_err(anyhow::Error::from)?;
        let use_long_timeout = is_stream || is_compact;
        let effective_pool = if bound_ids.is_empty() {
            self.token_manager.total_count()
        } else {
            bound_ids.len()
        };
        let max_retries = (effective_pool * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut last_rate_limit: Option<RateLimitError> = None;
        let api_type = if is_stream { "流式" } else { "非流式" };

        let model = Self::extract_model_from_request(request_body);
        let continuation_id = Self::extract_continuation_id_from_request(request_body);
        // 硬避让：RPM/并发满、刷新受限、全部有效端点冷却、资格不合格。
        // 软偏好：模型 API 普通 429（无有效 Retry-After）优先换号，无其它候选时
        // 同号未冷却端点仍可试。
        let mut hard_avoid: Vec<u64> = Vec::new();
        let mut soft_prefer: Vec<u64> = Vec::new();
        let mut sends = 0usize;
        let mut scans = 0usize;
        let max_scans = (effective_pool.max(1) * 4).max(max_retries + 1);

        loop {
            if sends >= max_retries || scans >= max_scans {
                break;
            }
            scans += 1;
            if let Some(err) = self.deadline_exit(
                &ticket,
                model.as_deref(),
                bound_ids,
                &last_rate_limit,
                &last_error,
            ) {
                return Err(err);
            }

            let acquire_bound: Vec<u64> = if hard_avoid.is_empty() {
                bound_ids.to_vec()
            } else {
                let allowed = self.hard_allowed_ids(bound_ids, &hard_avoid);
                if allowed.is_empty() {
                    return Err(self.finalize_outcome(
                        model.as_deref(),
                        bound_ids,
                        &last_rate_limit,
                        &last_error,
                        None,
                    ));
                }
                allowed
            };
            let ctx = match self
                .acquire_ctx_within(
                    &ticket,
                    model.as_deref(),
                    &acquire_bound,
                    continuation_id.as_deref(),
                    &soft_prefer,
                )
                .await
            {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => {
                    self.note_token_error(
                        e,
                        &mut last_error,
                        &mut last_rate_limit,
                        &mut hard_avoid,
                    );
                    continue;
                }
                Err(busy) => {
                    return Err(self.finalize_outcome(
                        model.as_deref(),
                        bound_ids,
                        &last_rate_limit,
                        &last_error,
                        Some(busy),
                    ));
                }
            };
            if ticket.expired() {
                return Err(self.finalize_outcome(
                    model.as_deref(),
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }

            if hard_avoid.contains(&ctx.id) {
                return Err(self.finalize_outcome(
                    model.as_deref(),
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }

            let Some((global_permit, cred_permit)) =
                try_acquire_pair(&self.concurrency_limit, &self.semaphore_for(ctx.id))
            else {
                self.mark_throttled(&mut hard_avoid, ctx.id, continuation_id.as_deref());
                continue;
            };

            let endpoint = match self.select_endpoint(&ctx.credentials, sends) {
                Some(e) => e,
                None => {
                    let endpoints: Vec<EndpointName> = ctx
                        .credentials
                        .effective_endpoints(
                            ctx.credentials
                                .effective_api_region(self.token_manager.config()),
                        )
                        .iter()
                        .map(|e| e.name)
                        .collect();
                    let ids = endpoints
                        .iter()
                        .map(|n| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    tracing::info!(
                        "[ENDPOINT] credential={} 端点全封（{}），避让并尝试其它账号",
                        ctx.id,
                        ids
                    );
                    Self::push_unique(&mut hard_avoid, ctx.id);
                    let rl = self
                        .endpoint_registry
                        .earliest_ready(ctx.id)
                        .map(|u| RateLimitError::at(crate::kiro::error::RateLimitKind::Upstream, u))
                        .unwrap_or_else(|| {
                            RateLimitError::upstream(Some(BUCKET_THROTTLE_DURATION))
                        });
                    RateLimitError::keep_earliest_real(&mut last_rate_limit, rl);
                    last_error = Some(anyhow::anyhow!(
                        "All endpoints throttled for credential {} (tried: [{}])",
                        ctx.id,
                        ids
                    ));
                    drop((global_permit, cred_permit));
                    continue;
                }
            };
            tracing::debug!(
                "[ENDPOINT] credential={} attempt={} selected={:?} host={}",
                ctx.id,
                sends + 1,
                endpoint.name,
                endpoint.host
            );

            let url = self.base_url_for(&ctx.credentials, &endpoint);
            let effective_body = Self::rewrite_request_body(
                request_body,
                &ctx.credentials,
                thinking_adaptive_requested,
            );
            let headers = match self.build_headers(&ctx, &effective_body, sends, &endpoint) {
                Ok(h) => h,
                Err(e) => {
                    drop((global_permit, cred_permit));
                    last_error = Some(e);
                    continue;
                }
            };
            let client = match self.client_for(&ctx.credentials, use_long_timeout) {
                Ok(c) => c,
                Err(e) => {
                    drop((global_permit, cred_permit));
                    last_error = Some(e);
                    continue;
                }
            };

            if let Err(rl) = self.reserve_rpm(ctx.id) {
                drop((global_permit, cred_permit));
                RateLimitError::keep_earliest_real(&mut last_rate_limit, rl);
                self.mark_throttled(&mut hard_avoid, ctx.id, continuation_id.as_deref());
                continue;
            }
            if ticket.expired() {
                drop((global_permit, cred_permit));
                return Err(self.finalize_outcome(
                    model.as_deref(),
                    bound_ids,
                    &last_rate_limit,
                    &last_error,
                    None,
                ));
            }
            sends += 1;

            tracing::debug!("[KIRO-REQUEST] url={} body={}", url, effective_body);
            let response = match client
                .post(&url)
                .headers(headers)
                .body(effective_body)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!("API 请求发送失败（尝试 {}/{}）: {}", sends, max_retries, e);
                    last_error = Some(e.into());
                    drop((global_permit, cred_permit));
                    self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                        .await;
                    continue;
                }
            };

            let status = response.status();
            if status.is_success() {
                self.token_manager.report_success(ctx.id);
                return Ok((
                    LeasedResponse::new(response, Some(global_permit), Some(cred_permit)),
                    ctx.id,
                ));
            }

            let retry_after = parse_retry_after_from_headers(response.headers());
            if status.as_u16() == 429
                && let Some(d) = retry_after
                && let Some(until) = Instant::now().checked_add(d)
            {
                self.token_manager.report_throttled(ctx.id);
                self.token_manager.report_throttled_for_rotation(ctx.id);
                if let Some(cid) = continuation_id.as_deref() {
                    self.token_manager.report_sticky_throttled(cid, ctx.id);
                }
                if let Some(ref store) = self.throttle_log_store {
                    store.record(
                        ctx.id,
                        "api",
                        429,
                        "(unread body)",
                        Some(endpoint.name.as_str()),
                    );
                }
                self.endpoint_registry
                    .throttle_until(ctx.id, endpoint.name, until);
                tracing::warn!(
                    "API 上游 429 带有效 Retry-After，未读取错误 body，credential={} endpoint={:?}",
                    ctx.id,
                    endpoint.name
                );
                drop(response);
                drop((global_permit, cred_permit));
                return Err(
                    RateLimitError::at(crate::kiro::error::RateLimitKind::Upstream, until).into(),
                );
            }
            let body = response.text().await.unwrap_or_default();
            drop((global_permit, cred_permit));

            if status.as_u16() == 402 && Self::is_monthly_request_limit(&body) {
                tracing::warn!(
                    "API 请求失败（额度已用尽，禁用账号并切换，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                self.token_manager.report_quota_exhausted(ctx.id);
                let desc = self
                    .token_manager
                    .describe_unavailable(model.as_deref(), bound_ids);
                if desc.contains(QUOTA_EXHAUSTED_ALL_MARKER) {
                    anyhow::bail!("{desc}");
                }
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            if status.as_u16() == 400 {
                if Self::is_profile_arn_required_error(&body) {
                    tracing::warn!(
                        "API 请求失败（账号缺少 profileArn，尝试 {}/{}）: {} {}",
                        sends,
                        max_retries,
                        status,
                        body
                    );
                    let has_available = self.token_manager.report_profile_arn_missing(ctx.id);
                    if let Some(ref store) = self.failure_log_store {
                        store.record(ctx.id, "api", status.as_u16(), &body);
                    }
                    if !has_available {
                        anyhow::bail!(
                            "{} API 请求失败（所有账号已用尽）: {} {}",
                            api_type,
                            status,
                            body
                        );
                    }
                    last_error = Some(anyhow::anyhow!(
                        "{} API 请求失败: {} {}",
                        api_type,
                        status,
                        body
                    ));
                    continue;
                }
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            if matches!(status.as_u16(), 401 | 403) {
                tracing::warn!(
                    "API 请求失败（可能为账号错误，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                let has_available = self.token_manager.report_failure(ctx.id);
                if let Some(ref store) = self.failure_log_store {
                    store.record(ctx.id, "api", status.as_u16(), &body);
                }
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有账号已用尽）: {} {}",
                        api_type,
                        status,
                        body
                    );
                }
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                continue;
            }

            if status.as_u16() == 429 {
                tracing::warn!(
                    "API 请求失败（上游限流，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                self.token_manager.report_throttled(ctx.id);
                self.token_manager.report_throttled_for_rotation(ctx.id);
                if let Some(cid) = continuation_id.as_deref() {
                    self.token_manager.report_sticky_throttled(cid, ctx.id);
                }
                if let Some(ref store) = self.throttle_log_store {
                    store.record(
                        ctx.id,
                        "api",
                        status.as_u16(),
                        &body,
                        Some(endpoint.name.as_str()),
                    );
                }
                if let Some(d) = retry_after
                    && let Some(until) = Instant::now().checked_add(d)
                {
                    self.endpoint_registry
                        .throttle_until(ctx.id, endpoint.name, until);
                    return Err(RateLimitError::at(
                        crate::kiro::error::RateLimitKind::Upstream,
                        until,
                    )
                    .into());
                }
                // 无有效 Retry-After：只冷却当前端点。优先换号（软偏好），
                // 无其它合格账号时同号未冷却端点仍可在 send 预算内继续。
                Self::push_unique(&mut soft_prefer, ctx.id);
                self.endpoint_registry
                    .throttle(ctx.id, endpoint.name, BUCKET_THROTTLE_DURATION);
                RateLimitError::keep_earliest_real(
                    &mut last_rate_limit,
                    RateLimitError::upstream(Some(BUCKET_THROTTLE_DURATION)),
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                self.sleep_retry(&ticket, sends.saturating_sub(1), true)
                    .await;
                continue;
            }

            if status.as_u16() == 408 || status.is_server_error() {
                tracing::warn!(
                    "API 请求失败（上游瞬态错误，尝试 {}/{}）: {} {}",
                    sends,
                    max_retries,
                    status,
                    body
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {} {}",
                    api_type,
                    status,
                    body
                ));
                self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                    .await;
                continue;
            }

            if status.is_client_error() {
                anyhow::bail!("{} API 请求失败: {} {}", api_type, status, body);
            }

            tracing::warn!(
                "API 请求失败（未知错误，尝试 {}/{}）: {} {}",
                sends,
                max_retries,
                status,
                body
            );
            last_error = Some(anyhow::anyhow!(
                "{} API 请求失败: {} {}",
                api_type,
                status,
                body
            ));
            self.sleep_retry(&ticket, sends.saturating_sub(1), false)
                .await;
        }

        Err(self.finish_error(
            model.as_deref(),
            bound_ids,
            last_rate_limit,
            last_error,
            &format!("{api_type} API 请求失败：已达到最大重试次数"),
        ))
    }

    fn retry_delay(attempt: usize) -> Duration {
        // 指数退避 + 少量抖动，避免上游抖动时放大故障
        const BASE_MS: u64 = 200;
        const MAX_MS: u64 = 5_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(6) as u32));
        let backoff = exp.min(MAX_MS);
        let jitter_max = (backoff / 4).max(1);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }

    /// 429 限流退避：随 attempt 递增，避免固定间隔反复命中同一限流窗口
    fn throttle_delay(attempt: usize) -> Duration {
        // 2s + attempt×1s（上限 8s）+ jitter
        let base = 2000u64.saturating_add((attempt as u64).saturating_mul(1000));
        let capped = base.min(8_000);
        let jitter = fastrand::u64(0..=1500);
        Duration::from_millis(capped.saturating_add(jitter))
    }

    fn reserve_rpm(&self, credential_id: u64) -> Result<(), RateLimitError> {
        let Some(rpm) = &self.rpm_tracker else {
            return Ok(());
        };
        let max_rpm = self.token_manager.config().max_rpm_per_credential;
        rpm.try_reserve_credential(credential_id, max_rpm)
            .map_err(RateLimitError::rpm)
    }

    fn push_unique(ids: &mut Vec<u64>, id: u64) {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }

    fn mark_throttled(&self, ids: &mut Vec<u64>, id: u64, continuation_id: Option<&str>) {
        self.token_manager.report_throttled_for_rotation(id);
        if let Some(cid) = continuation_id {
            self.token_manager.report_sticky_throttled(cid, id);
        }
        Self::push_unique(ids, id);
    }

    fn note_token_error(
        &self,
        e: anyhow::Error,
        last_error: &mut Option<anyhow::Error>,
        last_rate_limit: &mut Option<RateLimitError>,
        throttled_in_request: &mut Vec<u64>,
    ) {
        if let Some(rl) = e.downcast_ref::<RateLimitError>().cloned() {
            RateLimitError::keep_earliest_real(last_rate_limit, rl);
        }
        *last_error = Some(e);
        let _ = throttled_in_request;
    }

    fn deadline_exit(
        &self,
        ticket: &AdmissionTicket,
        model: Option<&str>,
        bound_ids: &[u64],
        last_rate_limit: &Option<RateLimitError>,
        last_error: &Option<anyhow::Error>,
    ) -> Option<anyhow::Error> {
        if !ticket.expired() {
            return None;
        }
        Some(self.finalize_outcome(model, bound_ids, last_rate_limit, last_error, None))
    }

    /// 402（原始 bound/model 作用域全额耗尽）> 真实 typed429（最早）> 原始上游错误 > 新 local busy。
    /// 新 busy 不得写入历史。
    fn finalize_outcome(
        &self,
        model: Option<&str>,
        bound_ids: &[u64],
        last_rate_limit: &Option<RateLimitError>,
        last_error: &Option<anyhow::Error>,
        new_busy: Option<RateLimitError>,
    ) -> anyhow::Error {
        let desc = self.token_manager.describe_unavailable(model, bound_ids);
        if desc.contains(QUOTA_EXHAUSTED_ALL_MARKER) {
            return anyhow::anyhow!("{desc}");
        }
        if let Some(rl) = last_rate_limit.clone() {
            return rl.into();
        }
        if let Some(e) = last_error
            && !e.to_string().contains(QUOTA_EXHAUSTED_ALL_MARKER)
        {
            return anyhow::anyhow!("{e}");
        }
        // Token selection may have inspected a narrowed hard-allowed subset.
        // Its quota marker cannot prove exhaustion of the original request scope;
        // only the authoritative check above may produce that terminal 402.
        new_busy
            .unwrap_or_else(|| RateLimitError::local_busy(Duration::from_secs(1)))
            .into()
    }

    fn hard_allowed_ids(&self, bound_ids: &[u64], hard_avoid: &[u64]) -> Vec<u64> {
        let base: Vec<u64> = if bound_ids.is_empty() {
            self.token_manager.credential_ids()
        } else {
            bound_ids.to_vec()
        };
        base.into_iter()
            .filter(|id| !hard_avoid.contains(id))
            .collect()
    }

    async fn acquire_ctx_within(
        &self,
        ticket: &AdmissionTicket,
        model: Option<&str>,
        bound_ids: &[u64],
        continuation_id: Option<&str>,
        avoid: &[u64],
    ) -> Result<anyhow::Result<crate::kiro::token_manager::CallContext>, RateLimitError> {
        if ticket.expired() {
            return Err(RateLimitError::local_busy(Duration::from_secs(1)));
        }
        let permit = match Arc::clone(&self.token_prep).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return Err(RateLimitError::local_busy(Duration::from_secs(1))),
        };
        let tm = Arc::clone(&self.token_manager);
        let model = model.map(str::to_string);
        let bound_ids = bound_ids.to_vec();
        let continuation_id = continuation_id.map(str::to_string);
        let avoid = avoid.to_vec();
        // 完整 token 准备（刷新→内存→persist）在独立任务中跑完；准备槽由该任务持有到结束。
        // 请求 timeout/Drop 只停止等待 JoinHandle，不 abort，以免丢掉已轮换的 refreshToken。
        let handle = tokio::spawn(async move {
            let _permit = permit;
            tm.acquire_context_sticky(
                model.as_deref(),
                &bound_ids,
                continuation_id.as_deref(),
                &avoid,
            )
            .await
        });
        match tokio::time::timeout(ticket.remaining(), handle).await {
            Ok(Ok(inner)) => Ok(inner),
            Ok(Err(_join)) => {
                // panic/任务异常：准备槽已随任务 unwind 释放；不得当成 invalid_grant
                Err(RateLimitError::local_busy(Duration::from_secs(1)))
            }
            Err(_elapsed) => {
                // JoinHandle drop = detach，后台继续完成轮换
                Err(RateLimitError::local_busy(Duration::from_secs(1)))
            }
        }
    }

    #[cfg(test)]
    pub fn token_prep_available(&self) -> usize {
        self.token_prep.available_permits()
    }

    fn finish_error(
        &self,
        model: Option<&str>,
        bound_ids: &[u64],
        last_rate_limit: Option<RateLimitError>,
        last_error: Option<anyhow::Error>,
        fallback: &str,
    ) -> anyhow::Error {
        let err = self.finalize_outcome(model, bound_ids, &last_rate_limit, &last_error, None);
        if last_rate_limit.is_none() && last_error.is_none() {
            let desc = err.to_string();
            if !desc.contains(QUOTA_EXHAUSTED_ALL_MARKER) && !desc.contains("429") {
                return anyhow::anyhow!(fallback.to_string());
            }
        }
        err
    }

    async fn sleep_retry(&self, ticket: &AdmissionTicket, attempt: usize, throttle: bool) {
        if ticket.expired() {
            return;
        }
        let delay = if throttle {
            Self::throttle_delay(attempt)
        } else {
            Self::retry_delay(attempt)
        };
        let delay = delay.min(ticket.remaining());
        if !delay.is_zero() {
            sleep(delay).await;
        }
    }

    fn is_monthly_request_limit(body: &str) -> bool {
        if body.contains("MONTHLY_REQUEST_COUNT") {
            return true;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return false;
        };

        if value
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == "MONTHLY_REQUEST_COUNT")
        {
            return true;
        }

        value
            .pointer("/error/reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == "MONTHLY_REQUEST_COUNT")
    }

    /// 检测数据面 400 是否为"账号缺少 profileArn"类错误。
    ///
    /// 企业 IdC 账号必须携带 profileArn，缺失时上游返回
    /// `400 {"message":"profileArn is required for this request."}`，
    /// 属账号级缺陷而非请求级错误，应故障转移到其他账号。
    fn is_profile_arn_required_error(body: &str) -> bool {
        body.contains("profileArn is required")
    }

    /// 无 profile_arn 账号的数据面 fallback ARN。
    ///
    /// 常量与推导逻辑统一定义在 [`crate::kiro::model::credentials`]（添加/加载账号时
    /// 即按此补全并持久化），此处委托保持行为一致。
    fn fallback_profile_arn(credentials: &KiroCredentials) -> Option<&'static str> {
        fallback_profile_arn_value(credentials)
    }

    /// 将请求 body 中的 `profileArn` 替换为当前选中账号的值。
    ///
    /// - 账号有 profile_arn → 设置 / 覆盖字段
    /// - 账号无 profile_arn → 按账号类型注入固定 ARN（见 [`Self::fallback_profile_arn`]）：
    ///   上游数据面对所有账号都要求该字段存在，缺失会 400 "profileArn is required"
    /// - JSON 解析失败 → 原样返回，不阻断请求
    fn rewrite_profile_arn(body: &str, credentials: &KiroCredentials) -> String {
        Self::rewrite_request_body(body, credentials, false)
    }

    /// 单次解析管线：profileArn 改写 + 按需 thinking adaptive 注入合并处理，
    /// 避免大请求体（Claude Code 场景可达数 MB）在链路内被多轮 parse/serialize。
    ///
    /// - JSON 解析失败 → 原样返回，不阻断请求
    /// - `requested=false` 时跳过注入，仅做 profileArn 改写（含 MCP 路径复用）
    fn rewrite_request_body(
        body: &str,
        credentials: &KiroCredentials,
        thinking_adaptive_requested: bool,
    ) -> String {
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(body) else {
            return body.to_string();
        };
        let obj = match value.as_object_mut() {
            Some(o) => o,
            None => return body.to_string(),
        };
        let arn = match &credentials.profile_arn {
            Some(arn) => Some(arn.as_str()),
            None => Self::fallback_profile_arn(credentials),
        };
        match arn {
            Some(arn) => {
                obj.insert(
                    "profileArn".to_string(),
                    serde_json::Value::String(arn.to_string()),
                );
            }
            None => {
                obj.remove("profileArn");
            }
        }

        // 按账号级开关注入 `additionalModelRequestFields.thinking`，复用同一份
        // 已解析的 value，不产生第二次 parse/serialize。注入条件（全部满足）：
        // - `thinking_adaptive_requested` 为 true（客户端请求了 adaptive）
        // - 账号级开关 `credentials.thinking_adaptive` 已开启
        // - 目标模型非 GPT 系且非 "4.5" 代际（复用 converter 侧
        //   `additional_fields_skipped` 谓词，与 `build_additional_model_request_fields`
        //   的整体跳过条件保持单一来源；modelId 取不到时 fail-closed 跳过）
        if thinking_adaptive_requested && credentials.thinking_adaptive {
            let model_id = obj
                .get("conversationState")
                .and_then(|cs| cs.get("currentMessage"))
                .and_then(|cm| cm.get("userInputMessage"))
                .and_then(|uim| uim.get("modelId"))
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();
            let model_id = model_id.as_str();
            if !model_id.is_empty()
                && !crate::anthropic::converter::additional_fields_skipped(model_id)
            {
                let fields = obj
                    .entry("additionalModelRequestFields")
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
                match fields.as_object_mut() {
                    Some(f) => {
                        f.insert(
                            "thinking".to_string(),
                            serde_json::json!({ "type": "adaptive" }),
                        );
                        tracing::debug!(
                            "[THINKING-ADAPTIVE] injected: credential={} model_id={} model_type=adaptive",
                            credentials.id.map(|i| i.to_string()).unwrap_or_default(),
                            model_id
                        );
                    }
                    None => tracing::warn!(
                        "[THINKING-ADAPTIVE] additionalModelRequestFields 非对象，跳过注入: credential={} model_id={}",
                        credentials.id.map(|i| i.to_string()).unwrap_or_default(),
                        model_id
                    ),
                }
            }
        }

        serde_json::to_string(&value).unwrap_or_else(|_| body.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kiro::model::credentials::{BUILDER_ID_PLACEHOLDER_PROFILE_ARN, SOCIAL_PROFILE_ARN};
    use crate::kiro::token_manager::CallContext;
    use crate::model::config::Config;

    fn create_test_provider(config: Config, credentials: KiroCredentials) -> KiroProvider {
        let tm = MultiTokenManager::new(config, vec![credentials], None, None, false).unwrap();
        KiroProvider::new(Arc::new(tm))
    }

    #[test]
    fn test_base_url() {
        let config = Config::default();
        let credentials = KiroCredentials::default();
        let provider = create_test_provider(config, credentials);
        assert!(provider.base_url().contains("amazonaws.com"));
        assert!(provider.base_url().contains("generateAssistantResponse"));
    }

    #[test]
    fn test_base_domain() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let credentials = KiroCredentials::default();
        let provider = create_test_provider(config, credentials);
        assert_eq!(provider.base_domain(), "q.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_build_headers() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        config.kiro_version = "0.8.0".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.profile_arn = Some("arn:aws:sso::123456789:profile/test".to_string());
        credentials.refresh_token = Some("a".repeat(150));

        let provider = create_test_provider(config, credentials.clone());
        let ctx = CallContext {
            id: 1,
            credentials,
            token: "test_token".to_string(),
        };
        let endpoint = Endpoint::by_name(EndpointName::Ide, "us-east-1");
        let headers = provider.build_headers(&ctx, "{}", 0, &endpoint).unwrap();

        assert_eq!(headers.get(CONTENT_TYPE).unwrap(), "application/json");
        assert_eq!(headers.get("x-amzn-codewhisperer-optout").unwrap(), "true");
        assert_eq!(headers.get("x-amzn-kiro-agent-mode").unwrap(), "vibe");
        assert!(
            headers
                .get(AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("Bearer ")
        );
        // Connection: close 已移除，启用 keep-alive 连接复用
        assert!(headers.get("connection").is_none());
    }

    #[test]
    fn test_is_monthly_request_limit_detects_reason() {
        let body = r#"{"message":"You have reached the limit.","reason":"MONTHLY_REQUEST_COUNT"}"#;
        assert!(KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_monthly_request_limit_nested_reason() {
        let body = r#"{"error":{"reason":"MONTHLY_REQUEST_COUNT"}}"#;
        assert!(KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_monthly_request_limit_false() {
        let body = r#"{"message":"nope","reason":"DAILY_REQUEST_COUNT"}"#;
        assert!(!KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_profile_arn_required_error_matches_upstream_message() {
        // 真实上游返回体（issue 场景：BuilderId 账号缺 profileArn）
        assert!(KiroProvider::is_profile_arn_required_error(
            r#"{"message":"profileArn is required for this request.","reason":null}"#
        ));
        assert!(KiroProvider::is_profile_arn_required_error(
            "400 Bad Request {\"message\":\"profileArn is required for this request.\"}"
        ));
        // 其他 400 不误判
        assert!(!KiroProvider::is_profile_arn_required_error(
            r#"{"message":"Invalid profileArn.","reason":null}"#
        ));
        assert!(!KiroProvider::is_profile_arn_required_error(""));
    }

    #[test]
    fn test_extract_agent_task_type_vibe_default() {
        assert_eq!(
            KiroProvider::extract_agent_task_type_from_request("{}"),
            "vibe"
        );
        assert_eq!(
            KiroProvider::extract_agent_task_type_from_request("invalid json"),
            "vibe"
        );
    }

    #[test]
    fn test_extract_agent_task_type_spectask() {
        let body = r#"{"conversationState":{"agentTaskType":"spectask","conversationId":"abc"}}"#;
        assert_eq!(
            KiroProvider::extract_agent_task_type_from_request(body),
            "spectask"
        );
    }

    #[test]
    fn test_extract_agent_task_type_vibe_explicit() {
        let body = r#"{"conversationState":{"agentTaskType":"vibe","conversationId":"abc"}}"#;
        assert_eq!(
            KiroProvider::extract_agent_task_type_from_request(body),
            "vibe"
        );
    }

    #[test]
    fn test_build_headers_spectask_mode() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        config.kiro_version = "0.8.0".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.profile_arn = Some("arn:aws:sso::123456789:profile/test".to_string());
        credentials.refresh_token = Some("a".repeat(150));

        let provider = create_test_provider(config, credentials.clone());
        let ctx = CallContext {
            id: 1,
            credentials,
            token: "test_token".to_string(),
        };
        let spectask_body = r#"{"conversationState":{"agentTaskType":"spectask"}}"#;
        let endpoint = Endpoint::by_name(EndpointName::Ide, "us-east-1");
        let headers = provider
            .build_headers(&ctx, spectask_body, 0, &endpoint)
            .unwrap();
        assert_eq!(headers.get("x-amzn-kiro-agent-mode").unwrap(), "spectask");
    }

    #[test]
    fn test_rewrite_profile_arn_overwrites_existing_field() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn"}"#;
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some("arn:aws:sso::111:profile/new".to_string());
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            v["profileArn"].as_str(),
            Some("arn:aws:sso::111:profile/new")
        );
    }

    #[test]
    fn test_rewrite_profile_arn_adds_field_when_missing() {
        // refresh_token 账号首次请求：body 无 profileArn，账号有 ARN → 应新增字段
        let body = r#"{"conversationState":{}}"#;
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some("arn:aws:sso::111:profile/new".to_string());
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            v["profileArn"].as_str(),
            Some("arn:aws:sso::111:profile/new")
        );
    }

    #[test]
    fn test_rewrite_profile_arn_injects_social_arn_when_none() {
        // 无 profile_arn 且无 clientId/secret（视为 social）→ 注入固定 Social ARN
        let body = r#"{"conversationState":{},"profileArn":"some-arn"}"#;
        let cred = KiroCredentials::default(); // profile_arn / auth_method 均为 None
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["profileArn"].as_str(), Some(SOCIAL_PROFILE_ARN));
    }

    #[test]
    fn test_rewrite_profile_arn_injects_builder_id_placeholder_for_idc_when_none() {
        // BuilderId 账号（auth_method 归一化为 idc，带 OIDC clientId/secret）缺 ARN
        // → 注入 Kiro IDE 官方占位符 ARN，而非移除字段
        let body = r#"{"conversationState":{}}"#;
        let mut cred = KiroCredentials::default();
        cred.auth_method = Some("idc".to_string());
        cred.client_id = Some("client".to_string());
        cred.client_secret = Some("secret".to_string());
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            v["profileArn"].as_str(),
            Some(BUILDER_ID_PLACEHOLDER_PROFILE_ARN)
        );
    }

    /// 构造开启 thinking_adaptive 的账号
    fn adaptive_cred() -> KiroCredentials {
        let mut cred = KiroCredentials::default();
        cred.thinking_adaptive = true;
        cred
    }

    #[test]
    fn test_inject_thinking_adaptive_injects_when_enabled_and_requested() {
        // 开关开启 + 客户端请求 adaptive + 非 4.5/GPT 模型 → 注入 thinking 字段
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-sonnet-4-6"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            v["additionalModelRequestFields"]["thinking"]["type"],
            serde_json::json!("adaptive")
        );
    }

    #[test]
    fn test_inject_thinking_adaptive_not_injected_when_switch_off() {
        // 客户端请求 adaptive 但账号开关关闭 → 不注入
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-sonnet-4-6"}}}}"#;
        let cred = KiroCredentials::default(); // thinking_adaptive = false
        let result = KiroProvider::rewrite_request_body(body, &cred, true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn test_inject_thinking_adaptive_not_injected_when_not_requested() {
        // 开关开启但客户端未请求 adaptive（enabled / 不传）→ 不注入
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-sonnet-4-6"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), false);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn test_inject_thinking_adaptive_skipped_for_4_5_models() {
        // "4.5" 代际模型 → 保持与 converter 整体跳过一致，不注入
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-sonnet-4.5"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn test_inject_thinking_adaptive_skipped_for_gpt_models() {
        // GPT 系模型 → 不注入
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"gpt-5.6-luna"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn test_inject_thinking_adaptive_creates_fields_when_missing() {
        // additionalModelRequestFields 不存在 → 创建新对象并插入 thinking
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-opus-4-6"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v["additionalModelRequestFields"].is_object());
        assert_eq!(
            v["additionalModelRequestFields"]["thinking"]["type"],
            serde_json::json!("adaptive")
        );
    }

    #[test]
    fn test_inject_thinking_adaptive_merges_into_existing_fields() {
        // additionalModelRequestFields 已存在 → 保留既有键，追加 thinking
        let body = r#"{"conversationState":{"currentMessage":{"userInputMessage":{"modelId":"claude-opus-4-6"}}},"additionalModelRequestFields":{"max_tokens":8192}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        let fields = &v["additionalModelRequestFields"];
        assert_eq!(fields["max_tokens"], serde_json::json!(8192));
        assert_eq!(fields["thinking"]["type"], serde_json::json!("adaptive"));
    }

    #[test]
    fn test_inject_thinking_adaptive_invalid_json_passthrough() {
        // JSON 解析失败 → 原样返回
        let body = "not-a-json";
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        assert_eq!(result, body);
    }

    #[test]
    fn test_inject_thinking_adaptive_skipped_when_model_id_missing() {
        // fail-closed：modelId 路径缺失（取不到模型）→ 跳过注入
        let body =
            r#"{"conversationState":{"currentMessage":{"userInputMessage":{"content":"hi"}}}}"#;
        let result = KiroProvider::rewrite_request_body(body, &adaptive_cred(), true);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn test_fallback_infer_idc_when_oidc_creds_present() {
        // auth_method 缺失但带 OIDC clientId/secret → 推断为 idc → BuilderId 占位符
        let mut cred = KiroCredentials::default();
        cred.client_id = Some("c".to_string());
        cred.client_secret = Some("s".to_string());
        assert_eq!(
            KiroProvider::fallback_profile_arn(&cred),
            Some(BUILDER_ID_PLACEHOLDER_PROFILE_ARN)
        );
    }

    #[test]
    fn test_fallback_infer_social_when_no_oidc_creds() {
        // auth_method 缺失且无 OIDC 凭据 → 推断为 social → 固定 Social ARN
        let cred = KiroCredentials::default();
        assert_eq!(
            KiroProvider::fallback_profile_arn(&cred),
            Some(SOCIAL_PROFILE_ARN)
        );
    }

    #[test]
    fn test_fallback_unknown_auth_method_returns_none() {
        // 未知/未归一化 auth_method（external_idp、enterprise 等）→ None
        // 移除字段交由上游 400 触发 ProfileArnMissing 禁用
        for method in ["external_idp", "enterprise", "unknown"] {
            let mut cred = KiroCredentials::default();
            cred.auth_method = Some(method.to_string());
            assert_eq!(
                KiroProvider::fallback_profile_arn(&cred),
                None,
                "auth_method={method} 应返回 None"
            );
        }
    }

    #[test]
    fn test_rewrite_profile_arn_removes_field_for_external_idp_when_none() {
        // 企业 IdC 账号缺 ARN：真实 ARN 因租户而异，仍移除字段，
        // 由上游 400 触发 ProfileArnMissing 禁用逻辑
        let body = r#"{"conversationState":{},"profileArn":"some-arn"}"#;
        let mut cred = KiroCredentials::default();
        cred.auth_method = Some("external_idp".to_string());
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        let v: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(v.get("profileArn").is_none());
    }

    #[test]
    fn test_rewrite_profile_arn_invalid_json_falls_back() {
        let body = "not-json";
        let mut cred = KiroCredentials::default();
        cred.profile_arn = Some("arn:aws:sso::111:profile/x".to_string());
        let result = KiroProvider::rewrite_profile_arn(body, &cred);
        assert_eq!(result, "not-json");
    }

    // -------- 多端点 LB: select_endpoint + amz_target 注入 --------

    #[test]
    fn test_select_endpoint_skips_throttled_buckets() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(1);
        creds.refresh_token = Some("a".repeat(150));
        let provider = create_test_provider(config, creds.clone());

        // 封禁 Ide 与 Runtime 两个桶
        provider
            .endpoint_registry
            .throttle(1, EndpointName::Ide, BUCKET_THROTTLE_DURATION);
        provider
            .endpoint_registry
            .throttle(1, EndpointName::Runtime, BUCKET_THROTTLE_DURATION);

        let picked = provider
            .select_endpoint(&creds, 0)
            .expect("应跳过封禁桶返回剩余端点");
        assert_eq!(picked.name, EndpointName::Codewhisperer);
    }

    #[test]
    fn test_select_endpoint_attempt_offset_rotates_endpoints() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(2);
        creds.refresh_token = Some("a".repeat(150));
        let provider = create_test_provider(config, creds.clone());

        // 4 桶均未封禁：attempt=0/1/2/3 应轮询 Ide/Runtime/Codewhisperer/Amazonq
        assert_eq!(
            provider.select_endpoint(&creds, 0).unwrap().name,
            EndpointName::Ide
        );
        assert_eq!(
            provider.select_endpoint(&creds, 1).unwrap().name,
            EndpointName::Runtime
        );
        assert_eq!(
            provider.select_endpoint(&creds, 2).unwrap().name,
            EndpointName::Codewhisperer
        );
        assert_eq!(
            provider.select_endpoint(&creds, 3).unwrap().name,
            EndpointName::Amazonq
        );
        // attempt=4 回卷到 Ide
        assert_eq!(
            provider.select_endpoint(&creds, 4).unwrap().name,
            EndpointName::Ide
        );
    }

    #[test]
    fn test_select_endpoint_returns_none_when_all_throttled() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(3);
        creds.refresh_token = Some("a".repeat(150));
        let provider = create_test_provider(config, creds.clone());

        // 封禁全部 4 桶
        for name in EndpointName::ALL {
            provider
                .endpoint_registry
                .throttle(3, name, BUCKET_THROTTLE_DURATION);
        }

        assert!(provider.select_endpoint(&creds, 0).is_none());
    }

    #[test]
    fn test_select_endpoint_respects_account_preferred_order() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(4);
        creds.refresh_token = Some("a".repeat(150));
        // 账号声明首选 Runtime
        creds.endpoint = Some(vec![EndpointName::Runtime]);
        let provider = create_test_provider(config, creds.clone());

        // attempt=0 应选 Runtime（首选在首）
        assert_eq!(
            provider.select_endpoint(&creds, 0).unwrap().name,
            EndpointName::Runtime
        );
        // attempt=1 跳过 Runtime → Ide（默认序下一个）
        assert_eq!(
            provider.select_endpoint(&creds, 1).unwrap().name,
            EndpointName::Ide
        );
    }

    #[test]
    fn test_build_headers_injects_amz_target_for_codewhisperer_endpoint() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        config.kiro_version = "0.8.0".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(5);
        creds.refresh_token = Some("a".repeat(150));

        let provider = create_test_provider(config, creds.clone());
        let ctx = CallContext {
            id: 5,
            credentials: creds,
            token: "tok".to_string(),
        };
        let endpoint = Endpoint::by_name(EndpointName::Codewhisperer, "us-east-1");
        let headers = provider.build_headers(&ctx, "{}", 0, &endpoint).unwrap();

        assert_eq!(
            headers.get("x-amz-target").unwrap(),
            "AmazonCodeWhispererStreamingService.GenerateAssistantResponse"
        );
        assert_eq!(
            headers.get("host").unwrap(),
            "codewhisperer.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_build_headers_omits_amz_target_for_ide_endpoint() {
        let mut config = Config::default();
        config.region = "us-east-1".to_string();
        let mut creds = KiroCredentials::default();
        creds.id = Some(6);
        creds.refresh_token = Some("a".repeat(150));

        let provider = create_test_provider(config, creds.clone());
        let ctx = CallContext {
            id: 6,
            credentials: creds,
            token: "tok".to_string(),
        };
        let endpoint = Endpoint::by_name(EndpointName::Ide, "us-east-1");
        let headers = provider.build_headers(&ctx, "{}", 0, &endpoint).unwrap();

        assert!(headers.get("x-amz-target").is_none());
        assert_eq!(headers.get("host").unwrap(), "q.us-east-1.amazonaws.com");
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod provider_tests;
