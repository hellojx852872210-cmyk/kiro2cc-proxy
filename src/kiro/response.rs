// Copyright (c) 2026 Harllan He. Licensed under MIT.
//! 携带活跃流许可的上游响应封装。
//!
//! 200 响应头返回时不释放许可；text/bytes/bytes_stream 直到 EOF / 传输错误 /
//! Drop 才归还。调用方不应剥离 guard。

use bytes::Bytes;
use futures::Stream;
use reqwest::Response;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{Notify, OwnedSemaphorePermit};

use super::gate::InFlightGuard;

/// 全局 semaphore 许可。Drop 时先归还许可再唤醒等待方。
pub struct GlobalLease {
    notify: Arc<Notify>,
    permit: Option<OwnedSemaphorePermit>,
}

impl GlobalLease {
    pub fn new(permit: OwnedSemaphorePermit, notify: Arc<Notify>) -> Self {
        Self {
            notify,
            permit: Some(permit),
        }
    }

    fn release(&mut self) {
        self.permit.take();
        self.notify.notify_waiters();
    }
}

impl Drop for GlobalLease {
    fn drop(&mut self) {
        self.release();
    }
}

/// 持有全局/账号活跃流许可的 reqwest 响应。
pub struct LeasedResponse {
    inner: Response,
    global: Option<GlobalLease>,
    account: Option<InFlightGuard>,
}

impl std::fmt::Debug for LeasedResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeasedResponse")
            .field("status", &self.inner.status())
            .finish_non_exhaustive()
    }
}

impl LeasedResponse {
    pub fn new(
        inner: Response,
        global: Option<GlobalLease>,
        account: Option<InFlightGuard>,
    ) -> Self {
        Self {
            inner,
            global,
            account,
        }
    }

    #[allow(dead_code)]
    pub fn status(&self) -> reqwest::StatusCode {
        self.inner.status()
    }

    #[allow(dead_code)]
    pub fn headers(&self) -> &reqwest::header::HeaderMap {
        self.inner.headers()
    }

    pub async fn text(self) -> reqwest::Result<String> {
        let LeasedResponse {
            inner,
            global,
            account,
        } = self;
        let result = inner.text().await;
        drop((global, account));
        result
    }

    pub async fn bytes(self) -> reqwest::Result<Bytes> {
        let LeasedResponse {
            inner,
            global,
            account,
        } = self;
        let result = inner.bytes().await;
        drop((global, account));
        result
    }

    pub fn bytes_stream(self) -> LeasedByteStream {
        let LeasedResponse {
            inner,
            global,
            account,
        } = self;
        LeasedByteStream {
            inner: Box::pin(inner.bytes_stream()),
            global,
            account,
        }
    }

    /// 测试辅助：包装无许可的 HTTP 响应。生产路径禁止无 guard 逃逸。
    #[cfg(test)]
    pub fn from_http_for_test(resp: http::Response<Bytes>) -> Self {
        Self {
            inner: Response::from(resp),
            global: None,
            account: None,
        }
    }
}

/// 许可跟随字节流直到 EOF / Err / Drop。
pub struct LeasedByteStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    global: Option<GlobalLease>,
    account: Option<InFlightGuard>,
}

impl LeasedByteStream {
    fn release(&mut self) {
        self.global.take();
        self.account.take();
    }
}

impl Stream for LeasedByteStream {
    type Item = Result<Bytes, reqwest::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                self.release();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.release();
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl Drop for LeasedByteStream {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kiro::gate::ConcurrencyGate;
    use crate::model::concurrency::ConcurrencySettings;
    use futures::StreamExt;
    use tokio::sync::Semaphore;

    fn http_ok(body: &'static [u8]) -> reqwest::Response {
        Response::from(
            http::Response::builder()
                .status(200)
                .body(Bytes::from_static(body))
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn headers_do_not_release_permits() {
        let sem = Arc::new(Semaphore::new(1));
        let notify = Arc::new(Notify::new());
        let permit = sem.clone().acquire_owned().await.unwrap();
        let gate = ConcurrencyGate::new(ConcurrencySettings {
            max_inflight_per_credential: 1,
            global_cooldown_enabled: false,
            ..Default::default()
        });
        let account = gate.try_admit(1).unwrap();
        let leased = LeasedResponse::new(
            http_ok(b"abc"),
            Some(GlobalLease::new(permit, notify)),
            Some(account),
        );
        assert_eq!(leased.status().as_u16(), 200);
        assert_eq!(sem.available_permits(), 0);
        assert_eq!(gate.inflight(1), 1);
        drop(leased);
        assert_eq!(sem.available_permits(), 1);
        assert_eq!(gate.inflight(1), 0);
    }

    #[tokio::test]
    async fn stream_eof_releases_immediately() {
        let sem = Arc::new(Semaphore::new(1));
        let notify = Arc::new(Notify::new());
        let permit = sem.clone().acquire_owned().await.unwrap();
        let leased = LeasedResponse::new(
            http_ok(b"abc"),
            Some(GlobalLease::new(permit, notify)),
            None,
        );
        let mut stream = leased.bytes_stream();
        assert_eq!(sem.available_permits(), 0);
        while stream.next().await.is_some() {}
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn stream_drop_releases_without_eof() {
        let sem = Arc::new(Semaphore::new(1));
        let notify = Arc::new(Notify::new());
        let permit = sem.clone().acquire_owned().await.unwrap();
        let leased = LeasedResponse::new(
            http_ok(b"abc"),
            Some(GlobalLease::new(permit, notify)),
            None,
        );
        let stream = leased.bytes_stream();
        assert_eq!(sem.available_permits(), 0);
        drop(stream);
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn text_holds_until_body_read() {
        let sem = Arc::new(Semaphore::new(1));
        let notify = Arc::new(Notify::new());
        let permit = sem.clone().acquire_owned().await.unwrap();
        let leased = LeasedResponse::new(
            http_ok(b"hello"),
            Some(GlobalLease::new(permit, notify)),
            None,
        );
        assert_eq!(sem.available_permits(), 0);
        let body = leased.text().await.unwrap();
        assert_eq!(body, "hello");
        assert_eq!(sem.available_permits(), 1);
    }
}
