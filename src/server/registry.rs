//! In-process session registry.
//!
//! `SessionRegistry` maps `jid -> SessionHandle` so the dispatcher can route
//! tasks to an already-running session in O(1) without consulting Redis.
//! Backed by `DashMap` for lock-free concurrent reads.

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::server::task::SessionCommand;

/// Handle held by the registry for each live session.
pub struct SessionHandle {
    pub jid: String,
    pub client: Arc<Client>,
    pub cmd_tx: mpsc::Sender<SessionCommand>,
    pub cancel: CancellationToken,
}

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("jid", &self.jid)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default)]
pub struct SessionRegistry {
    inner: Arc<DashMap<String, Arc<SessionHandle>>>,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, jid: &str) -> Option<Arc<SessionHandle>> {
        self.inner.get(jid).map(|r| r.value().clone())
    }

    pub fn insert(&self, handle: Arc<SessionHandle>) {
        self.inner.insert(handle.jid.clone(), handle);
    }

    /// Remove the entry for `jid` only if its `SessionHandle` is the same
    /// pointer (by `Arc` addr) as `expected`. Prevents a stale remover from
    /// evicting a freshly-recreated session.
    pub fn remove_if_matching(&self, jid: &str, expected: &Arc<SessionHandle>) -> bool {
        if let Some(entry) = self.inner.get_mut(jid)
            && Arc::ptr_eq(&*entry, expected)
        {
            drop(entry);
            self.inner.remove(jid);
            return true;
        }
        false
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Snapshot of all live session handles, for graceful shutdown.
    pub fn snapshot(&self) -> Vec<Arc<SessionHandle>> {
        self.inner.iter().map(|r| r.value().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use std::sync::Arc as StdArc;

    // The registry is a thin wrapper around DashMap<String, Arc<T>>. We test the
    // ptr-equality eviction logic with a trivial inner type instead of the real
    // SessionHandle (which owns an Arc<Client> that can't be cheaply faked).
    struct FakeInner;

    fn remove_if_matching_dashmap(
        map: &DashMap<String, Arc<FakeInner>>,
        key: &str,
        expected: &Arc<FakeInner>,
    ) -> bool {
        if let Some(entry) = map.get_mut(key)
            && Arc::ptr_eq(&*entry, expected)
        {
            drop(entry);
            map.remove(key);
            return true;
        }
        false
    }

    #[test]
    fn insert_get_remove() {
        let map: DashMap<String, Arc<FakeInner>> = DashMap::new();
        let h = StdArc::new(FakeInner);
        map.insert("a".into(), h.clone());
        assert!(map.get("a").is_some());
        assert!(remove_if_matching_dashmap(&map, "a", &h));
        assert!(map.get("a").is_none());
    }

    #[test]
    fn remove_if_matching_wont_evict_newer() {
        let map: DashMap<String, Arc<FakeInner>> = DashMap::new();
        let h1 = StdArc::new(FakeInner);
        map.insert("b".into(), h1.clone());
        let h2 = StdArc::new(FakeInner);
        map.insert("b".into(), h2.clone());
        assert!(!remove_if_matching_dashmap(&map, "b", &h1));
        assert!(map.get("b").is_some());
    }
}
