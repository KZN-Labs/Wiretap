//! Compiled, fast event filters.
//!
//! Three filter dimensions, AND-composed across dimensions, OR-composed within:
//!   * event type: exact `pkg::module::Struct` or wildcard `pkg::module::*`
//!   * package id: exact match against `event.package_id`
//!   * sender (transaction sender) / affected address (any balance_change
//!     address from the transaction).
//!
//! Large address watchlists go through a bloom filter pre-check before the
//! HashSet confirm — for tens of thousands of addresses this keeps the per-event
//! cost in the tens of nanoseconds when the watchlist misses.
//!
//! The real Sui v2 proto carries no top-level "affected addresses" list on a
//! transaction. We approximate it with the set of addresses appearing in
//! `ExecutedTransaction.balance_changes[].address` — the cleanest signal of
//! "this transaction touched address X's coins" — plus the transaction sender.

use crate::proto::{Event as ProtoEvent, ExecutedTransaction};
use bloomfilter::Bloom;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

fn new_bloom<T: ?Sized>(items: usize) -> Bloom<T> {
    // 0.1% FP target. ~14.4 bits/item; for very small `items` floor at 64 bits.
    let bits = ((items as f64 * 14.4).ceil() as usize).max(64);
    Bloom::new(bits / 8 + 1, items.max(1))
}

/// Declarative filter spec, usually built from `wiretap.toml` `[[watch]]` blocks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FilterSpec {
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub senders: Vec<String>,
    #[serde(default)]
    pub affected: Vec<String>,
}

impl FilterSpec {
    pub fn with_event(mut self, e: impl Into<String>) -> Self {
        self.events.push(e.into());
        self
    }
    pub fn with_package(mut self, p: impl Into<String>) -> Self {
        self.packages.push(p.into());
        self
    }
    pub fn with_sender(mut self, s: impl Into<String>) -> Self {
        self.senders.push(s.into());
        self
    }

    pub fn compile(&self) -> CompiledFilter {
        let mut exact_events = HashSet::new();
        let mut wildcard_prefixes = Vec::new();
        for e in &self.events {
            if let Some(prefix) = e.strip_suffix("::*") {
                wildcard_prefixes.push(format!("{prefix}::"));
            } else {
                exact_events.insert(e.clone());
            }
        }

        let packages: HashSet<String> = self.packages.iter().cloned().collect();
        let senders: HashSet<String> = self.senders.iter().cloned().collect();
        let affected: HashSet<String> = self.affected.iter().cloned().collect();

        const BLOOM_THRESHOLD: usize = 64;
        let affected_bloom = (affected.len() >= BLOOM_THRESHOLD).then(|| {
            let mut b = new_bloom::<String>(affected.len());
            for a in &affected {
                b.set(a);
            }
            b
        });
        let senders_bloom = (senders.len() >= BLOOM_THRESHOLD).then(|| {
            let mut b = new_bloom::<String>(senders.len());
            for a in &senders {
                b.set(a);
            }
            b
        });

        CompiledFilter {
            exact_events,
            wildcard_prefixes,
            packages,
            senders,
            affected,
            affected_bloom,
            senders_bloom,
            match_all: self.events.is_empty()
                && self.packages.is_empty()
                && self.senders.is_empty()
                && self.affected.is_empty(),
        }
    }
}

pub struct CompiledFilter {
    exact_events: HashSet<String>,
    wildcard_prefixes: Vec<String>,
    packages: HashSet<String>,
    senders: HashSet<String>,
    affected: HashSet<String>,
    affected_bloom: Option<Bloom<String>>,
    senders_bloom: Option<Bloom<String>>,
    match_all: bool,
}

impl CompiledFilter {
    /// Returns true when `event` (carried by `tx`) passes every active dimension.
    pub fn matches(&self, tx: &ExecutedTransaction, event: &ProtoEvent) -> bool {
        if self.match_all {
            return true;
        }

        let tx_sender: &str = tx
            .transaction
            .as_ref()
            .and_then(|t| t.sender.as_deref())
            .unwrap_or("");
        let event_sender: &str = event.sender.as_deref().unwrap_or("");
        let event_type: &str = event.event_type.as_deref().unwrap_or("");
        let event_pkg: &str = event.package_id.as_deref().unwrap_or("");

        if !self.exact_events.is_empty() || !self.wildcard_prefixes.is_empty() {
            let exact_hit = self.exact_events.contains(event_type);
            let wild_hit = self
                .wildcard_prefixes
                .iter()
                .any(|p| event_type.starts_with(p));
            if !exact_hit && !wild_hit {
                return false;
            }
        }

        if !self.packages.is_empty() && !self.packages.contains(event_pkg) {
            return false;
        }

        if !self.senders.is_empty() {
            let sender = if !event_sender.is_empty() {
                event_sender
            } else {
                tx_sender
            };
            if let Some(b) = &self.senders_bloom {
                if !b.check(&sender.to_string()) {
                    return false;
                }
            }
            if !self.senders.contains(sender) {
                return false;
            }
        }

        if !self.affected.is_empty() {
            let mut hit = false;
            // Sender is always considered "affected".
            if check_affected(&self.affected, &self.affected_bloom, tx_sender) {
                hit = true;
            }
            if !hit {
                for bc in &tx.balance_changes {
                    if let Some(addr) = bc.address.as_deref() {
                        if check_affected(&self.affected, &self.affected_bloom, addr) {
                            hit = true;
                            break;
                        }
                    }
                }
            }
            if !hit {
                return false;
            }
        }

        true
    }
}

fn check_affected(
    set: &HashSet<String>,
    bloom: &Option<Bloom<String>>,
    addr: &str,
) -> bool {
    if let Some(b) = bloom {
        if !b.check(&addr.to_string()) {
            return false;
        }
    }
    set.contains(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Bcs, Event as ProtoEvent, ExecutedTransaction, Transaction};

    fn ev(t: &str, pkg: &str, sender: &str) -> ProtoEvent {
        ProtoEvent {
            event_type: Some(t.into()),
            package_id: Some(pkg.into()),
            module: t.split("::").nth(1).map(|s| s.to_string()),
            sender: Some(sender.into()),
            contents: Some(Bcs::default()),
            json: None,
        }
    }
    fn tx() -> ExecutedTransaction {
        ExecutedTransaction {
            transaction: Some(Transaction::default()),
            ..Default::default()
        }
    }

    #[test]
    fn empty_spec_matches_all() {
        let f = FilterSpec::default().compile();
        assert!(f.matches(&tx(), &ev("a::b::C", "0x1", "0xs")));
    }

    #[test]
    fn exact_event_type() {
        let f = FilterSpec::default().with_event("0x2::pool::SwapEvent").compile();
        assert!(f.matches(&tx(), &ev("0x2::pool::SwapEvent", "0x2", "0xs")));
        assert!(!f.matches(&tx(), &ev("0x2::pool::AddLiquidity", "0x2", "0xs")));
    }

    #[test]
    fn wildcard_event_type() {
        let f = FilterSpec::default().with_event("0x2::pool::*").compile();
        assert!(f.matches(&tx(), &ev("0x2::pool::SwapEvent", "0x2", "")));
        assert!(f.matches(&tx(), &ev("0x2::pool::AddLiquidity", "0x2", "")));
        assert!(!f.matches(&tx(), &ev("0x2::farm::Harvest", "0x2", "")));
    }

    #[test]
    fn package_filter() {
        let f = FilterSpec::default().with_package("0xabc").compile();
        assert!(f.matches(&tx(), &ev("x::y::Z", "0xabc", "")));
        assert!(!f.matches(&tx(), &ev("x::y::Z", "0xdef", "")));
    }
}
