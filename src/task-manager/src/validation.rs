//! Shadow-validation state machine (Phase 2c of the intelligent-sync design).
//!
//! An inferred incremental strategy is only trusted once it has been *shadow
//! validated*: independently materialize what the incremental path would produce
//! and compare it to a fresh full pull. The pure state machine here decides what
//! a pass/divergence does to the table's lifecycle and when to audit next.
//!
//! Confidence determines the *starting* audit intensity; a passing track record
//! then decays it, and a divergence resets it and (eventually) demotes.

/// Result of one shadow-validation comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOutcome {
    /// The incremental candidate matched the full truth.
    Pass,
    /// They differed — the incremental contract does not hold for this table.
    Diverge,
}

/// Tunables (env-overridable by the caller).
#[derive(Debug, Clone, Copy)]
pub struct AuditThresholds {
    /// Consecutive passes for a `probation` table to graduate to `active`.
    pub probation_passes: u32,
    /// Accumulated divergences before a table is `rejected` (permanent full).
    pub divergence_max: u32,
    /// Base seconds until the next audit (decays with the pass streak).
    pub base_audit_secs: i64,
}

impl Default for AuditThresholds {
    fn default() -> Self {
        Self {
            probation_passes: 2,
            divergence_max: 2,
            base_audit_secs: 86_400,
        }
    }
}

/// The audit-relevant slice of a table's policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditState {
    /// suggested | active | rejected.
    pub status: String,
    /// trusted | probation | audited.
    pub tier: String,
    /// Consecutive successful audits.
    pub passes: u32,
    /// Accumulated divergences.
    pub divergence: u32,
}

/// What to do after an audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditTransition {
    pub status: String,
    pub passes: u32,
    pub divergence: u32,
    /// Run a full reconcile now to repair the cache (set on divergence).
    pub self_heal_full: bool,
    /// Seconds until the next audit, or `None` to stop auditing (a graduated
    /// probation table relies on the periodic full reconcile instead).
    pub next_audit_secs: Option<i64>,
}

/// Audit interval grows with the pass streak: base * 2^min(passes, 5). A table
/// that has passed many times in a row is audited ever less often.
fn decayed_interval(base: i64, passes: u32) -> i64 {
    base.saturating_mul(1i64 << passes.min(5))
}

/// Applies one audit outcome to a table's lifecycle. Pure.
pub fn apply_audit(
    state: &AuditState,
    outcome: AuditOutcome,
    th: &AuditThresholds,
) -> AuditTransition {
    match outcome {
        AuditOutcome::Pass => {
            let passes = state.passes + 1;
            // A clean pass clears the divergence streak.
            let divergence = 0;

            // Promotion: a still-`suggested` table earns `active` once it has
            // enough passes (trusted is already active and never enters here).
            let graduated = state.status == "suggested" && passes >= th.probation_passes;
            let status = if graduated {
                "active".to_string()
            } else {
                state.status.clone()
            };

            // `audited` keeps a (decaying) audit cadence forever; `probation`
            // stops auditing once it graduates and leans on the periodic full.
            let next_audit_secs = if status == "active" && state.tier == "probation" {
                None
            } else {
                Some(decayed_interval(th.base_audit_secs, passes))
            };

            AuditTransition {
                status,
                passes,
                divergence,
                self_heal_full: false,
                next_audit_secs,
            }
        }
        AuditOutcome::Diverge => {
            let divergence = state.divergence + 1;
            let rejected = divergence >= th.divergence_max;
            let status = if rejected {
                "rejected".to_string()
            } else {
                state.status.clone()
            };
            AuditTransition {
                status,
                passes: 0,
                divergence,
                // Always repair the cache on divergence.
                self_heal_full: true,
                // Stop auditing once rejected; otherwise re-check soon.
                next_audit_secs: if rejected {
                    None
                } else {
                    Some(th.base_audit_secs)
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn th() -> AuditThresholds {
        AuditThresholds {
            probation_passes: 2,
            divergence_max: 2,
            base_audit_secs: 100,
        }
    }

    fn state(status: &str, tier: &str, passes: u32, divergence: u32) -> AuditState {
        AuditState {
            status: status.to_string(),
            tier: tier.to_string(),
            passes,
            divergence,
        }
    }

    #[test]
    fn probation_graduates_after_enough_passes() {
        let s = state("suggested", "probation", 1, 0);
        let t = apply_audit(&s, AuditOutcome::Pass, &th());
        assert_eq!(t.status, "active");
        assert_eq!(t.passes, 2);
        // Graduated probation stops the dedicated audit cadence.
        assert_eq!(t.next_audit_secs, None);
    }

    #[test]
    fn probation_stays_suggested_before_threshold() {
        let s = state("suggested", "probation", 0, 0);
        let t = apply_audit(&s, AuditOutcome::Pass, &th());
        assert_eq!(t.status, "suggested");
        assert!(t.next_audit_secs.is_some());
    }

    #[test]
    fn audited_keeps_auditing_after_activation() {
        let s = state("suggested", "audited", 1, 0);
        let t = apply_audit(&s, AuditOutcome::Pass, &th());
        assert_eq!(t.status, "active");
        // Audited tier never stops auditing.
        assert!(t.next_audit_secs.is_some());
    }

    #[test]
    fn audit_interval_decays_with_pass_streak() {
        let early = apply_audit(&state("active", "audited", 1, 0), AuditOutcome::Pass, &th());
        let later = apply_audit(&state("active", "audited", 4, 0), AuditOutcome::Pass, &th());
        assert!(later.next_audit_secs.unwrap() > early.next_audit_secs.unwrap());
    }

    #[test]
    fn divergence_self_heals_and_resets_passes() {
        let s = state("active", "audited", 5, 0);
        let t = apply_audit(&s, AuditOutcome::Diverge, &th());
        assert!(t.self_heal_full);
        assert_eq!(t.passes, 0);
        assert_eq!(t.divergence, 1);
        assert_eq!(t.status, "active"); // one divergence is not fatal
    }

    #[test]
    fn repeated_divergence_rejects() {
        let s = state("active", "audited", 0, 1);
        let t = apply_audit(&s, AuditOutcome::Diverge, &th());
        assert_eq!(t.status, "rejected");
        assert!(t.self_heal_full);
        assert_eq!(t.next_audit_secs, None);
    }

    #[test]
    fn pass_clears_divergence_streak() {
        let s = state("active", "audited", 0, 1);
        let t = apply_audit(&s, AuditOutcome::Pass, &th());
        assert_eq!(t.divergence, 0);
    }
}
