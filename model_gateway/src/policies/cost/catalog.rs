//! The policy catalog: names, parameters, construction.
//!
//! Parameters are YAML (JSON is YAML) text; unknown parameters are rejected, so a typo cannot
//! silently fall back to a default. The product ships one selection policy, the cache-aware
//! default; the `bench-policies` feature adds the measured, bench-only `cache-aware-balanced`
//! and the replay harness's comparison baseline. Without the feature those names are rejected
//! with a message that says so, rather than read as typos.

#[cfg(feature = "bench-policies")]
use super::{balanced, reference_cost};
use super::{default, policy::WorkerSelectionPolicy};

/// The policy used when none is configured: the pre-policy cache-aware decision.
pub const DEFAULT_POLICY: &str = default::POLICY_NAME;

#[cfg(not(feature = "bench-policies"))]
pub const POLICY_NAMES: &[&str] = &[default::POLICY_NAME];
#[cfg(feature = "bench-policies")]
pub const POLICY_NAMES: &[&str] = &[
    default::POLICY_NAME,
    balanced::POLICY_NAME,
    reference_cost::POLICY_NAME,
];

/// The names the `bench-policies` feature adds, known to a build without it
/// so that asking for one names the feature instead of a typo.
#[cfg(not(feature = "bench-policies"))]
const BENCH_ONLY_POLICY_NAMES: &[&str] = &["cache-aware-balanced", "reference-cost"];

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("unknown selection policy '{0}'; known: {known}", known = POLICY_NAMES.join(", "))]
    Unknown(String),
    #[error(
        "selection policy '{0}' is a measured, bench-only policy: this build has no \
         `bench-policies` feature, so it cannot be selected (known: {known})",
        known = POLICY_NAMES.join(", ")
    )]
    BenchOnly(String),
    #[error("selection policy '{name}' parameters: {message}")]
    Parameters { name: String, message: String },
}

/// Parameters of the policies the `bench-policies` feature adds; the default takes none.
#[cfg(feature = "bench-policies")]
fn parse<T: Default + serde::de::DeserializeOwned>(
    name: &str,
    params: Option<&str>,
) -> Result<T, CatalogError> {
    match params.map(str::trim).filter(|p| !p.is_empty()) {
        None => Ok(T::default()),
        Some(text) => serde_yaml::from_str(text).map_err(|e| CatalogError::Parameters {
            name: name.to_string(),
            message: e.to_string(),
        }),
    }
}

/// The default policy at the given cache-aware temperature; it takes no parameters and cannot
/// fail to build.
pub fn default_policy(selection_temperature: f32) -> WorkerSelectionPolicy {
    default::policy(selection_temperature)
}

/// Build a policy by name. `selection_temperature` is the cache-aware temperature the default
/// policy keeps using.
pub fn build(
    name: &str,
    params: Option<&str>,
    selection_temperature: f32,
) -> Result<WorkerSelectionPolicy, CatalogError> {
    match name {
        default::POLICY_NAME => {
            if params.is_some_and(|p| !p.trim().is_empty()) {
                return Err(CatalogError::Parameters {
                    name: name.to_string(),
                    message: "takes no parameters; tune it with the cache-aware flags".into(),
                });
            }
            Ok(default::policy(selection_temperature))
        }
        #[cfg(feature = "bench-policies")]
        balanced::POLICY_NAME => {
            let p: balanced::BalancedParams = parse(name, params)?;
            p.validate().map_err(|message| CatalogError::Parameters {
                name: name.to_string(),
                message,
            })?;
            Ok(balanced::policy(p))
        }
        #[cfg(feature = "bench-policies")]
        reference_cost::POLICY_NAME => {
            let p: reference_cost::ReferenceCostParams = parse(name, params)?;
            p.validate().map_err(|message| CatalogError::Parameters {
                name: name.to_string(),
                message,
            })?;
            Ok(reference_cost::policy(p))
        }
        #[cfg(not(feature = "bench-policies"))]
        other if BENCH_ONLY_POLICY_NAMES.contains(&other) => {
            Err(CatalogError::BenchOnly(other.to_string()))
        }
        other => Err(CatalogError::Unknown(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_name_builds_with_defaults() {
        for name in POLICY_NAMES {
            let policy = build(name, None, 0.0).expect("default parameters build");
            assert_eq!(policy.name(), *name);
        }
    }

    #[test]
    fn unknown_names_and_stray_parameters_are_rejected() {
        assert!(matches!(
            build("nope", None, 0.0),
            Err(CatalogError::Unknown(_))
        ));
        assert!(matches!(
            build(DEFAULT_POLICY, Some("{x: 1}"), 0.0),
            Err(CatalogError::Parameters { .. })
        ));
    }

    #[cfg(not(feature = "bench-policies"))]
    #[test]
    fn the_bench_only_policies_are_refused_by_name_without_the_feature() {
        for name in ["cache-aware-balanced", "reference-cost"] {
            let err = build(name, None, 0.0).expect_err("bench-only");
            assert!(matches!(err, CatalogError::BenchOnly(_)), "{name}: {err}");
            assert!(
                err.to_string().contains("bench-policies"),
                "the message names the feature: {err}"
            );
        }
    }

    #[cfg(feature = "bench-policies")]
    #[test]
    fn the_balanced_policy_takes_parameters_and_checks_them() {
        assert!(build(
            "cache-aware-balanced",
            Some("{affinity_cap_tokens: 4096, mean_prefill_tokens: 2048}"),
            0.0
        )
        .is_ok());
        assert!(matches!(
            build(
                "cache-aware-balanced",
                Some("{mean_prefill_tokens: 0}"),
                0.0
            ),
            Err(CatalogError::Parameters { .. })
        ));
        assert!(matches!(
            build(
                "cache-aware-balanced",
                Some("{affinity_cap_blocks: 32}"),
                0.0
            ),
            Err(CatalogError::Parameters { .. })
        ));
    }

    #[cfg(feature = "bench-policies")]
    #[test]
    fn the_bench_baseline_takes_json_parameters_and_checks_them() {
        assert!(build("reference-cost", Some(r#"{"router_temperature":0.5}"#), 0.0).is_ok());
        assert!(matches!(
            build("reference-cost", Some("{overlap_score_credit: -1}"), 0.0),
            Err(CatalogError::Parameters { .. })
        ));
        assert!(matches!(
            build("reference-cost", Some("{alphaa: 2.5}"), 0.0),
            Err(CatalogError::Parameters { .. })
        ));
    }
}
