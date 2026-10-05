//! The policy catalog: names, parameters, construction.
//!
//! Parameters are YAML (JSON is YAML) text; unknown parameters are rejected, so a typo cannot
//! silently fall back to a default. The product ships one selection policy, the cache-aware
//! default; the `bench-policies` feature adds the replay harness's comparison baseline.

#[cfg(feature = "bench-policies")]
use super::reference_cost;
use super::{default, policy::WorkerSelectionPolicy};

/// The policy used when none is configured: the pre-policy cache-aware decision.
pub const DEFAULT_POLICY: &str = default::POLICY_NAME;

#[cfg(not(feature = "bench-policies"))]
pub const POLICY_NAMES: &[&str] = &[default::POLICY_NAME];
#[cfg(feature = "bench-policies")]
pub const POLICY_NAMES: &[&str] = &[default::POLICY_NAME, reference_cost::POLICY_NAME];

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("unknown selection policy '{0}'; known: {known}", known = POLICY_NAMES.join(", "))]
    Unknown(String),
    #[error("selection policy '{name}' parameters: {message}")]
    Parameters { name: String, message: String },
}

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
        reference_cost::POLICY_NAME => {
            let p: reference_cost::ReferenceCostParams = parse(name, params)?;
            p.validate().map_err(|message| CatalogError::Parameters {
                name: name.to_string(),
                message,
            })?;
            Ok(reference_cost::policy(p))
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

    #[cfg(not(feature = "bench-policies"))]
    #[test]
    fn the_bench_baseline_is_not_selectable_without_the_feature() {
        assert!(matches!(
            build("reference-cost", None, 0.0),
            Err(CatalogError::Unknown(_))
        ));
    }
}
