use crate::{
    error::{CommonError, CommonResult},
    spec::Plan,
};
use xxhash_rust::xxh3;

/// A 128-bit content hash of the logical plan, used as the result-cache key.
/// 128 bits makes accidental collisions (which would return a *different*
/// query's cached result) astronomically unlikely, unlike a 64-bit key.
pub fn get_plan_hash(plan: &Plan) -> CommonResult<u128> {
    let plan_bytes = serde_json::to_vec(&plan).map_err(|x| {
        CommonError::InternalError(format!("Failed to serde plan to json with error: {:?}", x))
    })?;

    let key = xxh3::xxh3_128(&plan_bytes);
    Ok(key)
}
