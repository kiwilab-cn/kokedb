use crate::{
    error::{CommonError, CommonResult},
    spec::Plan,
};
use xxhash_rust::xxh3;

pub fn get_plan_hash(plan: &Plan) -> CommonResult<u64> {
    let plan_bytes = serde_json::to_vec(&plan).map_err(|x| {
        CommonError::InternalError(format!("Failed to serde plan to json with error: {:?}", x))
    })?;

    let key = xxh3::xxh3_64(&plan_bytes);
    Ok(key)
}
