//! Runtime UDFs backing the column-mask functions. They are embedded directly
//! into the plan expressions (no session registration needed) by the read
//! resolver.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use kokedb_common::masking::{mask_string, MaskFunction};

/// A one-argument string-to-string mask UDF applying a fixed [`MaskFunction`].
/// The function is baked in at plan time (one UDF instance per masked column),
/// so the expression carries no user-tunable state.
#[derive(Debug)]
struct MaskUdf {
    name: String,
    func: MaskFunction,
    signature: Signature,
}

impl ScalarUDFImpl for MaskUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = args
            .args
            .first()
            .ok_or_else(|| DataFusionError::Internal("mask udf takes one argument".into()))?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(v.as_ref().map(|s| mask_string(s, &self.func))),
            )),
            ColumnarValue::Array(array) => {
                let strings = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "mask udf expects Utf8 input, got {}",
                            array.data_type()
                        ))
                    })?;
                let masked: StringArray = strings
                    .iter()
                    .map(|v| v.map(|s| mask_string(s, &self.func)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(masked) as ArrayRef))
            }
            other => Err(DataFusionError::Internal(format!(
                "mask udf got unsupported input: {other:?}"
            ))),
        }
    }
}

/// Builds the UDF for a mask function. `Null` masks are handled by the planner
/// with a typed NULL literal and never reach here.
pub fn mask_udf(func: &MaskFunction) -> ScalarUDF {
    let name = match func {
        MaskFunction::Hash => "kokedb_mask_hash".to_string(),
        MaskFunction::Redact => "kokedb_mask_redact".to_string(),
        MaskFunction::Partial { prefix, suffix } => {
            format!("kokedb_mask_partial_{prefix}_{suffix}")
        }
        MaskFunction::Null => "kokedb_mask_null".to_string(),
    };
    ScalarUDF::from(MaskUdf {
        name,
        func: func.clone(),
        signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    fn run(func: MaskFunction, values: Vec<Option<&str>>) -> Vec<Option<String>> {
        let udf = mask_udf(&func);
        let array: ArrayRef = Arc::new(StringArray::from(values));
        let num_rows = array.len();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
            number_rows: num_rows,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
        };
        let out = udf.inner().invoke_with_args(args).expect("invoke");
        match out {
            ColumnarValue::Array(a) => a
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 out")
                .iter()
                .map(|v| v.map(|s| s.to_string()))
                .collect(),
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }
    }

    #[test]
    fn masks_arrays_and_preserves_nulls() {
        let out = run(
            MaskFunction::Partial { prefix: 0, suffix: 4 },
            vec![Some("111-22-6789"), None, Some("ab")],
        );
        assert_eq!(out[0].as_deref(), Some("*******6789"));
        assert_eq!(out[1], None, "NULL inputs stay NULL");
        assert_eq!(out[2].as_deref(), Some("***"), "short values fully masked");

        let out = run(MaskFunction::Redact, vec![Some("secret")]);
        assert_eq!(out[0].as_deref(), Some("***"));

        let out = run(MaskFunction::Hash, vec![Some("secret"), Some("secret")]);
        assert_eq!(out[0], out[1], "hash is deterministic");
        assert_eq!(out[0].as_ref().map(|s| s.len()), Some(64));
    }
}
