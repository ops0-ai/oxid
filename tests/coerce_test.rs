use oxid::executor::engine::{coerce_value_to_cty_type, populate_object_from_cty};
use serde_json::json;

// ─── Scalar coercion: "string" ───────────────────────────────────────────────

#[test]
fn test_coerce_integer_to_string() {
    let result = coerce_value_to_cty_type(json!(8497), &json!("string"));
    assert_eq!(result, json!("8497"));
}

#[test]
fn test_coerce_float_to_string() {
    let result = coerce_value_to_cty_type(json!(2.75), &json!("string"));
    assert_eq!(result, json!("2.75"));
}

#[test]
fn test_coerce_bool_true_to_string() {
    let result = coerce_value_to_cty_type(json!(true), &json!("string"));
    assert_eq!(result, json!("true"));
}

#[test]
fn test_coerce_bool_false_to_string() {
    let result = coerce_value_to_cty_type(json!(false), &json!("string"));
    assert_eq!(result, json!("false"));
}

#[test]
fn test_coerce_string_to_string_noop() {
    let result = coerce_value_to_cty_type(json!("hello"), &json!("string"));
    assert_eq!(result, json!("hello"));
}

// ─── Scalar coercion: "number" ───────────────────────────────────────────────

#[test]
fn test_coerce_string_integer_to_number() {
    let result = coerce_value_to_cty_type(json!("8497"), &json!("number"));
    assert_eq!(result, json!(8497));
}

#[test]
fn test_coerce_string_float_to_number() {
    let result = coerce_value_to_cty_type(json!("2.75"), &json!("number"));
    assert_eq!(result, json!(2.75));
}

#[test]
fn test_coerce_unparseable_string_to_number_unchanged() {
    let result = coerce_value_to_cty_type(json!("not_a_number"), &json!("number"));
    assert_eq!(result, json!("not_a_number"));
}

#[test]
fn test_coerce_number_to_number_noop() {
    let result = coerce_value_to_cty_type(json!(42), &json!("number"));
    assert_eq!(result, json!(42));
}

#[test]
fn test_coerce_bool_true_to_number() {
    let result = coerce_value_to_cty_type(json!(true), &json!("number"));
    assert_eq!(result, json!(1));
}

#[test]
fn test_coerce_bool_false_to_number() {
    let result = coerce_value_to_cty_type(json!(false), &json!("number"));
    assert_eq!(result, json!(0));
}

// ─── Scalar coercion: "bool" ─────────────────────────────────────────────────

#[test]
fn test_coerce_string_true_to_bool() {
    let result = coerce_value_to_cty_type(json!("true"), &json!("bool"));
    assert_eq!(result, json!(true));
}

#[test]
fn test_coerce_string_false_to_bool() {
    let result = coerce_value_to_cty_type(json!("false"), &json!("bool"));
    assert_eq!(result, json!(false));
}

#[test]
fn test_coerce_string_one_to_bool() {
    let result = coerce_value_to_cty_type(json!("1"), &json!("bool"));
    assert_eq!(result, json!(true));
}

#[test]
fn test_coerce_string_zero_to_bool() {
    let result = coerce_value_to_cty_type(json!("0"), &json!("bool"));
    assert_eq!(result, json!(false));
}

#[test]
fn test_coerce_nonzero_number_to_bool_true() {
    let result = coerce_value_to_cty_type(json!(1), &json!("bool"));
    assert_eq!(result, json!(true));
}

#[test]
fn test_coerce_zero_to_bool_false() {
    let result = coerce_value_to_cty_type(json!(0), &json!("bool"));
    assert_eq!(result, json!(false));
}

#[test]
fn test_coerce_bool_to_bool_noop() {
    let result = coerce_value_to_cty_type(json!(true), &json!("bool"));
    assert_eq!(result, json!(true));
}

// ─── Null passthrough ────────────────────────────────────────────────────────

#[test]
fn test_coerce_null_stays_null_for_string() {
    let result = coerce_value_to_cty_type(json!(null), &json!("string"));
    assert_eq!(result, json!(null));
}

#[test]
fn test_coerce_null_stays_null_for_map() {
    let result = coerce_value_to_cty_type(json!(null), &json!(["map", "string"]));
    assert_eq!(result, json!(null));
}

// ─── Map coercion: ["map", "string"] ─────────────────────────────────────────

#[test]
fn test_coerce_map_string_coerces_integer_values() {
    let cty = json!(["map", "string"]);
    let value = json!({"port": 8497, "name": "web"});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!({"port": "8497", "name": "web"}));
}

#[test]
fn test_coerce_map_string_coerces_bool_values() {
    let cty = json!(["map", "string"]);
    let value = json!({"enabled": true, "name": "test"});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!({"enabled": "true", "name": "test"}));
}

#[test]
fn test_coerce_map_number_coerces_string_values() {
    let cty = json!(["map", "number"]);
    let value = json!({"port": "8080", "count": 3});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!({"port": 8080, "count": 3}));
}

#[test]
fn test_coerce_empty_map_unchanged() {
    let cty = json!(["map", "string"]);
    let result = coerce_value_to_cty_type(json!({}), &cty);
    assert_eq!(result, json!({}));
}

// ─── List/set coercion ───────────────────────────────────────────────────────

#[test]
fn test_coerce_list_string_coerces_integer_elements() {
    let cty = json!(["list", "string"]);
    let value = json!([1, 2, 3]);
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!(["1", "2", "3"]));
}

#[test]
fn test_coerce_set_string_coerces_mixed_elements() {
    let cty = json!(["set", "string"]);
    let value = json!([true, 42]);
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!(["true", "42"]));
}

#[test]
fn test_coerce_list_number_coerces_string_elements() {
    let cty = json!(["list", "number"]);
    let value = json!(["1", "2", "3"]);
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!([1, 2, 3]));
}

// ─── Nested collection coercion ──────────────────────────────────────────────

#[test]
fn test_coerce_list_of_map_string_deep_coercion() {
    let cty = json!(["list", ["map", "string"]]);
    let value = json!([{"port": 8080}, {"port": 9090}]);
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!([{"port": "8080"}, {"port": "9090"}]));
}

// ─── Object coercion with per-attribute types ────────────────────────────────

#[test]
fn test_coerce_object_coerces_attribute_types() {
    let cty = json!(["object", {"name": "string", "port": "number"}]);
    let value = json!({"name": "web", "port": "8080"});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!({"name": "web", "port": 8080}));
}

#[test]
fn test_coerce_object_populates_missing_and_coerces() {
    let cty = json!(["object", {"name": "string", "port": "number", "enabled": "bool"}]);
    let value = json!({"name": 42, "port": "8080"});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!({"name": "42", "port": 8080, "enabled": null}));
}

#[test]
fn test_populate_object_from_cty_coerces_values() {
    let cty = json!(["object", {"id": "string", "count": "number"}]);
    let value = json!({"id": 123, "count": "5"});
    let result = populate_object_from_cty(value, &cty);
    assert_eq!(result, json!({"id": "123", "count": 5}));
}

// ─── Regression: existing structural behavior ────────────────────────────────

#[test]
fn test_coerce_single_object_wrapped_in_list() {
    let cty = json!(["list", ["object", {"name": "string"}]]);
    let value = json!({"name": "test"});
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!([{"name": "test"}]));
}

#[test]
fn test_coerce_list_of_objects_populates_missing() {
    let cty = json!(["list", ["object", {"a": "string", "b": "number"}]]);
    let value = json!([{"a": "x"}]);
    let result = coerce_value_to_cty_type(value, &cty);
    assert_eq!(result, json!([{"a": "x", "b": null}]));
}

#[test]
fn test_coerce_dynamic_type_passthrough() {
    let result = coerce_value_to_cty_type(json!(42), &json!("dynamic"));
    assert_eq!(result, json!(42));
}

// ─── Scalar in list/set context (non-array, non-object) ──────────────────────

#[test]
fn test_coerce_scalar_in_list_string_context() {
    // A bare integer in a ["list", "string"] context should be coerced to string
    let cty = json!(["list", "string"]);
    let result = coerce_value_to_cty_type(json!(42), &cty);
    assert_eq!(result, json!("42"));
}
