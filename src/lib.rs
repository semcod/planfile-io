use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Value as JsonValue;
use std::path::Path;

mod atomic;
mod config;
mod data_gateway;
mod fleet_ops;
mod shards;
mod yaml_io;

fn json_to_py<'py>(py: Python<'py>, val: &JsonValue) -> PyResult<PyObject> {
    match val {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                Ok(n.to_string().into_py(py))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(arr) => {
            let list = PyList::empty_bound(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_py(py))
        }
        JsonValue::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_py(py))
        }
    }
}

fn py_to_json(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    let json_mod = py.import_bound("json")?;
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("ensure_ascii", false)?;
    kwargs.set_item("default", py.eval_bound("str", None, None)?)?;
    let json_str: String = json_mod
        .call_method("dumps", (data,), Some(&kwargs))?
        .extract()?;
    serde_json::from_str(&json_str).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
fn read_yaml_fast(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let json_val =
        yaml_io::read_yaml_fast(Path::new(path)).map_err(|e| PyIOError::new_err(e))?;
    json_to_py(py, &json_val)
}

#[pyfunction]
fn write_yaml_fast(py: Python<'_>, path: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
    let json_val = py_to_json(py, data)?;

    let yaml_str =
        serde_yaml::to_string(&json_val).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let p = Path::new(path);
    atomic::atomic_write_str(p, &yaml_str).map_err(|e| PyIOError::new_err(e.to_string()))?;

    let current_mtime = yaml_io::stat_mtime_ns(p).unwrap_or(0) as u64;
    let mirror = yaml_io::mirror_path(p);
    let _ = yaml_io::write_mirror_envelope(&mirror, current_mtime, &json_val);

    Ok(())
}

#[pyfunction]
fn audit_mirror_fast(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let p = Path::new(path);
    let (ok, healed, reason) = yaml_io::audit_mirror_fast(p);
    let out = PyDict::new_bound(py);
    out.set_item("path", path)?;
    out.set_item("ok", ok)?;
    out.set_item("healed", healed)?;
    out.set_item("reason", reason)?;
    Ok(out.into_py(py))
}

#[pyfunction]
fn resolve_shard_path(
    sprints_dir: &str,
    sprint: &str,
    ticket_id: &str,
    shard_size: usize,
    custom_shards: usize,
) -> String {
    let router = shards::ShardRouter::new(shard_size, custom_shards);
    router
        .shard_path(Path::new(sprints_dir), sprint, ticket_id)
        .to_string_lossy()
        .to_string()
}

#[pyfunction]
fn load_sharded_sprint(py: Python<'_>, sprints_dir: &str, sprint: &str) -> PyResult<PyObject> {
    let json_val = shards::load_sharded_sprint(Path::new(sprints_dir), sprint)
        .map_err(|e| PyIOError::new_err(e))?;
    json_to_py(py, &json_val)
}

#[pyfunction]
fn compute_source_signature(paths: Vec<String>) -> Vec<(String, u64, u64)> {
    shards::compute_source_signature(&paths)
}

#[pyfunction]
fn get_runtime_config(py: Python<'_>) -> PyResult<PyObject> {
    let cfg = config::StorageRuntimeConfig::from_env();
    let dict = PyDict::new_bound(py);
    dict.set_item("max_cacheable_yaml_bytes", cfg.max_cacheable_yaml_bytes)?;
    dict.set_item("shard_size", cfg.shard_size)?;
    dict.set_item("custom_shards", cfg.custom_shards)?;
    dict.set_item("sqlite_batch_size", cfg.sqlite_batch_size)?;
    dict.set_item("sqlite_cache_kb", cfg.sqlite_cache_kb)?;
    dict.set_item("sqlite_busy_timeout_ms", cfg.sqlite_busy_timeout_ms)?;
    dict.set_item("sqlite_commit_window_ms", cfg.sqlite_commit_window_ms)?;
    dict.set_item("sqlite_max_batch", cfg.sqlite_max_batch)?;
    dict.set_item("lock_timeout_seconds", cfg.lock_timeout_seconds)?;
    dict.set_item("max_inline_content_bytes", cfg.max_inline_content_bytes)?;
    Ok(dict.into_py(py))
}

#[pyfunction]
#[pyo3(signature = (text, strict=false))]
fn parse_gateway_line(
    py: Python<'_>,
    text: &str,
    strict: bool,
) -> PyResult<(String, String, PyObject, String)> {
    let (kind, target, vars, sha256) =
        data_gateway::parse_gateway_line(text, strict).map_err(PyValueError::new_err)?;
    Ok((kind, target, json_to_py(py, &vars)?, sha256))
}

#[pyfunction]
#[pyo3(signature = (kind, target, vars=None))]
fn format_gateway_line(
    py: Python<'_>,
    kind: &str,
    target: &str,
    vars: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let json_vars = match vars {
        Some(v) if !v.is_none() => py_to_json(py, v)?,
        _ => JsonValue::Object(serde_json::Map::new()),
    };
    data_gateway::format_gateway_line(kind, target, &json_vars).map_err(PyValueError::new_err)
}

#[pyfunction]
fn canonical_digest(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json_val = py_to_json(py, value)?;
    data_gateway::canonical_digest(&json_val).map_err(PyValueError::new_err)
}

#[pyfunction]
fn compute_event_hash(py: Python<'_>, event: &Bound<'_, PyAny>) -> PyResult<String> {
    let json_val = py_to_json(py, event)?;
    data_gateway::compute_event_hash(&json_val).map_err(PyValueError::new_err)
}

#[pyfunction]
fn discover_project_candidates_fast(base: &str) -> PyResult<Vec<String>> {
    fleet_ops::discover_project_candidates(Path::new(base)).map_err(PyRuntimeError::new_err)
}

#[pyfunction]
fn collect_sprint_done_fast(path: &str) -> Vec<String> {
    fleet_ops::collect_sprint_done_fast(Path::new(path))
}

#[pyfunction]
fn collect_dir_historical_done_fast(sprint_dir: &str) -> Vec<String> {
    fleet_ops::collect_dir_historical_done_fast(Path::new(sprint_dir))
}

#[pyfunction]
fn batch_inventory_sprints(py: Python<'_>, project_roots: Vec<String>) -> PyResult<PyObject> {
    let rows = py.allow_threads(|| fleet_ops::batch_inventory_sprints(&project_roots));
    let out = PyList::empty_bound(py);
    for row in &rows {
        out.append(json_to_py(py, row)?)?;
    }
    Ok(out.into_py(py))
}

#[pyfunction]
fn is_git_tracked_fast(project_root: &str, rel_path: &str) -> Option<bool> {
    fleet_ops::is_git_tracked_fast(Path::new(project_root), rel_path)
}

#[pyfunction]
fn collect_committed_ticket_ids_fast(py: Python<'_>, project_root: &str) -> Vec<String> {
    let root = Path::new(project_root).to_path_buf();
    py.allow_threads(move || fleet_ops::collect_committed_ticket_ids_fast(&root))
}

#[pymodule]
fn planfile_io_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_yaml_fast, m)?)?;
    m.add_function(wrap_pyfunction!(write_yaml_fast, m)?)?;
    m.add_function(wrap_pyfunction!(audit_mirror_fast, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_shard_path, m)?)?;
    m.add_function(wrap_pyfunction!(load_sharded_sprint, m)?)?;
    m.add_function(wrap_pyfunction!(compute_source_signature, m)?)?;
    m.add_function(wrap_pyfunction!(get_runtime_config, m)?)?;
    m.add_function(wrap_pyfunction!(parse_gateway_line, m)?)?;
    m.add_function(wrap_pyfunction!(format_gateway_line, m)?)?;
    m.add_function(wrap_pyfunction!(canonical_digest, m)?)?;
    m.add_function(wrap_pyfunction!(compute_event_hash, m)?)?;
    m.add_function(wrap_pyfunction!(discover_project_candidates_fast, m)?)?;
    m.add_function(wrap_pyfunction!(collect_sprint_done_fast, m)?)?;
    m.add_function(wrap_pyfunction!(collect_dir_historical_done_fast, m)?)?;
    m.add_function(wrap_pyfunction!(batch_inventory_sprints, m)?)?;
    m.add_function(wrap_pyfunction!(is_git_tracked_fast, m)?)?;
    m.add_function(wrap_pyfunction!(collect_committed_ticket_ids_fast, m)?)?;
    Ok(())
}
