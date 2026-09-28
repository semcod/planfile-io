use pyo3::prelude::*;
use std::path::Path;

mod atomic;
mod shards;
mod yaml_io;

#[pyfunction]
fn read_yaml_fast(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let json_val = yaml_io::read_yaml_fast(Path::new(path))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;

    let json_str = serde_json::to_string(&json_val)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let json_mod = py.import_bound("json")?;
    let py_obj = json_mod.call_method1("loads", (json_str,))?;
    Ok(py_obj.unbind())
}

#[pyfunction]
fn write_yaml_fast(py: Python<'_>, path: &str, data: &Bound<'_, PyAny>) -> PyResult<()> {
    let json_mod = py.import_bound("json")?;
    let json_str: String = json_mod.call_method1("dumps", (data,))?.extract()?;

    let json_val: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let yaml_str = serde_yaml::to_string(&json_val)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let p = Path::new(path);
    atomic::atomic_write_str(p, &yaml_str)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

    let current_mtime = yaml_io::stat_mtime_ns(p).unwrap_or(0);
    let mirror_val = serde_json::json!({
        "version": 1,
        "mtime_ns": current_mtime as u64,
        "data": json_val
    });
    let mirror = yaml_io::mirror_path(p);
    let mirror_str = serde_json::to_string(&mirror_val)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let _ = atomic::atomic_write_str(&mirror, &mirror_str);

    Ok(())
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

#[pymodule]
fn planfile_io_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_yaml_fast, m)?)?;
    m.add_function(wrap_pyfunction!(write_yaml_fast, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_shard_path, m)?)?;
    Ok(())
}
