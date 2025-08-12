use std::ffi::CString;
use std::sync::LazyLock;

use anyhow::anyhow;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::context::Context;
use crate::error::script_excerpt;

static SERVER_PY: LazyLock<Py<PyDict>> = LazyLock::new(|| {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let sys = py.import("sys").unwrap();
        sys.setattr(
            "stdout",
            LoggingStdout { err: false }.into_pyobject(py).unwrap(),
        )
        .unwrap();
        sys.setattr(
            "stderr",
            LoggingStdout { err: true }.into_pyobject(py).unwrap(),
        )
        .unwrap();
        PyDict::new(py).unbind()
    })
});

#[pyclass]
struct LoggingStdout {
    err: bool,
}
#[pymethods]
impl LoggingStdout {
    fn write(&self, data: &str) {
        match self.err {
            true => eprint!("{data}",),
            false => print!("{data}"),
        }
    }
}

pub fn run_python(script_text: &str) -> anyhow::Result<()> {
    let cstr = CString::new(script_text)?;
    let locals = &*SERVER_PY;
    // ctx, include script text
    Python::with_gil(|py| py.run(&cstr, Some(locals.bind(py)), None))
        .map_err(|error| anyhow!("failed to run python line: {error}, {script_text}"))
}

pub fn condition_python(script_text: &str) -> anyhow::Result<bool> {
    let cstr = CString::new(script_text)?;
    let locals = &*SERVER_PY;
    // ctx, include script text
    Python::with_gil(|py| py.eval(&cstr, Some(locals.bind(py)), None)?.is_truthy())
        .map_err(|error: PyErr| anyhow!("failed to evaluate condition: {error}, {script_text}"))
}

pub fn contextualize_res<T>(
    res: anyhow::Result<T>,
    ctx: Context,
    script_name: &str,
    script: &str,
) -> anyhow::Result<T> {
    res.map_err(|e| {
        if ctx.start_line_number == ctx.end_line_number {
            anyhow::anyhow!(
                "Error executing Python code on line ({}):\n{}\n\n{e:#}",
                ctx.start_line_number,
                script_excerpt(script_name, script, ctx),
            )
        } else {
            anyhow::anyhow!(
                "Error executing Python code on lines ({}-{}):\n{}\n\n{e:#}",
                ctx.start_line_number,
                ctx.end_line_number,
                script_excerpt(script_name, script, ctx),
            )
        }
    })
}
