use pyo3::prelude::*;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

#[pyfunction]
fn count_kmers_multithread_stl_hashmap(sequences: Vec<String>, k: usize) -> Py<PyAny> {
    let hm = sequences
        .par_iter()
        .map(|sequence| {
            let mut map: HashMap<_, u32> = HashMap::default();
            for i in 0..sequence.len() - k + 1 {
                *map.entry(&sequence[i..i + k]).or_insert(0) += 1;
            }
            map
        })
        .reduce(
            || HashMap::default(),
            |mut acc, x| {
                for (k, v) in x {
                    *acc.entry(k).or_insert(0) += v;
                }
                acc
            },
        );

    Python::with_gil(|py| hm.to_object(py))
}

#[pyfunction]
fn count_kmers_multithread_fx_hashmap(sequences: Vec<String>, k: usize) -> Py<PyAny> {
    let hm = sequences
        .par_iter()
        .map(|sequence| {
            let mut map: FxHashMap<_, u32> = FxHashMap::default();
            for i in 0..sequence.len() - k + 1 {
                *map.entry(&sequence[i..i + k]).or_insert(0) += 1;
            }
            map
        })
        .reduce(
            || FxHashMap::default(),
            |mut acc, x| {
                for (k, v) in x {
                    *acc.entry(k).or_insert(0) += v;
                }
                acc
            },
        );

    Python::with_gil(|py| hm.to_object(py))
}

#[pyfunction]
fn count_kmers_stl_hashmap(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm: HashMap<String, i32> = HashMap::new();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(sequence[j..j + k].to_string()).or_insert(0) += 1;
    }
    return Python::with_gil(|py| hm.to_object(py));
}

#[pyfunction]
fn count_kmers_stl_hashmap_pointer(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm: HashMap<&str, i32> = HashMap::new();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(&sequence[j..j + k]).or_insert(0) += 1;
    }
    return Python::with_gil(|py| hm.to_object(py));
}

#[pyfunction]
fn count_kmers_fx_hashmap(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm: FxHashMap<String, i32> = FxHashMap::default();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(sequence[j..j + k].to_string()).or_insert(0) += 1;
    }
    return Python::with_gil(|py| hm.to_object(py));
}

#[pyfunction]
fn count_kmers_fx_hashmap_pointer(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm: FxHashMap<&str, i32> = FxHashMap::default();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(&sequence[j..j + k]).or_insert(0) += 1;
    }
    return Python::with_gil(|py| hm.to_object(py));
}

#[pymodule]
fn scripts(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(count_kmers_multithread_stl_hashmap, m)?)?;
    m.add_function(wrap_pyfunction!(count_kmers_multithread_fx_hashmap, m)?)?;
    m.add_function(wrap_pyfunction!(count_kmers_stl_hashmap, m)?)?;
    m.add_function(wrap_pyfunction!(count_kmers_stl_hashmap_pointer, m)?)?;
    m.add_function(wrap_pyfunction!(count_kmers_fx_hashmap, m)?)?;
    m.add_function(wrap_pyfunction!(count_kmers_fx_hashmap_pointer, m)?)?;
    Ok(())
}
