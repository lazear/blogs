use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use rustc_hash::FxHashMap;

#[pyfunction]
fn count_kmers_multithread_stl_hashmap(sequences: Vec<String>, k: usize, num_threads: usize) -> Py<PyAny> {
    let pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads)
    .build()
    .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for seq in &sequences {
        let tx = tx.clone();
        let seq = seq.to_owned();
        pool.spawn(move || {
            let mut hm : HashMap<String, i32> = HashMap::new();
            let end = seq.chars().count() - k + 1;
            for i in 0..end {
                *hm.entry(seq[i..i+k].to_owned()).or_insert(0) += 1;
            }
            tx.send(hm).unwrap();
        });
    }

    drop(tx); // close all senders
    let results: Vec<HashMap<String, i32>> = rx.into_iter().collect();

    // Merge HashMaps
    let mut hm : HashMap<&str, i32> = HashMap::new();
    for curr_hm in &results {
        for (key, value) in curr_hm.into_iter() {
            *hm.entry(&key).or_insert(0) += value;
        }
    }

    return Python::with_gil(|py| {
        hm.to_object(py)
    });
}

#[pyfunction]
fn count_kmers_multithread_fx_hashmap(sequences: Vec<String>, k: usize, num_threads: usize) -> Py<PyAny> {
    let pool = rayon::ThreadPoolBuilder::new()
    .num_threads(num_threads)
    .build()
    .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    for seq in &sequences {
        let tx = tx.clone();
        let seq = seq.to_owned();
        pool.spawn(move || {
            let mut hm : FxHashMap<String, i32> = FxHashMap::default();
            let end = seq.chars().count() - k + 1;
            for i in 0..end {
                *hm.entry(seq[i..i+k].to_owned()).or_insert(0) += 1;
            }
            tx.send(hm).unwrap();
        });
    }

    drop(tx); // close all senders
    let results: Vec<FxHashMap<String, i32>> = rx.into_iter().collect();

    // Merge HashMaps
    let mut hm : FxHashMap<&str, i32> = FxHashMap::default();
    for curr_hm in &results {
        for (key, value) in curr_hm.into_iter() {
            *hm.entry(&key).or_insert(0) += value;
        }
    }

    return Python::with_gil(|py| {
        hm.to_object(py)
    });
}

#[pyfunction]
fn count_kmers_stl_hashmap(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm : HashMap<String, i32> = HashMap::new();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(sequence[j..j+k].to_string()).or_insert(0) += 1;
    }
    return Python::with_gil(|py| {
        hm.to_object(py)
    });
}

#[pyfunction]
fn count_kmers_stl_hashmap_pointer(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm : HashMap<&str, i32> = HashMap::new();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(&sequence[j..j+k]).or_insert(0) += 1;
    }
    return Python::with_gil(|py| {
        hm.to_object(py)
    });
}

#[pyfunction]
fn count_kmers_fx_hashmap(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm : FxHashMap<String, i32> = FxHashMap::default();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(sequence[j..j+k].to_string()).or_insert(0) += 1;
    }
    return Python::with_gil(|py| {
        hm.to_object(py)
    });
}

#[pyfunction]
fn count_kmers_fx_hashmap_pointer(sequence: String, k: usize) -> Py<PyAny> {
    let mut hm : FxHashMap<&str, i32> = FxHashMap::default();
    let end = sequence.chars().count() - k + 1;
    for j in 0..end {
        *hm.entry(&sequence[j..j+k]).or_insert(0) += 1;
    }
    return Python::with_gil(|py| {
        hm.to_object(py)
    });
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