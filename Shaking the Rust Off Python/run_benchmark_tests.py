"""
The purpose of this python3 script is to benchmark performance of python
and rust implementations of k-mer counting.
"""


import time
import pandas as pd
from count_kmers_python_multiprocess import count_kmers_python_multiprocess
from count_kmers_rust_multithread_stl_hashmap import count_kmers_rust_multithread_stl_hashmap
from count_kmers_rust_multithread_fx_hashmap import count_kmers_rust_multithread_fx_hashmap
from count_kmers_rust_multiprocess_fx_hashmap import count_kmers_rust_multiprocess_fx_hashmap
from count_kmers_rust_multiprocess_fx_hashmap_pointer import count_kmers_rust_multiprocess_fx_hashmap_pointer
from count_kmers_rust_multiprocess_stl_hashmap import count_kmers_rust_multiprocess_stl_hashmap
from count_kmers_rust_multiprocess_stl_hashmap_pointer import count_kmers_rust_multiprocess_stl_hashmap_pointer


FASTA_FILE = "<path>/hg38.fa"
OUTPUT_DIR = "<path>"
NUM_PROCESSES = 24
SEQUENCE_CHUNK = 1000000
K = 9
CHROMOSOMES = ['chr1']
ITERATIONS_PER_IMPLEMENTATION = 20


if __name__ == "__main__":
    data_execution_times = {
        'implementation': [],
        'total_duration': [],
        'step_1_duration': [],
        'step_2_duration': [],
        'step_3_duration': []
    }

    # 1. Python implementation
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_python_multiprocess(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('python3')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 2. Rust implementation (multi-threaded rust; STL HashMap)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multithread_stl_hashmap(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multithreaded_stl_hashmap')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 3. Rust implementation (multi-threaded rust; FX HashMap)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multithread_fx_hashmap(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multithreaded_fx_hashmap')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 4. Rust implementation (multi-process rust; FX HashMap)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multiprocess_fx_hashmap(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multiprocessed_fx_hashmap')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 5. Rust implementation (multi-process rust; FX HashMap pointer)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multiprocess_fx_hashmap_pointer(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multiprocessed_fx_hashmap_pointer')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 6. Rust implementation (multi-process rust; STL HashMap)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multiprocess_stl_hashmap(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multiprocessed_stl_hashmap')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    # 7. Rust implementation (multi-process rust; STL HashMap pointer)
    for i in range(0, ITERATIONS_PER_IMPLEMENTATION):
        df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multiprocess_stl_hashmap_pointer(
            fasta_file=FASTA_FILE,
            num_processes=NUM_PROCESSES,
            sequence_chunk=SEQUENCE_CHUNK,
            k=K,
            chromosomes=CHROMOSOMES
        )
        data_execution_times['implementation'].append('rust_multiprocessed_stl_hashmap_pointer')
        data_execution_times['total_duration'].append(duration_total)
        data_execution_times['step_1_duration'].append(duration_1)
        data_execution_times['step_2_duration'].append(duration_2)
        data_execution_times['step_3_duration'].append(duration_3)

    df_execution_times = pd.DataFrame(data_execution_times)
    df_execution_times.to_csv(OUTPUT_DIR + "/hg38_chr1_9-mers.tsv",
                              sep='\t', index=False)
