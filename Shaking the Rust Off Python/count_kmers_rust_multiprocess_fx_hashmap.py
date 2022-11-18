"""
python version of k-mer counting.
"""


import multiprocessing
import pysam
import time
import pandas as pd
import scripts
from functools import partial
from collections import Counter


def count_kmers_rust_multiprocess_fx_hashmap(fasta_file: str,
                                             num_processes: int,
                                             sequence_chunk: int,
                                             k: int,
                                             chromosomes: list) -> pd.DataFrame:
    """
    Counts all possible k-mers in a FASTA file.

    Parameters
    ----------
    fasta_file      :   FASTA file.
    num_processes   :   Number of processes.
    sequence_chunk  :   Sequence chunk.
    k               :   k-mer k.
    chromosomes     :   Chromosomes to count.

    Returns
    -------
    df_kmers        :   DataFrame of k-mers with the following columns:
                        'k_mer', 'count'
    duration_1      :   Duration of step 1.
    duration_2      :   Duration of step 2.
    duration_3      :   Duration of step 3.
    duration_total  :   Total duration.
    """
    # Step 1. Create tasks
    start_time_1 = time.time()
    fasta = pysam.FastaFile(fasta_file)
    sequences = []
    for curr_chromosome in chromosomes:
        curr_chromosome_len = fasta.get_reference_length(curr_chromosome)
        for i in range(0, curr_chromosome_len, sequence_chunk):
            curr_start = i
            curr_end = curr_start + sequence_chunk
            if curr_end > curr_chromosome_len:
                curr_end = curr_chromosome_len
            curr_chromosome_seq = fasta.fetch(
                reference=curr_chromosome,
                start=curr_start,
                end=curr_end
            )
            sequences.append(curr_chromosome_seq.upper())
    end_time_1 = time.time()

    # Step 2. Run
    start_time_2 = time.time()
    pool = multiprocessing.Pool(num_processes)
    results = pool.map(partial(scripts.count_kmers_fx_hashmap, k=k), sequences)
    end_time_2 = time.time()

    # Step 3. Merge dictionaries into one
    start_time_3 = time.time()
    outputs = Counter()
    for curr_result in results:
        for key, val in curr_result.items():
            outputs[key] += val
    df_kmers = pd.DataFrame.from_dict(outputs.items())
    df_kmers.columns = ['k_mer', 'count']
    df_kmers.sort_values(['count'], ascending=False, inplace=True)
    end_time_3 = time.time()
    return df_kmers, \
           end_time_1 - start_time_1, \
           end_time_2 - start_time_2, \
           end_time_3 - start_time_3, \
           end_time_3 - start_time_1


if __name__ == "__main__":
    print("Rust implementation of k-mer counting (multi-process and FX HashMap)")
    start_time = time.time()
    df_kmers, duration_1, duration_2, duration_3, duration_total = count_kmers_rust_multiprocess_fx_hashmap(
        fasta_file='hg38.fa',
        num_processes=6,
        sequence_chunk=1000000,
        k=9,
        chromosomes=['chr21']
    )
    print(df_kmers.head(n=5))
    print("Program took %f seconds in total" %
          (time.time() - start_time))
