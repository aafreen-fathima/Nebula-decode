#!/usr/bin/env python3
"""
Add Reverse Edges to Existing NebulaGraph Orkut Dataset
For each existing edge A->B, adds the reverse edge B->A
to create a bidirectional undirected graph
"""

import gzip
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import time
import sys

# Configuration
GRAPHD_HOST = '127.0.0.1'
GRAPHD_PORT = 9669
USERNAME = 'root'
PASSWORD = 'nebula'
SPACE_NAME = 'orkut'

# Dataset info
DATA_FILE = 'com-orkut.ungraph.txt.gz'
BATCH_SIZE = 10000  # large batch for faster insertion

def create_connection():
    config = Config()
    config.max_connection_pool_size = 20
    pool = ConnectionPool()
    pool.init([(GRAPHD_HOST, GRAPHD_PORT)], config)
    print(f"✓ Connected to NebulaGraph at {GRAPHD_HOST}:{GRAPHD_PORT}")
    return pool

def add_reverse_edges(session, data_file):
    print("Using the orkut space...", flush=True)
    result = session.execute(f'USE {SPACE_NAME};')
    if not result.is_succeeded():
        print(f"Error using space: {result.error_msg()}", flush=True)
        sys.exit(1)
    
    edge_batch = []
    total_edges = 0
    skipped_lines = 0
    start_time = time.time()

    print("Reading data file and adding reverse edges...", flush=True)
    
    with gzip.open(data_file, 'rt') as f:
        for line_num, line in enumerate(f, 1):
            if line.startswith('#'):
                skipped_lines += 1
                continue
            parts = line.strip().split()
            if len(parts) < 2:
                skipped_lines += 1
                continue
            
            src, dst = parts[0], parts[1]
            
            # Add the REVERSE edge (dst -> src) to the batch
            edge_batch.append((dst, src))

            # Insert edges when batch is full
            if len(edge_batch) >= BATCH_SIZE:
                ts = int(time.time())
                values_list = [f"{s}->{d}:({ts})" for s, d in edge_batch]
                query = "INSERT EDGE FRIEND(ts) VALUES " + ", ".join(values_list) + ";"
                result = session.execute(query)
                if not result.is_succeeded():
                    print(f"⚠ Warning inserting edges at line {line_num}: {result.error_msg()}")
                total_edges += len(edge_batch)
                edge_batch = []

                if total_edges % 100000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_edges / elapsed
                    remaining = 117185083 - total_edges
                    eta = remaining / rate if rate > 0 else 0
                    print(f"Progress: {total_edges:,} reverse edges added | Rate: {rate:.0f} edges/sec | ETA: {eta/60:.1f} min", flush=True)

        # Insert remaining edges
        if edge_batch:
            ts = int(time.time())
            values_list = [f"{s}->{d}:({ts})" for s, d in edge_batch]
            query = "INSERT EDGE FRIEND(ts) VALUES " + ", ".join(values_list) + ";"
            result = session.execute(query)
            if result.is_succeeded():
                total_edges += len(edge_batch)

    elapsed = time.time() - start_time
    print(f"✓ Total reverse edges added: {total_edges:,}", flush=True)
    print(f"✓ Time elapsed: {elapsed/60:.1f} minutes", flush=True)
    print(f"✓ Skipped lines: {skipped_lines}", flush=True)
    print(f"✓ Your graph is now bidirectional!", flush=True)

def main():
    pool = create_connection()
    session = pool.get_session(USERNAME, PASSWORD)
    add_reverse_edges(session, DATA_FILE)
    session.release()
    pool.close()
    print("✓ Reverse edge insertion complete!")

if __name__ == "__main__":
    main()
