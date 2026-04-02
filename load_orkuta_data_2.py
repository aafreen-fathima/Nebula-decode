#!/usr/bin/env python3
"""
Optimized Orkut Dataset Loader for NebulaGraph
Loads the com-orkut.ungraph.txt.gz dataset (~3M nodes, 117M+ edges)
"""

import gzip
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import time
import sys

# ------------------ Configuration ------------------ #

GRAPHD_HOST = "127.0.0.1"
GRAPHD_PORT = 9669
USERNAME = "root"
PASSWORD = "nebula"
SPACE_NAME = "orkut"

# Make sure this matches the actual file name in your folder
DATA_FILE = "com-orkut.ungraph.txt.gz"

BATCH_SIZE = 10_000  # batch size for vertex and edge inserts


# ------------------ Connection ------------------ #

def create_connection():
    config = Config()
    config.max_connection_pool_size = 20
    pool = ConnectionPool()
    ok = pool.init([(GRAPHD_HOST, GRAPHD_PORT)], config)
    if not ok:
        print("❌ Failed to connect to NebulaGraph")
        sys.exit(1)
    print(f"✓ Connected to NebulaGraph at {GRAPHD_HOST}:{GRAPHD_PORT}")
    return pool


# ------------------ Space + Schema ------------------ #

def initialize_space(session):
    print("Dropping existing space if it exists...", flush=True)
    session.execute(f"DROP SPACE IF EXISTS {SPACE_NAME};")
    time.sleep(10)

    print("Creating new space...", flush=True)
    # replica_factor must NOT exceed number of storaged nodes (you have 1)
    ngql = (
        f"CREATE SPACE IF NOT EXISTS {SPACE_NAME}"
        "(partition_num=20, replica_factor=1, vid_type=INT64);"
    )
    result = session.execute(ngql)
    if not result.is_succeeded():
        print(f"Error creating space: {result.error_msg()}", flush=True)
        sys.exit(1)

    print("Waiting for space to be ready...", flush=True)
    time.sleep(20)

    print(f"Switching to {SPACE_NAME} space...", flush=True)
    result = session.execute(f"USE {SPACE_NAME};")
    if not result.is_succeeded():
        print(f"Error using space: {result.error_msg()}", flush=True)
        sys.exit(1)

    print("Creating tag User...", flush=True)
    # user vertex with one property: id (INT64)
    result = session.execute("CREATE TAG IF NOT EXISTS User(id INT64);")
    if not result.is_succeeded():
        print(f"Error creating tag: {result.error_msg()}", flush=True)
        sys.exit(1)

    print("Creating edge FRIEND...", flush=True)
    # friend edge with one property: ts (timestamp)
    result = session.execute("CREATE EDGE IF NOT EXISTS FRIEND(ts INT64);")
    if not result.is_succeeded():
        print(f"Error creating edge: {result.error_msg()}", flush=True)
        sys.exit(1)

    print("Waiting for schema to propagate...", flush=True)
    time.sleep(30)

    # quick sanity check
    tags = session.execute("SHOW TAGS;")
    edges = session.execute("SHOW EDGES;")
    if tags.is_succeeded():
        print("✓ Tags:", tags.rows(), flush=True)
    if edges.is_succeeded():
        print("✓ Edges:", edges.rows(), flush=True)

    print("✓ Space, Tag, and Edge created and verified", flush=True)


# ------------------ Data Loading ------------------ #

def load_orkut_data(session, data_file):
    print(f"Ensuring we're using the {SPACE_NAME} space...", flush=True)
    result = session.execute(f"USE {SPACE_NAME};")
    if not result.is_succeeded():
        print(f"Error using space: {result.error_msg()}", flush=True)
        sys.exit(1)

    vertices = set()
    vertex_batch = []
    edge_batch = []

    total_edges = 0
    total_vertices = 0
    skipped_lines = 0
    start_time = time.time()

    print(f"Reading {data_file} ...", flush=True)
    with gzip.open(data_file, "rt") as f:
        for line_num, line in enumerate(f, 1):
            if line.startswith("#"):
                skipped_lines += 1
                continue

            parts = line.strip().split()
            if len(parts) < 2:
                skipped_lines += 1
                continue

            try:
                src = int(parts[0])
                dst = int(parts[1])
            except ValueError:
                skipped_lines += 1
                continue

            # collect unique vertices (INT64 IDs)
            if src not in vertices:
                vertices.add(src)
                vertex_batch.append(src)
            if dst not in vertices:
                vertices.add(dst)
                vertex_batch.append(dst)

            edge_batch.append((src, dst))

            # insert vertices batch
            if len(vertex_batch) >= BATCH_SIZE:
                values_list = [f"{v}:({v})" for v in vertex_batch]
                query = "INSERT VERTEX User(id) VALUES " + ", ".join(values_list) + ";"
                result = session.execute(query)
                if not result.is_succeeded():
                    print(f"⚠ Vertex insert warning at line {line_num}: {result.error_msg()}")
                total_vertices += len(vertex_batch)
                vertex_batch = []

            # insert edges batch
            if len(edge_batch) >= BATCH_SIZE:
                ts = int(time.time())
                values_list = [f"{s}->{d}:({ts})" for s, d in edge_batch]
                query = "INSERT EDGE FRIEND(ts) VALUES " + ", ".join(values_list) + ";"
                result = session.execute(query)
                if not result.is_succeeded():
                    print(f"⚠ Edge insert warning at line {line_num}: {result.error_msg()}")
                total_edges += len(edge_batch)
                edge_batch = []

                if total_edges % 100_000 == 0:
                    elapsed = time.time() - start_time
                    rate = total_edges / elapsed
                    # 117_185_083 is the full edges count; adjust if different
                    remaining = max(0, 117_185_083 - total_edges)
                    eta = remaining / rate if rate > 0 else 0
                    print(
                        f"Progress: {total_edges:,} edges, {total_vertices:,} vertices | "
                        f"Rate: {rate:.0f} edges/s | ETA: {eta/60:.1f} min",
                        flush=True,
                    )

    # remaining vertices
    if vertex_batch:
        values_list = [f"{v}:({v})" for v in vertex_batch]
        query = "INSERT VERTEX User(id) VALUES " + ", ".join(values_list) + ";"
        result = session.execute(query)
        if result.is_succeeded():
            total_vertices += len(vertex_batch)

    # remaining edges
    if edge_batch:
        ts = int(time.time())
        values_list = [f"{s}->{d}:({ts})" for s, d in edge_batch]
        query = "INSERT EDGE FRIEND(ts) VALUES " + ", ".join(values_list) + ";"
        result = session.execute(query)
        if result.is_succeeded():
            total_edges += len(edge_batch)

    elapsed = time.time() - start_time
    print(f"✓ Total vertices loaded: {total_vertices:,}", flush=True)
    print(f"✓ Total edges loaded: {total_edges:,}", flush=True)
    print(f"✓ Time elapsed: {elapsed/60:.1f} minutes", flush=True)
    print(f"✓ Skipped lines: {skipped_lines}", flush=True)


# ------------------ Main ------------------ #

def main():
    pool = create_connection()
    session = pool.get_session(USERNAME, PASSWORD)
    try:
        initialize_space(session)
        load_orkut_data(session, DATA_FILE)
    finally:
        session.release()
        pool.close()
    print("✓ Loading complete!")


if __name__ == "__main__":
    main()
