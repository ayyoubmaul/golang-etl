import asyncio
import aiomysql
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor

# Database connection configs
shards = {
    "db_1": {
        "host": "127.0.0.1",
        "port": 3307,
        "user": "root",
        "password": "root",
        "db": "jerry",
    },
    "db_2": {
        "host": "127.0.0.1",
        "port": 3308,
        "user": "root",
        "password": "root",
        "db": "pikachu",
    }
}

OUTPUT_DIR = "output_py"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Semaphore to limit concurrency
semaphore = asyncio.Semaphore(10)

# Simple ThreadPool for heavy work
executor = ThreadPoolExecutor(max_workers=5)

async def fetch_data_range(pool, table, primary_key, start_key, end_key):
    async with semaphore:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                sql = f"SELECT * FROM {table} WHERE {primary_key} >= %s AND {primary_key} < %s"
                await cur.execute(sql, (start_key, end_key))
                result = await cur.fetchall()
                return result

def write_parquet_sync(batch, output_path):
    # Running inside ThreadPoolExecutor
    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path, compression="snappy")

async def fetch_and_write(pool, table, primary_key, start_key, end_key, output_prefix):
    print(f"Fetching {start_key} to {end_key}")
    batch = await fetch_data_range(pool, table, primary_key, start_key, end_key)

    if not batch:
        print(f"No data from {start_key} to {end_key}")
        return

    output_file = os.path.join(OUTPUT_DIR, f"{output_prefix}_{start_key}_{end_key}.parquet")

    # Run the heavy file write inside executor
    await asyncio.get_event_loop().run_in_executor(executor, write_parquet_sync, batch, output_file)
    print(f"Written {output_file}")

async def get_primary_key_range(pool, table, primary_key):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = f"SELECT MIN({primary_key}), MAX({primary_key}) FROM {table}"
            await cur.execute(sql)
            min_id, max_id = await cur.fetchone()
            return min_id, max_id

async def process_table(shard_name, conn_cfg, table, primary_key):
    pool = await aiomysql.create_pool(**conn_cfg, minsize=1, maxsize=5)

    min_key, max_key = await get_primary_key_range(pool, table, primary_key)
    print(f"{shard_name}: key range {min_key} to {max_key}")

    tasks = []
    key_range_size = 10000
    start = min_key
    while start <= max_key:
        end = start + key_range_size
        tasks.append(fetch_and_write(pool, table, primary_key, start, end, shard_name + "_" + table))
        start = end

    await asyncio.gather(*tasks)
    pool.close()
    await pool.wait_closed()

async def main():
    start_time = time.time()

    tasks = []
    tables = [
        {"table": "big_table_1", "primary_key": "id"},
    ]

    for shard_name, conn_cfg in shards.items():
        for table_info in tables:
            tasks.append(process_table(shard_name, conn_cfg, table_info["table"], table_info["primary_key"]))

    await asyncio.gather(*tasks)

    print(f"All done in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
