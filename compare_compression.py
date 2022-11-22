import subprocess
from collections import Counter
from functools import wraps
from pathlib import Path
from time import perf_counter

from fastavro import reader, writer, schema
import pandas as pd
from humanize import naturaldelta, naturalsize
from rec_avro import from_rec_avro_destructive, rec_avro_schema, to_rec_avro_destructive


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        print(f"{func.__name__} finished in {naturaldelta(end - start)}")
        return result

    return wrapper


json_source = Path("cache/metadata") / "1m.jl"
zstd_parquet = json_source.with_suffix(".zstd.parquet")


@timeit
def df_from_json(json_source):
    df = pd.read_json(json_source, lines=True)
    for col in df.columns:
        df[col] = df[col].astype("string")
    df.set_index("url", drop=True, inplace=True)
    df["offset"] = df["offset"].astype(int)
    df["length"] = df["length"].astype(int)
    df["mime"] = df["mime"].astype("category")
    df["mime-detected"] = df["mime-detected"].astype("category")
    df["status"] = df["status"].astype(int).astype("category")
    df["charset"] = df["charset"].astype("category")
    df["languages"] = df["languages"].astype("category")
    df["truncated"] = df["truncated"].astype("category")

    return df


@timeit
def df_to_parquet(df, path, compression="snappy"):
    df.to_parquet(path, compression=compression)


@timeit
def df_from_parquet(path):
    return pd.read_parquet(path)


@timeit
def df_to_avro(df, path):
    avro_objects = (to_rec_avro_destructive(rec) for rec in df.to_dict("records"))
    with open(path, "wb") as out:
        writer(out, schema.parse_schema(rec_avro_schema()), avro_objects)


@timeit
def df_from_avro(path):
    with open(path, "rb") as f:
        return pd.DataFrame.from_records(
            from_rec_avro_destructive(rec) for rec in reader(f)
        )


def main():
    if zstd_parquet.exists():
        df = df_from_parquet(zstd_parquet)
    else:
        df = df_from_json(json_source)
        df_to_parquet(df, zstd_parquet, compression="zstd")

    avro_path = json_source.with_suffix(".avro")
    df_to_avro(df, avro_path)
    df_from_avro(avro_path)


if __name__ == "__main__":
    main()
