import io

import jsonlines
from cramjam import lz4, zstd


def zip_json(data, path) -> None:
    if path.suffixes != [".jsonl", ".zst"]:
        raise ValueError("Expected '.jsonl.zst' suffix")
    with io.BytesIO() as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(data)
        path.write_bytes(zstd.compress(f.getvalue()))


def unzip_json(path) -> list:
    if path.suffixes != [".jsonl", ".zst"]:
        raise ValueError("Expected '.jsonl.zst' suffix")
    with io.BytesIO(zstd.decompress(path.read_bytes())) as f:
        with jsonlines.Reader(f) as reader:
            return list(reader)


def zip_bytes(data, path) -> None:
    if path.suffix == ".lz4":
        path.write_bytes(lz4.compress(data))
    else:
        raise ValueError("Expected .lz4 suffix")


def unzip_bytes(path) -> bytes:
    if path.suffix == ".lz4":
        return lz4.decompress(path.read_bytes())
    raise ValueError("Expected .lz4 suffix")
