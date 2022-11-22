import functools
import os
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

from webget.compress import unzip_bytes, zip_bytes


def get_cache_path(url, cache_dir=".", cache_subdir="web", suffix=None):
    path = urlparse(url).path
    if path.startswith("/"):
        path = path[1:]
    p = Path(cache_dir) / "cache" / cache_subdir / path
    if suffix is not None:
        p = p.with_suffix(suffix)
    p.parent.mkdir(exist_ok=True, parents=True)
    return p


def get_with_progress_bar_and_cache(url):
    dest_file = get_cache_path(url, suffix=".lz4")
    if dest_file.exists():
        return dest_file.read_bytes()

    response = requests.get(url, stream=True, allow_redirects=True)
    file_size = int(response.headers.get("Content-Length", 0))
    response.raw.read = functools.partial(response.raw.read, decode_content=True)
    with tqdm.wrapattr(
        response.raw, "read", total=file_size, desc=dest_file.stem, leave=False
    ) as r_raw:
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            shutil.copyfileobj(r_raw, tmp_file)

    # Attempt atomic rename.
    dest_file.parent.mkdir(exist_ok=True, parents=True)
    try:
        os.rename(tmp_file.name, dest_file)
    except OSError:
        os.replace(tmp_file.name, dest_file)

    return dest_file.read_bytes()
