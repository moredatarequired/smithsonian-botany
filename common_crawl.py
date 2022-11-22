import json
import re
from pathlib import Path
from urllib.parse import urlparse

import tqdm
from cramjam import gzip

from webget.compress import unzip_json, zip_json
from webget.download import get_with_progress_bar_and_cache

base_url = "https://data.commoncrawl.org/"
common_crawl_index = base_url + "crawl-data/CC-MAIN-2022-40/cc-index.paths.gz"


json_body = re.compile(r"\{.*\}")


def get_zipped_list(url):
    data = get_with_progress_bar_and_cache(url)
    return bytes(gzip.decompress(data)).decode("utf-8").splitlines()


def site_data(url):
    assert url.endswith(".gz")
    base_name = Path(urlparse(url).path).stem
    cache_base = Path(".") / "cache" / "common_crawl" / base_name
    cache_path = cache_base.with_suffix(".jsonl.zstd")
    if cache_path.exists():
        return unzip_json(cache_path)
    cache_path.parent.mkdir(exist_ok=True, parents=True)

    entries = []
    data = get_zipped_list(url)
    for line in tqdm.tqdm(data, desc=f"extracting json from {base_name}", leave=False):
        try:
            parts = line.split(" ", maxsplit=2)
            entries.append(json.loads(parts[2]))
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse {line}") from e

    zip_json(entries, cache_path)
    return entries


def main():
    paths = get_zipped_list(common_crawl_index)
    subindexes = [base_url + p for p in paths if p.endswith(".gz")]
    entries = []
    for index_part in tqdm.tqdm(subindexes, desc="Top level indexes"):
        entries.extend(site_data(index_part))
        break

    json_path = Path(".") / "cache/metadata/json"
    for entry in tqdm.tqdm(entries, "writing individual json files"):
        try:
            digest = entry["digest"]
        except KeyError:
            continue
        file_path = json_path / digest[:2] / digest[2:4] / digest[4:]
        if file_path.exists():
            continue
        file_path.parent.mkdir(exist_ok=True, parents=True)
        file_path.with_suffix(".json").write_text(json.dumps(entry, indent=2))


if __name__ == "__main__":
    main()
