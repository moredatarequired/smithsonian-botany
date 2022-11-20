import functools
import json
import shutil
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from tqdm import tqdm

# See https://registry.opendata.aws/smithsonian-open-access/
root_url = "https://smithsonian-open-access.s3-us-west-2.amazonaws.com/metadata/edan/"
head_index = root_url + "index.txt"


def get_with_progress_bar_and_cache(url):
    assert url.startswith(root_url)
    path = url[len(root_url) :]
    dest_file = Path(".") / "cache" / "web" / path

    response = requests.get(url, stream=True, allow_redirects=True)
    file_size = int(response.headers.get("Content-Length", 0))

    if dest_file.exists() and dest_file.stat().st_size == file_size:
        return dest_file.read_text()

    desc = urlparse(url).path.split("/")[-1]
    dest_file.parent.mkdir(exist_ok=True, parents=True)
    response.raw.read = functools.partial(response.raw.read, decode_content=True)

    with tqdm.wrapattr(
        response.raw, "read", total=file_size, desc=desc, leave=False
    ) as r_raw:
        with dest_file.open("wb") as f:
            shutil.copyfileobj(r_raw, f)

    return dest_file.read_text()


def get_values(index_url):
    return get_with_progress_bar_and_cache(index_url).splitlines()


def dataframe_from_json_url(url):
    tag = Path(urlparse(url).path).stem
    df_cache = Path(".") / "cache" / "dataframes" / f"{tag}.parquet"
    if not df_cache.exists():
        df_cache.parent.mkdir(exist_ok=True, parents=True)
        json_lines = get_values(url)
        df = pd.DataFrame.from_dict([json.loads(j) for j in json_lines])
        df.to_parquet(df_cache)
    return pd.read_parquet(df_cache)


def main():
    top_level_index = get_values(head_index)
    botany = [i for i in top_level_index if "botany" in i][0]

    botany_index = get_values(botany)
    botany_metadata = []
    for subindex in tqdm(botany_index):
        botany_metadata.append(dataframe_from_json_url(subindex))

    df = pd.concat(botany_metadata)
    print(df.describe())


if __name__ == "__main__":
    main()
