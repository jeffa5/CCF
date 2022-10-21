# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.

import pandas as pd  # type: ignore
from generator import create_parquet, create_post


def make_requests(http_ver: int, host="127.0.0.1:8000"):
    df = pd.DataFrame(columns=["messageID", "request"])

    if http_ver == 1:
        http = "HTTP/1.1"
    else:
        http = "HTTP/2"

    for _ in range(10):
        path = "/v3/kv/range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    for _ in range(2000):
        path = "/v3/kv/range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    for _ in range(1000):
        path = "/v3/kv/put"
        data = '{"key":"aGVsbG8=","value":"d29ybGQ="}'
        create_post(df, host, path, http, data)

    for _ in range(10000):
        path = "/v3/kv/range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    for _ in range(1000):
        path = "/v3/kv/delete_range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    create_parquet(df, f"lskv-{http_ver}.parquet")


make_requests(1)
make_requests(2)
