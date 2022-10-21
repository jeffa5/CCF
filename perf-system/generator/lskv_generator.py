# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.

import pandas as pd  # type: ignore
from generator import create_parquet, create_post
from loguru import logger


def make_requests(host="127.0.0.1:8000"):
    logger.info("get new dataframe")
    df = []

    # not used in submitter
    http = "HTTP/0"

    logger.info("add range requests")
    for _ in range(2000):
        path = "/v3/kv/range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    logger.info("add put requests")
    for _ in range(1000):
        path = "/v3/kv/put"
        data = '{"key":"aGVsbG8=","value":"d29ybGQ="}'
        create_post(df, host, path, http, data)

    logger.info("add range requests")
    for _ in range(10000):
        path = "/v3/kv/range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    logger.info("add delete requests")
    for _ in range(1000):
        path = "/v3/kv/delete_range"
        data = '{"key":"aGVsbG8="}'
        create_post(df, host, path, http, data)

    create_parquet(df, f"lskv.parquet")


make_requests()
