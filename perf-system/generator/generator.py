# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Generate requests
"""

import pandas as pd  # type: ignore
from typing import List
from loguru import logger

# pylint: disable=import-error
import fastparquet as fp  # type: ignore

REQUEST_CONTENT_TYPE = "content-type: application/json"
REQUEST_LENGTH_TEXT = "content-length:"


def fill_df(df: List[List[str]], host, req_path, req_type, req_verb, req_iters, data):
    """
    Creates a dataframe with the data
    required for the requests
    """
    # entering the private file paths as metadata in the start of parquet

    logger.info("Starting generation of requests")
    for _ in range(req_iters):
        if req_verb == "POST":
            create_post(df, host, req_path, req_type, data)

        elif req_verb == "GET":
            create_get(df, host, req_path, req_type)

        elif req_verb == "DELETE":
            create_delete(df, host, req_path, req_type)

    logger.info("Finished generation of requests")


def create_get(df: List[List[str]], host, req_path, req_type):
    """
    Generate get queries
    """
    ind = len(df)
    df.append(
        [
            str(ind),
            "GET "
            + req_path
            + " "
            + req_type
            + "\r\n"
            + REQUEST_CONTENT_TYPE
            + "\r\n"
            + "host: "
            + host
            + "\r\n\r\n",
        ]
    )


def create_post(df: List[List[str]], host, req_path, req_type, request_message):
    """
    Generate post queries
    """
    ind = len(df)
    df.append(
        [
            str(ind),
            "POST "
            + req_path
            + " "
            + req_type
            + "\r\n"
            + REQUEST_LENGTH_TEXT
            + str(len(request_message))
            + "\r\n"
            + REQUEST_CONTENT_TYPE
            + "\r\n"
            + "host: "
            + host
            + "\r\n\r\n"
            + request_message,
        ]
    )


def create_delete(df: List[List[str]], host, req_path, req_type):
    """
    Generate delete queries
    """
    ind = len(df)
    df.append(
        [
            str(ind),
            "DELETE"
            + "$"
            + host
            + "$"
            + req_path
            + "$"
            + req_type
            + "$"
            + REQUEST_CONTENT_TYPE,
        ]
    )


def create_parquet(data: List[List[str]], parquet_filename: str):
    """
    Takes the dataframe data and stores them
    in a parquet file in the current directory
    """
    logger.info("Start writing requests to " + parquet_filename)
    df = pd.DataFrame(data, columns=["messageID", "request"])
    fp.write(parquet_filename, df)
    logger.info("Finished writing requests to " + parquet_filename)
