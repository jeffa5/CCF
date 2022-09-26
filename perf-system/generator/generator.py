# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Generate requests
"""

import pandas as pd  # type: ignore

# pylint: disable=import-error
import fastparquet as fp  # type: ignore

REQUEST_CONTENT_TYPE = "Content-Type: application/json"
REQUEST_LENGTH_TEXT = "Content-Length: "

df = pd.DataFrame(columns=["messageID", "request"])


def fill_df(host, req_path, req_type, req_verb, req_iters, data):
    """
    Creates a dataframe with the data
    required for the requests
    """
    # entering the private file paths as metadata in the start of parquet

    print("Starting generation of requests")
    for request in range(req_iters):
        if req_verb == "POST":
            create_post(host, req_path, req_type, data)

        elif req_verb == "GET":
            create_get(host, req_path, req_type)

        elif req_verb == "DELETE":
            create_get(host, req_path, req_type)

    print("Finished generation of requests")


def create_get(host, req_path, req_type):
    """
    Generate get queries
    """
    ind = len(df.index)
    df.loc[ind] = [
        str(ind),
        "GET"
        + "$"
        + host
        + "$"
        + req_path
        + "$"
        + req_type
        + "$"
        + REQUEST_CONTENT_TYPE,
    ]


def create_post(host, req_path, req_type, request_message):
    """
    Generate post queries
    """
    ind = len(df.index)
    df.loc[ind] = [
        str(ind),
        "POST"
        + "$"
        + host
        + "$"
        + req_path
        + "$"
        + req_type
        + "$"
        + REQUEST_CONTENT_TYPE
        + "$"
        + REQUEST_LENGTH_TEXT
        + str(len(request_message))
        + "$"
        + request_message,
    ]


def create_delete(host, req_path, req_type):
    """
    Generate delete queries
    """
    ind = len(df.index)
    df.loc[ind] = [
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


def create_parquet(parquet_filename):
    """
    Takes the dataframe data and stores them
    in a parquet file in the current directory
    """
    print("Start writing requests to " + parquet_filename)
    fp.write(parquet_filename, df)
    print("Finished writing requests to " + parquet_filename)
