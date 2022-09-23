# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Provide metrics based on the requests sent
"""

import argparse
import pandas as pd  # type: ignore

# pylint: disable=import-error
from prettytable import PrettyTable  # type: ignore
import numpy as np


def make_analysis(send_file, response_file):
    """
    Produce the analysis results
    """

    df_sends = pd.read_parquet(send_file, engine="fastparquet")
    df_receives = pd.read_parquet(response_file, engine="fastparquet")

    successful_reqs = 0
    time_spent_list = []
    time_spent_sum = 0

    # the number of rows in both dfs is the same
    for i in range(len(df_sends.index)):
        assert (
            df_sends.iloc[i]["messageID"] == df_receives.iloc[i]["messageID"]
        ), "the IDs do not match"
        req_resp = df_receives.iloc[i]["rawResponse"].split("$")
        if req_resp[2] == "OK":
            successful_reqs += 1
        time_spent_list.append(
            df_receives.iloc[i]["receiveTime"] - df_sends.iloc[i]["sendTime"]
        )

    successful_percent = successful_reqs / len(df_sends.index) * 100
    time_spent_sum = sum(time_spent_list)

    output_table = PrettyTable()
    output_table.field_names = [
        "Total Requests",
        "Total Time (s)",
        "Pass (%)",
        "Fail (%)",
        "Throughput (req/s)",
        "Latency (s)",
        "95th (s)",
    ]
    output_table.add_row(
        [
            len(df_sends.index),
            round(time_spent_sum, 1),
            round(successful_percent, 1),
            round(100 - successful_percent, 1),
            round(len(df_sends.index) / time_spent_sum, 1),
            round(time_spent_sum / len(df_sends.index), 3),
            round(np.percentile(time_spent_list, 95), 3),
        ]
    )
    print(output_table)


def main():
    """
    The function to receive the arguments
    from the command line
    """
    arg_send_file = "../submitter/sends.parquet"
    arg_response_file = "../submitter/receives.parquet"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-sf",
        "--send_file",
        help="Path to the parquet file that contains the submitted\
            requests. Default location `../submitter/sends.parquet`",
        type=str,
    )
    parser.add_argument(
        "-rf",
        "--response_file",
        help="Path to the parquet file that contains the responses\
            from the submitted requests. Default `../submitter/receives.parquet`",
        type=str,
    )

    args = parser.parse_args()
    make_analysis(
        args.send_file or arg_send_file, args.response_file or arg_response_file
    )


if __name__ == "__main__":
    main()
