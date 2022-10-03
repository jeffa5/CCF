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
import matplotlib.pyplot as plt
import sys


def make_analysis(send_file, response_file):
    """
    Produce the analysis results
    """
    df_sends = pd.read_parquet(send_file, engine="fastparquet")
    df_responses = pd.read_parquet(response_file, engine="fastparquet")

    successful_reqs = 0
    time_spent_list = []
    ms_time_spent_sum = 0

    # the number of rows in both dfs is the same
    for i in range(len(df_sends.index)):
        assert (
            df_sends.iloc[i]["messageID"] == df_responses.iloc[i]["messageID"]
        ), "the IDs do not match"
        if len(df_responses.iloc[i]["rawResponse"]) < 1:
            print("There is not Response Content")
            sys.exit(2)
        req_resp = df_responses.iloc[i]["rawResponse"].split("\n")
        status_list = req_resp[0].split(" ")
        req_type_received = status_list[0]
        if status_list[2][:2] == "OK" or status_list[1][:3] == "200":
            successful_reqs += 1
        time_spent_list.append(
            df_responses.iloc[i]["receiveTime"] - df_sends.iloc[i]["sendTime"]
        )

    successful_percent = successful_reqs / len(df_sends.index) * 100
    ms_time_spent_list = [x * 1000 for x in time_spent_list]
    ms_time_spent_sum = sum(ms_time_spent_list)

    # FOR MULTIPLEXING IS DIFFERENT TIME_SPENT
    ms_time_spent_sum = ms_time_spent_list[-1]

    generic_output_table = PrettyTable()
    generic_output_table.field_names = [
        "Total Requests",
        "Total Time (s)",
        "Pass (%)",
        "Fail (%)",
        "Throughput (req/s)",
    ]
    generic_output_table.add_row(
        [
            len(df_sends.index),
            round(ms_time_spent_sum / 1000, 1),
            round(successful_percent, 1),
            round(100 - successful_percent, 1),
            round(len(df_sends.index) / ms_time_spent_sum * 1000, 1),
        ]
    )
    latency_output_table = PrettyTable()
    latency_output_table.field_names = [
        "Latency (ms)",
        "Average Latency (ms)",
        "Latency 80th (ms)",
        "Latency 90th (ms)",
        "Latency 95th (ms)",
        "Latency 99th (ms)",
        "Latency 99.9th (ms)",
    ]
    latency_output_table.add_row(
        [
            round(np.percentile(ms_time_spent_list, 50), 3),
            round(ms_time_spent_sum / len(df_sends.index), 3),
            round(np.percentile(ms_time_spent_list, 80), 3),
            round(np.percentile(ms_time_spent_list, 90), 3),
            round(np.percentile(ms_time_spent_list, 95), 3),
            round(np.percentile(ms_time_spent_list, 99), 3),
            round(np.percentile(ms_time_spent_list, 99.9), 3),
        ]
    )

    print("The request type sent is ", req_type_received)

    time_unit = [x - df_sends["sendTime"][0] + 1 for x in df_sends["sendTime"]]
    print(generic_output_table)
    print(latency_output_table)
    plt.scatter(time_unit, ms_time_spent_list, s=1)
    plt.ylabel("Latency_ms")
    plt.savefig("latency.png")


def main():
    """
    The function to receive the arguments
    from the command line
    """
    arg_send_file = "../submitter/cpp_sends.parquet"
    arg_response_file = "../submitter/cpp_responses.parquet"

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
