# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Provide metrics based on the requests sent
"""

import pandas as pd  # type: ignore
from prettytable import PrettyTable

df_sends = pd.read_parquet("../submitter/sends.parquet", engine="fastparquet")
df_receives = pd.read_parquet("../submitter/receives.parquet", engine="fastparquet")

SUCCESSFULL_REQUESTS = 0
TIME_SPENT_SUM = 0

# the number of rows in both dfs is the same
for i in range(len(df_sends.index)):
    assert (
        df_sends.iloc[i]["messageID"] == df_receives.iloc[i]["messageID"]
    ), "the IDs do not match"
    req_resp = df_receives.iloc[i]["rawResponse"].split("$")
    if req_resp[2] == "OK":
        SUCCESSFULL_REQUESTS += 1
    TIME_SPENT_SUM += df_receives.iloc[i]["receiveTime"] - df_sends.iloc[i]["sendTime"]

PERCENTAGE_OF_SUCCESSFULL = SUCCESSFULL_REQUESTS / len(df_sends.index) * 100

output_table = PrettyTable()
output_table.field_names = [
    "Total Requests",
    "Total Time (s)",
    "Pass (%)",
    "Fail (%)",
    "Throughput (req/s)",
    "Avg (s/req)",
]
output_table.add_row(
    [
        len(df_sends.index),
        round(TIME_SPENT_SUM, 1),
        round(PERCENTAGE_OF_SUCCESSFULL, 1),
        round(100 - PERCENTAGE_OF_SUCCESSFULL, 1),
        round(len(df_sends.index) / TIME_SPENT_SUM, 1),
        round(TIME_SPENT_SUM / len(df_sends.index), 3),
    ]
)
print(output_table)
