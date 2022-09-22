# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Provide metrics based on the requests sent
"""

import pandas as pd

df_sends = pd.read_parquet('../submitter/sends.parquet', engine='fastparquet')
df_receives = pd.read_parquet('../submitter/receives.parquet', engine='fastparquet')

print(str(len(df_sends.index)), " requests sent")
SUCCESSFULL_REQUESTS = 0
TIME_SPENT_SUM = 0

# the number of rows in both dfs is the same
for i in range(len(df_sends.index)):
    assert df_sends.iloc[i]["messageID"] == df_receives.iloc[i]["messageID"], "the IDs do not match"
    req_resp = df_receives.iloc[i]["rawResponse"].split("$")
    if req_resp[2] == "OK":
        SUCCESSFULL_REQUESTS += 1
    TIME_SPENT_SUM += df_receives.iloc[i]["receiveTime"] - df_sends.iloc[i]["sendTime"]

print(round(TIME_SPENT_SUM, 1), "s total time for all requests")
print(round(TIME_SPENT_SUM / SUCCESSFULL_REQUESTS, 3), "s per request on average")
print(round(len(df_sends.index) /TIME_SPENT_SUM,1), "req/s")
