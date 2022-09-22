import pandas as pd

df_sends = pd.read_parquet('../Submitter/sends.parquet', engine='fastparquet')
df_receives = pd.read_parquet('../Submitter/receives.parquet', engine='fastparquet')

print(str(len(df_sends.index)), " requests sent")
successfull_reqs = 0
time_spent_sum = 0

# the number of rows in both dfs is the same
for i in range(len(df_sends.index)):
    assert df_sends.iloc[i]["messageID"] == df_receives.iloc[i]["messageID"], "the IDs do not match"
    req_resp = df_receives.iloc[i]["rawResponse"].split("$")
    if req_resp[2] == "OK":
        successfull_reqs += 1
    time_spent_sum += df_receives.iloc[i]["receiveTime"] - df_sends.iloc[i]["sendTime"]
    
    
print (round(time_spent_sum,1), "s total time for all requests")
print(round(time_spent_sum/successfull_reqs,3), "s per request on average")
print(round(len(df_sends.index) /time_spent_sum,1), "req/s")
