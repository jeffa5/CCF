#WARNING#
"""
With cp-ing workspace in CCF build folder you should be able to run submitter here
"""

import re
import aiohttp
import asyncio
import ssl
import pandas as pd
import fastparquet as fp
import sys

df = pd.read_parquet('../Generator/requests.parquet', engine='fastparquet')
df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
df_receives = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])

async def read():
    certificates = df.iloc[0]["request"].split("#")
    print(certificates[0])
    sslcontext = ssl.create_default_context(cafile=certificates[0])
    sslcontext.load_cert_chain(certificates[1], certificates[2])
    import time

    start = time.time()
    for i, r in df.iloc[1:].iterrows():
        async with aiohttp.ClientSession() as session:  
            
            req = r["request"].split("$")
            if(req[0]=="POST"):
                df_sends.loc[i-1] = [i, time.time()]
                async with session.post('https://127.0.0.1:8000' + req[1], data = req[5], ssl=sslcontext) as resp:
                    df_receives.loc[i-1] = [i, time.time(), resp.url.scheme + str(resp.version.major) + str(resp.version.minor) +\
                                            "$" + str(resp.status) + "$" + resp.reason + "$" + str(resp.raw_headers)]
            elif (req[0]=="GET"):
                df_sends.loc[i-1] = [i, time.time()]
                async with session.get('https://127.0.0.1:8000' + req[1], ssl=sslcontext) as resp:
                    t = await resp.text()
                    df_receives.loc[i-1] = [i, time.time(), t]


    end = time.time()
    print(end - start)
    # print(df_sends)
    # print(df_receives)
    fp.write("./sends.parquet", df_sends)
    fp.write("./receives.parquet", df_receives)
    sys.exit(2)

asyncio.run(read())