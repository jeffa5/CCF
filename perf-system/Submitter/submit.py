#WARNING#
"""
With cp-ing workspace in CCF build folder you should be able to run submitter here
"""

import aiohttp
import asyncio
import ssl
import pandas as pd
import sys

df = pd.read_parquet('../Generator/requests.parquet', engine='fastparquet')

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
            async with session.post('https://127.0.0.1:8000' + req[1], data = req[5], ssl=sslcontext) as resp:
                pass
                # print(resp)

    end = time.time()
    print(end - start)
    sys.exit(2)

asyncio.run(read())