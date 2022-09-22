# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Submit requests
"""

from ast import Num
import asyncio
import time
import ssl
import argparse
from urllib import response
import aiohttp
import pandas as pd
import fastparquet as fp


async def read(roots_cert, cert, key, req_df, df_sends, df_receives, duration):
    """
    Read the dataframes and call requests submission
    """
    sslcontext = ssl.create_default_context(cafile=roots_cert)
    sslcontext.load_cert_chain(cert, key)

    if duration > 0:
        end_time = time.time() + (duration)
        run_loop_once = False
        duration_run = True
    else:
        end_time = -1
        run_loop_once = True
        duration_run = False

    while (end_time > 0 and duration_run) or (end_time < 0 and run_loop_once):
        last_index = len(df_sends.index)
        for i, req_row in req_df.iloc[:].iterrows():
            async with aiohttp.ClientSession() as session:
                req = req_row["request"].split("$")
                if req[0]=="POST":
                    await submit_post(df_sends, i, last_index, session, req, sslcontext, df_receives)
                elif req[0]=="GET":
                    await submit_get(df_sends, i, last_index, session, req, sslcontext, df_receives)
                if time.time() > end_time and not run_loop_once:
                    duration_run = False
                    break
        run_loop_once = False

    fp.write("./sends.parquet", df_sends)
    fp.write("./receives.parquet", df_receives)


async def submit_post(df_sends, i, last_index, session, req, sslcontext, df_receives):
    df_sends.loc[i + last_index] = [i + last_index, time.time()]
    async with session.post(req[1] + req[2], data = req[6], ssl=sslcontext) as resp:
        write_response(resp, df_receives, i, last_index)

async def submit_get(df_sends, i, last_index, session, req, sslcontext, df_receives):
    df_sends.loc[i + last_index] = [i + last_index, time.time()]
    async with session.get(req[1] + req[2], ssl=sslcontext) as resp:
        write_response(resp, df_receives, i, last_index)

def write_response(resp, df_receives, i, last_index):
    df_receives.loc[i + last_index] = [i + last_index, time.time(),\
        resp.url.scheme + str(resp.version.major) +\
        str(resp.version.minor) + "$" + str(resp.status) +\
        "$" + resp.reason + "$" + str(resp.raw_headers)]

def main():
    """
    Receives the command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-ca", "--cacert", help="Use the specified certificate file to verify\
                        the peer", type=str)
    parser.add_argument("-c", "--cert", help="Use the specified client certificate file", type=str)
    parser.add_argument("-k", "--key", help="Private key file", type=str)
    parser.add_argument("-d", "--duration", help="Private key file", type=int)


    args = parser.parse_args()

    req_df = pd.read_parquet('../generator/requests.parquet', engine='fastparquet')
    df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
    df_receives = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])
    asyncio.run(read(args.cacert or "", args.cert or "",\
                args.key or "", req_df, df_sends, df_receives, args.duration or -1))

if __name__ == "__main__":
    main()
