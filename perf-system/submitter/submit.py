# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Submit requests
"""

import asyncio
import time
import ssl
import argparse
import aiohttp
import pandas as pd
import fastparquet as fp


async def read(roots_cert, cert, key, req_df, df_sends, df_receives):
    """
    Read the dataframes and submit requests
    """
    sslcontext = ssl.create_default_context(cafile=roots_cert)
    sslcontext.load_cert_chain(cert, key)

    for i, req_row in req_df.iloc[:].iterrows():
        async with aiohttp.ClientSession() as session:
            req = req_row["request"].split("$")
            if req[0]=="POST":
                df_sends.loc[i-1] = [i, time.time()]
                async with session.post(req[1] + req[2], data = req[6], ssl=sslcontext) as resp:
                    df_receives.loc[i-1] = [i, time.time(), resp.url.scheme +\
                                            str(resp.version.major) + str(resp.version.minor) +\
                                            "$" + str(resp.status) + "$" + resp.reason +\
                                            "$" + str(resp.raw_headers)]
            elif req[0]=="GET":
                df_sends.loc[i-1] = [i, time.time()]
                async with session.get(req[1] + req[2], ssl=sslcontext) as resp:
                    df_receives.loc[i-1] = [i, time.time(), resp.url.scheme +\
                                            str(resp.version.major) + str(resp.version.minor) +\
                                            "$" + str(resp.status) + "$" +\
                                            resp.reason + "$" + str(resp.raw_headers)]

    fp.write("./sends.parquet", df_sends)
    fp.write("./receives.parquet", df_receives)



def main():
    """
    Receives the command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-ca", "--cacert", help="Use the specified certificate file to verify the peer")
    parser.add_argument("-c", "--cert", help="Use the specified client certificate file")
    parser.add_argument("-k", "--key", help="Private key file")

    args = parser.parse_args()

    req_df = pd.read_parquet('../generator/requests.parquet', engine='fastparquet')
    df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
    df_receives = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])
    asyncio.run(read(args.cacert or "", args.cert or "",\
                args.key or "", req_df, df_sends, df_receives))

if __name__ == "__main__":
    main()
