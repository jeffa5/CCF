#WARNING#
"""
With cp-ing workspace in CCF build folder you should be able to run submitter here
"""

import aiohttp
import asyncio
import ssl
import pandas as pd
import fastparquet as fp
import sys
import time
import argparse


async def read(roots_cert, cert, key, df, df_sends, df_receives):
    sslcontext = ssl.create_default_context(cafile=roots_cert)
    sslcontext.load_cert_chain(cert, key)

    for i, r in df.iloc[:].iterrows():
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
                    df_receives.loc[i-1] = [i, time.time(), resp.url.scheme + str(resp.version.major) + str(resp.version.minor) +\
                                            "$" + str(resp.status) + "$" + resp.reason + "$" + str(resp.raw_headers)]
    
    fp.write("./sends.parquet", df_sends)
    fp.write("./receives.parquet", df_receives)



def main(argv):

    parser = argparse.ArgumentParser()
    parser.add_argument("-rc", "--roots-cert", help="that path a root custom CA file")
    parser.add_argument("-c", "--cert", help="the path to a certification configuration file")
    parser.add_argument("-k", "--key", help="the path to the private key file")

    args = parser.parse_args()
    
    df = pd.read_parquet('../Generator/requests.parquet', engine='fastparquet')
    df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
    df_receives = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])
    asyncio.run(read(args.roots_cert or "", args.cert or "",\
                args.key or "", df, df_sends, df_receives))

if __name__ == "__main__":
    main(sys.argv)