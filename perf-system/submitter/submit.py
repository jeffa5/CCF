# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Submit requests
"""

import asyncio
import time
import ssl
import argparse

# pylint: disable=import-error
import aiohttp  # type: ignore
import pandas as pd  # type: ignore

# pylint: disable=import-error
import fastparquet as fp  # type: ignore


async def read(certificates, file_names, duration):
    """
    Read the dataframes and call requests submission
    """
    req_df = pd.read_parquet(file_names[0], engine="fastparquet")
    df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
    df_responses = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])

    sslcontext = ssl.create_default_context(cafile=certificates[0])
    sslcontext.load_cert_chain(certificates[1], certificates[2])

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
                if req[0] == "POST":
                    df_sends.loc[i + last_index] = [i + last_index, time.time()]
                    async with session.post(
                        req[1] + req[2], data=req[6], ssl=sslcontext
                    ) as resp:
                        write_response(resp, df_responses, i, last_index)

                elif req[0] == "GET":
                    df_sends.loc[i + last_index] = [i + last_index, time.time()]
                    async with session.get(req[1] + req[2], ssl=sslcontext) as resp:
                        write_response(resp, df_responses, i, last_index)

                if time.time() > end_time and not run_loop_once:
                    duration_run = False
                    break
        run_loop_once = False

    fp.write(file_names[1], df_sends)
    fp.write(file_names[2], df_responses)


def write_response(resp, df_responses, i, last_index):
    """
    Populate the dataframe for responses
    """
    df_responses.loc[i + last_index] = [
        i + last_index,
        time.time(),
        resp.url.scheme
        + str(resp.version.major)
        + str(resp.version.minor)
        + "$"
        + str(resp.status)
        + "$"
        + resp.reason
        + "$"
        + str(resp.raw_headers),
    ]


def main():
    """
    Receives the command line arguments
    """
    arg_gen_file = "../generator/requests.parquet"
    arg_sends_file = "./sends.parquet"
    arg_response_file = "./responses.parquet"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-ca",
        "--cacert",
        help="Use the specified certificate file to verify\
                        the peer",
        type=str,
    )
    parser.add_argument(
        "-c", "--cert", help="Use the specified client certificate file", type=str
    )
    parser.add_argument("-k", "--key", help="Private key file", type=str)
    parser.add_argument(
        "-d", "--duration", help="Time duration for the submitter to run", type=int
    )
    parser.add_argument(
        "-gf",
        "--generator_file",
        help="Name of the parquet file with the generated requests\
            to be submitted. Default file `../generator/requests.parquet`",
        type=str,
    )
    parser.add_argument(
        "-sf",
        "--send_file",
        help="Name of the parquet file to store the submitted\
            requests. Default file `./sends.parquet`",
        type=str,
    )
    parser.add_argument(
        "-rf",
        "--response_file",
        help="Name of the parquet file to store the responses\
            from the submitted requests. Default file `./responses.parquet`",
        type=str,
    )

    args = parser.parse_args()

    asyncio.run(
        read(
            [args.cacert or "", args.cert or "", args.key or ""],
            [
                args.generator_file or arg_gen_file,
                args.send_file or arg_sends_file,
                args.response_file or arg_response_file,
            ],
            args.duration or -1,
        )
    )


if __name__ == "__main__":
    main()
