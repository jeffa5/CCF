# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Submit requests
"""

import asyncio
import time
import ssl
import argparse
from loguru import logger

# pylint: disable=import-error
import httpx
import pandas as pd  # type: ignore

# pylint: disable=import-error
import fastparquet as fp  # type: ignore


async def read(certificates, file_names, duration, server_address, http2: bool):
    """
    Read the dataframes and call requests submission
    """
    req_df = pd.read_parquet(file_names[0], engine="fastparquet")
    df_sends = pd.DataFrame(columns=["messageID", "sendTime"])
    df_responses = pd.DataFrame(columns=["messageID", "receiveTime", "rawResponse"])

    # formalize the server_address
    if not server_address.startswith("http"):
        server_address = "https://" + server_address

    req_details = []
    req_headers = []
    req_data = []

    logger.info("Starting Formalizing Data")

    # create the requests
    for i, req_row in req_df.iloc[:].iterrows():
        req = req_row["request"].split("\r\n")
        req_details.append(req[0].split(" "))
        req_headers.append({x.split(":")[0]: x.split(":")[1] for x in req[1:-2]})
        if req_details[i][0] == "GET":
            req_data.append("")
        else:
            req_data.append(req[-1])

    logger.info("Finished Formalizing Data")

    logger.info("Starting Submission")

    if duration > 0:
        duration_end_time = time.time() + (duration)
        run_loop_once = False
        duration_run = True
    else:
        duration_end_time = -1
        run_loop_once = True
        duration_run = False

    while (duration_end_time > 0 and duration_run) or (
        duration_end_time < 0 and run_loop_once
    ):
        last_index = len(df_sends.index)
        cacert = certificates[0]
        client_cert = (certificates[1], certificates[2])
        async with httpx.AsyncClient(
            http2=http2, verify=cacert, cert=client_cert
        ) as client:
            for i in range(len(req_details)):
                method = req_details[i][0]
                path = req_details[i][1]
                address = server_address + path
                headers = req_headers[i]

                if method == "POST":
                    data = req_data[i]
                    logger.debug(
                        "sending post request to {} data={} headers={}",
                        address,
                        data,
                        headers,
                    )
                    df_sends.loc[i + last_index] = [i + last_index, time.time()]
                    resp = await client.post(
                        address,
                        data=data,
                        headers=headers,
                    )
                    end_time = time.time()
                    logger.debug(
                        "got response from post request to {}: status={}",
                        address,
                        resp.status_code,
                    )
                    write_response(resp, df_responses, end_time, i, last_index)

                elif method == "GET":
                    logger.debug(
                        "sending get request to {} headers={}", address, headers
                    )
                    df_sends.loc[i + last_index] = [i + last_index, time.time()]
                    resp = await client.get(
                        address,
                        headers=headers,
                    )
                    end_time = time.time()
                    write_response(resp, df_responses, end_time, i, last_index)

                elif method == "DELETE":
                    logger.debug(
                        "sending delete request to {} headers={}", address, headers
                    )
                    df_sends.loc[i + last_index] = [i + last_index, time.time()]
                    resp = await client.delete(
                        address,
                        headers=headers,
                    )
                    end_time = time.time()
                    write_response(resp, df_responses, end_time, i, last_index)

                if time.time() > duration_end_time and not run_loop_once:
                    duration_run = False
                    break
            run_loop_once = False

    fp.write(file_names[1], df_sends)
    fp.write(file_names[2], df_responses)


def write_response(resp, df_responses, end_time, i, last_index):
    """
    Populate the dataframe for responses
    """
    df_responses.loc[i + last_index] = [
        i + last_index,
        end_time,
        str(resp.http_version)
        + " "
        + str(resp.status_code)
        + " "
        + resp.reason_phrase
        + "\n"
        + str(resp.headers),
    ]


def main():
    """
    Receives the command line arguments
    """
    arg_gen_file = "../generator/requests.parquet"
    arg_sends_file = "./sends.parquet"
    arg_response_file = "./responses.parquet"
    arg_server_address = "127.0.0.1:8000"
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

    parser.add_argument(
        "-sa",
        "--server_address",
        help="The address of the server to submit the requests\
            default is set to `127.0.0.1:8000`",
        type=str,
    )

    parser.add_argument(
        "--http2",
        action="store_true",
        help="The address of the server to submit the requests\
            default is set to `127.0.0.1:8000`",
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
            args.server_address or arg_server_address,
            args.http2,
        )
    )
    logger.info("Finished Submission")


if __name__ == "__main__":
    main()
