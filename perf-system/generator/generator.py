# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
"""
Generate requests
"""

import argparse
import pandas as pd
import fastparquet as fp

REQUEST_CONTENT_TYPE = "Content-Type: application/json"
REQUEST_LENGTH_TEXT = "Content-Length: "
PARQUET_FILE_NAME = "requests.parquet"

df = pd.DataFrame(columns=["messageID", "request"])

def fill_df(host, req_path, req_type, req_verb, req_iters):
    """
    Creates a dataframe with the data
    required for the requests
    """
    # entering the private file paths as metadata in the start of parquet

    if req_verb == "POST":
        create_post(host, req_path, req_type, req_iters)

    elif req_verb == "GET":
        create_get(host, req_path, req_type, req_iters)

def create_get(host, req_path, req_type, req_iters):
    """
    Generate get queries
    """
    last_index = len(df.index)
    for request in range(req_iters):
        df.loc[last_index + request] = [str(last_index + request), "GET" + "$" + host +\
            "$" + req_path + "$" + req_type + "$" + REQUEST_CONTENT_TYPE]

def create_post(host, req_path, req_type, req_iters):
    """
    Generate post queries
    """
    last_index = len(df.index)
    for request in range(req_iters):
        request_message = '{"id": ' + str(last_index + request) +\
                            ', "msg": "Send message with id ' +\
                            str(last_index + request) + '"}'
        df.loc[last_index + request] = [str(last_index + request), "POST" +\
                            "$" + host + "$" + req_path + "$" +\
                            req_type + "$" + REQUEST_CONTENT_TYPE + "$" +\
                            REQUEST_LENGTH_TEXT + str(len(request_message)) +\
                            "$" + request_message]


def create_parquet():
    """
    Takes the dataframe data and stores them
    in a parquet file in the current directory
    """
    fp.write(PARQUET_FILE_NAME, df)



def main():
    """
    The function to receive the arguments
    from the command line
    """
    arg_host = "https://127.0.0.1:8000"
    arg_path = "/app/log/private" #default path to request
    arg_type = "HTTP/1.1" #default type
    arg_verb = "POST" #default verb
    arg_iterations = 16

    parser = argparse.ArgumentParser()
    parser.add_argument("-hs", "--host",\
        help="The main host to submit the request. Default `http://localhost:8000`", type=str)
    parser.add_argument("-p", "--path",\
        help="The realtive path to submit the request. Default `app/log/private`", type=str)
    parser.add_argument("-t", "--type",\
        help="The type of the HTTP request (Only HTTP/1.1 which is the\
        default is supported for now)", type=str)
    parser.add_argument("-vr", "--verb",\
        help="The request action. Default `POST` (Only `POST` and `GET` are supported for now)")
    parser.add_argument("-r", "--rows",\
        help="The number of requests to send. Default `16` ", type=int)

    args = parser.parse_args()


    fill_df( args.host or arg_host, args.path or arg_path, args.type or arg_type, args.verb or\
                  arg_verb, args.rows or arg_iterations)
    # create_post("https://127.0.0.1:8000", "/app/log/private", "HTTP/1.1", 30)
    # create_get("https://127.0.0.1:8000", "/app/log/private?id=1", "HTTP/1.1", 20)


    create_parquet()

if __name__ == "__main__":
    main()
