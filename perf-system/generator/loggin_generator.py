# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
from generator import create_parquet, create_post, create_get
from typing import List

MYHOST = "127.0.0.1:8000"

DF: List[List[str]] = []

# for i in range(100200):
#     MYHOST = "http://127.0.0.1:8080"
#     MYTYPE = "HTTP/1"
#     MYPATH = "/love"
#     # DATA = '{"id": ' + str(i) + ', "msg": "Logged to private table"}'

#     create_get(df, MYHOST, MYPATH, MYTYPE)
for i in range(12):
    MYTYPE = "HTTP/1.1"
    MYPATH = "/app/log/private"
    DATA = '{"id": 45, "msg": "Logged to private table"}'

    create_post(DF, MYHOST, MYPATH, MYTYPE, DATA)
for i in range(13):

    MYTYPE = "HTTP/1.1"
    MYPATH = "/app/log/private?id=45"
    # DATA = '{"id": ' + str(i) + ', "msg": "Logged to private table"}'

    create_get(DF, MYHOST, MYPATH, MYTYPE)
# for i in range(10000):
#     MYTYPE = "HTTP/1.1"
#     MYPATH = "/app/log/private"
#     DATA = '{"id": ' + str(i) + ', "msg": "Logged to private table"}'

#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(10):
#     MYTYPE = "HTTP/2"
#     MYPATH = "/v3/kv/range"
#     DATA = '{"key":"aGVsbG8="}'

#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(2000):
#     MYPATH = "/v3/kv/range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(1000):
#     MYPATH = "/v3/kv/put"
#     DATA = '{"key":"aGVsbG8=","value":"d29ybGQ="}'
#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(10000):
#     MYPATH = "/v3/kv/range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(1000):
#     MYPATH = "/v3/kv/delete_range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(df, MYHOST, MYPATH, MYTYPE, DATA)

# for i in range(900):
#     create_get(df, MYHOST, MYPATH + "?id=" + str(i%100), MYTYPE)

# for i in range(80):
#     create_delete(df, MYHOST, MYPATH + "?id=" + str(i), MYTYPE)


create_parquet(DF, "new_raw.parquet")
