# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
from generator import *
from generator import create_parquet

MYHOST = "https://127.0.0.1:8000"

# for i in range(1020):
#     MYTYPE = "HTTP/1"
#     MYPATH = "/app/log/private"
#     DATA = '{"id": 42, "msg": "Logged to private table"}'

#     create_post(MYHOST, MYPATH, MYTYPE, DATA)
for i in range(100200):
    MYTYPE = "HTTP/2"
    MYPATH = "/v3/kv/range"
    DATA = '{"key":"aGVsbG8="}'

    create_post(MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(2000):
#     MYPATH = "/v3/kv/range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(1000):
#     MYPATH = "/v3/kv/put"
#     DATA = '{"key":"aGVsbG8=","value":"d29ybGQ="}'
#     create_post(MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(10000):
#     MYPATH = "/v3/kv/range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(MYHOST, MYPATH, MYTYPE, DATA)
# for i in range(1000):
#     MYPATH = "/v3/kv/delete_range"
#     DATA = '{"key":"aGVsbG8="}'
#     create_post(MYHOST, MYPATH, MYTYPE, DATA)

# for i in range(900):
#     create_get(MYHOST, MYPATH + "?id=" + str(i%100), MYTYPE)

# for i in range(80):
#     create_delete(MYHOST, MYPATH + "?id=" + str(i), MYTYPE)


create_parquet("requests.parquet")
