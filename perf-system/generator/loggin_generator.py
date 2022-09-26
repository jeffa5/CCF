# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.

from generator import *
from generator import create_parquet

MYHOST = "https://127.0.0.1:8000"
MYPATH = "/app/log/private"
MYTYPE = "HTTP/1.1"

for i in range(10):
    DATA = '{"id": ' + str(i) + ', "msg": "built message ' + str(i) + '"}'
    create_post(MYHOST, MYPATH, MYTYPE, DATA)

for i in range(10):
    create_get(MYHOST, MYPATH, MYTYPE)


create_parquet("requests.parquet")
