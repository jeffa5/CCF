# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.

from generator import *
from generator import create_parquet

MYHOST = "https://127.0.0.1:8000"
MYPATH = "/app/log/private"
MYTYPE = "HTTP/1.1"

for i in range(100):
    if i%8==0 and i>0:
        create_get(MYHOST, "/app/commit", MYTYPE)
    elif i%9==0 and i>0:
        create_get(MYHOST, "/app/log/private/count", MYTYPE)
    else:
        DATA = '{"id": ' + str(i) + ', "msg": "built message ' + str(i) + '"}'
        create_post(MYHOST, MYPATH, MYTYPE, DATA)

for i in range(900):
    create_get(MYHOST, MYPATH + "?id=" + str(i%100), MYTYPE)

for i in range(80):
    create_delete(MYHOST, MYPATH + "?id=" + str(i), MYTYPE)


create_parquet("requests.parquet")
