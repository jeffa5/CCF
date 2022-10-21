#!/usr/bin/env bash

set -xe

. .venv/bin/activate

# generate the requests to send
python3 generator/lskv_generator.py

# submit the requests (http2)
python submitter/submit.py  --cacert ../../lskv/workspace/sandbox_common/service_cert.pem --cert ../../lskv/workspace/sandbox_common/user0_cert.pem --key ../../lskv/workspace/sandbox_common/user0_privk.pem -gf lskv.parquet --duration 10 --http2

# generate the report
python3 analyzer/analysis.py -sf sends.parquet -rf responses.parquet