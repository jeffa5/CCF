// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#ifndef PARQUET_DATA_H
#define PARQUET_DATA_H

#include <iostream>
#include <vector>

class ParquetData
{
public:
  std::vector<std::string> IDS;
  std::vector<std::string> REQ_VERB;
  std::vector<std::string> REQ_HOST;
  std::vector<std::string> REQ_PATH;
  std::vector<std::string> REQ_TYPE;
  std::vector<std::string> REQ_HEADER;
  std::vector<std::string> REQ_LENGTH;
  std::vector<std::string> REQ_DATA;
  std::vector<std::string> RAW_RESPONSE;
  std::vector<double> SEND_TIME;
  std::vector<double> RESPONSE_TIME;

  ParquetData();
};

#endif