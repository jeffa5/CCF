// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#ifndef HANDLE_ARGUMENTS_H
#define HANDLE_ARGUMENTS_H
#include <iostream>

class ArgumentParser
{
public:
  std::string cert;
  std::string key;
  std::string rootCa;
  std::string sendFilename = "./cpp_send.parquet";
  std::string responseFilename = "./cpp_respond.parquet";
  std::string generatorFilename = "../generator/requests.parquet";
  bool isMulitplex = false;
  int duration = -1;
  ArgumentParser();
  void argument_assigner(int argc, char** argv);
};

#endif