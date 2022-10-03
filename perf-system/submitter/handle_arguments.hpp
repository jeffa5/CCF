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
  ArgumentParser();
  void argument_assigner(int argc, char** argv);
};

#endif