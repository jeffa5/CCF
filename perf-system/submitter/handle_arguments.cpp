// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include "handle_arguments.hpp"

#include "stdcpp.h"

#include <iostream>

ArgumentParser::ArgumentParser() {}

void ArgumentParser::argument_assigner(int argc, char** argv)
{
  for (int argIter = 1; argIter < argc; argIter++)
  {
    if (strcmp(argv[argIter], "-c") == 0)
    {
      cert = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-k") == 0)
    {
      key = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-ca") == 0)
    {
      rootCa = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-sf") == 0)
    {
      send_filename = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-rf") == 0)
    {
      response_filename = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-pipeline") == 0)
    {
      isPipeline = true;
    }
    else if (strcmp(argv[argIter], "-gf") == 0)
    {
      generator_filename = argv[argIter + 1];
    }
    else if (strcmp(argv[argIter], "-d") == 0)
    {
      duration = atoi(argv[argIter + 1]);
    }
    else if (strcmp(argv[argIter], "-sa") == 0)
    {
      server_address = argv[argIter + 1];
    }
  }
}