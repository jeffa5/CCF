// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include "handle_arguments.hpp"

#include <bits/stdc++.h>
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
    if (strcmp(argv[argIter], "-k") == 0)
    {
      key = argv[argIter + 1];
    }
    if (strcmp(argv[argIter], "-ca") == 0)
    {
      rootCa = argv[argIter + 1];
    }
    if (strcmp(argv[argIter], "-sf") == 0)
    {
      sendFilename = argv[argIter + 1];
    }
    if (strcmp(argv[argIter], "-rf") == 0)
    {
      responseFilename = argv[argIter + 1];
    }
    if (strcmp(argv[argIter], "-multiplex") == 0)
    {
      isMulitplex = true;
    }
    if (strcmp(argv[argIter], "-gf") == 0)
    {
      generatorFilename = argv[argIter + 1];
    }
  }
}