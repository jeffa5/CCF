// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#pragma once

#include "ccf/hooks/user_hooks.h"
#include "ccf/kv/untyped.h"
#include "kv/store.h"

namespace ccf::hooks
{
  class UserHooksInternal : public UserHooks
  {
  protected:
    std::shared_ptr<kv::Store> tables;

  public:
    UserHooksInternal(std::shared_ptr<kv::Store> tables_) : tables(tables_) {}

    // Install a new hook that gets invoked with entries once they have been
    // globally committed (gone through consensus).
    //
    // Returns whether the install overwrote an existing hook.
    bool install_global_hook(
      const std::string& map_name, const kv::untyped::CommitHook& hook)
    {
      return tables->set_global_user_hook(map_name, hook);
    }
  };
}