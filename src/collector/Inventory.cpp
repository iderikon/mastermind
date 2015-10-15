/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "Inventory.h"
#include "WorkerApplication.h"

#include <dlfcn.h>

Inventory::Inventory(WorkerApplication & app)
    :
    m_app(app),
    m_handle(nullptr),
    m_inventory_error(nullptr),
    m_get_dc_by_host(nullptr)
{}

int Inventory::open_shared_library(const std::string & file_name)
{
    m_handle = dlopen(file_name.c_str(), RTLD_LAZY | RTLD_LOCAL);

    if (m_handle == nullptr) {
        const char *err = dlerror();
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                "Inventory: dlopen() failed for '%s': %s",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        return -1;
    }

    if ((m_inventory_error = (const char* (*)()) dlsym(m_handle, "inventory_error")) == nullptr ||
            (m_get_dc_by_host = (int (*)(const char*, char*, size_t)) dlsym(m_handle, "get_dc_by_host")) == nullptr) {
        const char *err = dlerror();
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                "Inventory: dlsym() failed for '%s': %s",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));

        m_inventory_error = nullptr;
        m_get_dc_by_host = nullptr;
        dlclose(m_handle);
        return -1;
    }

    return 0;
}

std::string Inventory::get_dc_by_host(const std::string & addr)
{
    if (m_get_dc_by_host != nullptr && m_inventory_error != nullptr) {
        char buf[256];
        if (m_get_dc_by_host(addr.c_str(), buf, sizeof(buf)) != 0) {
            BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                    "Inventory: get_dc_by_host(%s) failed: %s",
                    addr.c_str(), m_inventory_error());
            return std::string();
        }
        return std::string(buf);
    }
    return addr;
}
