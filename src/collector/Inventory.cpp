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

Inventory::Inventory()
    :
    m_handle(nullptr),
    m_driver(new DefaultInventory)
{}

Inventory::~Inventory()
{
    m_driver.reset();
    if (m_handle != nullptr)
        dlclose(m_handle);
}

std::string DefaultInventory::get_dc_by_host(const std::string & addr, std::string & error_text)
{
    if (!addr.empty()) {
        BH_LOG(app::logger(), DNET_LOG_DEBUG,
                "Default inventory: Returning DC '%s' for host '%s'",
                addr.c_str(), addr.c_str());
    } else {
        error_text = "Default inventory: Get DC: addr is empty";
    }
    return addr;
}

int Inventory::open_shared_library(const std::string & file_name)
{
    if (m_handle != nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Internal error: Inventory is already opened");
        return -1;
    }

    m_handle = dlopen(file_name.c_str(), RTLD_LAZY | RTLD_LOCAL);

    if (m_handle == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: dlopen() failed for '%s': %s",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        return -1;
    }

    InventoryDriver *(*create_inventory)() = (InventoryDriver *(*)()) dlsym(m_handle, "create_inventory");
    if (create_inventory == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot find symbol 'create_inventory' in '%s': '%s'",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        close();
        return -1;
    }

    InventoryDriver *driver = create_inventory();
    if (driver == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Inventory: inventory_create() returned null");
        close();
        return -1;
    }

    m_driver.reset(driver);

    return 0;
}

void Inventory::close()
{
    if (m_handle != nullptr) {
        m_driver.reset(new DefaultInventory);
        dlclose(m_handle);
        m_handle = nullptr;
    }
}

std::string Inventory::get_dc_by_host(const std::string & addr)
{
    std::string error_text;
    std::string result = m_driver->get_dc_by_host(addr, error_text);

    if (result.empty()) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot resolve DC for host '%s': %s",
                addr.c_str(), error_text.c_str());
    } else {
        BH_LOG(app::logger(), DNET_LOG_DEBUG,
                "Inventory: Resolved DC '%s' for host '%s'",
                result.c_str(), addr.c_str());
    }

    return result;
}
