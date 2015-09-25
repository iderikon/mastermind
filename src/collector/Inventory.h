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

#ifndef __bbd6c304_2898_4ea0_afae_ee8b16e21893
#define __bbd6c304_2898_4ea0_afae_ee8b16e21893

#include <string>

class Inventory
{
public:
    Inventory();

    int open_shared_library(const std::string & file_name);

    std::string get_dc_by_host(const std::string & addr);

private:
    void *m_handle;
    const char *(*m_inventory_error)(void);
    int (*m_get_dc_by_host)(const char *, char *, size_t);
};

#endif

