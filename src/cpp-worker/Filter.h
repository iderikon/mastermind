/*
 * Copyright (c) YANDEX LLC, 2015. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */

#ifndef __8ad6cf0b_8b75_48cc_87b4_b532d146d6d5
#define __8ad6cf0b_8b75_48cc_87b4_b532d146d6d5

#include <string>
#include <vector>

struct Filter
{
    std::vector<std::string> namespaces;
    std::vector<std::string> couples;
    std::vector<int> groups;
    std::vector<std::string> backends;
    std::vector<std::string> nodes;
    std::vector<std::string> filesystems;
};

#endif

