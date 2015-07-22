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

#include "FilterParser.h"

#include <cstddef>

enum FilterKey
{
    FilterSec   = 2,
    Namespaces  = 4,
    Couples     = 8,
    Groups      = 0x10,
    Backends    = 0x20,
    Nodes       = 0x40,
    Filesystems = 0x80
};

static const Parser::Folder filter_1[] = {
    { "filter", 0, FilterSec },
    { NULL, 0, 0 }
};

static const Parser::Folder filter_2[] = {
    { "namespaces",  FilterSec, Namespaces  },
    { "couples",     FilterSec, Couples     },
    { "groups",      FilterSec, Groups      },
    { "backends",    FilterSec, Backends    },
    { "nodes",       FilterSec, Nodes       },
    { "filesystems", FilterSec, Filesystems },
    { NULL, 0, 0 }
};

static const Parser::Folder * const filter_folders[] = {
    filter_1,
    filter_2
};

static const Parser::UIntInfo filter_uint_info[] = {
    { 0, 0, 0 }
};

FilterParser::FilterParser(Filter & filter)
    :
    super(filter_folders, sizeof(filter_folders)/sizeof(filter_folders[0]),
            filter_uint_info, (uint8_t *) &filter),
    m_filter(filter),
    m_array_depth(0)
{}

bool FilterParser::String(const char* str, rapidjson::SizeType length, bool copy)
{
    if (m_array_depth != 1)
        return true;

    std::string string(str, length);

    switch (m_keys - 1)
    {
    case FilterSec|Namespaces:
        m_filter.namespaces.emplace_back(std::move(string));
        break;
    case FilterSec|Couples:
        m_filter.couples.emplace_back(std::move(string));
        break;
    case FilterSec|Backends:
        m_filter.backends.emplace_back(std::move(string));
        break;
    case FilterSec|Nodes:
        m_filter.nodes.emplace_back(std::move(string));
        break;
    case FilterSec|Filesystems:
        m_filter.filesystems.emplace_back(std::move(string));
        break;
    }

    return true;
}

bool FilterParser::UInteger(uint64_t val)
{
    if (m_array_depth != 1)
        return true;

    if (m_keys != (FilterSec|Groups|1))
        return true;

    m_filter.groups.push_back(int(val));
    return true;
}
