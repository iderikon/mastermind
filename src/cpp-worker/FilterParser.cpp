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
    Namespaces  = 2,
    Couples     = 4,
    Groups      = 8,
    Backends    = 0x10,
    Nodes       = 0x20,
    Filesystems = 0x40
};

static const Parser::Folder filter_1[] = {
    { "namespaces",  0, Namespaces },
    { "couples",     0, Couples },
    { "groups",      0, Groups },
    { "backends",    0, Backends },
    { "nodes",       0, Nodes },
    { "filesystems", 0, Filesystems },
    { NULL, 0, 0 }
};

static const Parser::Folder * const filter_folders[] = {
    filter_1
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
        return false;

    std::string string(str, length);

    switch (m_keys - 1)
    {
    case Namespaces:
        m_filter.namespaces.emplace_back(std::move(string));
        break;
    case Couples:
        m_filter.couples.emplace_back(std::move(string));
        break;
    case Backends:
        m_filter.backends.emplace_back(std::move(string));
        break;
    case Nodes:
        m_filter.nodes.emplace_back(std::move(string));
        break;
    case Filesystems:
        m_filter.filesystems.emplace_back(std::move(string));
        break;
    default:
        return false;
    }

    return true;
}

bool FilterParser::UInteger(uint64_t val)
{
    if (m_array_depth != 1)
        return false;

    if (m_keys != (Groups|1))
        return false;

    m_filter.groups.push_back(int(val));
    return true;
}
