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

#include "Parser.h"

#include <algorithm>
#include <string>

namespace {

struct FolderLess
{
    bool operator () (const Parser::Folder & f1, const Parser::Folder & f2) const
    {
        if (f1.keys != f2.keys)
            return f1.keys < f2.keys;
        return std::strcmp(f1.str, f2.str) < 0;
    }
};

struct FolderSearch
{
    FolderSearch(uint32_t k, const char *s)
        :
        found_eq(false),
        keys(k),
        str(s)
    {}

    mutable bool found_eq;
    uint32_t keys;
    const char *str;
};

struct FolderSearchLess
{
    bool operator () (const Parser::Folder & f1, const FolderSearch & search) const
    {
        if (f1.keys != search.keys)
            return f1.keys < search.keys;

        if (f1.str[0] == *MATCH_ANY) {
            search.found_eq = true;
            return false;
        }

        if (f1.str[0] == *NOT_MATCH) {
            if (!std::strcmp(f1.str + 1, search.str))
                return true;

            search.found_eq = true;
            return false;
        }

        int res = std::strcmp(f1.str, search.str);
        if (!res) {
            search.found_eq = true;
            return false;
        }
        return res < 0;
    }
};

struct UIntInfoLess
{
    bool operator () (const Parser::UIntInfo & i1, const Parser::UIntInfo & i2) const
    {
        return i1.keys < i2.keys;
    }

    bool operator () (const Parser::UIntInfo & info, uint32_t keys) const
    {
        return info.keys < keys;
    }
};

struct StringInfoLess
{
    bool operator () (const Parser::StringInfo & i1, const Parser::StringInfo & i2) const
    {
        return i1.keys < i2.keys;
    }

    bool operator () (const Parser::StringInfo & info, uint32_t keys) const
    {
        return info.keys < keys;
    }
};

} // unnamed namespace

Parser::Parser(Folder **fold, int max_depth, UIntInfo *uint_info,
        StringInfo *string_info, uint8_t *dest)
    :
    m_keys(1),
    m_depth(0),
    m_max_depth(max_depth),
    m_fold(fold),
    m_uint_info(uint_info),
    m_uint_info_size(0),
    m_string_info(string_info),
    m_string_info_size(0),
    m_dest(dest)
{
    m_fold_size.resize(max_depth);

    for (int i = 0; i < max_depth; ++i) {
        size_t & folder_size = m_fold_size[i];
        for (folder_size = 0; m_fold[i][folder_size].str != nullptr; ++folder_size);
        std::sort(m_fold[i], m_fold[i] + folder_size, FolderLess());
    }

    if (m_uint_info != nullptr) {
        for (; m_uint_info[m_uint_info_size].keys; ++m_uint_info_size);
        std::sort(m_uint_info, m_uint_info + m_uint_info_size, UIntInfoLess());
    }

    if (m_string_info != nullptr) {
        for (; m_string_info[m_string_info_size].keys; ++m_string_info_size);
        std::sort(m_string_info, m_string_info + m_string_info_size, StringInfoLess());
    }
}

bool Parser::UInteger(uint64_t val)
{
    if (m_uint_info == nullptr)
        return true;

    if (key_depth() != (m_depth + 1))
        return true;

    auto info = std::lower_bound(m_uint_info, m_uint_info + m_uint_info_size, m_keys - 1, UIntInfoLess());

    // if we haven't found the UIntInfo, something is wrong
    if (info == (m_uint_info + m_uint_info_size) || info->keys != (m_keys - 1))
        return false;

    uint64_t *dst_val = (uint64_t *) (m_dest + info->off);
    switch (info->action)
    {
    case SET:
        *dst_val = val;
        break;

    case SUM:
        *dst_val += val;
        break;

    case MAX:
        if (*dst_val < val)
            *dst_val = val;
        break;
    }

    clear_key();
    return true;
}

bool Parser::String(const char* str, rapidjson::SizeType length, bool copy)
{
    if (m_string_info == nullptr)
        return true;

    if (key_depth() != (m_depth + 1))
        return true;

    auto info = std::lower_bound(m_string_info, m_string_info + m_string_info_size,
            m_keys - 1, StringInfoLess());

    // if we haven't found the StringInfo, something is wrong
    if (info == (m_string_info + m_string_info_size) || info->keys != (m_keys - 1))
        return false;

    std::string & dst_val = *(std::string *) (m_dest + info->off);
    dst_val.assign(str, length);

    clear_key();
    return true;
}

bool Parser::Key(const char* str, rapidjson::SizeType length, bool copy)
{
    int kdepth = key_depth();

    if (m_depth != kdepth)
        return true;

    if (kdepth > m_max_depth)
        return false;

    FolderSearch search(m_keys - 1, str);
    int idx = m_depth - 1;
    size_t size = m_fold_size[idx];
    auto fold = std::lower_bound(m_fold[idx], m_fold[idx] + size, search, FolderSearchLess());

    if (search.found_eq)
        m_keys |= fold->token;

    return true;
}
