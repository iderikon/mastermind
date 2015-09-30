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

// Hash Functions: http://www.cse.yorku.ca/~oz/hash.html
uint32_t djb2(unsigned char *str)
{
    uint32_t hash = 5381;
    int c;

    while (!!(c = *str++))
        hash = ((hash << 5) + hash) + c;

    return hash;
}

struct FolderLess
{
    bool operator () (const Parser::Folder & f1, const Parser::Folder & f2) const
    {
        if (*f1.str == *NOT_MATCH || *f2.str == *NOT_MATCH) {
            if (*f1.str == *NOT_MATCH)
                return true;
            else
                return false;
        }
        if (f1.keys != f2.keys)
            return f1.keys < f2.keys;
        if (f1.str_hash != f2.str_hash)
            return f1.str_hash < f2.str_hash;
        return std::strcmp(f1.str, f2.str) < 0;
    }
};

struct FolderSearch
{
    FolderSearch(uint64_t k, const char *s)
        :
        found_eq(false),
        keys(k),
        str(s)
    {
        str_hash = djb2((unsigned char *) str);
    }

    mutable bool found_eq;
    uint64_t keys;
    const char *str;
    uint32_t str_hash;
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

        if (f1.str_hash != search.str_hash)
            return f1.str_hash < search.str_hash;

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

    bool operator () (const Parser::UIntInfo & info, uint64_t keys) const
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

    bool operator () (const Parser::StringInfo & info, uint64_t keys) const
    {
        return info.keys < keys;
    }
};

} // unnamed namespace

Parser::FolderVector::FolderVector(std::initializer_list<Folder> list)
    : std::vector<Folder>(list)
{
    for (auto it = begin(); it != end(); ++it) {
        Folder & fold = *it;
        if (*fold.str != *NOT_MATCH) {
            fold.str_hash = djb2((unsigned char *) fold.str);
        } else {
            // calculate hash of pattern in case of special condition
            fold.str_hash = djb2((unsigned char *) (fold.str + 1));
        }
    }

    std::sort(begin(), end(), FolderLess());
}

Parser::UIntInfoVector::UIntInfoVector(std::initializer_list<UIntInfo> list)
    : std::vector<UIntInfo>(list)
{
    std::sort(begin(), end(), UIntInfoLess());
}

Parser::StringInfoVector::StringInfoVector(std::initializer_list<StringInfo> list)
    : std::vector<StringInfo>(list)
{
    std::sort(begin(), end(), StringInfoLess());
}

Parser::Parser(const std::vector<FolderVector> & folders,
        const UIntInfoVector & uint_info,
        const StringInfoVector & string_info,
        uint8_t *dest)
    :
    m_keys(1),
    m_depth(0),
    m_folders(folders),
    m_uint_info(uint_info),
    m_string_info(string_info),
    m_dest(dest)
{}

bool Parser::UInteger(uint64_t val)
{
    if (m_uint_info.empty())
        return true;

    if (key_depth() != (m_depth + 1))
        return true;

    auto info = std::lower_bound(m_uint_info.begin(), m_uint_info.end(), m_keys - 1, UIntInfoLess());

    // if we haven't found the UIntInfo, something is wrong
    if (info == m_uint_info.end() || info->keys != (m_keys - 1))
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
    if (m_string_info.empty())
        return true;

    if (key_depth() != (m_depth + 1))
        return true;

    auto info = std::lower_bound(m_string_info.begin(), m_string_info.end(),
            m_keys - 1, StringInfoLess());

    // if we haven't found the StringInfo, something is wrong
    if (info == m_string_info.end() || info->keys != (m_keys - 1))
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

    if (size_t(kdepth) > m_folders.size())
        return false;

    FolderSearch search(m_keys - 1, str);
    int idx = m_depth - 1;

    // NOT_MATCH keys are always in the beginning
    auto begin = m_folders[idx].begin();
    for (; begin != m_folders[idx].end() && *begin->str == *NOT_MATCH; ++begin) {
        if (begin->keys != (m_keys - 1))
            continue;

        // hash for NOT_MATCH keys is calculated beginning from the first
        // character of a pattern, special character is not included
        if (begin->str_hash != search.str_hash || std::strcmp(begin->str + 1, str)) {
            m_keys |= begin->token;
            return true;
        }
    }

    auto fold = std::lower_bound(begin, m_folders[idx].end(), search, FolderSearchLess());

    if (search.found_eq)
        m_keys |= fold->token;

    return true;
}
