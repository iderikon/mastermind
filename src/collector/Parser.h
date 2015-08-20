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

#ifndef __60719e6b_2f14_4a95_b411_d9baa339f032
#define __60719e6b_2f14_4a95_b411_d9baa339f032

#include <cstdint>
#include <rapidjson/reader.h>
#include <vector>

#define NOT_MATCH "\001"
#define MATCH_ANY "\002"

#define SET 0
#define SUM 1
#define MAX 2

class Parser
{
public:
    struct Folder
    {
        const char *str;
        uint32_t keys;
        uint32_t token;
    };

    struct UIntInfo
    {
        uint32_t keys;
        int action;
        size_t off;
    };

public:
    Parser(const Folder * const *fold, int max_depth,
            const UIntInfo *info, uint8_t *dest)
        :
        m_keys(1),
        m_depth(0),
        m_max_depth(max_depth),
        m_fold(fold),
        m_uint_info(info),
        m_dest(dest)
    {}

    virtual ~Parser()
    {}

    virtual bool Null()
    { return true; }
    virtual bool Bool(bool b)
    { return true; }
    virtual bool Int(int i)
    { return true; }
    virtual bool Int64(int64_t i)
    { return true; }
    virtual bool Double(double d)
    { return true; }
    virtual bool String(const char* str, rapidjson::SizeType length, bool copy)
    { return true; }
    virtual bool StartArray()
    { return true; }
    virtual bool EndArray(rapidjson::SizeType nr_elements)
    { return true; }

    virtual bool Uint(unsigned u)
    { return UInteger(u); }
    virtual bool Uint64(uint64_t u)
    { return UInteger(u); }

    virtual bool Key(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool StartObject()
    {
        ++m_depth;
        return true;
    }

    virtual bool EndObject(rapidjson::SizeType nr_members)
    {
        if (m_depth == key_depth())
            clear_key();
        --m_depth;
        return true;
    }

    virtual bool good()
    {
        return (m_keys == 1 && m_depth == 0);
    }

protected:
    void clear_key()
    {
        if (m_keys != 1) {
            uint32_t msig = 1 << (31 - __builtin_clz(m_keys));
            m_keys ^= msig;
        }
    }

    int key_depth() const
    {
        return __builtin_popcount(m_keys);
    }

    virtual bool UInteger(uint64_t val);

protected:
    uint32_t m_keys;
    int m_depth;
    int m_max_depth;

private:
    const Folder * const *m_fold;
    const UIntInfo *m_uint_info;
    uint8_t *m_dest;
};

#endif

