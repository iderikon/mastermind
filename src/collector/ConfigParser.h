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

#ifndef __3d3b42fb_427b_4146_bdf4_42e8719d976f
#define __3d3b42fb_427b_4146_bdf4_42e8719d976f

#include "Config.h"
#include "Parser.h"

#include <string>
#include <vector>

class ConfigParser : public Parser
{
    typedef Parser super;

public:
    ConfigParser(Config & config);

    virtual bool Bool(bool b)
    { return UInteger(uint64_t(b)); }

    virtual bool String(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool StartArray();

    virtual bool UInteger(uint64_t val);

    virtual bool EndArray(rapidjson::SizeType nr_elements);

    const Config & get_config() const
    { return m_config; }

private:
    int m_array_depth;

    Config::NodeInfo m_current_node;
    Config & m_config;
};

#endif

