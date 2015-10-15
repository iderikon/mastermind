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

#ifndef __0a9112e2_6324_4744_86e3_735f642bb929
#define __0a9112e2_6324_4744_86e3_735f642bb929

#include <rapidjson/writer.h>

#include <string>

class Host
{
public:
    Host(const std::string & addr)
        :
        m_addr(addr)
    {}

    const std::string & get_addr() const
    { return m_addr; }

    const std::string & get_name() const
    { return m_name; }

    const std::string & get_dc() const
    { return m_dc; }

    void set_name(const std::string & name)
    { m_name = name; }

    void set_dc(const std::string & dc)
    { m_dc = dc; }

    void merge(const Host & other, bool & have_newer);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    std::string m_addr;
    std::string m_name;
    std::string m_dc;
};

#endif

