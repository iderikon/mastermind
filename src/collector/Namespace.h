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

#ifndef __3eb3aef5_b268_43d7_b417_44b800716ce3
#define __3eb3aef5_b268_43d7_b417_44b800716ce3

#include <rapidjson/writer.h>

#include <set>
#include <string>
#include <vector>

class Couple;

class Namespace
{
public:
    struct CoupleLess
    {
        bool operator () (const Couple & a, const Couple & b) const
        { return &a < &b; }
    };
    typedef std::set<std::reference_wrapper<Couple>, CoupleLess> Couples;

    struct Settings
    {
        Settings()
            : reserved_space(0.0)
        {}

        double reserved_space;
    };

public:
    Namespace(const std::string & id);

    const std::string & get_id() const
    { return m_id; }

    void add_couple(Couple & couple)
    { m_couples.insert(couple); }

    void remove_couple(Couple & couple)
    { m_couples.erase(couple); }

    bool default_settings() const
    { return m_default_settings; }

    const Settings & get_settings() const
    { return m_settings; }

    // Obtain a list of items of certain types related to this namespace,
    // e.g. couples, their groups. References to objects will be pushed
    // into specified vector.
    // Note that some items may be duplicated.
    void push_items(std::vector<std::reference_wrapper<Group>> & groups) const;
    void push_items(std::vector<std::reference_wrapper<Node>> & nodes) const;
    void push_items(std::vector<std::reference_wrapper<Backend>> & backends) const;
    void push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const;
    void push_items(std::vector<std::reference_wrapper<Couple>> & couples) const;

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    const std::string m_id;

    // Set of references to couples in this namespace. The objects shouldn't be
    // modified directly but only used by push_items().
    Couples m_couples;

    bool m_default_settings;
    Settings m_settings;
};

#endif

