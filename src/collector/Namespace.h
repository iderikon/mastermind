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

#include <set>
#include <string>
#include <vector>

class Group;

class Namespace
{
public:
    Namespace(const std::string & name)
        : m_name(name)
    {}

    const std::string & get_name() const
    { return m_name; }

    void add_group(Group & group)
    { m_groups.insert(&group); }

    void remove_group(Group & group)
    { m_groups.erase(&group); }

    std::set<Group*> & get_groups()
    { return m_groups; }

    // NB: get_items() may return duplicates

    void get_items(std::vector<Group*> & groups) const
    {
        groups.insert(groups.end(), m_groups.begin(), m_groups.end());
    }

    void get_items(std::vector<Node*> & nodes) const
    {
        for (Group *group : m_groups)
            group->get_items(nodes);
    }

    void get_items(std::vector<Backend*> & backends) const
    {
        for (Group *group : m_groups)
            group->get_items(backends);
    }

    void get_items(std::vector<FS*> & filesystems) const
    {
        for (Group *group : m_groups)
            group->get_items(filesystems);
    }

    void get_items(std::vector<Couple*> & couples) const
    {
        for (Group *group : m_groups)
            group->get_items(couples);
    }

private:
    const std::string m_name;
    std::set<Group*> m_groups;
};

#endif

