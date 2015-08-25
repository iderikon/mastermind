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

class Couple;

class Namespace
{
public:
    Namespace(const std::string & name)
        : m_name(name)
    {}

    const std::string & get_name() const
    { return m_name; }

    void add_couple(Couple & couple)
    { m_couples.insert(&couple); }

    void remove_couple(Couple & couple)
    { m_couples.erase(&couple); }

    std::set<Couple*> & get_couples()
    { return m_couples; }

    // NB: get_items() may return duplicates

    void get_items(std::vector<Couple*> & couples) const
    {
        couples.insert(couples.end(), m_couples.begin(), m_couples.end());
    }

    void get_items(std::vector<Node*> & nodes) const
    {
        for (Couple *couple : m_couples)
            couple->get_items(nodes);
    }

    void get_items(std::vector<Backend*> & backends) const
    {
        for (Couple *couple : m_couples)
            couple->get_items(backends);
    }

    void get_items(std::vector<Group*> & groups) const
    {
        for (Couple *couple : m_couples)
            couple->get_items(groups);
    }

    void get_items(std::vector<FS*> & filesystems) const
    {
        for (Couple *couple : m_couples)
            couple->get_items(filesystems);
    }

private:
    const std::string m_name;
    std::set<Couple*> m_couples;
};

#endif

