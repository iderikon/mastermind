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

#ifndef __3eb3aef5_b268_43d7_b417_44b800716ce3
#define __3eb3aef5_b268_43d7_b417_44b800716ce3

#include "RWSpinLock.h"

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

    void add_couple(Couple *couple);

    size_t get_couple_count() const;
    void get_couples(std::vector<Couple*> & couples) const;

private:
    const std::string m_name;

    std::set<Couple*> m_couples;
    mutable RWSpinLock m_couples_lock;
};

#endif

