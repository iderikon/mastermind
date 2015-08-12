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

#include <Couple.h>
#include <Group.h>
#include <Namespace.h>
#include <WorkerApplication.h>

#include <gtest/gtest.h>

TEST(Namespace, Constructor)
{
    Namespace ns("summer");

    ns.~Namespace();
    memset(&ns, 0xAA, sizeof(ns));
    new (&ns) Namespace("summer");

    EXPECT_EQ(std::string("summer"), ns.get_name());
    EXPECT_EQ(0, ns.get_couple_count());

    Couple couple(std::vector<Group*>(), false);

    ns.add_couple(&couple);
    EXPECT_EQ(1, ns.get_couple_count());

    std::vector<Couple*> couples;
    ns.get_couples(couples);
    ASSERT_EQ(1, couples.size());
    EXPECT_EQ(&couple, couples[0]);
}
