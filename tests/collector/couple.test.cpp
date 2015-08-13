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
#include <WorkerApplication.h>

#include <gtest/gtest.h>

TEST(Couple, Constructor)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();

    Group g1(1, storage);
    Group g2(2, storage);
    Group g3(3, storage);

    Couple couple(std::vector<Group*>({&g1, &g2, &g3}), false);
    couple.~Couple();
    memset(&couple, 0xAA, sizeof(couple));
    new (&couple) Couple(std::vector<Group*>({&g1, &g2, &g3}), false);

    ASSERT_TRUE(couple.check(std::vector<int>({1, 2, 3})));
    EXPECT_EQ(Couple::INIT, couple.get_status());
    EXPECT_EQ(0, couple.get_update_status_time());
    EXPECT_NE(nullptr, couple.get_status_text());

    couple.bind_groups();
    EXPECT_EQ(&couple, g1.get_couple());
    EXPECT_EQ(&couple, g2.get_couple());
    EXPECT_EQ(&couple, g3.get_couple());
    EXPECT_EQ(std::string("1:2:3"), couple.get_key());

    std::vector<int> group_ids;
    couple.get_group_ids(group_ids);
    ASSERT_EQ(std::vector<int>({1, 2, 3}), group_ids);

    std::vector<Group*> groups;
    couple.get_groups(groups);
    ASSERT_EQ(std::vector<Group*>({&g1, &g2, &g3}), groups);
}
