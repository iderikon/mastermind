/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
  
   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.
  
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
  
   You should have received a copy of the GNU Lesser General Public
   License along with this library.
*/

#include <Filter.h>
#include <FilterParser.h>

#include <rapidjson/reader.h>

#include <gtest/gtest.h>

static const char *filter_str = "\
{\
    \"filter\" : {\
        \"namespaces\" : [ \"default\" ],\
        \"couples\" : [ \"1:2\", \"3:4\" ],\
        \"groups\" : [ 1, 2, 3, 4 ],\
        \"nodes\" : [ \"::1:1025:10\" ],\
        \"filesystems\" : [ \"::1:1025:10/123\" ],\
        \"backends\" : [ \"::1:1025:10/1\" ]\
    },\
    \"item_types\" : [ \"couple\", \"group\", \"node\", \"fs\", \"backend\" ]\
}";

TEST(FilterParserTest, ParseFull)
{
    Filter filter;

    EXPECT_EQ(true, filter.empty());
    EXPECT_EQ(0, filter.item_types);

    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(filter_str);
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());

    filter.sort();

    EXPECT_EQ(1, filter.namespaces.size());
    EXPECT_EQ(2, filter.couples.size());
    EXPECT_EQ(4, filter.groups.size());
    EXPECT_EQ(1, filter.nodes.size());
    EXPECT_EQ(1, filter.filesystems.size());
    EXPECT_EQ(1, filter.backends.size());

    EXPECT_EQ(Filter::Couple | Filter::Group | Filter::Node | Filter::FS | Filter::Backend,
            filter.item_types);

    ASSERT_FALSE(filter.empty());

    EXPECT_EQ(std::vector<std::string>({ "default" }), filter.namespaces);
    EXPECT_EQ(std::vector<std::string>({ "1:2", "3:4" }), filter.couples);
    EXPECT_EQ(std::vector<int>({ 1, 2, 3, 4 }), filter.groups);
    EXPECT_EQ(std::vector<std::string>({ "::1:1025:10" }), filter.nodes);
    EXPECT_EQ(std::vector<std::string>({ "::1:1025:10/123" }), filter.filesystems);
    EXPECT_EQ(std::vector<std::string>({ "::1:1025:10/1" }), filter.backends);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
