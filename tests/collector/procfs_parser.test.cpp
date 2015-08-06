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

#include <Node.h>
#include <ProcfsParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

static const char *procfs_str =
"{\
    \"procfs\": {\
        \"net\": {\
            \"net_interfaces\": {\
                \"eth0\": {\
                    \"receive\": {\
                        \"bytes\": 101\
                    },\
                    \"transmit\": {\
                        \"bytes\": 103\
                    }\
                },\
                \"eth1\": {\
                    \"receive\": {\
                        \"bytes\": 107\
                    },\
                    \"transmit\": {\
                        \"bytes\": 109\
                    }\
                },\
                \"lo\": {\
                    \"receive\": {\
                        \"bytes\": 127\
                    },\
                    \"transmit\": {\
                        \"bytes\": 131\
                    }\
                }\
            }\
        },\
        \"vm\": {\
            \"la\": [\
                137,\
                139,\
                149\
            ]\
        }\
    },\
    \"timestamp\": {\
        \"tv_sec\": 151,\
        \"tv_usec\": 157\
    }\
}";

TEST(ProcfsParserTest, ParseFull)
{
    ProcfsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream stream(procfs_str);
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());

    const NodeStat & stat = parser.get_stat();
    EXPECT_EQ(101 + 107, stat.rx_bytes);
    EXPECT_EQ(103 + 109, stat.tx_bytes);
    EXPECT_EQ(137, stat.la1);
    EXPECT_EQ(151, stat.ts_sec);
    EXPECT_EQ(157, stat.ts_usec);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
