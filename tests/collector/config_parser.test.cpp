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

#include <Config.h>
#include <ConfigParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

namespace {

struct TestConfig : public Config
{
    TestConfig()
    {
        monitor_port = 1;
        wait_timeout = 2;
        forbidden_dht_groups = 0;
        forbidden_unmatched_group_total_space = 1;
        reserved_space = 3;
        dnet_log_mask = 5;
        net_thread_num = 7;
        io_thread_num = 11;
        nonblocking_io_thread_num = 13;

        NodeInfo node;
        node.host = "::17";
        node.port = 19;
        node.family = 23;
        nodes.push_back(node);
        node.host = "::29";
        node.port = 31;
        node.family = 37;
        nodes.push_back(node);
    }
};

TestConfig s_config;

void generate_json(std::string & str)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
        writer.Key("elliptics");
        writer.StartObject();
            writer.Key("monitor_port");
            writer.Uint64(s_config.monitor_port);
            writer.Key("wait_timeout");
            writer.Uint64(s_config.wait_timeout);
            writer.Key("nodes");
            writer.StartArray();
            for (Config::NodeInfo & node : s_config.nodes) {
                writer.StartArray();
                    writer.String(node.host.c_str());
                    writer.Uint64(node.port);
                    writer.Uint64(node.family);
                writer.EndArray();
            }
            writer.EndArray();
        writer.EndObject();
        writer.Key("forbidden_dht_groups");
        writer.Bool(s_config.forbidden_dht_groups);
        writer.Key("forbidden_unmatched_group_total_space");
        writer.Bool(s_config.forbidden_unmatched_group_total_space);
        writer.Key("reserved_space");
        writer.Uint64(s_config.reserved_space);
        writer.Key("dnet_log_mask");
        writer.Uint64(s_config.dnet_log_mask);
        writer.Key("net_thread_num");
        writer.Uint64(s_config.net_thread_num);
        writer.Key("io_thread_num");
        writer.Uint64(s_config.io_thread_num);
        writer.Key("nonblocking_io_thread_num");
        writer.Uint64(s_config.nonblocking_io_thread_num);
    writer.EndObject();

    str = buf.GetString();
}

} // unnamed namespace

TEST(ConfigParserTest, ParseFull)
{
    std::string str;
    generate_json(str);

    Config config;
    ConfigParser parser(config);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(str.c_str());
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());

    EXPECT_EQ(s_config.monitor_port, config.monitor_port);
    EXPECT_EQ(s_config.wait_timeout, config.wait_timeout);
    EXPECT_EQ(s_config.forbidden_dht_groups, config.forbidden_dht_groups);
    EXPECT_EQ(s_config.forbidden_unmatched_group_total_space, config.forbidden_unmatched_group_total_space);
    EXPECT_EQ(s_config.reserved_space, config.reserved_space);
    EXPECT_EQ(s_config.dnet_log_mask, config.dnet_log_mask);
    EXPECT_EQ(s_config.net_thread_num, config.net_thread_num);
    EXPECT_EQ(s_config.io_thread_num, config.io_thread_num);
    EXPECT_EQ(s_config.nonblocking_io_thread_num, config.nonblocking_io_thread_num);

    EXPECT_EQ(s_config.nodes.size(), config.nodes.size());
    for (size_t i = 0; i < s_config.nodes.size(); ++i) {
        EXPECT_EQ(s_config.nodes[i].host, config.nodes[i].host);
        EXPECT_EQ(s_config.nodes[i].port, config.nodes[i].port);
        EXPECT_EQ(s_config.nodes[i].family, config.nodes[i].family);
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
