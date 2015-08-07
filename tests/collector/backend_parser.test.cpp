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

#include <Backend.h>
#include <BackendParser.h>

#include <rapidjson/writer.h>

#include <gtest/gtest.h>

namespace {

struct TestBackendStat1 : public BackendStat
{
    TestBackendStat1()
    {
        backend_id = 1;
        state = 2;
        vfs_blocks = 3;
        vfs_bavail = 5;
        vfs_bsize = 7;
        records_total = 11;
        records_removed = 13;
        records_removed_size = 17;
        base_size = 19;
        fsid = 23;
        defrag_state = 29;
        want_defrag = 31;
        read_ios = 37;
        write_ios = 41;
        error = 43;
        blob_size_limit = 47;
        max_blob_base_size = 53;
        blob_size = 59;
        group = 61;
    }
} b1_stat;

struct TestBackendStat2 : public BackendStat
{
    TestBackendStat2()
    {
        backend_id = 71;
        state = 73;
        vfs_blocks = 79;
        vfs_bavail = 83;
        vfs_bsize = 89;
        records_total = 97;
        records_removed = 101;
        records_removed_size = 103;
        base_size = 107;
        fsid = 109;
        defrag_state = 113;
        want_defrag = 127;
        read_ios = 131;
        write_ios = 137;
        error = 139;
        blob_size_limit = 149;
        max_blob_base_size = 151;
        blob_size = 157;
        group = 163;
    }
} b2_stat;

const int s_ts_sec = 173;
const int s_ts_usec = 179;

void generate_json(rapidjson::Writer<rapidjson::StringBuffer> & writer, const BackendStat & stat)
{
    writer.StartObject();
        writer.Key("backend_id");
        writer.Uint64(stat.backend_id);

        writer.Key("backend");
        writer.StartObject();
            writer.Key("dstat");
            writer.StartObject();
                writer.Key("read_ios");
                writer.Uint64(stat.read_ios);
                writer.Key("write_ios");
                writer.Uint64(stat.write_ios);
                writer.Key("error");
                writer.Uint64(stat.error);
            writer.EndObject();

            writer.Key("vfs");
            writer.StartObject();
                writer.Key("blocks");
                writer.Uint64(stat.vfs_blocks);
                writer.Key("bavail");
                writer.Uint64(stat.vfs_bavail);
                writer.Key("bsize");
                writer.Uint64(stat.vfs_bsize);
                writer.Key("fsid");
                writer.Uint64(stat.fsid);
            writer.EndObject();

            writer.Key("summary_stats");
            writer.StartObject();
                writer.Key("records_total");
                writer.Uint64(stat.records_total);
                writer.Key("records_removed");
                writer.Uint64(stat.records_removed);
                writer.Key("records_removed_size");
                writer.Uint64(stat.records_removed_size);
                writer.Key("want_defrag");
                writer.Uint64(stat.want_defrag);
                writer.Key("base_size");
                writer.Uint64(stat.base_size);
            writer.EndObject();

            writer.Key("config");
            writer.StartObject();
                writer.Key("blob_size_limit");
                writer.Uint64(stat.blob_size_limit);
                writer.Key("blob_size");
                writer.Uint64(stat.blob_size);
                writer.Key("group");
                writer.Uint64(stat.group);
            writer.EndObject();

            writer.Key("base_stats");
            writer.StartObject();
                writer.Key("little_file");
                writer.StartObject();
                    writer.Key("base_size");
                    writer.Uint64(stat.max_blob_base_size - 1);
                writer.EndObject();

                writer.Key("big_file");
                writer.StartObject();
                    writer.Key("base_size");
                    writer.Uint64(stat.max_blob_base_size);
                writer.EndObject();
            writer.EndObject();

        writer.EndObject();

        writer.Key("status");
        writer.StartObject();
            writer.Key("defrag_state");
            writer.Uint64(stat.defrag_state);
            writer.Key("state");
            writer.Uint64(stat.state);
        writer.EndObject();

    writer.EndObject();
}

void generate_json(std::string & str)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
        writer.Key("backends");
        writer.StartObject();
            writer.Key("1");
            generate_json(writer, b1_stat);
            writer.Key("2");
            generate_json(writer, b2_stat);
        writer.EndObject();
    writer.EndObject();

    str = buf.GetString();
}

void check_backend_stat(const BackendStat & stat)
{
    static int s_stats_received = 0;
    static bool b1_processed = false;
    static bool b2_processed = false;

    ASSERT_LT(s_stats_received, 2);
    ++s_stats_received;

    const BackendStat *ref = NULL;

    if (stat.backend_id == b1_stat.backend_id) {
        ASSERT_FALSE(b1_processed);
        ref = &b1_stat;
        b1_processed = true;
    } else if (stat.backend_id == b2_stat.backend_id) {
        ASSERT_FALSE(b2_processed);
        ref = &b2_stat;
        b2_processed = true;
    } else {
        ASSERT_TRUE(false) << "Unexpected backend id " << stat.backend_id;
    }

    EXPECT_EQ(ref->ts_sec, stat.ts_sec);
    EXPECT_EQ(ref->ts_usec, stat.ts_usec);
    EXPECT_EQ(ref->backend_id, stat.backend_id);
    EXPECT_EQ(ref->state, stat.state);
    EXPECT_EQ(ref->vfs_blocks, stat.vfs_blocks);
    EXPECT_EQ(ref->vfs_bavail, stat.vfs_bavail);
    EXPECT_EQ(ref->vfs_bsize, stat.vfs_bsize);
    EXPECT_EQ(ref->records_total, stat.records_total);
    EXPECT_EQ(ref->records_removed, stat.records_removed);
    EXPECT_EQ(ref->records_removed_size, stat.records_removed_size);
    EXPECT_EQ(ref->base_size, stat.base_size);
    EXPECT_EQ(ref->fsid, stat.fsid);
    EXPECT_EQ(ref->defrag_state, stat.defrag_state);
    EXPECT_EQ(ref->want_defrag, stat.want_defrag);
    EXPECT_EQ(ref->read_ios, stat.read_ios);
    EXPECT_EQ(ref->write_ios, stat.write_ios);
    EXPECT_EQ(ref->error, stat.error);
    EXPECT_EQ(ref->blob_size_limit, stat.blob_size_limit);
    EXPECT_EQ(ref->max_blob_base_size, stat.max_blob_base_size);
    EXPECT_EQ(ref->blob_size, stat.blob_size);
    EXPECT_EQ(ref->group, stat.group);
}

} // unnamed namespace

TEST(BackendParserTest, ParseFull)
{
    std::string str;
    generate_json(str);

    b1_stat.ts_sec = s_ts_sec;
    b1_stat.ts_usec = s_ts_usec;
    b2_stat.ts_sec = s_ts_sec;
    b2_stat.ts_usec = s_ts_usec;

    BackendParser parser(s_ts_sec, s_ts_usec, check_backend_stat);

    rapidjson::Reader reader;
    rapidjson::StringStream stream(str.c_str());
    reader.Parse(stream, parser);

    ASSERT_TRUE(parser.good());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
