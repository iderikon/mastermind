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

#include <Backend.h>
#include <Discovery.h>
#include <Group.h>
#include <Node.h>
#include <Storage.h>
#include <ThreadPool.h>
#include <WorkerApplication.h>

#include "Samples.h"

#include <boost/asio.hpp>
#include <gtest/gtest.h>

using namespace boost::asio::ip;

void send_empty_stat()
{
    try {
        boost::asio::io_service service;
        tcp::acceptor acceptor(service, tcp::endpoint(tcp::v6(), 12000));

        tcp::socket socket(service);
        acceptor.accept(socket);

        char buf[128];
        std::memset(buf, 0, sizeof(buf));
        socket.read_some(boost::asio::buffer(buf));
        ASSERT_EQ(0, std::strncmp("GET /?categories=80 HTTP", buf, 24));

        const char *response = 
            "HTTP/1.1 200 OK\n"
            "Date: This ain't August '69\n"
            "Connection: close\n"
            "Content-Type: application/json\n"
            "Content-length: 46\n\n"
            "{\"timestamp\":{\"tv_sec\":934736400,\"tv_usec\":0}}";

        boost::asio::write(socket, boost::asio::buffer(response));
    } catch (std::exception & e) {
        ASSERT_TRUE(0) << e.what();
    }
}

void make_timeout()
{
    try {
        boost::asio::io_service service;
        tcp::acceptor acceptor(service, tcp::endpoint(tcp::v6(), 12000));

        tcp::socket socket(service);
        acceptor.accept(socket);

        sleep(3);
    } catch (std::exception & e) {
        ASSERT_TRUE(0) << e.what();
    }
}

TEST(Discovery, Download)
{
    WorkerApplication app;
    init_logger(app);
    app.get_config().monitor_port = 12000;

    Discovery & discovery = app.get_discovery();
    Storage & storage = app.get_storage();

    storage.add_node("::1", 1025, 10);

    std::vector<Node*> nodes;
    storage.get_nodes(nodes);
    ASSERT_EQ(1, nodes.size());
    ASSERT_EQ(std::string("::1:1025:10"), nodes[0]->get_key());

    std::thread thread(send_empty_stat);

    ThreadPool & thread_pool = app.get_thread_pool();
    thread_pool.start();

    sleep(1);

    discovery.init_curl();
    int rc = discovery.discover_nodes(nodes);

    thread.join();

    ASSERT_EQ(1, rc);

    thread_pool.flush();

    EXPECT_EQ(934736400, nodes[0]->get_stat().ts_sec);
}

TEST(Discovery, Timeout)
{
    WorkerApplication app;
    init_logger(app);
    app.get_config().monitor_port = 12000;
    app.get_config().wait_timeout = 1;

    Discovery & discovery = app.get_discovery();
    Storage & storage = app.get_storage();

    storage.add_node("::1", 1025, 10);

    std::vector<Node*> nodes;
    storage.get_nodes(nodes);
    ASSERT_EQ(1, nodes.size());
    ASSERT_EQ(std::string("::1:1025:10"), nodes[0]->get_key());

    std::thread thread(make_timeout);

    app.get_thread_pool().start();
   
    sleep(1);

    discovery.init_curl();
    int rc = app.get_discovery().discover_nodes(nodes);

    thread.join();

    ASSERT_EQ(0, rc);
}
