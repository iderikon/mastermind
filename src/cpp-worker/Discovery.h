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

#ifndef __d3b51655_27e8_4227_a7eb_a598183c39cd
#define __d3b51655_27e8_4227_a7eb_a598183c39cd

#include <curl/curl.h>
#include <elliptics/session.hpp>
#include <sys/time.h>

#include <map>
#include <memory>
#include <utility>
#include <vector>

class Node;
class WorkerApplication;

class Discovery
{
public:
    Discovery(WorkerApplication & app);
    ~Discovery();

    int init();

    void resolve_nodes();

    int start();

    bool in_progress() const
    { return m_in_progress; }

    void end();

    void schedule_start();

    double get_last_duration() const
    { return m_last_duration; }

private:
    CURL *create_easy_handle(Node *node, const char *stat);

private:
    static int handle_socket(CURL *easy, curl_socket_t fd,
            int action, void *userp, void *socketp);

    static int handle_timer(CURLM *multi, long timeout_ms, void *userp);

    static size_t write_func(char *ptr, size_t size,
            size_t nmemb, void *userdata);

private:
    class DiscoveryStart;

private:
    WorkerApplication & m_app;

    std::unique_ptr<ioremap::elliptics::node> m_node;
    std::unique_ptr<ioremap::elliptics::session> m_session;

    DiscoveryStart *m_discovery_start;

    int m_http_port;
    int m_epollfd;
    CURLM *m_curl_handle;
    long m_timeout_ms;

    timeval m_start_tv;
    double m_last_duration;
    bool m_in_progress;
};

#endif

