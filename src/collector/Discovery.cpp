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

#include "Config.h"
#include "Discovery.h"
#include "Guard.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

#include <sys/epoll.h>

#include <set>

#include <errno.h>
#include <fcntl.h>
#include <string.h>

using namespace ioremap;

namespace {

struct dnet_addr_compare
{
    bool operator () (const dnet_addr & a, const dnet_addr & b) const
    {
        return memcmp(&a, &b, sizeof(dnet_addr)) < 0;
    }
};

class CurlGuard
{
public:
    CurlGuard(CURLM* & multi, int & epollfd)
        :
        m_multi(multi),
        m_epollfd(epollfd)
    {}

    ~CurlGuard()
    {
        if (m_multi != NULL) {
            curl_multi_cleanup(m_multi);
            m_multi = NULL;
        }

        if (m_epollfd >= 0) {
            close(m_epollfd);
            m_epollfd = -1;
        }
    }

private:
    CURLM* & m_multi;
    int & m_epollfd;
};

} // unnamed namespace

class Discovery::DiscoveryStart : public ThreadPool::Job
{
public:
    DiscoveryStart(Discovery & discovery)
        : m_discovery(discovery)
    {}

    virtual void execute()
    {
        m_discovery.start();
    }

    virtual void dispose()
    {
        clear();
    }

private:
    Discovery & m_discovery;
};

class Discovery::UpdateStorage : public ThreadPool::Job
{
public:
    UpdateStorage(WorkerApplication & app, elliptics::session & session)
        :
        m_app(app),
        m_session(session.clone())
    {}

    virtual void execute()
    {
        Discovery & discovery = m_app.get_discovery();

        clock_stop(discovery.m_clock.finish_monitor_stats_clk);
        discovery.m_clock.finish_monitor_stats = discovery.m_clock.finish_monitor_stats_clk;

        m_app.get_storage().schedule_update(m_session);
    }

private:
    WorkerApplication & m_app;
    elliptics::session m_session;
};

Discovery::Discovery(WorkerApplication & app)
    :
    m_app(app),
    m_http_port(10025),
    m_epollfd(-1),
    m_curl_handle(NULL),
    m_timeout_ms(0),
    m_in_progress(false)
{
    m_discovery_start = new DiscoveryStart(*this);
    m_snapshot_jobs.reserve(512);
}

Discovery::~Discovery()
{
    curl_global_cleanup();
    delete m_discovery_start;
}

int Discovery::init_curl()
{
    if (curl_global_init(CURL_GLOBAL_ALL)) {
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "Failed to initialize libcurl");
        return -1;
    }
    m_http_port = m_app.get_config().monitor_port;
    return 0;
}

int Discovery::init_elliptics()
{
    const Config & config = m_app.get_config();

    dnet_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.wait_timeout = config.wait_timeout;
    cfg.net_thread_num = config.net_thread_num;
    cfg.io_thread_num = config.io_thread_num;
    cfg.nonblocking_io_thread_num = config.nonblocking_io_thread_num;

    m_node.reset(new elliptics::node(
                elliptics::logger(m_app.get_elliptics_logger(), blackhole::log::attributes_t()), cfg));

    BH_LOG(m_app.get_logger(), DNET_LOG_NOTICE, "Initializing discovery");

    for (size_t i = 0; i < config.nodes.size(); ++i) {
        const Config::NodeInfo & info = config.nodes[i];

        try {
            m_node->add_remote(elliptics::address(info.host, info.port, info.family));
        } catch (std::exception & e) {
            BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "Failed to add remote '%s': %s\n", info.host, e.what());
            continue;
        } catch (...) {
            BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "Failed to add remote '%s' with unknown reason", info.host);
            continue;
        }
    }

    m_session.reset(new elliptics::session(*m_node));
    m_session->set_cflags(DNET_FLAGS_NOLOCK);

    return 0;
}

void Discovery::resolve_nodes()
{
    Stopwatch watch(m_clock.resolve_nodes);

    if (m_session == NULL) {
        BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "resolve_nodes: session is empty");
        return;
    }

    std::set<dnet_addr, dnet_addr_compare> addresses;

    std::vector<dnet_route_entry> routes;
    routes = m_session->get_routes();

    for (size_t i = 0; i < routes.size(); ++i)
        addresses.insert(routes[i].addr);

    for (auto it = addresses.begin(); it != addresses.end(); ++it) {
        const dnet_addr & addr = *it;

        const char *host = dnet_addr_host_string(&addr);
        int port = dnet_addr_port(&addr);

        m_app.get_storage().add_node(host, port, addr.family);
    }
}

int Discovery::start()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Starting discovery");

    m_in_progress = true;
    clock_start(m_clock.full_clk);

    resolve_nodes();

    std::vector<Node*> nodes;
    m_app.get_storage().get_nodes(nodes);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Have %lu nodes", nodes.size());

    if (nodes.empty())
        return 0;

    if (discover_nodes(nodes) < 0)
        return -1;

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Node discovery completed");

    assert(m_session != NULL);

    clock_start(m_clock.finish_monitor_stats_clk);
    m_app.get_thread_pool().dispatch_after(new UpdateStorage(m_app, *m_session));

    return 0;
}

int Discovery::discover_nodes(const std::vector<Node*> & nodes)
{
    Stopwatch watch(m_clock.discover_nodes);

    CurlGuard guard(m_curl_handle, m_epollfd);

    int res = 0;

    m_curl_handle = curl_multi_init();
    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETFUNCTION, handle_socket);
    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETDATA, (void *) this);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERFUNCTION, handle_timer);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERDATA, (void *) this);

    m_epollfd = epoll_create(1);
    if (m_epollfd < 0) {
        int err = errno;
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "epoll_create() failed: %s", strerror(err));
        return -1;
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        Node & node = *nodes[i];

        BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Scheduling stat download for node %s",
                node.get_key().c_str());

        CURL *easy = create_easy_handle(&node);
        if (easy == NULL) {
            BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                    "Cannot create easy handle to download node stat");
            return -1;
        }
        curl_multi_add_handle(m_curl_handle, easy);
    }

    int running_handles = 0;

    // kickstart download
    curl_multi_socket_action(m_curl_handle, CURL_SOCKET_TIMEOUT, 0, &running_handles);

    while (running_handles) {
        struct epoll_event event;
        int rc = epoll_wait(m_epollfd, &event, 1, 100);

        if (rc < 0) {
            if (errno == EINTR)
                continue;

            int err = errno;
            BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "epoll_wait() failed: %s", strerror(err));
            return -1;
        }

        if (rc == 0)
            curl_multi_socket_action(m_curl_handle, CURL_SOCKET_TIMEOUT, 0, &running_handles);
        else
            curl_multi_socket_action(m_curl_handle, event.data.fd, 0, &running_handles);

        struct CURLMsg *msg;
        do {
            int msgq = 0;
            msg = curl_multi_info_read(m_curl_handle, &msgq);

            if (msg && (msg->msg == CURLMSG_DONE)) {
                CURL *easy = msg->easy_handle;

                Node *node = NULL;
                CURLcode cc = curl_easy_getinfo(easy, CURLINFO_PRIVATE, &node);
                if (cc != CURLE_OK) {
                    BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "curl_easy_getinfo() failed");
                    return -1;
                }

                if (msg->data.result == CURLE_OK) {
                    BH_LOG(m_app.get_logger(), DNET_LOG_INFO,
                            "Node %s stat download completed", node->get_key().c_str());

                    ++res;
                    ThreadPool::Job *job = node->create_stats_parse_job();
                    m_app.get_thread_pool().dispatch(job);
                } else {
                    BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "Node %s stats download failed, "
                            "result: %d", node->get_key().c_str(), msg->data.result);

                    node->drop_download_data();
                }

                curl_multi_remove_handle(m_curl_handle, easy);
                curl_easy_cleanup(easy);

            }
        } while (msg);
    }

    return res;
}

void Discovery::end()
{
    if (!m_in_progress)
        return;

    clock_stop(m_clock.full_clk);
    m_clock.full = m_clock.full_clk;

    LockGuard<SpinLock> guard(m_progress_lock);

    for (ThreadPool::Job *job : m_snapshot_jobs)
        m_app.get_thread_pool().dispatch_aggregate(job);
    m_snapshot_jobs.clear();
    m_in_progress = false;
}

void Discovery::schedule_start()
{
    LockGuard<SpinLock> guard(m_progress_lock);

    m_in_progress = true;
    m_app.get_thread_pool().dispatch_after(m_discovery_start);
}

void Discovery::take_over_snapshot(ThreadPool::Job *job)
{
    LockGuard<SpinLock> guard(m_progress_lock);

    if (!m_in_progress)
        m_app.get_thread_pool().dispatch_aggregate(job);
    else
        m_snapshot_jobs.push_back(job);
}

CURL *Discovery::create_easy_handle(Node *node)
{
    CURL *easy;
    easy = curl_easy_init();

    if (easy == NULL)
        return NULL;

    char buf[128];
    sprintf(buf, "http://%s:%u/?categories=80", node->get_host().c_str(), m_http_port);

    curl_easy_setopt(easy, CURLOPT_URL, buf);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, node);
    curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "deflate");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &write_func);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, node);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, m_app.get_config().wait_timeout);

    return easy;
}

int Discovery::handle_socket(CURL *easy, curl_socket_t fd,
        int action, void *userp, void *socketp)
{
    Discovery *self = (Discovery *) userp;
    if (self == NULL)
        return -1;

    struct epoll_event event;
    event.events = 0;
    event.data.fd = fd;

    if (action == CURL_POLL_REMOVE) {
        int rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_DEL, fd, &event);
        if (rc != 0 && errno != EBADF) {
            int err = errno;
            BH_LOG(self->m_app.get_logger(), DNET_LOG_WARNING, "CURL_POLL_REMOVE: %s", strerror(err));
        }
        return 0;
    }

    event.events = (action == CURL_POLL_INOUT) ? (EPOLLIN | EPOLLOUT) :
                   (action == CURL_POLL_IN) ? EPOLLIN :
                   (action == CURL_POLL_OUT) ? EPOLLOUT : 0;
    if (!event.events)
        return 0;

    int rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_ADD, fd, &event);
    if (rc < 0) {
        if (errno == EEXIST)
            rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_MOD, fd, &event);
        if (rc < 0) {
            int err = errno;
            BH_LOG(self->m_app.get_logger(), DNET_LOG_WARNING, "EPOLL_CTL_MOD: %s", strerror(err));
            return -1;
        }
    }

    return 0;
}

int Discovery::handle_timer(CURLM *multi, long timeout_ms, void *userp)
{
    Discovery *self = (Discovery *) userp;
    if (self == NULL)
        return -1;

    self->m_timeout_ms = timeout_ms;

    return 0;
}

size_t Discovery::write_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    if (userdata == NULL)
        return 0;

    Node *node = (Node *) userdata;
    node->add_download_data(ptr, size * nmemb);
    return size * nmemb;
}
