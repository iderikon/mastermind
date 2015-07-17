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
#include "Node.h"
#include "Storage.h"
#include "ThreadPool.h"
#include "WorkerApplication.h"

#include <sys/epoll.h>

#include <set>

#include <errno.h>
#include <fcntl.h>
#include <string.h>

using namespace ioremap;

namespace {

class UpdateStorage : public ThreadPool::Job
{
public:
    UpdateStorage(Storage & storage, elliptics::session & session)
        :
        m_storage(storage),
        m_session(session.clone())
    {}

    virtual void execute()
    {
        m_storage.schedule_update(m_session);
    }

private:
    Storage & m_storage;
    elliptics::session m_session;
};

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

Discovery::Discovery(WorkerApplication & app)
    :
    m_app(app),
    m_http_port(10025),
    m_epollfd(-1),
    m_curl_handle(NULL),
    m_timeout_ms(0)
{
    m_discovery_start = new DiscoveryStart(*this);
}

Discovery::~Discovery()
{
    curl_global_cleanup();
    delete m_discovery_start;
}

int Discovery::init()
{
    if (curl_global_init(CURL_GLOBAL_ALL)) {
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "Failed to initialize libcurl");
        return -1;
    }

    const Config & config = m_app.get_config();

    m_http_port = config.monitor_port;

    m_node.reset(new elliptics::node(elliptics::logger(m_app.get_logger(), blackhole::log::attributes_t())));

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

    return 0;
}

void Discovery::resolve_nodes()
{
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

        if (m_app.get_storage().add_node(host, port, addr.family)) {
            elliptics::address el_addr(addr);
            m_node->add_remote(el_addr);
        }
    }
}

int Discovery::start()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Starting discovery");

    resolve_nodes();

    std::vector<Node*> nodes;
    m_app.get_storage().get_nodes(nodes);

    if (nodes.empty())
        return 0;

    CurlGuard guard(m_curl_handle, m_epollfd);

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

        if (node.get_download_state() != Node::DownloadStateEmpty) {
            BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG, "Node %s is in download state %d",
                    node.get_host().c_str(), node.get_download_state());
            continue;
        }

        node.set_download_state(Node::DownloadStateBackend);
        CURL *easy = create_easy_handle(&node, "backend");
        if (easy == NULL)
            return -1;
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

                curl_multi_remove_handle(m_curl_handle, easy);
                curl_easy_cleanup(easy);

                if (node->get_download_state() == Node::DownloadStateBackend) {
                    BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG,
                            "Node %s statistics: backend done", node->get_host().c_str());

                    ThreadPool::Job *job = node->create_backend_parse_job();
                    m_app.get_thread_pool().dispatch(job);

                    node->set_download_state(Node::DownloadStateProcfs);
                    easy = create_easy_handle(node, "procfs");
                    curl_multi_add_handle(m_curl_handle, easy);

                    if (running_handles == 0)
                        curl_multi_socket_action(m_curl_handle, CURL_SOCKET_TIMEOUT, 0, &running_handles);

                } else if (node->get_download_state() == Node::DownloadStateProcfs) {
                    BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG,
                            "Node %s statistics: procfs done", node->get_host().c_str());

                    ThreadPool::Job *job = node->create_procfs_parse_job();
                    m_app.get_thread_pool().dispatch(job);
                    node->set_download_state(Node::DownloadStateEmpty);
                }
            }
        } while (msg);
    }

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Discovery completed successfully");

    assert(m_session != NULL);
    m_app.get_thread_pool().dispatch_after(new UpdateStorage(m_app.get_storage(), *m_session));

    return 0;
}

void Discovery::dispatch_start()
{
    m_app.get_thread_pool().dispatch(new DiscoveryStart(*this));
}

CURL *Discovery::create_easy_handle(Node *node, const char *stat)
{
    CURL *easy;
    easy = curl_easy_init();

    if (easy == NULL)
        return NULL;

    char buf[128];
    sprintf(buf, "http://%s:%u/%s", node->get_host().c_str(), m_http_port, stat);

    curl_easy_setopt(easy, CURLOPT_URL, buf);
    curl_easy_setopt(easy, CURLOPT_PRIVATE, node);
    curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "deflate");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &write_func);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, node);

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
