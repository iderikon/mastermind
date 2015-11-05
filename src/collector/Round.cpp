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

#include "CocaineHandlers.h"
#include "FS.h"
#include "GroupHistoryEntry.h"
#include "Metrics.h"
#include "Round.h"
#include "WorkerApplication.h"

#include "Job.h"

#include <errno.h>
#include <mongo/client/dbclient.h>
#include <sys/epoll.h>
#include <fcntl.h>

using namespace ioremap;

namespace {

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
        if (m_multi != nullptr) {
            curl_multi_cleanup(m_multi);
            m_multi = nullptr;
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

void empty_function(void *)
{}

} // unnamed namespace

Round::ClockStat::ClockStat()
{
    std::memset(this, 0, sizeof(*this));
}

Round::ClockStat & Round::ClockStat::operator = (const ClockStat & other)
{
    std::memcpy(this, &other, sizeof(*this));
    return *this;
}

Round::Round(Collector & collector)
    :
    m_collector(collector),
    m_old_storage_version(collector.get_storage_version()),
    m_session(collector.get_discovery().get_session().clone()),
    m_type(REGULAR),
    m_epollfd(-1),
    m_curl_handle(nullptr),
    m_timeout_ms(0)
{
    clock_start(m_clock.total);
    m_storage.reset(new Storage(collector.get_storage()));
    m_queue = dispatch_queue_create("round", DISPATCH_QUEUE_CONCURRENT);
}

Round::Round(Collector & collector, std::shared_ptr<on_force_update> handler)
    :
    m_collector(collector),
    m_old_storage_version(collector.get_storage_version()),
    m_session(collector.get_discovery().get_session().clone()),
    m_type(FORCED_FULL),
    m_epollfd(-1),
    m_curl_handle(nullptr),
    m_timeout_ms(0)
{
    clock_start(m_clock.total);
    m_storage.reset(new Storage(collector.get_storage()));
    m_queue = dispatch_queue_create("round", DISPATCH_QUEUE_CONCURRENT);
    m_on_force_handler = handler;
}

Round::Round(Collector & collector, std::shared_ptr<on_refresh> handler)
    :
    m_collector(collector),
    m_old_storage_version(collector.get_storage_version()),
    m_session(collector.get_discovery().get_session().clone()),
    m_type(FORCED_PARTIAL),
    m_epollfd(-1),
    m_curl_handle(nullptr),
    m_timeout_ms(0)
{
    clock_start(m_clock.total);
    m_storage.reset(new Storage(collector.get_storage()));
    m_queue = dispatch_queue_create("round", DISPATCH_QUEUE_CONCURRENT);
    m_on_refresh_handler = handler;
}

Round::~Round()
{
    dispatch_release(m_queue);
}

void Round::update_storage(Storage & storage, uint64_t version, bool & have_newer)
{
    Stopwatch watch(m_clock.merge_time);

    m_old_storage_version = version;
    m_storage->merge(storage, have_newer);
}

void Round::start()
{
    BH_LOG(app::logger(), DNET_LOG_INFO,
            "Starting %s discovery with %lu nodes",
            (m_type == REGULAR) ? "regular" : (m_type == FORCED_FULL) ? "forced full" : "forced partial",
            (m_type == FORCED_PARTIAL ? m_entries.nodes.size() : m_storage->get_nodes().size()));

    dispatch_async_f(m_queue, this, &Round::step2_1_jobs_and_history);
    dispatch_async_f(m_queue, this, &Round::step2_2_curl_download);
}

void Round::step2_1_jobs_and_history(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    Stopwatch watch(self.m_clock.mongo);

    try {
        const Config & config = app::config();

        if (config.metadata.url.empty() || config.metadata.jobs.db.empty()) {
            BH_LOG(app::logger(), DNET_LOG_WARNING,
                    "Not connecting to jobs database because it was not configured");
            return;
        }

        std::string errmsg;
        mongo::ConnectionString cs = mongo::ConnectionString::parse(config.metadata.url, errmsg);
        if (!cs.isValid()) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Mongo client ConnectionString error: %s", errmsg.c_str());
            return;
        }

        std::unique_ptr<mongo::DBClientReplicaSet> conn((mongo::DBClientReplicaSet *) cs.connect(
                errmsg, double(config.metadata.options.connectTimeoutMS) / 1000.0));
        if (conn == nullptr) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Connection failed: %s", errmsg.c_str());
            return;
        }

        // Jobs

        mongo::BSONObjBuilder builder;
        builder.append("id", 1);
        builder.append("status", 1);
        builder.append("group", 1);
        builder.append("type", 1);
        mongo::BSONObj jobs_fields = builder.obj();

        std::unique_ptr<mongo::DBClientCursor> cursor(conn->query(config.metadata.jobs.db + ".jobs",
                MONGO_QUERY("status" << mongo::NE << "completed"
                         << "status" << mongo::NE << "cancelled").readPref(
                             mongo::ReadPreference_PrimaryOnly, mongo::BSONArray()),
                0, 0, &jobs_fields).release());

        uint64_t ts = 0;
        clock_get_real(ts);
        std::vector<Job> jobs;
        size_t count = 0;

        while (cursor->more()) {
            mongo::BSONObj obj = cursor->next();
            try {
                Job job(obj, ts);
                jobs.emplace_back(std::move(job));
            } catch (const mongo::MsgAssertionException & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Failed to parse database record: %s\nBSON object: %s",
                        e.what(), obj.jsonString(mongo::Strict, 1));
            } catch (const std::exception & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Failed to initialize job: %s\nBSON object: %s",
                        e.what(), obj.jsonString(mongo::Strict, 1));
            } catch (...) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Initializing job: Unknown exception thrown");
            }
            ++count;
        }

        BH_LOG(app::logger(), DNET_LOG_INFO,
                "Successfully processed %lu of %lu active jobs", jobs.size(), count);

        self.m_storage->save_new_jobs(std::move(jobs), ts);

        // History

        time_t group_history_ts = ::time(nullptr);
        double previous_ts = self.m_storage->get_group_history_ts();

        std::vector<GroupHistoryEntry> group_history;

        cursor = conn->query(config.metadata.history.db + ".history",
                MONGO_QUERY("nodes.timestamp" << mongo::GT << previous_ts).readPref(
                        mongo::ReadPreference_PrimaryOnly, mongo::BSONArray()));

        while (cursor->more()) {
            mongo::BSONObj obj = cursor->next();

            try {
                GroupHistoryEntry entry(obj);
                if (!entry.empty()) {
                    BH_LOG(app::logger(), DNET_LOG_INFO, "Loaded group history entry:\n%s",
                            entry.to_string().c_str());
                    group_history.emplace_back(std::move(entry));
                }
            } catch (const mongo::MsgAssertionException & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Failed to parse history database record: %s\nBSON object: %s",
                        e.what(), obj.jsonString(mongo::Strict, 1));
            } catch (const std::exception & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Failed to initialize history entry: %s\nBSON object: %s",
                        e.what(), obj.jsonString(mongo::Strict, 1));
            } catch (...) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Initializing history entry: Unknown exception thrown\nBSON object: %s",
                        obj.jsonString(mongo::Strict, 1));
            }
        }

        self.m_storage->save_group_history(std::move(group_history), group_history_ts);

    } catch (const mongo::DBException & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "MongoDB thrown exception: %s", e.what());
    } catch (const std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Exception thrown while connecting to jobs database: %s", e.what());
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Unknown exception thrown while connecting to jobs database");
    }
}

void Round::step2_2_curl_download(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    if (self.m_type == FORCED_PARTIAL)
        self.m_storage->select(self.m_on_refresh_handler->get_filter(), self.m_entries);

    self.perform_download();

    clock_start(self.m_clock.finish_monitor_stats_and_jobs);
    dispatch_barrier_async_f(self.m_queue, &self, &Round::step3_prepare_metadata_download);
}

void Round::step3_prepare_metadata_download(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    clock_stop(self.m_clock.finish_monitor_stats_and_jobs);

    self.m_storage->process_node_backends();
    self.m_storage->process_new_jobs();

    self.m_nr_groups = (self.m_type != FORCED_PARTIAL
            ? self.m_storage->get_groups().size()
            : self.m_entries.groups.size());

    if (!self.m_nr_groups) {
        BH_LOG(app::logger(), DNET_LOG_INFO,
                "No groups to download metadata");
        step4_perform_update(arg);
    }

    BH_LOG(app::logger(), DNET_LOG_INFO,
            "Scheduling metadata download for %lu groups", self.m_nr_groups);

    clock_start(self.m_clock.metadata_download);

    self.m_groups_to_read.reserve(self.m_nr_groups);
    self.m_group_read_sessions.reserve(self.m_nr_groups);

    if (self.m_type != FORCED_PARTIAL) {
        std::map<int, Group> & groups = self.m_storage->get_groups();
        for (auto it = groups.begin(); it != groups.end(); ++it)
            self.m_groups_to_read.push_back(it->second);
    } else {
        for (Group & group : self.m_entries.groups)
            self.m_groups_to_read.push_back(group);
    }

    for (int i = 0; i < self.m_nr_groups; ++i)
        self.m_group_read_sessions.push_back(self.m_session.clone());

    dispatch_async_f(self.m_queue, &self, &Round::request_metadata_apply_helper);
}

void Round::request_metadata_apply_helper(void *arg)
{
    Round & self = *static_cast<Round*>(arg);
    dispatch_apply_f(self.m_nr_groups, self.m_queue, &self, &Round::request_group_metadata);
}

void Round::step4_perform_update(void *arg)
{
    Round & self = *static_cast<Round*>(arg);

    self.m_groups_to_read.clear();
    self.m_group_read_sessions.clear();

    Stopwatch watch(self.m_clock.storage_update);
    self.m_storage->update();
    watch.stop();

    self.m_collector.finalize_round(&self);
}

void Round::request_group_metadata(void *arg, size_t idx)
{
    Round & self = *static_cast<Round*>(arg);

    std::vector<int> group_id(1, self.m_groups_to_read[idx].get().get_id());
    static const elliptics::key key("symmetric_groups");

    elliptics::session & session = self.m_group_read_sessions[idx];
    session.set_namespace("metabalancer");
    session.set_groups(group_id);

    BH_LOG(app::logger(), DNET_LOG_DEBUG,
            "Scheduling metadata download for group %d", group_id[0]);

    elliptics::async_read_result res = session.read_data(key, group_id, 0, 0);
    res.connect(std::bind(&Round::result, &self, idx, std::placeholders::_1),
            std::bind(&Round::final, &self, idx, std::placeholders::_1));
}

int Round::perform_download()
{
    Stopwatch watch(m_clock.perform_download);

    CurlGuard guard(m_curl_handle, m_epollfd);

    int res = 0;

    m_curl_handle = curl_multi_init();
    if (!m_curl_handle) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "curl_multi_init() failed");
        return -1;
    }

    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETFUNCTION, handle_socket);
    curl_multi_setopt(m_curl_handle, CURLMOPT_SOCKETDATA, (void *) this);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERFUNCTION, handle_timer);
    curl_multi_setopt(m_curl_handle, CURLMOPT_TIMERDATA, (void *) this);

    m_epollfd = epoll_create(1);
    if (m_epollfd < 0) {
        int err = errno;
        BH_LOG(app::logger(), DNET_LOG_ERROR, "epoll_create() failed: %s", strerror(err));
        return -1;
    }

    if (m_type == FORCED_PARTIAL) {
        for (Node & node : m_entries.nodes)
            add_download(node);
    } else {
        std::map<std::string, Node> & storage_nodes = m_storage->get_nodes();
        for (auto it = storage_nodes.begin(); it != storage_nodes.end(); ++it)
            add_download(it->second);
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
            BH_LOG(app::logger(), DNET_LOG_ERROR, "epoll_wait() failed: %s", strerror(err));
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

                Node *node = nullptr;
                CURLcode cc = curl_easy_getinfo(easy, CURLINFO_PRIVATE, &node);
                if (cc != CURLE_OK) {
                    BH_LOG(app::logger(), DNET_LOG_ERROR, "curl_easy_getinfo() failed");
                    return -1;
                }

                if (msg->data.result == CURLE_OK) {
                    BH_LOG(app::logger(), DNET_LOG_INFO,
                            "Node %s stat download completed", node->get_key().c_str());
                    dispatch_async_f(m_queue, node, &Node::parse_stats);
                } else {
                    BH_LOG(app::logger(), DNET_LOG_ERROR, "Node %s stats download failed, "
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

int Round::add_download(Node & node)
{
    BH_LOG(app::logger(), DNET_LOG_INFO,
            "Scheduling stat download for node %s", node.get_key().c_str());

    CURL *easy = create_easy_handle(&node);
    if (easy == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Cannot create easy handle to download node stat");
        return -1;
    }
    curl_multi_add_handle(m_curl_handle, easy);

    return 0;
}

CURL *Round::create_easy_handle(Node *node)
{
    CURL *easy = curl_easy_init();
    if (easy == nullptr)
        return nullptr;

    std::ostringstream url;
    url << "http://" << node->get_host().get_addr() << ':'
        << app::config().monitor_port << "/?categories="
        << uint32_t(DNET_MONITOR_PROCFS | DNET_MONITOR_BACKEND |
                DNET_MONITOR_STATS | DNET_MONITOR_COMMANDS);

    curl_easy_setopt(easy, CURLOPT_URL, url.str().c_str());
    curl_easy_setopt(easy, CURLOPT_PRIVATE, node);
    curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "deflate");
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &write_func);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, node);
    curl_easy_setopt(easy, CURLOPT_TIMEOUT, app::config().wait_timeout);

    return easy;
}

int Round::handle_socket(CURL *easy, curl_socket_t fd,
        int action, void *userp, void *socketp)
{
    Round *self = (Round *) userp;
    if (self == nullptr)
        return -1;

    struct epoll_event event;
    event.events = 0;
    event.data.fd = fd;

    if (action == CURL_POLL_REMOVE) {
        int rc = epoll_ctl(self->m_epollfd, EPOLL_CTL_DEL, fd, &event);
        if (rc != 0 && errno != EBADF) {
            int err = errno;
            BH_LOG(app::logger(), DNET_LOG_WARNING, "CURL_POLL_REMOVE: %s", strerror(err));
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
            BH_LOG(app::logger(), DNET_LOG_WARNING, "EPOLL_CTL_MOD: %s", strerror(err));
            return -1;
        }
    }

    return 0;
}

int Round::handle_timer(CURLM *multi, long timeout_ms, void *userp)
{
    Round *self = (Round *) userp;
    if (self == nullptr)
        return -1;

    self->m_timeout_ms = timeout_ms;

    return 0;
}

size_t Round::write_func(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    if (userdata == nullptr)
        return 0;

    Node *node = (Node *) userdata;
    node->add_download_data(ptr, size * nmemb);
    return size * nmemb;
}

void Round::result(size_t group_idx, const elliptics::read_result_entry & entry)
{
    const struct dnet_time & ts = entry.io_attribute()->timestamp;
    uint64_t timestamp_ns = ts.tsec * 1000000000ULL + ts.tnsec;

    elliptics::data_pointer file = entry.file();
    m_groups_to_read[group_idx].get().save_metadata((const char *) file.data(), file.size(), timestamp_ns);
}

void Round::final(size_t group_idx, const elliptics::error_info & error)
{
    if (error)
        m_groups_to_read[group_idx].get().handle_metadata_download_failed(error.message());
    else
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Successfully downloaded metadata for group %d",
                m_groups_to_read[group_idx].get().get_id());

    if (! --m_nr_groups) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "Group metadata download completed");
        clock_stop(m_clock.metadata_download);

        dispatch_async_f(m_queue, this, &Round::step4_perform_update);
    }
}
