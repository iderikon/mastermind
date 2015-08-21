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

#ifndef __afb88826_875d_494f_9652_520b90a5c9d1
#define __afb88826_875d_494f_9652_520b90a5c9d1

#include "Filter.h"
#include "WorkerApplication.h"

#include <cocaine/framework/dispatch.hpp>

class on_summary :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_summary>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_summary(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
};

class on_group_info :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_group_info>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_group_info(WorkerApplication & app)
        :
        super(app),
        m_app(app),
        m_group_id(0)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    int get_group_id() const
    { return m_group_id; }

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
    int m_group_id;
};

class on_list_nodes :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_list_nodes>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_list_nodes(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
};

class on_node_info :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_node_info>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_node_info(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    const std::string & get_node_name() const
    { return m_node_name; }

private:
    WorkerApplication & m_app;
    std::string m_node_name;
};

class on_node_list_backends :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_node_list_backends>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_node_list_backends(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    const std::string & get_node_name() const
    { return m_node_name; }

private:
    WorkerApplication & m_app;
    std::string m_node_name;
};

class on_backend_info :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_backend_info>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_backend_info(WorkerApplication & app)
        :
        super(app),
        m_app(app),
        m_backend_id(0)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    const std::string & get_node_name() const
    { return m_node_name; }

    uint64_t get_backend_id() const
    { return m_backend_id; }

private:
    WorkerApplication & m_app;
    std::string m_node_name;
    uint64_t m_backend_id;
};

class on_fs_info :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_fs_info>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_fs_info(WorkerApplication & app)
        :
        super(app),
        m_app(app),
        m_fsid(0)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    const std::string & get_node_name() const
    { return m_node_name; }

    uint64_t get_fsid() const
    { return m_fsid; }

private:
    WorkerApplication & m_app;
    std::string m_node_name;
    uint64_t m_fsid;
};

class on_list_namespaces :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_list_namespaces>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_list_namespaces(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
};

class on_group_couple_info :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_group_couple_info>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_group_couple_info(WorkerApplication & app)
        :
        super(app),
        m_app(app),
        m_group_id(0)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    int get_group_id() const
    { return m_group_id; }

private:
    WorkerApplication & m_app;
    int m_group_id;
};

class on_force_update :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_force_update>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_force_update(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
};

class on_get_snapshot :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_get_snapshot>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_get_snapshot(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    Filter & get_filter()
    { return m_filter; }

private:
    WorkerApplication & m_app;
    Filter m_filter;
};

class on_refresh :
    public cocaine::framework::handler<WorkerApplication>,
    public std::enable_shared_from_this<on_refresh>
{
public:
    typedef cocaine::framework::handler<WorkerApplication> super;

    on_refresh(WorkerApplication & app)
        :
        super(app),
        m_app(app)
    {}

    WorkerApplication & get_app()
    { return m_app; }

    void on_chunk(const char *chunk, size_t size);

    Filter & get_filter()
    { return m_filter; }

private:
    WorkerApplication & m_app;
    Filter m_filter;
};

#endif

