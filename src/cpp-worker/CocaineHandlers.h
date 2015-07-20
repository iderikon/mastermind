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

#ifndef __afb88826_875d_494f_9652_520b90a5c9d1
#define __afb88826_875d_494f_9652_520b90a5c9d1

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
        m_app(app)
    {}

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
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

    void on_chunk(const char *chunk, size_t size);

private:
    WorkerApplication & m_app;
};

#endif

