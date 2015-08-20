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

#ifndef __d3b51655_27e8_4227_a7eb_a598183c39cd
#define __d3b51655_27e8_4227_a7eb_a598183c39cd

#include <elliptics/session.hpp>

#include <memory>

class Round;
class WorkerApplication;

class Discovery
{
public:
    Discovery(WorkerApplication & app);
    ~Discovery();

    int init_curl();
    int init_elliptics();

    ioremap::elliptics::session & get_session()
    { return *m_session; }

    void resolve_nodes(Round & round);

private:
    WorkerApplication & m_app;

    std::unique_ptr<ioremap::elliptics::node> m_node;
    std::unique_ptr<ioremap::elliptics::session> m_session;
};

#endif

