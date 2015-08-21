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

#ifndef __4e5e1746_2d12_42c1_9384_b1a59c632ca3
#define __4e5e1746_2d12_42c1_9384_b1a59c632ca3

#include "Discovery.h"
#include "Round.h"
#include "Storage.h"

#include <dispatch/dispatch.h>

#include <memory>

class WorkerApplication;

class on_backend_info;
class on_force_update;
class on_fs_info;
class on_get_snapshot;
class on_group_couple_info;
class on_group_info;
class on_list_namespaces;
class on_list_nodes;
class on_node_info;
class on_node_list_backends;
class on_summary;
class on_refresh;

class Collector
{
public:
    Collector(WorkerApplication & app);

    WorkerApplication & get_app()
    { return m_app; }

    Discovery & get_discovery()
    { return m_discovery; }

    Storage & get_storage()
    { return m_storage; }

    int init();

    void start();

    void finalize_round(Round *round);

public:
    void force_update(std::shared_ptr<on_force_update> handler);
    void get_snapshot(std::shared_ptr<on_get_snapshot> handler);
    void group_info(std::shared_ptr<on_group_info> handler);
    void list_nodes(std::shared_ptr<on_list_nodes> handler);
    void node_info(std::shared_ptr<on_node_info> handler);
    void summary(std::shared_ptr<on_summary> handler);
    void backend_info(std::shared_ptr<on_backend_info> handler);
    void fs_info(std::shared_ptr<on_fs_info> handler);
    void group_couple_info(std::shared_ptr<on_group_couple_info> handler);
    void list_namespaces(std::shared_ptr<on_list_namespaces> handler);
    void node_list_backends(std::shared_ptr<on_node_list_backends> handler);
    void refresh(std::shared_ptr<on_refresh> handler);

private:
    static void step1_start_round(void *arg);
    static void step1_start_forced(void *arg);
    static void step1_start_refresh(void *arg);
    static void step5_merge(void *arg);

    static void execute_force_update(void *arg);
    static void execute_get_snapshot(void *arg);
    static void execute_group_info(void *arg);
    static void execute_list_nodes(void *arg);
    static void execute_node_info(void *arg);
    static void execute_summary(void *arg);
    static void execute_backend_info(void *arg);
    static void execute_fs_info(void *arg);
    static void execute_group_couple_info(void *arg);
    static void execute_list_namespaces(void *arg);
    static void execute_node_list_backends(void *arg);

private:
    WorkerApplication & m_app;

    Discovery m_discovery;
    Storage m_storage;
    dispatch_queue_t m_queue;

    Round::ClockStat m_round_clock;
    uint64_t m_merge_time;
};

#endif

