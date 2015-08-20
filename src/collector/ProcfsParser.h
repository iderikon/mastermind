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

#ifndef __fcc5929b_b189_4dd0_add4_743ec4f888cc
#define __fcc5929b_b189_4dd0_add4_743ec4f888cc

#include "Node.h"
#include "Parser.h"

class ProcfsParser : public Parser
{
    typedef Parser super;

public:
    ProcfsParser();

    const NodeStat & get_stat() const
    { return m_stat; }

private:
    NodeStat m_stat;
};

#endif

