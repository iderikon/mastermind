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

#include "Parser.h"

#include <iostream>

bool Parser::UInteger(uint64_t val)
{
    if (key_depth() != (m_depth + 1))
        return true;

    const UIntInfo *info = m_uint_info;
    while (info->keys) {
        if (info->keys == (m_keys - 1)) {
            uint64_t *dst_val = (uint64_t *) (m_dest + info->off);
            switch (info->action)
            {
            case SET:
                *dst_val = val;
                break;

            case SUM:
                *dst_val += val;
                break;

            case MAX:
                if (*dst_val < val)
                    *dst_val = val;
                break;
            }

            clear_key();
            return true;
        }
        ++info;
    }

    return false;
}

bool Parser::Key(const char* str, rapidjson::SizeType length, bool copy)
{
    int kdepth = key_depth();

    if (m_depth != kdepth)
        return true;

    if (kdepth > m_max_depth)
        return false;

    const Folder *fold = m_fold[m_depth - 1];
    while (fold->str != NULL) {
        if (fold->keys == (m_keys - 1)) {
            switch (fold->str[0]) {
            case NOT_MATCH[0]:
                if (std::strcmp(fold->str + 1, str)) {
                    m_keys |= fold->token;
                    return true;
                }
                break;

            case MATCH_ANY[0]:
                m_keys |= fold->token;
                return true;

            default:
                if (!std::strcmp(fold->str, str)) {
                    m_keys |= fold->token;
                    return true;
                }
            }
        }
        ++fold;
    }

    return true;
}
