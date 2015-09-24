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

#ifndef __60719e6b_2f14_4a95_b411_d9baa339f032
#define __60719e6b_2f14_4a95_b411_d9baa339f032

#include <cstdint>
#include <initializer_list>
#include <rapidjson/reader.h>
#include <vector>

#define NOT_MATCH "\001"
#define MATCH_ANY "\002"

#define SET 0
#define SUM 1
#define MAX 2

/* Parser implements a handler for a RapidJSON SAX-style reader. While the reader
   processes a document, it publishes events such as Key, String value, Int value,
   Start of the array, Start of the object and so on. While receiving Key events,
   Parser maintains a state -- sequence of keys of nested objects and current
   value's key. Only needed keys cause state transition. If we don't need to save
   current value or don't have anything interesting in object just started, the
   state is unchanged. On value events, if the value is something known, Parser looks
   for the instructions what to do with the value -- simply save, find a maximum,
   calculate sum, and stores the result in a target structure's field. The
   instructions are found in a table with search criteria of the current state.

   Consider the following Example:

   {
       "foo": {
           "bar": {
               "baz": 1,
               "qux": 2
           },
           "quux": {
               3": {
                   "corge": 5,
                   "grault": 7
               },
               "4": {
                   "corge": 11,
                   "grault": 13
               }
           }
       },
       "garply": 17,
       "waldo": 19
   }

   Each key ("foo", "corge") has a token associated with it. The token is represented
   by a number with exactly one bit set. Deeper keys within a sequence must correspond
   to higher order numbers. For example, token for "corge" must be higher-order than
   one for "foo", but the relation between tokens for "corge" and "waldo" is not
   important.

   The state is bitwise-or'd tokens.

   For the example above tokens may be built up as follows:

   enum {
       Foo    = 2,
       Bar    = 4,
       Baz    = 8,
       Qux    = 0x10,
       Quux   = 0x20,
       QuuxID = 0x40,
       Corge  = 0x80,
       Grault = 0x100,
       Garply = 0x200,
       Waldo  = 0x400
   };

   Numbering must begin with 2 due to implementation details.

   QuuxID is a special token. Sometimes we want to process the description of arbitrary
   number of objects, e.g. gather statistics of all network interfaces.

   NB: For convenience, we name tokens acoording to keys. However, if keys with the same
       name appear in different positions within the same sequence, we must define
       different tokens.

       {
           "fred": {
               "plugh": {
                   "fred": 23
               }
           },
       }

       In this example two different tokens for the key "fred" must be defined.

   In Parser implementation the state is stored in the member 'm_keys'. In addition,
   when entering into the object, the value of m_depth is incremented. When leaving
   the object (EndObject() is called), we decrement the value.

   Mapping between keys and tokens is stored in a vector of vectors of structures of
   type Folder. The first dimension is the depth. A set of known keys on certain
   level of depth is numbered in second dimension.

   struct Folder
   {
       const char *str;
       uint64_t keys;
       uint64_t token;
       uint32_t str_hash;
   };

   'str' is the key name. 'keys' is the preceding state. The current state will be
   or'd with 'token' if the newly coming key matches 'str'. 'str_hash' is calculated
   by Parser and is not exposed outside.

   Here is the array of objects of type Folder describing the first level of
   depth in Example document.

   {
       { "foo",    0, Foo    },
       { "garply", 0, Garply },
       { "waldo",  0, Waldo  }
   }

   The first column, 'str', contains the key names. The second one contains keys
   prior to a new one. It is always zero at the first level. The last one contains
   the token.

   Second level:

   {
       { "bar",  Foo, Bar  },
       { "quux", Foo, Quux }
   }

   "bar" and "quux" keys are found inside "foo" object, it is indicated in the second
   column.

   Third level:

   {
       { "baz",     Foo|Bar,  Baz    },
       { "qux",     Foo|Bar,  Qux    },
       { MATCH_ANY, Foo|Quux, QuuxID }
   }

   Here the second column contains bitwise or of two previously matched tokens.
   Note the macro MATCH_ANY. It indicates that we accept all keys inside "quux" object.
   There is another macro NOT_MATCH which follows an undesired value. For example,
   if we'd be interested in all objects except "3" we'd define the following structure:
   Note that these macros only make sense being a single case given previous matched
   sequence of keys, i.e. Parser doesn't support conjunctions/disjunctions of such
   conditions.

   { NOT_MATCH "3", Foo|Quux, QuuxID }

   Fourth level:

   {
       { "corge", Foo|Quux|QuuxID, Corge }
   }

   The absense of "grault" key means that we don't need this attribute so Parser
   will skip it.

   Search in the folders is only performed when a number of bits set in m_keys
   (calculated in method key_depth()) is comparable with current depth value,
   we don't need to check keys in nested objects if previously nested object
   didn't match.

   Another case when we gain from key_depth() is when we get a new value. We do
   know that the value must be processed when key_depth() equals to the object depth.

   In base class implementation, we clear the most significant bit set to 1 after
   value processing and when EndObject() is called for known object.

   We instruct Parser what to do with an integer value defining an array of UIntInfo
   structures.

   struct UIntInfo
   {
       uint64_t keys;
       int action;
       size_t off;
   };

   'keys' is the current state. 'action' can either be set to SET, SUM, or MAX.
   'off' is an offset in the target structure. It makes a limitation to objects
   we can fill up: they must be of standard layout type.

   For example, we need to fill up the structure:

   struct Wibble
   {
       uint64_t baz;
       uint64_t qux;
       uint64_t max_corge;
       uint64_t garply;
       uint64_t waldo;
   };

   We pass a UIntInfoVector to Parser.

   {
       { Foo|Bar|Baz,           SET, offsetof(Wibble, baz)       },
       { Foo|Bar|Qux,           SET, offsetof(Wibble, qux)       },
       { Foo|Quux|QuuxID|Corge, MAX, offsetof(Wibble, max_corge) },
       { Garply,                SET, offsetof(Wibble, garply)    },
       { Waldo,                 SET, offsetof(Wibble, waldo)     }
   }

   StringInfo is used for string values in the same way. Offset of a field of
   type std::string is specified in 'off'. There is no 'action' field in
   StringInfo, only SET method is supported.

   struct StringInfo
   {
       uint64_t keys;
       size_t off;
   };

   Initially, m_keys is set to 1 (note that the least significant bit is not set
   in the array of Folder objects). Let's consider how the state is changed
   throughout the document scan.

   [ initial values:       ]
   [ m_keys: 1  m_depth: 0 ]

   {
   [ StartObject()         ]
   [ m_keys: 1  m_depth: 1 ]

       "foo":
   [ Key("foo")                ]
   [ m_keys: Foo|1  m_depth: 1 ]

       {
   [ StartObject()             ]
   [ m_keys: Foo|1  m_depth: 2 ]

           "bar":
   [ Key("bar")                    ]
   [ m_keys: Foo|Bar|1  m_depth: 2 ]

           {
   [ StartObject()                 ]
   [ m_keys: Foo|Bar|1  m_depth: 3 ]

               "baz":
   [ Key("baz")                       ]
   [ m_keys: Foo|Bar|Baz|1 m_depth: 3 ]

                      1
   [ Uint64(1)  { key_depth() == 4 } ]
   [ m_keys: Foo|Bar|1  m_depth: 3   ]

               "qux":
   [ Key("qux")                        ]
   [ m_keys: Foo|Bar|Qux|1  m_depth: 3 ]

                      2
   [ Uint64(2)  { key_depth() == 4 } ]
   [ m_keys: Foo|Bar|1  m_depth: 3   ]

           },
   [ EndObject()               ]
   [ m_keys: Foo|1  m_depth: 2 ]

           "quux":
   [ Key("quux")                    ]
   [ m_keys: Foo|Quux|1  m_depth: 2 ]

           {
   [ StartObject()                  ]
   [ m_keys: Foo|Quux|1  m_depth: 3 ]

               "3":
   [ Key("3")                              ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 3 ]

               {
   [ StartObject()                         ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                   "corge":
   [ Key("corge")                                ]
   [ m_keys: Foo|Quux|QuuxID|Corge|1  m_depth: 4 ]

                            5,
   [ Uint64(5)  { key_depth() == 5 }       ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                   "grault":
   [ Key("grault")                         ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                             7
   [ Uint64(7)  { key_depth() == 4 }       ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

               },
   [ EndObject()                    ]
   [ m_keys: Foo|Quux|1  m_depth: 3 ]

               "4":
   [ Key("4")                              ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 3 ]

               {
   [ StartObject()                         ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                   "corge":
   [ Key("corge")                                ]
   [ m_keys: Foo|Quux|QuuxID|Corge|1  m_depth: 4 ]

                            11,
   [ Uint64(11)  { key_depth() == 5 }      ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                   "grault":
   [ Key("grault")                         ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

                             13
   [ Key("grault")                         ]
   [ m_keys: Foo|Quux|QuuxID|1  m_depth: 4 ]

               }
   [ EndObject()                    ]
   [ m_keys: Foo|Quux|1  m_depth: 3 ]

           }
   [ EndObject()               ]
   [ m_keys: Foo|1  m_depth: 2 ]

       },
   [ EndObject()           ]
   [ m_keys: 1  m_depth: 1 ]

       "garply":
   [ Key("garply")                ]
   [ m_keys: Garply|1  m_depth: 1 ]

                 17,
   [ Uint64(17)  { key_depth() == 2 } ]
   [ m_keys: 1  m_depth: 1            ]

       "waldo":
   [ Key("waldo")                ]
   [ m_keys: Waldo|1  m_depth: 1 ]

                19
   [ Uint64(19)  { key_depth() == 2 } ]
   [ m_keys: 1  m_depth: 1            ]

   }
   [ EndObject()           ]
   [ m_keys: 1  m_depth: 0 ]

   */

class Parser
{
public:
    struct Folder
    {
        const char *str;
        uint64_t keys;
        uint64_t token;
        uint32_t str_hash; // calculated internally
    };

    class FolderVector : public std::vector<Folder>
    {
    public:
        FolderVector(std::initializer_list<Folder> list);
    };

    struct UIntInfo
    {
        uint64_t keys;
        int action;
        size_t off;
    };

    class UIntInfoVector : public std::vector<UIntInfo>
    {
    public:
        UIntInfoVector() {}
        UIntInfoVector(std::initializer_list<UIntInfo> list);
    };

    struct StringInfo
    {
        uint64_t keys;
        size_t off;
    };

    class StringInfoVector : public std::vector<StringInfo>
    {
    public:
        StringInfoVector() {}
        StringInfoVector(std::initializer_list<StringInfo> list);
    };

public:
    Parser(const std::vector<FolderVector> & folders,
            const UIntInfoVector & uint_info,
            const StringInfoVector & string_info,
            uint8_t *dest);

    virtual bool Null()
    { return true; }
    virtual bool Bool(bool b)
    { return true; }
    virtual bool Int(int i)
    { return true; }
    virtual bool Int64(int64_t i)
    { return true; }
    virtual bool Double(double d)
    { return true; }
    virtual bool StartArray()
    { return true; }
    virtual bool EndArray(rapidjson::SizeType nr_elements)
    { return true; }

    virtual bool Uint(unsigned u)
    { return UInteger(u); }
    virtual bool Uint64(uint64_t u)
    { return UInteger(u); }

    virtual bool Key(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool String(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool StartObject()
    {
        ++m_depth;
        return true;
    }

    virtual bool EndObject(rapidjson::SizeType nr_members)
    {
        if (m_depth == key_depth())
            clear_key();
        --m_depth;
        return true;
    }

    virtual bool good()
    {
        return (m_keys == 1 && m_depth == 0);
    }

protected:
    void clear_key()
    {
        if (m_keys != 1) {
            uint64_t msig = 1ULL << (63 - __builtin_clzll(m_keys));
            m_keys ^= msig;
        }
    }

    int key_depth() const
    {
        return __builtin_popcountll(m_keys);
    }

    virtual bool UInteger(uint64_t val);

protected:
    uint64_t m_keys;
    int m_depth;

private:
    const std::vector<FolderVector> & m_folders;
    const UIntInfoVector & m_uint_info;
    const StringInfoVector & m_string_info;
    uint8_t *m_dest;
};

#endif

