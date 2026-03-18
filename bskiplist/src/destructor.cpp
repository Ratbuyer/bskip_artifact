// Copyright 2025 Yicong Luo and contributors

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>

#include "../include/BSkipList.hpp"

template <typename traits>

BSkip<traits>::~BSkip()
{
    BSkipNodeInternal<traits> *curr_node, *next_node;
    for (uint32_t i = 1; i < MAX_HEIGHT; i++)
    {
        curr_node = ((BSkipNodeInternal<traits> *)headers[i]);
        while (curr_node)
        {
            next_node = (BSkipNodeInternal<traits> *)curr_node->next;
            delete curr_node;
            curr_node = next_node;
        }
    }
    BSkipNodeLeaf<traits> *curr_leaf, *next_leaf;
    curr_leaf = (BSkipNodeLeaf<traits> *)headers[0];
    while (curr_leaf)
    {
        next_leaf = (BSkipNodeLeaf<traits> *)curr_leaf->next;
        delete curr_leaf;
        curr_leaf = next_leaf;
    }
}

#include "instances.cpp"