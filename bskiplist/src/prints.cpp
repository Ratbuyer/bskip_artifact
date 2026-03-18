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
void BSkip<traits>::print_leaves()
{
    auto n = headers[0];
    while (n)
    {
        n->print_keys();
        n = n->next;
    }
}

template <typename traits>
void BSkip<traits>::print_all()
{
    for (int i = MAX_HEIGHT - 1; i >= 0; i--)
    {
        printf("LEVEL %d\n", i);
        auto n = headers[i];
        while (n)
        {
            n->print_keys();
            n = n->next;
        }
    }
}

// count the number of elements in the skiplist
template <typename traits>
void BSkip<traits>::count_all() {
    // print number of keys on every level
    for (int i = MAX_HEIGHT - 1; i >= 0; i--) {
        uint64_t level_total = 0;
        auto n = headers[i];
        while (n) {
            level_total += n->num_elts;
            n = n->next;
        }
        // subtract 2 for sentinels
        printf("total elements in level %d: %lu\n", i, level_total - 2);
    }
}

#include "instances.cpp"