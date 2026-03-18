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
BSkip<traits>::BSkip()
{
    // initialize bskip with array of headers
    for (uint32_t i = 0; i < MAX_HEIGHT; i++)
    {
        if (i > 0)
        {
            headers[i] = new BSkipNodeInternal<traits>();
            auto end_sentinel = new BSkipNodeInternal<traits>();

            end_sentinel->level = i;
            end_sentinel->num_elts++;

            end_sentinel->set_key_at_rank(0, std::numeric_limits<K>::max());
            headers[i]->next = end_sentinel;
        }
        else
        {
            headers[i] = new BSkipNodeLeaf<traits>();
            auto end_sentinel = new BSkipNodeLeaf<traits>();
            typename traits::element_type sentinel;
            std::get<0>(sentinel) =
                std::numeric_limits<typename traits::key_type>::max();
            end_sentinel->set_elt_at_rank(0, sentinel);

            end_sentinel->level = i;
            end_sentinel->num_elts++;

            headers[i]->next = end_sentinel;
        }
    }

    // set min elt at start of header
    for (int i = MAX_HEIGHT - 1; i >= 0; i--)
    {
        headers[i]->num_elts++;
        if (i > 0)
        {
            ((BSkipNodeInternal<traits> *)headers[i])
                ->set_key_at_rank(0, std::numeric_limits<K>::min());
        }
        else
        {
            typename traits::element_type sentinel;
            std::get<0>(sentinel) =
                std::numeric_limits<typename traits::key_type>::min();
            ((BSkipNodeLeaf<traits> *)headers[i])->set_elt_at_rank(0, sentinel);
        }
        headers[i]->level = i;
        if (i > 0)
        {
            ((BSkipNodeInternal<traits> *)headers[i])
                ->set_child_at_rank(0, headers[i - 1]);
        }

        if (i == 0)
        {
            break;
        }
    }

    for (uint32_t i = 0; i < MAX_HEIGHT; i++)
    {
        assert(headers[i]->num_elts == 1);
        assert(headers[i]->next->num_elts == 1);
        assert(headers[i]->level == i);
    }
}

#include "instances.cpp"