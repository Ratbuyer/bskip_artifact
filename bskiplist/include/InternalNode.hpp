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

#include "Node.hpp"

template <typename traits>
class BSkipNodeInternal : public BSkipNode<traits>
{
    using K = traits::key_type;

    K keys[traits::MAX_KEYS];
    BSkipNode<traits> *children[traits::MAX_KEYS];

public:
    mutable std::conditional_t<traits::concurrent, ReaderWriterLock, Empty> mutex_;

    BSkipNodeInternal() {}

    inline K get_header() { return keys[0]; }

    // given rank, set the key at that rank
    void set_key_at_rank(uint32_t rank, K key)
    {
        assert(this->num_elts > 0);
        keys[rank] = key;
    }

    // add child pointer
    void insert_child_at_rank(uint32_t rank, BSkipNode<traits> *elt, bool flag = true)
    {
        assert(this->num_elts <= traits::MAX_KEYS);
        // shift everything over by 1
        if (flag)
        {
            memmove(children + rank + 1, children + rank,
                    (this->num_elts - rank - 1) * sizeof(BSkipNode<traits> *));
        }
        else
        {
            memmove(children + rank + 1, children + rank,
                    (this->num_elts - rank) * sizeof(BSkipNode<traits> *));
        }
        // set it
        children[rank] = elt;
    }

    void delete_child_at_rank(uint32_t rank)
    {
        assert(this->num_elts - 1 >= 0);
        memmove(children + rank, children + rank + 1,
                (this->num_elts - rank - 1) * sizeof(BSkipNode<traits> *));
    }

    BSkipNode<traits> *get_child_at_rank(uint32_t rank)
    {
        tbassert(this->num_elts >= rank, "num elts %d, asked for rank %d\n",
                 this->num_elts, rank);
        return children[rank];
    }

    void set_child_at_rank(uint32_t rank, BSkipNode<traits> *elt)
    {
        tbassert(this->num_elts >= rank, "num elts %d, asked for rank %d\n",
                 this->num_elts, rank);
        children[rank] = elt;
    }

    void move_children(BSkipNodeInternal<traits> *dest, uint32_t starting_rank,
                       uint32_t num_elts_to_move, uint32_t dest_rank = 1)
    {
        assert(this->num_elts > 0);
        assert(num_elts_to_move < traits::MAX_KEYS);
        memmove(dest->children + dest_rank, children + starting_rank,
                num_elts_to_move * sizeof(BSkipNode<traits> *));
    }

    #include "../src/InternalNode.cpp"
};
