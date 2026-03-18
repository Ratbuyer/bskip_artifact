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

std::pair<uint32_t, bool> find_key_and_check(traits::key_type k)
{
    uint32_t i;
    assert(BSkipNode<traits>::num_elts > 0);
#ifndef NDEBUG
    for (i = 1; i < BSkipNode<traits>::num_elts; i++)
    {
        if (keys[i] < keys[i - 1])
        {
            tbassert(keys[i] > keys[i - 1], "keys[%u] = %lu, keys[%u] = %lu\n",
                        i - 1, keys[i - 1], i, keys[i]);
        }
    }
    if (k < keys[0])
    {
        tbassert(k >= keys[0], "key = %lu, min = %lu\n", k, keys[0]);
    }
#endif

    for (i = 0; i < BSkipNode<traits>::num_elts; i++)
    {
        if (keys[i] > k)
            break;
    }
    if (i == 0) {
        return {0, keys[i] == k};
    }
    return {i - 1, keys[i-1] == k};
}

traits::key_type get_key_at_rank(uint32_t rank)
{
    tbassert(rank < this->num_elts,
                "at internal node, has %u elts, asked for rank %u\n",
                BSkipNode<traits>::num_elts, rank);
    return keys[rank];
};

// only for internal nodes
void insert_key_at_rank(uint32_t rank, traits::key_type key)
{
    assert(this->level > 0);
    assert(this->num_elts + 1 <= traits::MAX_KEYS);

    memmove(keys + rank + 1, keys + rank,
            (this->num_elts - rank) * sizeof(K));
    keys[rank] = key;
}

void delete_key_at_rank(uint32_t rank)
{
    assert(this->level > 0);
    assert(this->num_elts - 1 >= 0);
    memmove(keys + rank, keys + rank + 1,
            (this->num_elts - rank - 1) * sizeof(K));
}

int split_keys(BSkipNode<traits> *dest, uint32_t starting_rank,
                uint32_t dest_rank = 1)
{
    uint32_t num_elts_to_move = this->num_elts - starting_rank;

    assert(starting_rank <= this->num_elts);
    assert(num_elts_to_move <= this->num_elts);
    assert(num_elts_to_move < traits::MAX_KEYS);

    memmove(((BSkipNodeInternal<traits> *)(dest))->keys + dest_rank,
            keys + starting_rank, num_elts_to_move * sizeof(K));

    this->num_elts = starting_rank;
    dest->num_elts += num_elts_to_move;

    return num_elts_to_move;
}

void print_keys()
{
    printf("\tlevel = %u, num keys = %u\n", BSkipNode<traits>::level,
            BSkipNode<traits>::num_elts);
    for (uint32_t i = 0; i < BSkipNode<traits>::num_elts; i++)
    {
        printf("\t\tkey[%d] = %lu\n", i, keys[i]);
    }
    printf("\n");
}