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