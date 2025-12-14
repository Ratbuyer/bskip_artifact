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