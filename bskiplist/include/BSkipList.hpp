#pragma once

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <random>
#include <stack>
#include <string.h>
#include <utility>
#include <vector>
#include <cassert>

#include <ParallelTools/parallel.h>
#include <ParallelTools/reducer.h>

#include "StructOfArrays/SizedInt.hpp"
#include "StructOfArrays/aos.hpp"
#include "StructOfArrays/soa.hpp"

#include "tbassert.h"

#include "Node.hpp"
#include "InternalNode.hpp"
#include "LeafNode.hpp"
#include "Traits.hpp"

template <typename traits>
class BSkip
{
private:
    using K = traits::key_type;

    static constexpr uint32_t MAX_HEIGHT = 5;

    static_assert(MAX_HEIGHT > 1);

    int node_size;

    // promotion probability
    static constexpr double promotion_probability = (double)(1.0) / (double)traits::p;

public:
    // headers for each level
    BSkipNode<traits> *headers[MAX_HEIGHT];

public:
    BSkip();

    ~BSkip();

    // public apis
    bool insert(traits::element_type k);
    void delete_key(traits::key_type k);
    bool exists(traits::key_type k) const;
    traits::value_type value(traits::key_type k) const;
    #include "../src/range.cpp"

    // prints
    void print_leaves();
    void print_all();
    void count_all();

    // stats
    void clear_stats() {}
    void get_size_stats();
    void validate_structure();

private:
    void sum_helper(std::vector<uint64_t> &sums, BSkipNode<traits> *node,
                    int level, traits::key_type max);

    uint32_t flip_coins(K k)
    {
        uint32_t result = 0;
        size_t h = std::hash<K>{}(k);
        uint64_t flip = h % traits::p;
        while (flip == 0)
        {
            result++;
            if (result > MAX_HEIGHT - 1)
            {
                result = MAX_HEIGHT - 1;
                break;
            }
            h /= traits::p;
            flip = h % traits::p;
        }
        assert(result < MAX_HEIGHT);
        return result;
    }
};
