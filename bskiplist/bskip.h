/*
 * ============================================================================
 *
 *       Filename:  bskip.h
 *
 *         Author:  Helen Xu, hjxu@lbl.gov
 *   Organization:  Lawrence Berkeley Laboratory
 *
 * ============================================================================
 */

#ifndef _BSKIP_H_
#define _BSKIP_H_

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

#include "tbassert.h"

#include <ParallelTools/Lock.hpp>
#include <ParallelTools/parallel.h>
#include <ParallelTools/reducer.h>

#include "StructOfArrays/SizedInt.hpp"
#include "StructOfArrays/aos.hpp"
#include "StructOfArrays/soa.hpp"
#include "tools.h"

// TODO: replace with SOA for vals
#define BINARY_SEARCH 0

template <typename traits>
class BSkipNode;

template <bool is_concurrent, size_t B, size_t promotion_prob, class T,
          typename... Ts>
class BSkip_traits
{
public:
    static constexpr uint64_t MAX_KEYS = B;

    using key_type = T;
    static constexpr bool binary = sizeof...(Ts) == 0;
    // using val_type = std::tuple<Ts...>;
    static constexpr bool concurrent = is_concurrent;
    static constexpr size_t p = promotion_prob;

    using element_type =
        typename std::conditional<binary, std::tuple<key_type>,
                                  std::tuple<key_type, Ts...>>::type;

    using value_type =
        typename std::conditional<binary, std::tuple<>, std::tuple<Ts...>>::type;

    template <int I>
    using NthType = typename std::tuple_element<I, value_type>::type;
    static constexpr int num_types = sizeof...(Ts);

    //   using SOA_type = typename std::conditional<binary, SOA<key_type>,
    //                                              SOA<key_type, Ts...>>::type;
    using SOA_leaf_type = typename std::conditional<binary, AOS<key_type>,
                                                    SOA<key_type, Ts...>>::type;

    // internal node is key only
    static key_type get_key(element_type e) { return std::get<0>(e); }
};

template <typename T = uint64_t>
using bskip_set_settings = BSkip_traits<true, 1024, 32, T>;

template <typename T = uint64_t>
using bskip_map_settings = BSkip_traits<true, 1024, 32, T, T>;

// superclass for bskipnodes (internal and leaf)
template <typename traits>
class BSkipNode
{

public:
    uint32_t num_elts;
    uint32_t level;
    BSkipNode<traits> *next;
    traits::key_type next_header;

    // initialize the skiplist node with default container size
    BSkipNode()
    {
        num_elts = 0;
        next = NULL;
        next_header = std::numeric_limits<typename traits::key_type>::max();

        // #if STATS
        //         num_comparisons = 0;
        // #endif
    }

    virtual ~BSkipNode() {}

    //! True if this is a leaf node.
    bool is_leafnode() const { return (level == 0); }

    // TODO: write derived versions
    virtual traits::key_type get_header() = 0;

    virtual uint32_t find_key(traits::key_type k) = 0;
    virtual std::pair<uint32_t, bool> find_key_and_check(traits::key_type k) = 0;

    // HERE
    virtual traits::key_type get_key_at_rank(uint32_t rank) = 0;

    virtual void insert_key_at_rank(uint32_t rank, traits::key_type key) = 0;

    virtual int split_keys(BSkipNode<traits> *dest, uint32_t starting_rank,
                           uint32_t dest_rank = 1) = 0;

    virtual void print_keys() = 0;
};

class Empty
{
};

// extra case for internal node
template <typename traits>
class BSkipNodeLeaf : public BSkipNode<traits>
{
public:
    using K = traits::key_type;
    using V = traits::value_type;
    using elt_type = traits::element_type;

    static constexpr traits::key_type NULL_VAL = {};
    static constexpr size_t max_element_size = sizeof(K);

    using SOA_type = traits::SOA_leaf_type;
    static constexpr uint64_t max_size = traits::MAX_KEYS;

    // TODO: is a regular lock fine for the leaves? maybe this should be RWLock2?
    // mutable Lock mutex_;
    //
    mutable std::conditional_t<traits::concurrent, ReaderWriterLock2, Empty> mutex_;

    std::array<uint8_t, SOA_type::get_size_static(max_size)> array = {0};

    inline K get_key_array(uint32_t index) const
    {
        return std::get<0>(
            SOA_type::template get_static<0>(array.data(), max_size, index));
    }

    auto blind_read_key(uint32_t index) const
    {
        return std::get<0>(SOA_type::get_static(array.data(), max_size, index));
    }
    auto blind_read_val(uint32_t index) const
    {
        return std::get<1>(SOA_type::get_static(array.data(), max_size, index));
    }
    void blind_write_array(void *arr, size_t len, uint32_t index,
                           traits::element_type e)
    {
        SOA_type::get_static(arr, len, index) = e;
    }

    void blind_write(traits::element_type e, uint32_t index)
    {
        SOA_type::get_static(array.data(), max_size, index) = e;
    }

    auto blind_read(uint32_t index) const
    {
        return SOA_type::get_static(array.data(), max_size, index);
    }
    auto blind_read_array(void *arr, size_t size, uint32_t index) const
    {
        return SOA_type::get_static(arr, size, index);
    }

    auto blind_read_key_array(void *arr, size_t size, uint32_t index) const
    {
        return std::get<0>(SOA_type::get_static(arr, size, index));
    }

    size_t count_up_elts() const
    {
        size_t result = 0;
        for (size_t i = 0; i < max_size; i++)
        {
            result += (blind_read_key(i) != NULL_VAL);
        }
        return result;
    }

    // TODO: this could have an optimized version depending on the container type
    inline K get_header()
    {
        return blind_read_key(0); // return keys[0];
    }

    uint64_t sum_keys()
    {
        uint64_t result = 0;
        for (uint32_t i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            result += blind_read_key(i); // keys[i];
        }
        return result;
    }

    uint64_t sum_vals()
    {
        uint64_t result = 0;
        for (uint32_t i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            result += blind_read_val(i);
        }
        return result;
    }

    void print_keys()
    {
        printf("\tlevel = %u, num keys = %u\n", BSkipNode<traits>::level,
               BSkipNode<traits>::num_elts);
        for (uint32_t i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            printf("\t\tkey[%d] = %lu\n", i, blind_read_key(i));
        }
        printf("\n");
    }

    // return rank of key largest key at most k
    uint32_t find_key(K k)
    {
#if BINARY_SEARCH
        return find_index_binary(k);
#else
#if DEBUG_PRINT
        printf("searching for key %lu\n", k);
        print_keys();
#endif
        return find_index_linear(k);
#endif
    }

    std::pair<uint32_t, bool> find_key_and_check(K k)
    {
#if BINARY_SEARCH
        auto index = find_index_binary(k);
        return {index, (blind_read_key(index) == k)};
#else
#if DEBUG_PRINT
        printf("searching for key %lu\n", k);
        print_keys();
#endif
        auto index = find_index_linear(k);
        return {index, (blind_read_key(index) == k)};
#endif
    }

    // add key elt at rank, shifting everything down if necessary
    void insert_elt_at_rank(uint32_t rank, traits::element_type elt)
    {
        // keep track of num_elts here
        assert(BSkipNode<traits>::num_elts + 1 <= max_size);

        // shift everything over by 1
        for (size_t j = BSkipNode<traits>::num_elts; j > rank; j--)
        {
            SOA_type::get_static(array.data(), max_size, j) =
                SOA_type::get_static(array.data(), max_size, j - 1);
        }
        blind_write(elt, rank);

        // memmove(keys + rank + 1, keys + rank, (num_elts - rank) * sizeof(K));
        // set it
        // keys[rank] = elt;
    }

    inline K get_key_at_rank(uint32_t rank)
    {
        return blind_read_key(rank);
        // return keys[rank];
    }

    inline V get_value_at_rank(uint32_t rank)
    {
        return blind_read_val(rank);
    }

    void insert_key_at_rank(uint32_t rank, traits::key_type key)
    {
        (void)rank;
        (void)key;
        assert(false);
    }

    inline void set_elt_at_rank(uint32_t rank, traits::element_type elt)
    {
        assert(rank < BSkipNode<traits>::num_elts);
        blind_write(elt, rank);
        // keys[rank] = k;
    }

    // move keys starting from starting_rank into dest node starting from
    // dest_rank
    int split_keys(BSkipNode<traits> *dest, uint32_t starting_rank,
                   uint32_t dest_rank = 1)
    {
        uint32_t num_elts_to_move = BSkipNode<traits>::num_elts - starting_rank;

        assert(starting_rank <= BSkipNode<traits>::num_elts);
        assert(num_elts_to_move <= BSkipNode<traits>::num_elts);
        assert(num_elts_to_move < traits::MAX_KEYS);

        for (uint32_t j = 0; j < num_elts_to_move; j++)
        {
            ((BSkipNodeLeaf<traits> *)dest)
                ->blind_write(blind_read(starting_rank + j), dest_rank + j);
        }
        // clear_range(starting_rank, starting_rank + num_elts_to_move);

        // memmove(dest->keys + dest_rank, keys + starting_rank, num_elts_to_move *
        // sizeof(K));

        BSkipNode<traits>::num_elts = starting_rank;
        dest->num_elts += num_elts_to_move;

        return num_elts_to_move;
    }

private:
    uint32_t find_index_linear(K k)
    {
        uint32_t i;
        assert(BSkipNode<traits>::num_elts > 0);

        for (i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            // if (keys[i] < k)
            if (blind_read_key(i) < k)
                continue;
            else if (k == blind_read_key(i))
            { // (k == keys[i]) {
                // tbassert(pos - 1 == i, "pos = %u, i = %u\n", pos, i);
                // return pos - 1;
                return i;
            }
            else
                break;
        }
        return i - 1;
        /*
            assert(pos > 0);
            tbassert(pos - 1 == i - 1, "pos = %u, correct = %u\n", pos, i - 1);
                        return pos - 1;
        */
    }

    // TODO: add binary search
    uint32_t find_index_binary(K k)
    {
        uint32_t left = 0;
        uint32_t right = BSkipNode<traits>::num_elts - 1;
        while (left <= right)
        {
            int mid = left + (right - left) / 2;
            if (blind_read_key(mid) == k)
            { // (keys[mid] == k) {
                left = mid;
                break;
            }
            else if (blind_read_key(mid) < k)
            { // (keys[mid] < k) {
                left = mid + 1;
            }
            else
            {
                if (mid == 0)
                {
                    break;
                }
                right = mid - 1;
            }
        }
        if (left == BSkipNode<traits>::num_elts || blind_read_key(left) > k)
        {
            assert(left > 0);
            left--;
        }

#if DEBUG
        tbassert(left < BSkipNode<traits>::num_elts, "left = %u, num elts %u\n",
                 left, BSkipNode<traits>::num_elts);
        tbassert(left == find_index_linear(k), "k %lu, binary = %u, linear = %u\n",
                 k, left, find_index_linear(k));
#endif
        return left;
    }
};

// extra case for internal node
template <typename traits>
class BSkipNodeInternal : public BSkipNode<traits>
{
    using K = traits::key_type;

    // using SOA_internal_type = typename AOS<K, BSkipNode<traits>*>::type;
public:
    mutable std::conditional_t<traits::concurrent, ReaderWriterLock, Empty> mutex_;

    // mutable ReaderWriterLock mutex_;
    // no values in internal
    K keys[traits::MAX_KEYS];
    BSkipNode<traits> *children[traits::MAX_KEYS];

    BSkipNodeInternal() {}
    // ~BSkipNodeInternal() { }

    inline K get_header() { return keys[0]; }


    // given rank, set the key at that rank
    void set_key_at_rank(uint32_t rank, K key)
    {
        keys[rank] = key;
    }

    // add child pointer
    void insert_child_at_rank(uint32_t rank, BSkipNode<traits> *elt, bool flag = true)
    {
        assert(this->num_elts <= traits::MAX_KEYS);
#if DEBUG_PRINT
        printf("INSERT CHILD AT RANK: rank %u, max keys %u, num elts %u\n", rank, traits::MAX_KEYS, this->num_elts);
#endif
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

    BSkipNode<traits> *get_child_at_rank(uint32_t rank)
    {
#if DEBUG
        tbassert(this->num_elts >= rank, "num elts %d, asked for rank %d\n",
                 this->num_elts, rank);
#endif
        return children[rank];
    }

    void set_child_at_rank(uint32_t rank, BSkipNode<traits> *elt)
    {
#if DEBUG
        tbassert(this->num_elts >= rank, "num elts %d, asked for rank %d\n",
                 this->num_elts, rank);
#endif
        children[rank] = elt;
    }

    void move_children(BSkipNodeInternal<traits> *dest, uint32_t starting_rank,
                       uint32_t num_elts_to_move, uint32_t dest_rank = 1)
    {
#if DEBUG
        assert(num_elts_to_move < traits::MAX_KEYS);
#endif
        memmove(dest->children + dest_rank, children + starting_rank,
                num_elts_to_move * sizeof(BSkipNode<traits> *));
    }

    // TODO: write derived versions

    uint32_t find_key(traits::key_type k)
    {
        uint32_t i;
        assert(BSkipNode<traits>::num_elts > 0);
#if DEBUG
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
        // uint32_t pos = 0;

        for (i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            // if (keys[i] < k)
            if (keys[i] > k)
                break;
        }
        if (i == 0) {
            return 0;
        }
        return i - 1;
    }

    std::pair<uint32_t, bool> find_key_and_check(traits::key_type k)
    {
        uint32_t i;
        assert(BSkipNode<traits>::num_elts > 0);
#if DEBUG
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
        // uint32_t pos = 0;

        for (i = 0; i < BSkipNode<traits>::num_elts; i++)
        {
            // if (keys[i] < k)
            if (keys[i] > k)
                break;
        }
         if (i == 0) {
            return {0, keys[i] == k};
        }
        return {i - 1, keys[i-1] == k};
    }

    // HERE
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
#if DEBUG_PRINT
        printf("INSERT KEY AT RANK: rank %u, max keys %u, num elts %u\n", rank, traits::MAX_KEYS, this->num_elts);
#endif
        memmove(keys + rank + 1, keys + rank,
                (this->num_elts - rank) * sizeof(K));
        keys[rank] = key;
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
};

// bskip structure
template <typename traits>
class BSkip
{
    using K = traits::key_type;

    static constexpr uint32_t MAX_HEIGHT = 5;

public:
#if STATS
    mutable std::atomic<int> query_counter = 0;
    mutable std::atomic<int> steps_counter = 0;
    mutable ThreadSafeVector<int> steps_vector; // for percentile steps
    mutable std::atomic<int> write_lock_counter = 0;
    mutable std::atomic<int> read_lock_counter = 0;
    mutable std::atomic<int> range_length_counter = 0;
    mutable std::atomic<int> range_length_node_counter = 0;
#endif

    // int curr_max_height; // max height of current sl
    static constexpr double promotion_probability = (double)(1.0) / (double)traits::p; // promotion probability
    bool has_zero = false;        // using 0 as first sentinel, so just keep it separate


    // headers for each node
    BSkipNode<traits> *headers[MAX_HEIGHT];

    // return locking time spent if timer is enabled
#if ENABLE_TRACE_TIMER
    ParallelTools::Reducer_sum<uint64_t> read_lock_count;
    ParallelTools::Reducer_sum<uint64_t> write_lock_count;
    ParallelTools::Reducer_sum<uint64_t> leaf_lock_count;
    

    void get_lock_counts()
    {
        printf("read lock count = %lu, write_lock_count = %lu, leaf lock count = "
               "%lu\n",
               read_lock_count.get(), write_lock_count.get(),
               leaf_lock_count.get());
    }

    void reset_lock_counts()
    {
        read_lock_count -= read_lock_count.get();
        write_lock_count -= write_lock_count.get();
        leaf_lock_count -= leaf_lock_count.get();
    }

    uint64_t get_read_lock_count() { return read_lock_count.get(); }
    uint64_t get_write_lock_count() { return write_lock_count.get(); }
    uint64_t get_leaf_lock_count() { return leaf_lock_count.get(); }

    uint64_t insert(traits::element_type k);
#else
    bool insert(traits::element_type k);
#endif

    BSkipNode<traits> *find(traits::key_type k) const;
    traits::value_type value(traits::key_type k) const;
    bool exists(traits::key_type k) const;

    template <class F>
    void map_range_length(traits::key_type start, uint64_t length, F f) const;
    
    template <class F>
    void map_range(traits::key_type min, traits::key_type max, F f) const;

    uint64_t sum();
    uint64_t sum_vals();
    uint64_t psum();
    void validate_structure();
    void get_size_stats();
    void get_avg_comparisons();
    uint32_t flip_coins(traits::key_type k);

    int node_size;
    
    void clear_stats() {
    #if STATS
    	query_counter = 0;
		steps_counter = 0;
		write_lock_counter = 0;
		read_lock_counter = 0;
		range_length_counter = 0;
		range_length_node_counter = 0;
	#endif
    }

    static_assert(MAX_HEIGHT > 1);
    BSkip()
    {

        // initialize bskip with array of headers
        for (uint32_t i = 0; i < MAX_HEIGHT; i++)
        {
            // printf("init height %u, max is %lu\n", i,
            // std::numeric_limits<K>::max());
            if (i > 0)
            {
                headers[i] = new BSkipNodeInternal<traits>();
                auto end_sentinel = new BSkipNodeInternal<traits>();

                end_sentinel->level = i;
                end_sentinel->num_elts++;

                end_sentinel->set_key_at_rank(0, std::numeric_limits<K>::max());
                headers[i]->next = end_sentinel;

                // TODO: do the end sentinels need a down pointer?
            }
            else
            {
                headers[i] = new BSkipNodeLeaf<traits>();
                auto end_sentinel = new BSkipNodeLeaf<traits>();
                typename traits::element_type sentinel;
                std::get<0>(sentinel) =
                    std::numeric_limits<typename traits::key_type>::max();
                end_sentinel->blind_write(sentinel, 0);

                end_sentinel->level = i;
                end_sentinel->num_elts++;
                end_sentinel->blind_write(sentinel, 0);

                headers[i]->next = end_sentinel;
            }

#if DEBUG_PRINT
            ((BSkipNodeInternal<traits> *)headers[i])->print_keys();
            ((BSkipNodeInternal<traits> *)(headers[i]->next))->print_keys();
#endif
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
                ((BSkipNodeLeaf<traits> *)headers[i])->blind_write(sentinel, 0);
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

#if DEBUG
        for (uint32_t i = 0; i < MAX_HEIGHT; i++)
        {
            assert(headers[i]->num_elts == 1);
            assert(headers[i]->next->num_elts == 1);
            assert(headers[i]->level == i);
        }
#endif
    }

    ~BSkip()
    {
#if STATS
        int queries = query_counter.load();
        int steps = steps_counter.load();
        printf("average query horizontal steps = %f\n", (double)steps / (double)queries);

        // int max_steps = this->steps_vector.get_max();
        // printf("max: %d\n", max_steps);

        // int percentile_90 = this->steps_vector.get_percentile(90);
        // printf("90th percentile: %d\n", percentile_90);
        
        int write_locks = write_lock_counter.load();
        printf("write locks aquired = %d\n", write_locks);
        
        int read_locks = read_lock_counter.load();
        printf("read locks aquired = %d\n", read_locks);
        
        int range_length = range_length_counter.load();
        int range_length_nodes = range_length_node_counter.load();
        printf("range length average nodes searched: %f\n", (double)range_length_nodes / (double)range_length);
#endif

        BSkipNodeInternal<traits> *curr_node, *next_node;
        for (uint32_t i = 1; i < MAX_HEIGHT; i++)
        {
            curr_node = ((BSkipNodeInternal<traits> *)headers[i]);
            while (curr_node)
            {
                // printf("level %u, curr node header = %lu\n", i,
                // curr_node->get_header());
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

    void print_leaves()
    {
        auto n = headers[0];
        while (n)
        {
            n->print_keys();
            n = n->next;
        }
    }

private:
    void sum_helper(std::vector<uint64_t> &sums, BSkipNode<traits> *node,
                    int level, traits::key_type max);
};

template <typename traits>
uint32_t BSkip<traits>::flip_coins(K k)
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

template <typename traits>
#if ENABLE_TRACE_TIMER
uint64_t BSkip<traits>::insert(traits::element_type k)
{
#else
bool BSkip<traits>::insert(traits::element_type k)
{
#endif
#if ENABLE_TRACE_TIMER
    // timer lock_timer("lock_timer");
#endif
    // TODO: in the weighted case, 0 has a val attached
    // special case for 0 since we use it as the front sentinel
    // TODO: also do for max int

    typename traits::key_type key = std::get<0>(k);
    if (key == 0)
    {
        has_zero = true;
        return true;
    }

    // int cpuid = ParallelTools::getWorkerNum();
    int cpuid = sched_getcpu();
    ReaderWriterLock *parent_lock = nullptr;

    // flip coins to determine your promotion level
    uint32_t level_to_promote = flip_coins(key);
#if DEBUG_PRINT
    printf("\tlevel to promote %u\n", level_to_promote);
#endif
    BSkipNodeInternal<traits> *parent_node = NULL;

#if DEBUG
    uint32_t parent_rank;
#endif

    // init all the new nodes you will need due to promotion split
    BSkipNode<traits>*  new_nodes[level_to_promote];
    if (level_to_promote > 1)
    {
        for (uint32_t i = 0; i < level_to_promote - 1; i++)
        {
            auto n = new BSkipNodeInternal<traits>();
            if constexpr (traits::concurrent)
            {
                // lock_timer.start();
                n->mutex_.write_lock();
                // lock_timer.stop();
                #if STATS
                write_lock_counter++;
                #endif
            }
            new_nodes[i] = n;
        }
    }

    if (level_to_promote > 0)
    {
        auto n = new BSkipNodeLeaf<traits>();
        if constexpr (traits::concurrent)
        {
            // lock_timer.start();
            n->mutex_.write_lock();
            // lock_timer.stop();
            #if STATS
            write_lock_counter++;
            #endif
        }
        new_nodes[level_to_promote - 1] = n;
    }

    uint32_t num_split = 0;

    assert(level_to_promote < MAX_HEIGHT);
#if DEBUG_PRINT
    printf("\n\ninserting key %lu, promoted to level %d\n", key,
           level_to_promote);
#endif

    auto curr_node = headers[MAX_HEIGHT - 1];
#if DEBUG_PRINT
    printf("printing header and next keys\n");
    ((BSkipNodeInternal<traits> *)(curr_node))->print_keys();
    ((BSkipNodeInternal<traits> *)(curr_node->next))->print_keys();
#endif
    for (uint level = MAX_HEIGHT; level-- > 0;)
    {
#if DEBUG_PRINT
        if (level > 0)
        {
            printf("\nthread %u, key = %lu, loop start: curr node head %lu at level "
                   "%d\n",
                   cpuid, key,
                   ((BSkipNodeInternal<traits> *)(curr_node))->get_header(), level);
        }
        else
        {
            printf("\nthread %u, key = %lu, loop start: curr node head %lu at level "
                   "%d\n",
                   cpuid, key, ((BSkipNodeLeaf<traits> *)(curr_node))->get_header(),
                   level);
        }
#endif
        if constexpr (traits::concurrent)
        {
            // if at an internal level, grab your current lock
#if DEBUG_PRINT
            printf("\n\ttry to grab lock on self %lu at level %u\n",
                   curr_node->get_header(), level);
#endif
            // lock_timer.start();
            if (level > 0)
            {
                if (level_to_promote < level)
                {
                    ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
                   	#if STATS
                    	read_lock_counter++;
                    #endif
#if ENABLE_TRACE_TIMER
                    read_lock_count.inc();
#endif
                }
                else
                {
                    ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.write_lock();
                    #if STATS
                    write_lock_counter++;
                    #endif
                }
            }
            else
            {
                // if at a leaf, lock yourself
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_lock();
                #if STATS
                write_lock_counter++;
                #endif
            }

#if DEBUG
            if (parent_node)
            {
                tbassert(parent_node->get_key_at_rank(parent_rank) ==
                             curr_node->get_header(),
                         "level = %u, level_to_promote = %u, inserting key %lu, parent "
                         "node header %lu, parent_keys[%u] = %lu, curr header = %lu\n",
                         level, level_to_promote, k, parent_node->get_header(),
                         parent_rank, parent_node->get_key_at_rank(parent_rank),
                         curr_node->get_header());
            }
#endif
#if DEBUG_PRINT
            printf("\tsuccessfully locked self with header %lu at level %u\n",
                   curr_node->get_header(), level);
#endif

            // if the previous level was higher than your promotion level, you had the
            // lock on the parent
            if (parent_lock)
            {
                if (level + 1 > level_to_promote)
                {
                    parent_lock->read_unlock(cpuid);
                    parent_lock = nullptr;
                }
                else
                {
                    assert(level_to_promote > level);
                    parent_lock->write_unlock();
                    parent_lock = nullptr;
                }
            }
            // lock_timer.stop();
        }
        tbassert(key >= curr_node->get_header(),
                 "level = %u, k = %lu, header = %lu\n", level, k,
                 curr_node->get_header());

        auto prev_node = curr_node;

        // find the node to insert the key in in this level
        while (curr_node->next_header <= key)
        {
            tbassert(curr_node->get_header() < curr_node->next->get_header(),
                     "curr node header %lu, next header %lu\n",
                     curr_node->get_header(), curr_node->next->get_header());
            tbassert(key >= curr_node->get_header(),
                     "key = %lu, curr node header = %lu, next node header = %lu\n", k,
                     curr_node->get_header(), curr_node->next->get_header());

            // grab next step in the search
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    if (level_to_promote < level)
                    {
                   	#if STATS
                    	read_lock_counter++;
                    #endif
                        ((BSkipNodeInternal<traits> *)(curr_node->next))
                            ->mutex_.read_lock(cpuid);
#if ENABLE_TRACE_TIMER
                        read_lock_count.inc();
#endif
                    }
                    else
                    {
                        ((BSkipNodeInternal<traits> *)(curr_node->next))
                            ->mutex_.write_lock();
                        #if STATS
                        write_lock_counter++;
                        #endif
                    }
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.write_lock();
                    #if STATS
                    write_lock_counter++;
                    #endif
                }
            }

            prev_node = curr_node;

            tbassert(curr_node->next->get_header() <= key,
                     "k = %lu, prev node = %lu, next node = %lu\n", k,
                     prev_node->get_header(), curr_node->next->get_header());

            curr_node = curr_node->next;

            // unlock prev node
            if constexpr (traits::concurrent)
            {
                // lock_timer.start();
                if (level > 0)
                {
                    if (level_to_promote < level)
                    {
                        ((BSkipNodeInternal<traits> *)(prev_node))
                            ->mutex_.read_unlock(cpuid);
                    }
                    else
                    {
                        ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.write_unlock();
                    }
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.write_unlock();
                }
                // lock_timer.stop();
            }
        }
        tbassert(curr_node->get_header() <= key,
                 "k = %lu, level to promote %u, level = %u, prev_node_header = "
                 "%lu, curr node = %lu\n",
                 k, level_to_promote, level, prev_node->get_header(),
                 curr_node->get_header());

        // now we are at the correct node - look for the key
        auto [rank, found_key] = curr_node->find_key_and_check(key);

        // if the key was found
        if (found_key)
        {
            // release your lock and do nothing else in the key-only mode
            if constexpr (traits::concurrent)
            {
                // lock_timer.start();
                if (level > 0)
                {
                    if (level_to_promote < level)
                    {
                        ((BSkipNodeInternal<traits> *)(curr_node))
                            ->mutex_.read_unlock(cpuid);
                    }
                    else
                    {
                        ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.write_unlock();
                    }
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
                }
                // lock_timer.stop();
            }

            // check if map or set
            if constexpr (!traits::binary)
            {
                if (level == 0)
                {
                    if constexpr (traits::concurrent)
                    {
                        ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_lock();
                        #if STATS
                        write_lock_counter++;
                        #endif
                    }
                    // change leaf value
                    ((BSkipNodeLeaf<traits> *)curr_node)->blind_write(k, rank);
                    if constexpr (traits::concurrent)
                    {
                        ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
                    }
                }
                else
                {
                    // curr_node is internal, need to traverse down to the leaf
                    if constexpr (traits::concurrent)
                    {
                   	#if STATS
                    	read_lock_counter++;
                    #endif
                        ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
                    }
                    bool flag = true;
                    while (curr_node->level > 1)
                    {
                        prev_node = curr_node;
                        if (flag)
                        {
                            curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
                            flag = false;
                        }
                        else
                        {
                            curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(0);
                        }
                        if constexpr (traits::concurrent)
                        {
   	                   	#if STATS
                        	read_lock_counter++;
                        #endif
                            // lock curr
                            ((BSkipNodeInternal<traits> *)curr_node)->mutex_.read_lock(cpuid);
                            // unlock prev
                            ((BSkipNodeInternal<traits> *)prev_node)->mutex_.read_unlock(cpuid);
                        }
                    }

                    // now level = 1, curr_node is internal and locked
                    assert(curr_node->level == 1);

                    prev_node = curr_node;

                    if (flag)
                    {
                        curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
                    }
                    else
                    {
                        curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(0);
                    }

                    if constexpr (traits::concurrent)
                    {
                        // lock curr
                        ((BSkipNodeLeaf<traits> *)curr_node)->mutex_.write_lock();
                        #if STATS
                        write_lock_counter++;
                        #endif
                        // unlock prev
                        ((BSkipNodeInternal<traits> *)prev_node)->mutex_.read_unlock(cpuid);
                    }

                    // change leaf value
                    assert(curr_node->level == 0);
                    ((BSkipNodeLeaf<traits> *)curr_node)->blind_write(k, 0);

                    if constexpr (traits::concurrent)
                    {
                        ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
                    }
                }
            }

            return true;
        }
        else
        { // otherwise, this key was not found at this level
            assert(curr_node->get_key_at_rank(rank) < key);
#if DEBUG_PRINT
            printf("\tnot found at level %u\n", level);
#endif
            if (level_to_promote < level)
            {
                // case 1: do not promote to this level.
#if DEBUG_PRINT
                printf("\t\tcurr node num elts %u\n", curr_node->num_elts);
                printf("\t\tcurr node elt %lu, level %d\n",
                       curr_node->get_key_at_rank(rank), level);
#endif

                // drop down a level (if you are here, you are at an internal node)
                assert(curr_node->level > 0);
                if constexpr (traits::concurrent)
                {
                    // lock_timer.start();
                    parent_lock = &(((BSkipNodeInternal<traits> *)curr_node)->mutex_);
                    parent_node = ((BSkipNodeInternal<traits> *)curr_node);
#if DEBUG
                    parent_rank = rank;
#endif
                    // lock_timer.stop();
                }

                curr_node =
                    ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);

                assert(curr_node != NULL);
                continue;
            }
            else if (level_to_promote == level)
            {
                // Case 2: insert but not split due to promotion
                // split if overfull
                if (curr_node->num_elts + 1 > traits::MAX_KEYS)
                {
                    BSkipNode<traits> *new_node;
                    if (level > 0)
                    {
                        new_node = new BSkipNodeInternal<traits>();
                    }
                    else
                    {
                        new_node = new BSkipNodeLeaf<traits>();
                    }

                    // grab lock on the new node
                    if constexpr (traits::concurrent)
                    {
                        // lock_timer.start();
                        if (level > 0)
                        {
                            ((BSkipNodeInternal<traits> *)(new_node))->mutex_.write_lock();
                            #if STATS
                            write_lock_counter++;
                            #endif
                        }
                        else
                        {
                            ((BSkipNodeLeaf<traits> *)(new_node))->mutex_.write_lock();
                            #if STATS
                            write_lock_counter++;
                            #endif
                        }
                        // lock_timer.stop();
                    }

                    // fixup next pointers
                    new_node->next = curr_node->next;
                    new_node->next_header = curr_node->next_header;
                    curr_node->next = new_node;
                    new_node->level = level;

                    // do the split
                    int half_keys = curr_node->num_elts / 2;

                    // move second half of keys into new node
                    // returns the number of elements that were moved
                    // updates the number of elts in each node
                    uint32_t elts_moved = curr_node->split_keys(new_node, half_keys, 0);
                    curr_node->next_header = new_node->get_header();

                    // move children if necessary
                    if (level > 0)
                    {
                        ((BSkipNodeInternal<traits> *)curr_node)
                            ->move_children(((BSkipNodeInternal<traits> *)new_node),
                                            half_keys, elts_moved, 0);
                    }

#if DEBUG_PRINT
                    printf("split moved %u elts\n", elts_moved);
                    printf("curr node keys:\n");
                    curr_node->print_keys();
                    printf("new node keys\n");
                    new_node->print_keys();
#endif
                    // new elt goes into first node
                    if (rank + 1 <= curr_node->num_elts)
                    {
                        // release lock on the new node
                        if constexpr (traits::concurrent)
                        {
                            // lock_timer.start();
                            if (level > 0)
                            {
                                ((BSkipNodeInternal<traits> *)(new_node))
                                    ->mutex_.write_unlock();
                            }
                            else
                            {
                                ((BSkipNodeLeaf<traits> *)(new_node))->mutex_.write_unlock();
                            }
                            // lock_timer.stop();
                        }
                        assert(key < new_node->get_header());
                        assert(key > curr_node->get_header());
                        if (level > 0)
                        {
                            ((BSkipNodeInternal<traits> *)(curr_node))
                                ->insert_key_at_rank(rank + 1, key);
                        }
                        else
                        {
                            ((BSkipNodeLeaf<traits> *)(curr_node))
                                ->insert_elt_at_rank(rank + 1, k);
                        }
                        curr_node->num_elts++;

#if DEBUG_PRINT
                        printf("\tinserting key in first node at rank %u\n", rank + 1);
                        curr_node->print_keys();
#endif
                        // if you are an internal node, make space for the new element's
                        // child
                        assert(num_split == 0);
                        if (level > 0)
                        {
                            BSkipNodeInternal<traits> *curr_node_cast =
                                (BSkipNodeInternal<traits> *)curr_node;
                            // point to the first split node
                            curr_node_cast->insert_child_at_rank(rank + 1,
                                                                 new_nodes[num_split]);
                            if constexpr (traits::concurrent)
                            {
                                // lock_timer.start();
                                parent_lock = &((curr_node_cast)->mutex_);
                                parent_node = curr_node_cast;
#if DEBUG
                                parent_rank = rank;
#endif
                                // lock_timer.stop();
                            }
                            curr_node = curr_node_cast->get_child_at_rank(rank);
                        }
                    }
                    else
                    { // insert it into the new node
                        assert(key > new_node->get_header());
                        assert(new_node->num_elts > 0);
                        int rank_to_insert = rank - curr_node->num_elts;

                        // release left
                        if constexpr (traits::concurrent)
                        {
                            // lock_timer.start();
                            if (level > 0)
                            {
                                ((BSkipNodeInternal<traits> *)(curr_node))
                                    ->mutex_.write_unlock();
                            }
                            else
                            {
                                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
                            }
                            // lock_timer.stop();
                        }
#if DEBUG_PRINT
                        printf("rank %u, prev node num elts %u, rank to insert %u\n", rank,
                               curr_node->num_elts, rank_to_insert);
#endif
                        // insert into right
                        if (level > 0)
                        {
                            ((BSkipNodeInternal<traits> *)(new_node))
                                ->insert_key_at_rank(rank_to_insert + 1, key);
                        }
                        else
                        {
                            ((BSkipNodeLeaf<traits> *)(new_node))
                                ->insert_elt_at_rank(rank_to_insert + 1, k);
                        }
                        new_node->num_elts++;

#if DEBUG_PRINT
                        printf("insert key into new node at rank %d\n", rank_to_insert + 1);
                        new_node->print_keys();
#endif

                        if (level > 0)
                        {
                            BSkipNodeInternal<traits> *new_node_cast =
                                (BSkipNodeInternal<traits> *)new_node;
                            new_node_cast->insert_child_at_rank(rank_to_insert + 1,
                                                                new_nodes[num_split]);
                            if constexpr (traits::concurrent)
                            {
                                // lock_timer.start();
                                parent_node = new_node_cast;
                                parent_lock = &((new_node_cast)->mutex_);
#if DEBUG
                                parent_rank = rank_to_insert;
#endif
                                // lock_timer.stop();
                            }

                            // drop down to next level
                            curr_node = new_node_cast->get_child_at_rank(rank_to_insert);

                            assert(curr_node);
                            assert(curr_node->get_header() ==
                                   new_node_cast->get_child_at_rank(rank_to_insert)
                                       ->get_header());
                        }
                        else
                        {
                            curr_node = new_node;
                        }
                    }
                }
                else
                { // did not fill this node
                    assert(level_to_promote == level);
                    // add key (and if needed, value) to this node
                    if (level > 0)
                    {
                        ((BSkipNodeInternal<traits> *)curr_node)
                            ->insert_key_at_rank(rank + 1, key);
                    }
                    else
                    {
                        ((BSkipNodeLeaf<traits> *)curr_node)
                            ->insert_elt_at_rank(rank + 1, k);
                    }

#if DEBUG_PRINT
                    printf("\n\tinserted %lu into level %u at rank %u\n", key, level,
                           rank + 1);
                    curr_node->print_keys();
#endif

                    // make space for the child pointer
                    if (level > 0)
                    {
                        BSkipNodeInternal<traits> *curr_node_cast =
                            (BSkipNodeInternal<traits> *)curr_node;
                        // update the down pointer for the new elt
                        assert(num_split == 0);
                        assert(rank < curr_node->num_elts);
                        curr_node_cast->insert_child_at_rank(rank + 1,
                                                             new_nodes[num_split], false);

                        // keep track of the lock
                        if constexpr (traits::concurrent)
                        {
                            // lock_timer.start();
                            parent_lock = &((curr_node_cast)->mutex_);
                            parent_node = curr_node_cast;
#if DEBUG
                            parent_rank = rank;
#endif
                            // lock_timer.stop();
                        }
                        // update num elts
                        curr_node->num_elts++;
                        tbassert(curr_node->num_elts <= traits::MAX_KEYS, "num elts %lu, max keys %lu\n", curr_node->num_elts, traits::MAX_KEYS);
                        // drop down
                        curr_node = curr_node_cast->get_child_at_rank(rank);

#if DEBUG_PRINT
                        printf("\tmake space at rank %u on level %d, node head %lu\n",
                               rank + 1, level, curr_node->get_header());
#endif
                        assert(curr_node != NULL);
                    }
                    else
                    {
                        curr_node->num_elts++;
                    }
                }
            }
            else
            { // case 3: do a split
#if DEBUG_PRINT
                printf("split at level %d with elt %lu\n", level, key);
#endif
                assert(level_to_promote > level);
                BSkipNode<traits> *new_node = new_nodes[num_split];
                num_split++;

                assert(new_node);

                // fixup pointer from new to next node
                new_node->next = curr_node->next;
                new_node->next_header = curr_node->next_header;

                // set the level
                new_node->level = level;

                // put the new key at the start of the new node
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)new_node)->insert_key_at_rank(0, key);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(new_node))->insert_elt_at_rank(0, k);
                }
                new_node->num_elts++;

                // move all keys in the curr node starting from rank + 1 into the new
                // node returns the number of elements that were moved updates the
                // number of elts in each node
                uint32_t elts_moved = curr_node->split_keys(new_node, rank + 1);

#if DEBUG_PRINT
                printf("split at level %u moved %u elts\n", level, elts_moved);
                printf("curr node keys:\n");
                curr_node->print_keys();
                printf("\n");
                printf("new node keys\n");
                new_node->print_keys();
                printf("\n");
#endif

                // if you are an internal node, make space for the child of this head
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)new_node)
                        ->insert_child_at_rank(0, new_nodes[num_split]);

                    ((BSkipNodeInternal<traits> *)curr_node)
                        ->move_children(((BSkipNodeInternal<traits> *)new_node), rank + 1,
                                        elts_moved);
                }

                assert(curr_node->get_header() < curr_node->next->get_header());
                assert(new_node->get_header() < curr_node->next->get_header());
                assert(curr_node->get_header() < new_node->get_header());
#if DEBUG_PRINT
                printf("curr header %lu, next header %lu, new header %lu\n",
                       curr_node->get_header(), curr_node->next->get_header(),
                       new_node->get_header());
#endif

                // fixup next pointers
                curr_node->next = new_node;
                curr_node->next_header = new_node->get_header();

                // unlock new node
                if constexpr (traits::concurrent)
                {
                    // lock_timer.start();
                    if (level > 0)
                    {
                        ((BSkipNodeInternal<traits> *)(new_node))->mutex_.write_unlock();
                    }
                    else
                    {
                        ((BSkipNodeLeaf<traits> *)(new_node))->mutex_.write_unlock();
                    }
                    // lock_timer.stop();
                }

                // if we are at an internal level, move on and save info for lower
                // splits
                if (level > 0)
                {
                    // save parent lock

                    BSkipNodeInternal<traits> *curr_node_cast =
                        (BSkipNodeInternal<traits> *)curr_node;
                    if constexpr (traits::concurrent)
                    {
                        // lock_timer.start();
                        parent_lock = &(curr_node_cast->mutex_);
                        parent_node = curr_node_cast;
#if DEBUG
                        parent_rank = rank;
#endif
                        // lock_timer.stop();
                    }

                    // drop down one level
                    curr_node = curr_node_cast->get_child_at_rank(rank);
                    assert(curr_node);
                }
            }
        }
        if (level == 0)
        {
            if constexpr (traits::concurrent)
            {
#if DEBUG_PRINT
                printf("*** unlock leaf with header %lu at end\n",
                       curr_node->get_header());
#endif
                // lock_timer.start();
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
                // lock_timer.stop();
            }
            assert(curr_node);
#if ENABLE_TRACE_TIMER
            // return lock_timer.get_elapsed_time();
#else
            return true;
#endif
        }
    }
    return true;
}

template <typename traits>
BSkipNode<traits> *BSkip<traits>::find(traits::key_type k) const
{
    // TODO: deal with if if you look for 0. should this return true for the
    // unweighted case and the val in the weighted case? why is the node the thing
    // getting returned?
#if DEBUG_PRINT
    printf("searching for %lu\n", k);
#endif

    // int cpuid = ParallelTools::getWorkerNum();
    int cpuid = sched_getcpu();
    ReaderWriterLock *parent_lock = nullptr;

    // start search from the top node
    auto curr_node = headers[MAX_HEIGHT - 1];

    // should never be here because the header always has something init
    if (!curr_node)
    {
        assert(false);
    }

    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        if constexpr (traits::concurrent)
        {
            // grab current lock
            if (level > 0)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }

            // if you have a parent, release it
            if (parent_lock)
            {
                parent_lock->read_unlock(cpuid);
            }
        }
        tbassert(k >= curr_node->get_header(),
                 "level = %u, k = %lu, header = %lu\n", level, k,
                 curr_node->get_header());

        auto prev_node = curr_node;

        // move forward until we find the right node that contains the key range
        while (curr_node->next_header <= k)
        {
#if DEBUG_PRINT
            printf("\tcurr node header %lu\n", curr_node->get_header());
            printf("\tnext node header %lu\n", curr_node->next->get_header());
#endif
            assert(curr_node->get_header() < curr_node->next->get_header());

            tbassert(k >= curr_node->get_header(),
                     "key = %lu, curr node header = %lu, next node header = %lu\n", k,
                     curr_node->get_header(), curr_node->next->get_header());
#if DEBUG
            auto next_header = curr_node->next->get_header();
#endif
            // grab next step in the search
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeInternal<traits> *)(curr_node->next))
                        ->mutex_.read_lock(cpuid);
                }
                else
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.read_lock(cpuid);
                }
            }

            tbassert(next_header >= curr_node->next->get_header(),
                     "next header before lock %lu, next header after lock %lu\n",
                     next_header, curr_node->next->get_header());

            prev_node = curr_node;

            tbassert(curr_node->next->get_header() <= k,
                     "k = %lu, prev node = %lu, next node = %lu\n", k,
                     prev_node->get_header(), curr_node->next->get_header());

            curr_node = curr_node->next;

            // unlock prev node
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.read_unlock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.read_unlock();
                }
            }
        }
        assert(curr_node->get_header() <= k);

        // look for the largest element that is at most the search key
        auto [rank, found_key] = curr_node->find_key_and_check(k);

        // if it is found, return the node (returns the topmost node the key is
        // found in
        if (curr_node->get_key_at_rank(rank) == k)
        {
#if DEBUG_PRINT
            printf("found key at rank %u\n", rank);
            curr_node->print_keys();
#endif

            // unlock your current node
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_unlock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
                }
            }

            return curr_node;
        }

        // if not found, drop down a level
        if (level > 0)
        {
            if constexpr (traits::concurrent)
            {
                parent_lock = &(((BSkipNodeInternal<traits> *)curr_node)->mutex_);
            }

            curr_node =
                ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
        }
    }
    return NULL;
}

template <typename traits>
traits::value_type BSkip<traits>::value(traits::key_type k) const
{
    // TODO: deal with if if you look for 0. should this return true for the
    // unweighted case and the val in the weighted case? why is the node the thing
    // getting returned?
#if DEBUG_PRINT
    printf("searching for %lu\n", k);
#endif

// stats for how many steps horizontally taken
#if STATS
    this->query_counter++;
    int local_step_counter = 0;
#endif

    // int cpuid = ParallelTools::getWorkerNum();
    int cpuid = sched_getcpu();
    ReaderWriterLock *parent_lock = nullptr;

    // start search from the top node
    auto curr_node = headers[MAX_HEIGHT - 1];

    // should never be here because the header always has something init
    if (!curr_node)
    {
        assert(false);
    }

    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        if constexpr (traits::concurrent)
        {
            // grab current lock
            if (level > 0)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }

            // if you have a parent, release it
            if (parent_lock)
            {
                parent_lock->read_unlock(cpuid);
            }
        }
        tbassert(k >= curr_node->get_header(),
                 "level = %u, k = %lu, header = %lu\n", level, k,
                 curr_node->get_header());

        auto prev_node = curr_node;

        // move forward until we find the right node that contains the key range
        // if k <= curr_node->max() break // if k < curr_node->next->min() break // else continue the loop
        assert(curr_node->next != nullptr);
        while (k >= curr_node->next_header)
        {
#if DEBUG_PRINT
            printf("\tcurr node header %lu\n", curr_node->get_header());
            printf("\tnext node header %lu\n", curr_node->next->get_header());
#endif
            assert(curr_node->get_header() < curr_node->next->get_header());

            tbassert(k >= curr_node->get_header(),
                     "key = %lu, curr node header = %lu, next node header = %lu\n", k,
                     curr_node->get_header(), curr_node->next->get_header());
#if DEBUG
            auto next_header = curr_node->next->get_header();
#endif
            // grab next step in the search
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeInternal<traits> *)(curr_node->next))
                        ->mutex_.read_lock(cpuid);
                }
                else
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.read_lock(cpuid);
                }
            }

            tbassert(next_header >= curr_node->next->get_header(),
                     "next header before lock %lu, next header after lock %lu\n",
                     next_header, curr_node->next->get_header());

            prev_node = curr_node;

            tbassert(curr_node->next->get_header() <= k,
                     "k = %lu, prev node = %lu, next node = %lu\n", k,
                     prev_node->get_header(), curr_node->next->get_header());

            curr_node = curr_node->next;

            // unlock prev node
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.read_unlock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.read_unlock();
                }
            }

#if STATS
            this->steps_counter++;
            local_step_counter++;
#endif
        }
        assert(curr_node->get_header() <= k);

        // look for the largest element that is at most the search key
        auto [rank, found_key] = curr_node->find_key_and_check(k);

        // if it is found, return the node returns the topmost node the key is
        // found in
        if (found_key)
        {
            // printf("found key at rank %u at level %u\n", rank, level);
            // printf("curr node elements: %d\n", curr_node->num_elts);
#if DEBUG_PRINT
            printf("found key at rank %u\n", rank);
            curr_node->print_keys();
#endif

            typename traits::value_type return_value;

            if (level == 0)
            {
                return_value = ((BSkipNodeLeaf<traits> *)curr_node)->get_value_at_rank(rank);

                if constexpr (traits::concurrent)
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
                }

#if STATS
                this->steps_vector.push_back(local_step_counter);
#endif

                return return_value;
            }
            else
            {
                // curr_node is internal, need to traverse down to the leaf
                bool flag = true;
                while (curr_node->level > 1)
                {
                    prev_node = curr_node;
                    if (flag)
                    {
                        curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
                        flag = false;
                    }
                    else
                    {
                        curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(0);
                    }
                    if constexpr (traits::concurrent)
                    {
                   	#if STATS
                    	read_lock_counter++;
                    #endif
                        // lock curr
                        ((BSkipNodeInternal<traits> *)curr_node)->mutex_.read_lock(cpuid);
                        // unlock prev
                        ((BSkipNodeInternal<traits> *)prev_node)->mutex_.read_unlock(cpuid);
                    }
                }

                // now level = 1, curr_node is internal and locked
                assert(curr_node->level == 1);

                prev_node = curr_node;

                if (flag)
                {
                    curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
                }
                else
                {
                    curr_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(0);
                }

                if constexpr (traits::concurrent)
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    // lock curr
                    ((BSkipNodeLeaf<traits> *)curr_node)->mutex_.read_lock(cpuid);
                    // unlock prev
                    ((BSkipNodeInternal<traits> *)prev_node)->mutex_.read_unlock(cpuid);
                }

                return_value = ((BSkipNodeLeaf<traits> *)curr_node)->get_value_at_rank(0);

                if constexpr (traits::concurrent)
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
                }

#if STATS
                this->steps_vector.push_back(local_step_counter);
#endif

                return return_value;
            }
        }

        // if not found, drop down a level
        if (level > 0)
        {
            if constexpr (traits::concurrent)
            {
                parent_lock = &(((BSkipNodeInternal<traits> *)curr_node)->mutex_);
            }

            curr_node =
                ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
        }
    }
    
    if constexpr (traits::concurrent)
    {
    	((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
    }

#if STATS
    this->steps_vector.push_back(local_step_counter);
#endif

    return NULL;
}

// map_range given a start and end
// Todo, turn the locking find into a separate function to save space
template <typename traits>
template <class F>
void BSkip<traits>::map_range(traits::key_type min, traits::key_type max, F f) const
{
    // Concurrency mechanisms
    // int cpuid = ParallelTools::getWorkerNum();
    int cpuid = sched_getcpu();
    ReaderWriterLock *parent_lock = nullptr;

    // Sequential
    // Get node with starting element that is at least min
    // Start at the very top leftmost node
    BSkipNode<traits> *current = this->headers[MAX_HEIGHT - 1];
    uint32_t current_key_rank = 0;
    bool found = false;

    // Start search at top level, going down as much as needed
    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        // If concurrent let's grab a read lock
        if constexpr (traits::concurrent)
        {
            if (level > 0)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release parent lock
            if (parent_lock)
            {
                parent_lock->read_unlock(cpuid);
            }
        }

        auto previous = current;
        // Search through each level until you find the correct node range
        // Is get header able to be called on a node that does not have a read lock
        while (current->next_header <= min)
        {
            // Concurrency hand over hand locking
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeInternal<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
                else
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
            }
            previous = current;
            current = current->next;
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(previous))->mutex_.read_unlock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
                }
            }
        }

        // Check if the current node contains the key if not move down a level if possible
        auto [rank, found_key] = current->find_key_and_check(min);
        current_key_rank = rank;
        if (found)
        {
            found = found_key;
            break;
        }
        else if (level != 0)
        {
            if constexpr (traits::concurrent)
            {
                parent_lock = &(((BSkipNodeInternal<traits> *)current)->mutex_);
            }
            current = ((BSkipNodeInternal<traits> *)current)->get_child_at_rank(current_key_rank);
        }
    }

    // For use to determine what to cast pointer as
    uint32_t cur_level = current->level;
    // Get the bottommost node the key is found in, if key is not found, we are already on level 0
    while (cur_level != 0)
    {
        // Concurrency for jumping down levels, save the current as the new parent
        if constexpr (traits::concurrent)
        {
            parent_lock = &(((BSkipNodeInternal<traits> *)current)->mutex_);
        }
        current = ((BSkipNodeInternal<traits> *)current)->get_child_at_rank(current_key_rank);
        // Now we unlock the parent and lock the current
        if constexpr (traits::concurrent)
        {
            if (cur_level > 1)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release the parent now
            parent_lock->read_unlock(cpuid);
        }
        cur_level = current->level;
        current_key_rank = current->find_key(min);
    }

    // To make the range query inclusive, in the case min is not found we need to increment the rank by 1
    if (!found)
    {
        current_key_rank += 1;
        // Check that we have not exceeded the bounds of the node
        if (current_key_rank == current->num_elts)
        {
            auto previous = current;
            if constexpr (traits::concurrent)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
            }
            current = current->next;
            current_key_rank = 0;
            if constexpr (traits::concurrent)
            {
                ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
            }
        }
    }

    // Iterate through all the elements less than max, applying function F along the way
    while (current->get_key_at_rank(current_key_rank) <= max && current->get_key_at_rank(current_key_rank) != std::numeric_limits<K>::max())
    {
        f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->blind_read_val(current_key_rank));
        current_key_rank += 1;
        // Check if we have reached the end of current
        if (current_key_rank == current->num_elts)
        {
            auto previous = current;
            if constexpr (traits::concurrent)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
            }
            current = current->next;
            current_key_rank = 0;
            if constexpr (traits::concurrent)
            {
                ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
            }
        }
    }

    // Unlock after the query is complete
    if constexpr (traits::concurrent)
    {
        ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_unlock();
    }
}

// map_range given a start point and an end point that is guaranteed to be within bounds
template <typename traits>
template <class F>
void BSkip<traits>::map_range_length(traits::key_type start, uint64_t length, F f) const
{
    // Concurrency mechanisms
    int cpuid = sched_getcpu();
    ReaderWriterLock *parent_lock = nullptr;

    // Get node with starting element that is at least start
    // Start at the very top leftmost node
    BSkipNode<traits> *current = this->headers[MAX_HEIGHT - 1];
    uint32_t current_key_rank = 0;
    bool found = false;

    // Start search at top level, going down as much as needed
    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        // If concurrent let's grab a read lock
        if constexpr (traits::concurrent)
        {
            if (level > 0)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release parent lock
            if (parent_lock)
            {
                parent_lock->read_unlock(cpuid);
            }
        }

        auto previous = current;
        // Search through each level until you find the correct node range
        // Is get header able to be called on a node that does not have a read lock
        while (start >= current->next_header)
        // while (current->next->get_header() <= start)
        {
            // Concurrency hand over hand locking
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeInternal<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
                else
                {
               	#if STATS
                	read_lock_counter++;
                #endif
                    ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
            }
            previous = current;
            current = current->next;
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(previous))->mutex_.read_unlock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
                }
            }
        }

        // Check if the current node contains the key if not move down a level if possible
        auto [rank, found_key] = current->find_key_and_check(start);
        current_key_rank = rank;
        if (found_key)
        {
            found = found_key;
            break;
        }
        else if (level != 0)
        {
            if constexpr (traits::concurrent)
            {
                parent_lock = &(((BSkipNodeInternal<traits> *)current)->mutex_);
            }
            current = ((BSkipNodeInternal<traits> *)current)->get_child_at_rank(current_key_rank);
        }
    }

    // For use to determine what to cast pointer as
    uint32_t cur_level = current->level;
    // Get the bottommost node the key is found in, if key is not found, we are already on level 0
    while (cur_level != 0)
    {
        // Concurrency for jumping down levels, save the current as the new parent
        if constexpr (traits::concurrent)
        {
            parent_lock = &(((BSkipNodeInternal<traits> *)current)->mutex_);
        }
        current = ((BSkipNodeInternal<traits> *)current)->get_child_at_rank(current_key_rank);
        // Now we unlock the parent and lock the current
        if constexpr (traits::concurrent)
        {
            if (cur_level > 1)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release the parent now
            parent_lock->read_unlock(cpuid);
        }
        cur_level = current->level;
        current_key_rank = current->find_key(start);
    }

    // To make the range query inclusive, in the case min is not found we need to increment the rank by 1
    if (!found)
    {
        current_key_rank += 1;
        // Check that we have not exceeded the bounds of the node
        if (current_key_rank == current->num_elts)
        {
            auto previous = current;
            if constexpr (traits::concurrent)
            {
           	#if STATS
            	read_lock_counter++;
            #endif
                ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
            }
            current = current->next;
            current_key_rank = 0;
            if constexpr (traits::concurrent)
            {
                ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
            }
        }
    }
    
    // int num_remaining = length;
	// while (curr->head != std::numeric_limits<K>::max() && remaining) {
	//  int iteration = min(curr->num_elements, remaiming);
	//  for (int i=0; i<iteration;i++) {
	//   f(curr->get_key_at_rank(i), curr->get_val_at_rank(i));
	// }
	// num_remaining -= iteration;
	// if (remaining) curr = curr->next;
	// }
	
	#if STATS
		this->range_length_counter++;
	#endif
	
	uint32_t num_remaining = length;
	while (current->get_key_at_rank(current_key_rank) != std::numeric_limits<K>::max() && num_remaining) {
		int iteration = std::min(current->num_elts - current_key_rank, num_remaining);
		for (int i=0; i<iteration;i++) {
			f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->blind_read_val(current_key_rank));
			current_key_rank++;
		}
		num_remaining -= iteration;
		if (num_remaining) {
			auto previous = current;
			if constexpr (traits::concurrent)
			{
   					#if STATS
                    	read_lock_counter++;
                    #endif
				((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
			}
			current = current->next;
			current_key_rank = 0;
			if constexpr (traits::concurrent)
			{
				((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
			}
		}
		#if STATS
		this->range_length_node_counter++;
		#endif
	}


    // // Iterate through all the elements length to the right, applying function F along the way
    // for (uint64_t counter = 0; counter < length; counter++)
    // {
    //     // Check that we are not exceeding the bounds
    //     if (current->get_key_at_rank(current_key_rank) == std::numeric_limits<K>::max())
    //     {
    //         break;
    //     }
    //     f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->blind_read_val(current_key_rank));
    //     current_key_rank += 1;
    //     // Check if we have reached the end of current
    //     if (current_key_rank == current->num_elts)
    //     {
    //         auto previous = current;
    //         if constexpr (traits::concurrent)
    //         {
    // 			  #if STATS
    //            	read_lock_counter++;
    //            #endif
    //             ((BSkipNodeLeaf<traits> *)(current->next))->mutex_.read_lock(cpuid);
    //         }
    //         current = current->next;
    //         current_key_rank = 0;
    //         if constexpr (traits::concurrent)
    //         {
    //             ((BSkipNodeLeaf<traits> *)(previous))->mutex_.read_unlock();
    //         }
    //     }
    // }

    // Unlock after the query is complete
    if constexpr (traits::concurrent)
    {
        ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_unlock();
    }
}

// true if found
template <typename traits>
bool BSkip<traits>::exists(K k) const
{
    auto node = find(k);
    return node ? true : false;
}

// sums all keys
template <typename traits>
uint64_t BSkip<traits>::sum()
{
    auto curr_node = headers[0];
    uint64_t result = 0;
    while (curr_node->get_header() < std::numeric_limits<K>::max())
    {
#if DEBUG_PRINT
        printf("node header %lu, add sum %lu\n", curr_node->get_header(),
               ((BSkipNodeLeaf<traits> *)curr_node)->sum_keys());
#endif
        result += ((BSkipNodeLeaf<traits> *)curr_node)->sum_keys();
        curr_node = curr_node->next;
    }
    return result;
}

// sums all keys
template <typename traits>
uint64_t BSkip<traits>::sum_vals()
{
    auto curr_node = headers[0];
    uint64_t result = 0;
    while (curr_node->get_header() < std::numeric_limits<K>::max())
    {
        result += ((BSkipNodeLeaf<traits> *)curr_node)->sum_vals();
        curr_node = curr_node->next;
    }
    return result;
}

// sums all keys
template <typename traits>
void BSkip<traits>::sum_helper(std::vector<uint64_t> &sums,
                               BSkipNode<traits> *node, int level,
                               K local_max)
{
    assert(node);
    assert(node->next);
    if (level == 0)
    {
        if (node->next_header < local_max)
        {
            sum_helper(sums, node->next, level, local_max);
        }
#if DEBUG_PRINT
        printf("\tnode header %lu, add sum %lu\n", node->get_header(),
               node->sum_keys());
#endif
        sums[ParallelTools::getWorkerNum() * 8] += node->sum_keys();
    }
    else
    {
        BSkipNodeInternal<traits> *curr_node_cast =
            (BSkipNodeInternal<traits> *)node;
        if (curr_node_cast->next_header < local_max)
        {
            sum_helper(sums, node->next, level, local_max);
        }
#if DEBUG_PRINT
        printf("start key %lu, end key %lu\n", node->get_header(),
               node->get_key_at_rank(node->num_elts - 1));
#endif
        for (int i = 0; i < node->num_elts; i++)
        {
            if (i < node->num_elts - 1)
            {
                sum_helper(sums, curr_node_cast->get_child_at_rank(i), level - 1,
                           node->get_key_at_rank(i + 1));
            }
            else
            {
                sum_helper(sums, curr_node_cast->get_child_at_rank(i), level - 1,
                           node->next_header);
            }
        }
    }
}

// sums all keys
template <typename traits>
uint64_t BSkip<traits>::psum()
{
    std::vector<uint64_t> partial_sums(ParallelTools::getWorkers() * 8);
    //  auto curr_node = headers[curr_max_height];
    int start_level = 2;
    auto top_node = headers[start_level];
    while (top_node->get_header() < std::numeric_limits<K>::max())
    {
        BSkipNodeInternal<traits> *top_node_cast =
            (BSkipNodeInternal<traits> *)top_node;
        // iterate over the 2nd level
#if CILK
        cilk_for(uint32_t i = 0; i < top_node->num_elts; i++)
        {
#else
        for (uint32_t i = 0; i < top_node->num_elts; i++)
        {
#endif
            auto curr_node = top_node_cast->get_child_at_rank(i);
            K end;
            if (i < top_node->num_elts - 1)
            {
                end = top_node->get_key_at_rank(i + 1);
            }
            else
            {
                end = top_node->next_header;
            }

            while (curr_node->get_header() < end)
            {
                BSkipNodeInternal<traits> *curr_node_cast =
                    (BSkipNodeInternal<traits> *)curr_node;
#if CILK
                cilk_for(uint32_t j = 0; j < curr_node->num_elts; j++)
                {
#else
                for (uint32_t j = 0; j < curr_node->num_elts; j++)
                {
#endif
                    auto curr_leaf = curr_node_cast->get_child_at_rank(j);
                    K leaf_end = 0;
                    if (j < curr_node->num_elts - 1)
                    {
                        leaf_end = curr_node->get_key_at_rank(j + 1);
                    }
                    else
                    {
                        leaf_end = curr_node->next_header;
                    }

                    while (curr_leaf->get_header() < leaf_end)
                    {
                        partial_sums[ParallelTools::getWorkerNum() * 8] +=
                            ((BSkipNodeLeaf<traits> *)curr_leaf)->sum_keys();
                        curr_leaf = curr_leaf->next;
                    }
                }

                curr_node = curr_node->next;
            }
        }
        top_node = top_node->next;
    }
    /*
    if (i < curr_node->num_elts - 1) {
            sum_helper(partial_sums, curr_node_cast->get_child_at_rank(i), 0,
    curr_node->get_key_at_rank(i+1)); } else { sum_helper(partial_sums,
    curr_node_cast->get_child_at_rank(i), 0, curr_node->next->get_header());
    }
    */

    // sum_helper(partial_sums, curr_node, start_level,
    // std::numeric_limits<K>::max());

    // add up results
    uint64_t result = 0;
    for (int i = 0; i < ParallelTools::getWorkers(); i++)
    {
        result += partial_sums[i * 8];
    }
    return result;
}

template <typename traits>
void BSkip<traits>::get_size_stats()
{
    // we want the density per level
    uint64_t elts_per_level[MAX_HEIGHT];
    uint64_t nodes_per_level[MAX_HEIGHT];

    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        // count nodes and actual elts in them
        auto curr_node = headers[level];
        nodes_per_level[level] = 0;
        elts_per_level[level] = 0;
        while (curr_node->get_header() < std::numeric_limits<K>::max())
        {
            nodes_per_level[level]++;
            elts_per_level[level] += curr_node->num_elts;
            curr_node = curr_node->next;
        }

        // add in last sentinel
        nodes_per_level[level]++;
        elts_per_level[level]++;
    }

    uint64_t total_elts = 0;
    uint64_t total_nodes = 0;
    double density_per_level[MAX_HEIGHT];
    uint64_t num_internal_nodes = 0;
    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        // count up for total size and density
        total_elts += elts_per_level[level];
        total_nodes += nodes_per_level[level];

        if (level > 0)
        {
            num_internal_nodes += nodes_per_level[level];
        }

        // get density at this level
        double density = (double)elts_per_level[level] /
                         (double)(nodes_per_level[level] * traits::MAX_KEYS);
        printf("level %d, elts %lu, nodes %lu, total slots %lu, density = %f\n",
               level, elts_per_level[level], nodes_per_level[level],
               nodes_per_level[level] * traits::MAX_KEYS, density);

        density_per_level[level] = density;
    }

    // printf("size of internal %lu, size of leaf %lu\n",
    // sizeof(BSkipNodeInternal<traits>), sizeof(BSkipNode<traits>));
    uint64_t internal_size =
        num_internal_nodes * sizeof(BSkipNodeInternal<traits>);
    uint64_t size_in_bytes =
        internal_size + nodes_per_level[0] * sizeof(BSkipNode<traits>);
    double overhead = (double)internal_size / (double)size_in_bytes;
    double overall_density =
        (double)total_elts / (double)(total_nodes * traits::MAX_KEYS);

    double leaf_avg = elts_per_level[0] / nodes_per_level[0];
    // min, max, var of leaves
    double var_numerator = 0;
    uint32_t min_leaf = std::numeric_limits<uint32_t>::max();
    uint32_t max_leaf = std::numeric_limits<uint32_t>::min();
    auto curr_node = headers[0];
    while (curr_node->get_header() < std::numeric_limits<K>::max())
    {
        if (curr_node->num_elts < min_leaf)
        {
            min_leaf = curr_node->num_elts;
        }
        if (curr_node->num_elts > max_leaf)
        {
            max_leaf = curr_node->num_elts;
        }
        double x = (double)curr_node->num_elts - leaf_avg;
        var_numerator += (x * x);
        curr_node = curr_node->next;
    }
    double var = var_numerator / (double)(nodes_per_level[0]);
    double stdev = sqrt(var);

    printf("avg %f, min %u, max %u, var %f, stddev %f\n", leaf_avg, min_leaf,
           max_leaf, var, stdev);
    FILE *file = fopen("bskip_sizes.csv", "a+");
    fprintf(file,
            "%lu,%lu,%d,%lu,%lu,%f,%f,%lu,%f,%lu,%f,%lu,%f,%lu,%f,%lu,%f,%f,%u,%"
            "u,%f,%f\n",
            elts_per_level[0], traits::MAX_KEYS, MAX_HEIGHT, internal_size,
            size_in_bytes, overhead, overall_density, nodes_per_level[4],
            density_per_level[4], nodes_per_level[3], density_per_level[3],
            nodes_per_level[2], density_per_level[2], nodes_per_level[1],
            density_per_level[1], nodes_per_level[0], density_per_level[0],
            leaf_avg, min_leaf, max_leaf, var, stdev);
}

template <typename traits>
void BSkip<traits>::get_avg_comparisons()
{
    uint64_t total_comparisons = 0;
    uint64_t num_nodes = 0;
    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        auto curr_node = headers[level];
        while (curr_node->get_header() < std::numeric_limits<K>::max())
        {
            total_comparisons += curr_node->num_comparisons;
            num_nodes++;
            curr_node = curr_node->next;
        }
    }
    double avg_comparisons = (double)total_comparisons / (double)num_nodes;
    printf("avg comparisons = %f\n", avg_comparisons);
}

template <typename traits>
void BSkip<traits>::validate_structure()
{

    int violations = 0;

    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        auto curr_node = headers[level];

        while (curr_node->get_header() < std::numeric_limits<K>::max())
        {
            tbassert(curr_node->get_header() < curr_node->next->get_header(),
                     "level %d, curr node header %lu, next node header %lu\n",
                     curr_node->get_header(), curr_node->next->get_header());
            for (uint32_t i = 1; i < curr_node->num_elts; i++)
            {
                tbassert(curr_node->get_key_at_rank(i - 1) <
                             curr_node->get_key_at_rank(i),
                         "level %d, elts[%u] = %lu, elts[%u] = %lu\n", i - 1,
                         curr_node->get_key_at_rank(i - 1), i,
                         curr_node->get_key_at_rank(i));
            }

			if (curr_node->next->get_header() < std::numeric_limits<K>::max()) {
				if (curr_node->next_header != curr_node->next->get_header()) {
					violations++;
				}
			}

			curr_node = curr_node->next;
        }
    }

    printf("violations = %d\n", violations);
}

#endif
