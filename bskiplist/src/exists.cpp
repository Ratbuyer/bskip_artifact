#include <cstdint>

#include "../include/BSkipList.hpp"

template <typename traits>
bool BSkip<traits>::exists(traits::key_type k) const
{
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
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }
            else
            {
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
            assert(curr_node->get_header() < curr_node->next->get_header());

            tbassert(k >= curr_node->get_header(),
                     "key = %lu, curr node header = %lu, next node header = %lu\n", k,
                     curr_node->get_header(), curr_node->next->get_header());
            // grab next step in the search
            if constexpr (traits::concurrent)
            {
                if (level > 0)
                {
                    ((BSkipNodeInternal<traits> *)(curr_node->next))
                        ->mutex_.read_lock(cpuid);
                }
                else
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.read_lock(cpuid);
                }
            }

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

        // if it is found, return true
        if (curr_node->get_key_at_rank(rank) == k)
        {
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

            return true;
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
    return false;
}

#include "instances.cpp"