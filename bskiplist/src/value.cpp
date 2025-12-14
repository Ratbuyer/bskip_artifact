#include <cstdint>

#include "../include/BSkipList.hpp"

template <typename traits>
traits::value_type BSkip<traits>::value(traits::key_type k) const
{
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
        // if k <= curr_node->max() break // if k < curr_node->next->min() break // else continue the loop
        assert(curr_node->next != nullptr);
        while (k >= curr_node->next_header)
        {
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
                    ((BSkipNodeInternal<traits> *)(curr_node->next))
                        ->mutex_.read_lock(cpuid);
                }
                else
                {
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
            typename traits::value_type return_value;

            if (level == 0)
            {
                return_value = ((BSkipNodeLeaf<traits> *)curr_node)->get_value_at_rank(rank);

                if constexpr (traits::concurrent)
                {
                    ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
                }
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

    return NULL;
}

#include "instances.cpp"