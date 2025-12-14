#include <cstdint>

#include "../include/BSkipList.hpp"

template <typename traits>
void BSkip<traits>::delete_key(traits::key_type k)
{
    // cannot delete sentinels
    if (k == traits::min_sentinel || k == traits::max_sentinel) {
        return;
    }

    int cpuid = sched_getcpu();

    // can only be internal
    ReaderWriterLock *parent_lock = nullptr;

    // start search from the top node
    auto curr_node = headers[MAX_HEIGHT - 1];

    if (!curr_node) {
        assert(false);
    }

    const int promote_level = flip_coins(k);
    
    int internal_level = -1;
    // phase 1: go down until promotion height, read locks only
    for (internal_level = MAX_HEIGHT - 1; internal_level > promote_level; internal_level--) {
        if constexpr (traits::concurrent) {
            // grab current lock
            if (internal_level > 0) {
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }
            else {
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_lock(cpuid);
            }

            // if you have a parent, release it
            if (parent_lock) {
                parent_lock->read_unlock(cpuid);
                parent_lock = nullptr;
            }
        }

        tbassert(k >= curr_node->get_header(),
                "level = %u, k = %lu, header = %lu\n", internal_level, k,
                curr_node->get_header());

        auto prev_node = curr_node;

        // move rightward
        while (curr_node->next_header <= k) {
            assert(curr_node->get_header() < curr_node->next->get_header());

            tbassert(k >= curr_node->get_header(),
                    "key = %lu, curr node header = %lu, next node header = %lu\n", k,
                    curr_node->get_header(), curr_node->next->get_header());

            // grab next step in the search
            if constexpr (traits::concurrent) {
                if (internal_level > 0) {
                    ((BSkipNodeInternal<traits> *)(curr_node->next))
                        ->mutex_.read_lock(cpuid);
                }
                else {
                    ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.read_lock(cpuid);
                }
            }

            prev_node = curr_node;
            curr_node = curr_node->next;

            // unlock prev node
            if constexpr (traits::concurrent) {
                if (internal_level > 0) {
                    ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.read_unlock(cpuid);
                }
                else {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.read_unlock();
                }
            }
        }
        
        assert(curr_node->get_header() <= k);

        // look for the largest element that is at most the search key
        auto [rank, found] = curr_node->find_key_and_check(k);

        // cannot be found before promotion level
        assert(!found);

        // drop down
        if (internal_level > 0) {
            if constexpr (traits::concurrent) {
                parent_lock = &(((BSkipNodeInternal<traits> *)curr_node)->mutex_);
            }

            curr_node =
                ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
        } else {
            // Reached leaf level without finding the key
            if constexpr (traits::concurrent) {
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.read_unlock();
            }
        }
    }

    // phase 2 at promotion height now
    // try to lock(write) current
    if constexpr (traits::concurrent) {
        // grab current lock
        if (internal_level > 0) {
            assert(promote_level > 0);
            ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.write_lock();
        }
        else {
            tbassert(internal_level == 0 && promote_level == 0, "internal_level = %d\n", internal_level);
            ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_lock();
        }

        // if you have a parent, release it
        if (parent_lock) {
            parent_lock->read_unlock(cpuid);
            parent_lock = nullptr;
        }
    }

    // look for the largest element that is at most the search key
    auto [rank, found] = curr_node->find_key_and_check(k);

    BSkipNode<traits> * prev_node = nullptr;

    while(!found && curr_node->next_header <= k) {
        // lock next node
        if constexpr (traits::concurrent) {
            if (internal_level > 0) {
                ((BSkipNodeInternal<traits> *)(curr_node->next))
                    ->mutex_.write_lock();
            }
            else {
                ((BSkipNodeLeaf<traits> *)(curr_node->next))->mutex_.write_lock();
            }
        }

        prev_node = curr_node;
        curr_node = curr_node->next;

        std::tie(rank, found) = curr_node->find_key_and_check(k);

        // unlock prev node
        if constexpr (traits::concurrent) {
            if (!found) {
                if (internal_level > 0) {
                    ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.write_unlock();
                }
                else {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.write_unlock();
                }
            }
        }
    }

    if (!found) {
        // not was deleted by other threads
        if constexpr (traits::concurrent) {
            if (internal_level > 0) {
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.write_unlock();
            }
            else {
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
            }
        }

        return;
    }

    assert(curr_node->get_key_at_rank(rank) == k);

    // phase 3: found key and need to delete it

    BSkipNode<traits> *current;
    BSkipNode<traits> *left_node;

    // if not the head, delete it
    if (rank != 0) {
        // leaf node, not the head
        if (curr_node->level == 0) {
            ((BSkipNodeLeaf<traits> *)curr_node)->delete_key_at_rank(rank);
            assert(curr_node->level == 0);
            curr_node->num_elts--;
            if constexpr (traits::concurrent) {
                if (prev_node) {
                    ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.write_unlock();
                }
                ((BSkipNodeLeaf<traits> *)(curr_node))->mutex_.write_unlock();
            }
            return;
        } else {
            tbassert(curr_node->level > 0, "level = %d\n", curr_node->level);
            // internal node, not the head
            // get meta info
            current = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank);
            left_node = ((BSkipNodeInternal<traits> *)curr_node)->get_child_at_rank(rank - 1);
            
            // delete from internal node
            assert(curr_node->level > 0);
            ((BSkipNodeInternal<traits> *)curr_node)->delete_key_at_rank(rank);
            ((BSkipNodeInternal<traits> *)curr_node)->delete_child_at_rank(rank);
            curr_node->num_elts--;

            // lock left and current
            // unlock curr_node and prev_node
            if constexpr (traits::concurrent) {
                // assert(current->level > 0);
                if (current->level == 0) {
                    ((BSkipNodeLeaf<traits> *)(current))->mutex_.write_lock();
                    ((BSkipNodeLeaf<traits> *)(left_node))->mutex_.write_lock();
                } else {
                    ((BSkipNodeInternal<traits> *)(current))->mutex_.write_lock();
                    ((BSkipNodeInternal<traits> *)(left_node))->mutex_.write_lock();
                }
                if (prev_node) {
                    assert(prev_node->level > 0);
                    ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.write_unlock();
                }
                assert(curr_node->level > 0);
                ((BSkipNodeInternal<traits> *)(curr_node))->mutex_.write_unlock();
            }
            // Continue to lower levels to delete k there too
        }
    } else {
        // k is the head, we came from left
        assert(prev_node);
        left_node = prev_node;
        current = curr_node;
    }

    // now we have left_node and current locked
    assert(left_node && current);
    assert(left_node->level == current->level);

    // delete from internal levels
    prev_node = nullptr;
    for (int level = current->level; level > 0; level--) {
        assert(current->level > 0);
        // move left_node to right before current
        while(left_node->next != current) {
            // lock next node
            if constexpr (traits::concurrent) {
                ((BSkipNodeInternal<traits> *)(left_node->next))->mutex_.write_lock();
            }

            prev_node = left_node;
            left_node = left_node->next;

            assert(left_node->level > 0);

            // unlock prev node
            if constexpr (traits::concurrent) {
                ((BSkipNodeInternal<traits> *)(prev_node))->mutex_.write_unlock();
            }
        }

        assert(current->get_key_at_rank(0) == k);
        assert(left_node->next_header == k);

        auto old_curr = current;
        auto old_left = left_node;

        tbassert(current->level > 0, "level = %d\n", current->level);
        current = ((BSkipNodeInternal<traits> *)current)->get_child_at_rank(0);
        left_node = ((BSkipNodeInternal<traits> *)left_node)->get_child_at_rank(left_node->num_elts -1);

        if (old_curr->num_elts == 1) {
            // delete the entire node
            old_left->next_header = old_curr->next_header;
            old_left->next = old_curr->next;

            old_curr->num_elts = 0;

            if constexpr (traits::concurrent)
                ((BSkipNodeInternal<traits> *)(old_curr))->mutex_.write_unlock();

            delete old_curr;
        } else {
            assert(old_curr->num_elts > 1);
            assert(old_curr->level > 0);
            ((BSkipNodeInternal<traits> *)old_curr)->delete_key_at_rank(0);
            ((BSkipNodeInternal<traits> *)old_curr)->delete_child_at_rank(0);
            old_left->next_header = old_curr->get_key_at_rank(0);
            old_curr->num_elts--;
            if constexpr (traits::concurrent) ((BSkipNodeInternal<traits> *)(old_curr))->mutex_.write_unlock();
        }

        if constexpr (traits::concurrent) {
            assert(old_left->level == static_cast<uint>(level));
            if (level > 1) {
                ((BSkipNodeInternal<traits> *)(current))->mutex_.write_lock();
                ((BSkipNodeInternal<traits> *)(left_node))->mutex_.write_lock();
            } else {
                assert(level == 1);
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.write_lock();
                ((BSkipNodeLeaf<traits> *)(left_node))->mutex_.write_lock();
                assert(current->level == 0);
            }

            assert(old_left->level > 0);
            ((BSkipNodeInternal<traits> *)(old_left))->mutex_.write_unlock();
        }
    }

    // now at leaf level
    assert(current->level == 0 && left_node->level == 0);
    // move left_node to right before current
    prev_node = nullptr;
    while(left_node->next != current) {
        // lock next node
        if constexpr (traits::concurrent) {
            ((BSkipNodeLeaf<traits> *)(left_node->next))->mutex_.write_lock();
        }

        prev_node = left_node;
        left_node = left_node->next;

        // unlock prev node
        if constexpr (traits::concurrent) {
            ((BSkipNodeLeaf<traits> *)(prev_node))->mutex_.write_unlock();
        }
    }

    tbassert(current->get_key_at_rank(0) == k, 
        "curr head = %lu, k = %lu\n", current->get_key_at_rank(0), k);
    tbassert(left_node->next_header == k, 
        "left next header = %lu, curr head = %lu, k = %lu\n", 
        left_node->next_header, current->get_key_at_rank(0), k);

    if (current->num_elts == 1) {
        // delete the entire node
        left_node->next = current->next;
        left_node->next_header = current->next_header;
        current->num_elts = 0;

        if constexpr (traits::concurrent)
            ((BSkipNodeLeaf<traits> *)(current))->mutex_.write_unlock();

        delete current;
    } else {
        assert(current->num_elts > 1);
        assert(current->level == 0);
        ((BSkipNodeLeaf<traits> *)current)->delete_key_at_rank(0);
        left_node->next_header = current->get_key_at_rank(0);
        current->num_elts--;
        if constexpr (traits::concurrent) ((BSkipNodeLeaf<traits> *)(current))->mutex_.write_unlock();
    }

    if constexpr (traits::concurrent) {
        ((BSkipNodeLeaf<traits> *)(left_node))->mutex_.write_unlock();
    }
    
    return;
}

#include "instances.cpp"
