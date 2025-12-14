template <typename F>
void map_range_length(traits::key_type start, uint64_t length, F f) const
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
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
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
                    ((BSkipNodeInternal<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
                else
                {
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
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release the parent now
            parent_lock->read_unlock(cpuid);
        }
        cur_level = current->level;
        current_key_rank = get<0>(current->find_key_and_check(start));
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

	uint32_t num_remaining = length;
	while (current->get_key_at_rank(current_key_rank) != std::numeric_limits<K>::max() && num_remaining) {
		int iteration = std::min(current->num_elts - current_key_rank, num_remaining);
		for (int i=0; i<iteration;i++) {
			f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->get_value_at_rank(current_key_rank));
			current_key_rank++;
		}
		num_remaining -= iteration;
		if (num_remaining) {
			auto previous = current;
			if constexpr (traits::concurrent)
			{
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


    // // Iterate through all the elements length to the right, applying function F along the way
    // for (uint64_t counter = 0; counter < length; counter++)
    // {
    //     // Check that we are not exceeding the bounds
    //     if (current->get_key_at_rank(current_key_rank) == std::numeric_limits<K>::max())
    //     {
    //         break;
    //     }
    //     f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->get_value_at_rank(current_key_rank));
    //     current_key_rank += 1;
    //     // Check if we have reached the end of current
    //     if (current_key_rank == current->num_elts)
    //     {
    //         auto previous = current;
    //         if constexpr (traits::concurrent)
    //         {
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

template <typename F>
void map_range(traits::key_type min, traits::key_type max, F f) const
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
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
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
                    ((BSkipNodeInternal<traits> *)(current->next))->mutex_.read_lock(cpuid);
                }
                else
                {
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
                ((BSkipNodeInternal<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            else
            {
                ((BSkipNodeLeaf<traits> *)(current))->mutex_.read_lock(cpuid);
            }
            // Release the parent now
            parent_lock->read_unlock(cpuid);
        }
        cur_level = current->level;
        current_key_rank = std::get<0>(current->find_key_and_check(min));
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
        f(current->get_key_at_rank(current_key_rank), ((BSkipNodeLeaf<traits> *)(current))->get_value_at_rank(current_key_rank));
        current_key_rank += 1;
        // Check if we have reached the end of current
        if (current_key_rank == current->num_elts)
        {
            auto previous = current;
            if constexpr (traits::concurrent)
            {
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