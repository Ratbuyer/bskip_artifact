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

#pragma once

#include <cstdint>
#include <utility>
#include <cstddef>
#include <limits>
#include <assert.h>

#include <ParallelTools/Lock.hpp>

class Empty
{
};

template <typename traits> class BSkipNode {
public:
	uint32_t num_elts;
	uint32_t level;
	BSkipNode<traits> *next;
	traits::key_type next_header;

	// initialize the skiplist node with default container size
	BSkipNode() {
		num_elts = 0;
		next = NULL;
		next_header = traits::max_sentinel;
        level = -1;
	}

	virtual ~BSkipNode() {}

	bool is_leafnode() const { return (level == 0); }

	virtual traits::key_type get_header() = 0;

	virtual std::pair<uint32_t, bool>
	find_key_and_check(traits::key_type k) = 0;

	virtual traits::key_type get_key_at_rank(uint32_t rank) = 0;

	virtual int split_keys(BSkipNode<traits> *dest, uint32_t starting_rank,
						   uint32_t dest_rank = 1) = 0;

	virtual void print_keys() = 0;
};
