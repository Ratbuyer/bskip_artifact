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

#include <cstddef>
#include <cstdint>
#include <limits>
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

#include "StructOfArrays/SizedInt.hpp"
#include "StructOfArrays/aos.hpp"
#include "StructOfArrays/soa.hpp"

template <bool is_concurrent, size_t B, size_t promotion_prob, class T,
		  typename... Ts>
class BSkip_traits {
  public:
	static constexpr uint64_t MAX_KEYS = B;
	using key_type = T;
	static constexpr bool binary = sizeof...(Ts) == 0;
	static constexpr bool concurrent = is_concurrent;
	static constexpr size_t p = promotion_prob;
    static constexpr key_type max_sentinel = std::numeric_limits<T>::max();
    static constexpr key_type min_sentinel = std::numeric_limits<T>::min();

	using element_type =
		typename std::conditional<binary, std::tuple<key_type>,
								  std::tuple<key_type, Ts...>>::type;

	static key_type get_key(element_type e) { return std::get<0>(e); }
    
	using value_type = typename std::conditional<binary, std::tuple<>,
												 std::tuple<Ts...>>::type;

	using SOA_leaf_type = typename std::conditional<binary, AOS<key_type>,
													SOA<key_type, Ts...>>::type;
};
