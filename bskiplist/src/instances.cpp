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

// Instantiate the whole class template for this Traits
template class BSkip<BSkip_traits<false, 64ul, 64ul, unsigned long, unsigned long>>;
template class BSkip<BSkip_traits<false, 16ul, 16ul, unsigned long, unsigned long>>;
template class BSkip<BSkip_traits<true, 64ul, 64ul, unsigned long, unsigned long>>;
template class BSkip<BSkip_traits<true, 16ul, 16ul, unsigned long, unsigned long>>;
template class BSkip<BSkip_traits<true, 128ul, 64ul, unsigned long, unsigned long>>;
template class BSkip<BSkip_traits<true, 8ul, 8ul, unsigned long, unsigned long>>;
