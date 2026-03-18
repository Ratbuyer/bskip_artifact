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

uint32_t find_index_linear(K k) {
    uint32_t i;
    assert(BSkipNode<traits>::num_elts > 0);

    for (i = 0; i < BSkipNode<traits>::num_elts; i++)
    {
        if (this->blind_read_key(i) < k)
            continue;
        else if (k == this->blind_read_key(i))
        {
            return i;
        }
        else
            break;
    }
    return i - 1;
}

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

    tbassert(left < BSkipNode<traits>::num_elts, "left = %u, num elts %u\n",
                left, BSkipNode<traits>::num_elts);
    tbassert(left == find_index_linear(k), "k %lu, binary = %u, linear = %u\n",
                k, left, find_index_linear(k));
    return left;
}