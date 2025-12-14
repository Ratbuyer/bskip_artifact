#include <cstdint>

#include "../include/BSkipList.hpp"

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
    }
    else
    {
        BSkipNodeInternal<traits> *curr_node_cast =
            (BSkipNodeInternal<traits> *)node;
        if (curr_node_cast->next_header < local_max)
        {
            sum_helper(sums, node->next, level, local_max);
        }
        for (uint i = 0; i < node->num_elts; i++)
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
void BSkip<traits>::validate_structure()
{
    for (int level = MAX_HEIGHT - 1; level >= 0; level--)
    {
        auto curr_node = headers[level];

        while (curr_node->get_header() < std::numeric_limits<K>::max())
        {
            tbassert(curr_node->get_header() < curr_node->next->get_header(),
                     "level %d, curr node header %lu, next node header %lu\n",
                     level, curr_node->get_header(), curr_node->next->get_header());
            for (uint32_t i = 1; i < curr_node->num_elts; i++)
            {
                tbassert(curr_node->get_key_at_rank(i - 1) <
                             curr_node->get_key_at_rank(i),
                         "level %d, elts[%lu] = %lu\n", 
                         level,
                         curr_node->get_key_at_rank(i - 1),
                         curr_node->get_key_at_rank(i));
            }

			if (curr_node->get_header() < std::numeric_limits<K>::max()) {
				assert(curr_node->next_header == curr_node->next->get_header());
			} else {
                assert(curr_node->next_header == std::numeric_limits<K>::max());
            }

            tbassert(curr_node->level == static_cast<uint>(level), "wrong level\n");

			curr_node = curr_node->next;
        }
    }
}

#include "instances.cpp"
