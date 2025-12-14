#include "Node.hpp"

#include <array>

template <typename traits>
class BSkipNodeLeaf : public BSkipNode<traits>
{
private:
    using K = traits::key_type;
    using V = traits::value_type;
    using elt_type = traits::element_type;

    static constexpr traits::key_type NULL_VAL = {};
    static constexpr size_t max_element_size = sizeof(K);

    using SOA_type = traits::SOA_leaf_type;
    static constexpr uint64_t max_size = traits::MAX_KEYS;

    std::array<uint8_t, SOA_type::get_size_static(max_size)> array = {0};
public:
    // if not concurrent, lock is an empty class
    mutable std::conditional_t<traits::concurrent, ReaderWriterLock2, Empty> mutex_;

private:

    K blind_read_key(uint32_t index) const
    {
        assert(this->num_elts > 0);
        return std::get<0>(SOA_type::get_static(array.data(), max_size, index));
    }
    V blind_read_val(uint32_t index) const
    {
        assert(this->num_elts > 0);
        return std::get<1>(SOA_type::get_static(array.data(), max_size, index));
    }

    void blind_write(traits::element_type e, uint32_t index)
    {
        SOA_type::get_static(array.data(), max_size, index) = e;
    }

    auto blind_read(uint32_t index) const
    {
        assert(this->num_elts > 0);
        return SOA_type::get_static(array.data(), max_size, index);
    }

public:

    inline K get_header()
    {
        return blind_read_key(0);
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
        // return find_index_binary(k);
        return find_index_linear(k);
    }

    std::pair<uint32_t, bool> find_key_and_check(K k)
    {
        // auto index = find_index_binary(k);
        auto index = find_index_linear(k);
        return {index, (blind_read_key(index) == k)};
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

    void delete_key_at_rank(uint32_t rank)
    {
        // keep track of num_elts here
        assert(BSkipNode<traits>::num_elts - 1 >= 0);
        // shift everything over by 1
        for (size_t j = rank; j < BSkipNode<traits>::num_elts - 1; j++)
        {
            SOA_type::get_static(array.data(), max_size, j) =
                SOA_type::get_static(array.data(), max_size, j + 1);
        }
    }

    inline K get_key_at_rank(uint32_t rank)
    {
        return blind_read_key(rank);
    }

    inline V get_value_at_rank(uint32_t rank)
    {
        return blind_read_val(rank);
    }

    inline void set_elt_at_rank(uint32_t rank, traits::element_type elt)
    {
        blind_write(elt, rank);
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

    #include "../src/LeafNode.cpp"
};