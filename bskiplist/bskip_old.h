/*
 * ============================================================================
 *
 *       Filename:  btree.h
 *
 *         Author:  Helen Xu, hjxu@lbl.gov
 *   Organization:  Lawrence Berkeley Laboratory
 *
 * ============================================================================
 */

#ifndef _BSKIP_H_
#define _BSKIP_H_

#include <string.h>
#include <random>
#include <iostream>
#include <stack>
#include <utility>
#include <vector>
#include <limits>
#include <algorithm> 

#include "tbassert.h"
#include "array.h"

#define DEBUG_PRINT 0
// #define DEBUG 1
#define WEIGHTED 0

#define MAX_HEIGHT 5

// leaf node is the default
#if WEIGHTED
template <typename K, typename V, template<typename> typename C> class BSkipNode {
#else
template <typename K, template<typename> typename C> class BSkipNode {
#endif
public:
  C<K>* keys;
#if WEIGHTED
  C<V> vals;
#endif

  uint32_t num_elts;
#if WEIGHTED
  BSkipNode<K, V, C>* next;
#else
  BSkipNode<K, C>* next;
#endif

  // initialize the skiplist node with default container size
  BSkipNode() {
    num_elts = 0;
    keys = new C<K>;
#if WEIGHTED
    vals = new C<V>;
#endif

    next = NULL;
  }

  // initialize the skiplist node and set the initial capacity
  BSkipNode(int starting_size) {
    num_elts = 0;
    keys = new C<K>(starting_size);
#if WEIGHTED
    vals = new C<V>(starting_size);
#endif

    next = NULL;
  }

	// TODO: this could have an optimized version depending on the container type
  inline K get_header() {
    return keys->get_first_elt();
  }
};

// default is leaf node
/*
#if WEIGHTED
template <typename K, typename V, template<typename> typename C> class BSkipNodeLeaf : public BTreeNode<K, V, C>  {
#else
template <typename K, template<typename> typename C> class BSkipNodeLeaf : public BSkipNode<K, C> {
#endif
	public:
		BSkipNodeLeaf();
		BSkipNodeLeaf(int starting_size);
};
*/

// extra case for internal node
#if WEIGHTED
template <typename K, typename V, template<typename> typename C> class BSkipNodeInternal: public BSkipNode<K, V, C> {
#else
template <typename K, template<typename> typename C> class BSkipNodeInternal : public BSkipNode<K, C> {
#endif
public:
// TODO: split this class up into internal and leaf for children pointers
#if WEIGHTED
  C<BSkipNode<K, V, C>*>* children;
#else
  C<BSkipNode<K, C>*>* children;
#endif

#if WEIGHTED
	BSkipNodeInternal() : BSkipNode<K, V, C>() {
#else
	BSkipNodeInternal() : BSkipNode<K, C>() {
#endif
#if WEIGHTED
	  children = new C<BSkipNode<K, V, C>*>;
#else
  	children = new C<BSkipNode<K, C>*>;
#endif
	}

#if WEIGHTED
	BSkipNodeInternal(int starting_size) : BSkipNode<K, V, C>(starting_size) {
#else
	BSkipNodeInternal(int starting_size) : BSkipNode<K, C>(starting_size) {
#endif
#if WEIGHTED
	  children = new C<BSkipNode<K, V, C>*>(starting_size);
#else
  	children = new C<BSkipNode<K, C>*>(starting_size);
#endif
	}
};

#if WEIGHTED
template <typename K, typename V, template<typename> class C> class BSkip {
#else
template <typename K, template<typename> class C> class BSkip {
#endif

public:
  int curr_max_height; // max height of current sl
  float promotion_probability; // promotion probability
  bool has_zero = false; // using 0 as first sentinel, so just keep it separate
  
  std::random_device random_device;
  std::mt19937 random_engine{random_device()};
  // headers for each node
#if WEIGHTED
  BSkipNode<K, V, C>* headers[MAX_HEIGHT];
#else
  BSkipNode<K, C>* headers[MAX_HEIGHT];
#endif

#if WEIGHTED
  bool insert(K k, V v);
  BSkipNode<K, V, C> *find(K k);
#else
  bool insert(K k);
  BSkipNode<K, C> *find(K k);
#endif
	uint64_t sum();
	void get_size_stats();
  int flip_coins();
  int node_size;
  BSkip(int p) {
	  node_size = p;
    // https://cpppatterns.com/patterns/flip-a-biased-coin.html
    promotion_probability = (float)1/p;
    curr_max_height = 0;
    
    // initialize bskip with array of headers
    for(int i = 0; i < MAX_HEIGHT; i++) {
			if (i > 0) {
#if WEIGHTED
				headers[i] = new BSkipNodeInternal<K, V, C>(p);
				auto end_sentinel = new BSkipNodeInternal<K, V, C>(1);
#else
				headers[i] = new BSkipNodeInternal<K, C>(p);
				auto end_sentinel = new BSkipNodeInternal<K, C>(1);
#endif
				end_sentinel->keys->insert_elt_at_rank(0, std::numeric_limits<K>::max());
				headers[i]->next = end_sentinel;
				// TODO: do the end sentinels need a down pointer?
			} else {
#if WEIGHTED
				headers[i] = new BSkipNode<K, V, C>(p);
				auto end_sentinel = new BSkipNode<K, V, C>(1);
#else
				headers[i] = new BSkipNode<K, C>(p);
				auto end_sentinel = new BSkipNode<K, C>(1);
#endif
				end_sentinel->keys->insert_elt_at_rank(0, std::numeric_limits<K>::max());
				headers[i]->next = end_sentinel;
			}
		}

    // set min elt at start of header
    for(int i = MAX_HEIGHT - 1; i >= 0; i--) {
      headers[i]->keys->insert_elt_at_rank(0, std::numeric_limits<K>::min());
			if (i > 0) {
#if WEIGHTED
				((BSkipNodeInternal<K, V, C>*)headers[i])->children->insert_elt_at_rank(0, headers[i-1]);
#else
				((BSkipNodeInternal<K, C>*)headers[i])->children->insert_elt_at_rank(0, headers[i-1]);
#endif
			}
#if WEIGHTED
			headers[i]->vals->insert_elt_at_rank(0, std::numeric_limits<V>::min());
#endif
    }

		// ???
		/*
		auto curr_node = headers[MAX_HEIGHT - 1];
	  for(int level = MAX_HEIGHT - 1; level >= 0; level--) {
#if DEBUG_PRINT
			printf("init node header %lu, level %d\n", curr_node->get_header(), level);
#endif
			if (level > 0) { curr_node = curr_node->children->get_elt_at_rank(0); }
		}
		*/
  }
};


#if WEIGHTED
template <typename K, typename V, template<typename> class C>
#else
template <typename K, template<typename> class C>
#endif
#if WEIGHTED
int BSkip<K, V, C>::flip_coins() {
#else
int BSkip<K, C>::flip_coins() {
#endif
  int result = 0;
  std::bernoulli_distribution coin_distribution{promotion_probability};
  while(coin_distribution(random_engine) && result < MAX_HEIGHT) { result++; }
	if (result > MAX_HEIGHT - 1) { result = MAX_HEIGHT - 1; }
  return result;
}

#if WEIGHTED
template <typename K, typename V, template<typename> class C>
#else
template <typename K, template<typename> class C>
#endif
#if WEIGHTED
bool BSkip<K, V, C>::insert(K k, V v) {
#else
bool BSkip<K, C>::insert(K k) {
#endif
	// special case for 0 since we use it as the front sentinel
	if (k == 0) { has_zero = true; return true; }

  // flip coins to determine your promotion level
  int level_to_promote = flip_coins();
  assert(level_to_promote < MAX_HEIGHT); 
#if DEBUG_PRINT
  printf("\n\ninserting key %lu, promoted to level %d\n", k, level_to_promote); 
#endif
  // update max level if it has been raised
  if (level_to_promote > curr_max_height) {
    curr_max_height = level_to_promote;
  }
  // start inserting from the top level that exists
  auto curr_node = headers[curr_max_height];

  int rank_above; // save rank of key in parent to fixup pointers if splitting down

  // save node from above that we will insert parent into after split
#if WEIGHTED
  BSkipNodeInternal<K, V, C>* node_above = NULL;
#else
  BSkipNodeInternal<K, C>* node_above = NULL;
#endif

  for(int level = curr_max_height; level >= 0; level--) {
#if DEBUG_PRINT
	  printf("\nloop start: curr node head %lu at level %d\n", curr_node->get_header(), level);
#endif
#if WEIGHTED
    BSkipNode<K, V, C>* next_node = curr_node->next;
#else
    BSkipNode<K, C>* next_node = curr_node->next;
#endif

    // find the node to insert the key in in this level
    while(next_node->get_header() < k) {
#if DEBUG_PRINT
			printf("\tcurrent header %lu, next header %lu\n", curr_node->get_header(), next_node->get_header());
#endif
			assert(curr_node->get_header() < next_node->get_header());
      curr_node = next_node;
      next_node = curr_node->next;
			assert(curr_node->get_header() != 0);
    }

		// now we are at the correct node - look for the key
		uint32_t rank = curr_node->keys->find(k);
#if DEBUG_PRINT
		printf("\trank found %u\n", rank);
#endif
		// if the key was found
		if(curr_node->keys->get_elt_at_rank(rank) == k) {
#if WEIGHTED
			// if the key is there, update values all the way down (in the weighted case)
			curr_node->set_val_at_rank(rank, v);
			level--;
			if (level > 0) {
				curr_node = curr_node->children->get_elt_at_rank(rank);
				level--;
				while(level >= 0) {
					curr_node->set_val_at_rank(0, v);
					if (level > 0) {
					  curr_node = curr_node->children->get_elt_at_rank(0);
					}
					level--;
				}
			} 
# endif
			return true;
		} else { // otherwise, this key was not found at this level
#if DEBUG_PRINT
		  printf("\tnot found at level %u\n", level);
#endif
			if (level_to_promote < level) {
				// case 1: do not promote to this level.
#if DEBUG_PRINT
				printf("\t\tcurr node num elts %u\n", curr_node->keys->num_elts);
				printf("\t\tcurr node elt %lu, level %d\n", curr_node->keys->get_elt_at_rank(rank), level);
			  curr_node->keys->print();
#endif

				// drop down a level (if you are here, you are at an internal node)
#if WEIGHTED
				curr_node = ((BSkipNodeInternal<K, V, C>*)curr_node)->children->get_elt_at_rank(rank);
#else
				curr_node = ((BSkipNodeInternal<K, C>*)curr_node)->children->get_elt_at_rank(rank);
#endif

#if DEBUG
				assert(curr_node != NULL);
#endif
				continue;
			} else if (level_to_promote == level) {
				// Case 2: insert but not split
				// add key (and if needed, value) to this node 
				curr_node->num_elts++; // idk if we need this in both container and node but here for now
				curr_node->keys->insert_elt_at_rank(rank + 1, k);

#if DEBUG_PRINT
				printf("\n\tinserted %lu into level %u at rank %u\n", k, level, rank + 1);
#endif

#if WEIGHTED
				curr_node->vals->insert_elt_at_rank(rank + 1, v);
#endif
				// make space for the child pointer
				if (level > 0) {
#if WEIGHTED
					BSkipNodeInternal<K, V, C>* curr_node_cast = (BSkipNodeInternal<K, V, C>*)curr_node;
#else
					BSkipNodeInternal<K, C>* curr_node_cast = (BSkipNodeInternal<K, C>*)curr_node;
#endif
#if WEIGHTED
					curr_node_cast->children->insert_elt_at_rank(rank+1, NULL);
#else
					curr_node_cast->children->insert_elt_at_rank(rank+1, NULL);
#endif

					rank_above = rank + 1; // save current rank in case we need it for a lower split
					node_above = curr_node_cast; // save current node for pointers in lower split
#if WEIGHTED
				curr_node = curr_node_cast->children->get_elt_at_rank(rank);
#else
				curr_node = curr_node_cast->children->get_elt_at_rank(rank);
#endif

#if DEBUG_PRINT
					printf("\tmake space at rank %u on level %d, node head %u\n", rank + 1, level, curr_node->get_header());
#endif
#if DEBUG
					assert(curr_node != NULL);
#endif
				}
			} else { // case 3: do a split
#if DEBUG_PRINT
			  printf("split at level %d with elt %u\n", level, k);
#endif
#if WEIGHTED
				BSkipNode<K, V, C>* new_node;
#else
				BSkipNode<K, C>* new_node;
#endif
				if (level > 0) {
#if WEIGHTED
					new_node = new BSkipNodeInternal<K, V, C>(node_size);
#else
					new_node = new BSkipNodeInternal<K, C>(node_size);
#endif
				} else {
#if WEIGHTED
			  	new_node = new BSkipNode<K, V, C>(node_size);
#else
					new_node = new BSkipNode<K, C>(node_size);
#endif
				}

#if DEBUG
				tbassert(new_node->keys->capacity > 0, "split with new capacity %u\n", new_node->keys->capacity);
#endif
				new_node->keys->insert_elt_at_rank(0, k);
#if DEBUG_PRINT
				printf("\tnew node: ");
				new_node->keys->print();
#endif
				// move all keys in the curr node starting from rank + 1 into the new node
				// returns the number of elements that were moved
				uint32_t elts_moved = curr_node->keys->split_into(new_node->keys, rank + 1);
#if DEBUG_PRINT
				printf("elts moved %u\n", elts_moved);
				printf("prev node: ");
				curr_node->keys->print();
				printf("new node: ");
				new_node->keys->print();
#endif
# if WEIGHTED
				new_node->vals->insert_elt_at_rank(0, v);
				// move all vals in the curr node starting from rank + 1 into new node
			  curr_node->vals->split_into(new_node->vals, rank + 1);
#endif
				// if you are an internal node, make space for the child of this head
				if (level > 0) {
					// new_node->children->insert_elt_at_rank(0, NULL);
#if WEIGHTED
					((BSkipNodeInternal<K, V, C>*)new_node)->children->insert_elt_at_rank(0, NULL);
#else
					((BSkipNodeInternal<K, C>*)new_node)->children->insert_elt_at_rank(0, NULL);
#endif
				
					// curr_node->children->split_into(new_node->children, rank + 1);
#if WEIGHTED
					((BSkipNodeInternal<K, V, C>*)curr_node)->children->split_into(((BSkipNodeInternal<K, V, C>*)new_node)->children, rank + 1);
#else
					((BSkipNodeInternal<K, C>*)curr_node)->children->split_into(((BSkipNodeInternal<K, C>*)new_node)->children, rank + 1);
#endif
				}

				// fixup next pointers
#if DEBUG
				assert(curr_node->get_header() < curr_node->next->get_header());
				assert(new_node->get_header() < curr_node->next->get_header());
				assert(curr_node->get_header() < new_node->get_header());
#endif
#if DEBUG_PRINT
				printf("curr header %u, next header %u, new header %u\n", curr_node->get_header(), curr_node->next->get_header(), new_node->get_header());
#endif
				new_node->next = curr_node->next;
				curr_node->next = new_node;
				
				// update num elts per node
				new_node->num_elts = elts_moved + 1;
				curr_node->num_elts -= elts_moved;

				// TODO: the error is somewhere about setting the child pointers?
				// fixup child pointer above
#if DEBUG_PRINT
				printf("setting above pointer\n");
				printf("\tabove node: ");
				node_above->keys->print();
#endif
				node_above->children->set_elt_at_rank(rank_above, new_node);
				assert(node_above->children->get_elt_at_rank(rank_above)->get_header() == new_node->get_header());

				// if we are at an internal level, move on and save info for lower splits
				if (level > 0) {
#if WEIGHTED
					curr_node = ((BSkipNodeInternal<K, V, C>*)curr_node)->children->get_elt_at_rank(rank);
#else
					curr_node = ((BSkipNodeInternal<K, C>*)curr_node)->children->get_elt_at_rank(rank);
#endif
					rank_above = 0;
#if WEIGHTED
					node_above = (BSkipNodeInternal<K, V, C>*)new_node;
#else
					node_above = (BSkipNodeInternal<K, C>*)new_node;
#endif
			  }
			}
		}
  }
	return true;
}


#if WEIGHTED
template <typename K, typename V, template<typename> class C>
#else
template <typename K, template<typename> class C>
#endif
#if WEIGHTED
  BSkipNode<K, V, C>* BSkip<K, V, C>::find(K k) {
#else
  BSkipNode<K, C>* BSkip<K, C>::find(K k) {
#endif
	// TODO: deal with if if you look for 0. should this return true for the unweighted case and the val in the weighted case? why is the node the thing getting returned?

	// start search from the top node
	auto curr_node = headers[curr_max_height];
	
	for(int level = curr_max_height; level >= 0; level--) {
		auto next_node = curr_node->next;

		// move forward until we find the right node that contains the key range
		while(next_node->get_header() < k) {
			curr_node = next_node;
			next_node = curr_node->next;
		}

		// look for the largest element that is at most the search key
		uint32_t rank = curr_node->keys->find(k);

		// if it is found, return the node (returns the topmost node the key is found in
		if(curr_node->keys->get_elt_at_rank(rank) == k) {
#if DEBUG_PRINT
			printf("returning node at level %d with key %u at rank %u\n", level, curr_node->keys->get_elt_at_rank(rank), rank);
#endif
			return curr_node;
		}

		// if not found, drop down a level
		if (level > 0) {
			// curr_node = curr_node->children->get_elt_at_rank(rank);
#if WEIGHTED
			curr_node = ((BSkipNodeInternal<K, V, C>*)curr_node)->children->get_elt_at_rank(rank);
#else
			curr_node = ((BSkipNodeInternal<K, C>*)curr_node)->children->get_elt_at_rank(rank);
#endif
		}
	}
	return NULL;
}

// sums all keys
// TODO: parallel sum
#if WEIGHTED
template <typename K, typename V, template<typename> class C>
#else
template <typename K, template<typename> class C>
#endif
#if WEIGHTED
  uint64_t BSkip<K, V, C>::sum() {
#else
  uint64_t BSkip<K, C>::sum() {
#endif
  auto curr_node = headers[0];
	uint64_t result = 0;
	while(curr_node->get_header() < std::numeric_limits<K>::max()) {
		result += curr_node->keys->sum();
		curr_node = curr_node->next;
	}
	return result;
}

#if WEIGHTED
template <typename K, typename V, template<typename> class C>
#else
template <typename K, template<typename> class C>
#endif
#if WEIGHTED
  void BSkip<K, V, C>::get_size_stats() {
#else
  void BSkip<K, C>::get_size_stats() {
#endif
	// get num nodes, total size in bytes, average num elts per node

	uint64_t num_nodes = 0;
	uint64_t size_in_bytes = 0;
	uint64_t total_elts = 0;
	uint64_t internal_size = 0;

	for(int level = curr_max_height; level >= 0; level--) {
  	auto curr_node = headers[level];
		
		while(curr_node->get_header() < std::numeric_limits<K>::max()) {
			num_nodes++;
			
			// count in size of keys
			size_in_bytes += sizeof(K) * curr_node->keys->capacity;

			// count in children if internal node
			if (level > 0) {
				internal_size += sizeof(K) * curr_node->keys->capacity;
			
#if WEIGHTED
				size_in_bytes += sizeof(BSkip<K, V, C>*) * curr_node->keys->capacity;
				internal_size += sizeof(BSkip<K, V, C>*) * curr_node->keys->capacity;
#else
				size_in_bytes += sizeof(BSkip<K, C>*) * curr_node->keys->capacity;
				internal_size += sizeof(BSkip<K, C>*) * curr_node->keys->capacity;
#endif
			}

			// count in actual num elts
			total_elts += curr_node->keys->num_elts;

#if WEIGHTED
			size_in_bytes += sizeof(V) * curr_node->vals->capacity;
			if (level > 0) {
				internal_size += sizeof(V) * curr_node->vals->capacity;
			}
#endif
			curr_node = curr_node->next;
		}
	}
	
	assert(internal_size < size_in_bytes);
	double overhead = (double) internal_size / size_in_bytes;
	double avg_elts = (double) total_elts / num_nodes;

	printf("total size %lu, internal size %lu, overhead %f\n", size_in_bytes, internal_size, overhead);
	printf("avg elts per node %f, total num nodes %lu\n", avg_elts, num_nodes);
}



#endif
