#ifndef _ARRAY_H_
#define _ARRAY_H_

#define DEFAULT_START_SIZE 100
#define BINARY_SEARCH 0

template <typename T> class Array {
public:
  T* arr;
  int num_elts;
  int capacity;

	void print();
  uint32_t find(T k);
  T get_first_elt();
  T get_elt_at_rank(uint32_t idx);
  void set_elt_at_rank(uint32_t rank, T t);
  void insert_elt_at_rank(uint32_t rank, T t);
  int split_into(Array<T>* dest, uint32_t starting_rank, uint32_t dest_rank = 1);
 	uint64_t sum();

  Array(int starting_capacity = DEFAULT_START_SIZE) {
    num_elts = 0;
    capacity = starting_capacity;
		assert(capacity > 0);
    arr = (T*)(malloc(capacity * sizeof(T)));
	}
private:
  uint32_t find_index_linear(T k);
  uint32_t find_index_binary(T k);
};

template <class T>
inline T Array<T>::get_first_elt() {
#if DEBUG
	tbassert(num_elts > 0, "num elts == 0 in find first elt\n");
#endif
  return arr[0];
}

template <class T>
T Array<T>::get_elt_at_rank(uint32_t idx) {
#if DEBUG
  tbassert(num_elts > idx, "num elts %u, looking for elt at rank %u\n", num_elts, idx);
#endif
  return arr[idx];
}

template<class T>
void Array<T>::set_elt_at_rank(uint32_t rank, T t) {
#if DEBUG
  tbassert(num_elts > rank, "num elts %u, rank to set %u\n", num_elts, rank);
#endif
#if DEBUG_PRINT
	std::cout << "set rank " << rank << " to " << t << std::endl;
#endif
  arr[rank] = t;
}

// linear search through elements
template <class T>
uint32_t Array<T>::find_index_linear(T k) {
  uint32_t i;
  for (i = 0; i < num_elts; i++) {
#if DEBUG_PRINT
		printf("\tarr[%u] = %lu\n", i, arr[i]);
#endif
#if DEBUG
		if(i > 0) { 
			if(arr[i] < arr[i-1]) { 
				printf("num elts %u, arr[%d] = %lu, arr[%d] = %lu\n", num_elts, i-1, arr[i-1], i, arr[i]);
				if (num_elts < 50) { print(); }
				// print(); 
			  assert(false);
			}
		}
#endif
    if (arr[i] < k)
      continue;
    else if (k == arr[i]) {
      return i;
    } else
      break;
  }
#if DEBUG_PRINT
	printf("\tfind index linear: searched for %lu, rank %u\n", k, i);
#endif

#if DEBUG
  for (int j = 1; j < num_elts; j++) {
		assert(arr[j] > arr[j-1]);
	}
  tbassert ( i > 0 , "in find, i == 0\n");
#endif
	return i - 1;
}

template <class T>
uint32_t Array<T>::find_index_binary(T k) {
  uint32_t left = 0;
  uint32_t right = num_elts - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (arr[mid] == k) {
      left = mid;
      break;
    } else if (arr[mid] < k) {
      left = mid + 1;
    } else {
      if (mid == 0) break;
      right = mid - 1;
    }
  }
  return left;
}

// wrapper for find methods
template <class T>
uint32_t Array<T>::find(T k) {
#if BINARY_SEARCH
  return find_index_binary(k);
#else
  return find_index_linear(k);
#endif
}

template <class T>
void Array<T>::insert_elt_at_rank(uint32_t rank, T elt) {
  // if we are adding over capacity, resize 
  if (num_elts + 1 > capacity) {
    capacity *= 2;
    arr = (T*)realloc(arr, sizeof(T) * capacity);
  }

  // shift everything over by 1
  memmove(arr + rank + 1, arr + rank, (num_elts - rank) * sizeof(T));
  num_elts++;
  // set it
  arr[rank] = elt;

#if DEBUG
	if (std::is_same<T, uint32_t>::value) {
		for(int i = 1; i < num_elts; i++) {
			if (arr[i] < arr[i-1]) { print(); }
			assert(arr[i] > arr[i-1]);
		}
	}
#endif
}

// move all elts from this container into the dest starting from starting_rank
// starting from dest_rank in the dest
// return the number of elements you added
template <class T>
int Array<T>::split_into(Array<T>* dest, uint32_t starting_rank, uint32_t dest_rank) {
	uint32_t num_elts_to_move = num_elts - starting_rank;
	assert(starting_rank <= num_elts);
	assert(num_elts_to_move <= num_elts);

#if DEBUG_PRINT
	printf("num elts to move = %u, num elts %u, starting rank %u, dest rank %u\n", num_elts_to_move, num_elts, starting_rank, dest_rank);
#endif
  // resize if necessary
  tbassert(dest->capacity > 0, "dest capacity %u\n", dest->capacity);
	if (dest->capacity < num_elts_to_move + dest->num_elts) {
		while(dest->capacity < num_elts_to_move + dest->num_elts) {
			dest->capacity *= 2;
		}

#if DEBUG_PRINT
		printf("resizing dest in split to %u\n", dest->capacity); 
#endif
    dest->arr = (T*)realloc(dest->arr, sizeof(T) * dest->capacity);
  }

  // move into dest
  memmove(dest->arr + dest_rank, arr + starting_rank, num_elts_to_move * sizeof(T));

  // update src and dest num elts
  num_elts = starting_rank;
  dest->num_elts += num_elts_to_move;
#if DEBUG
	assert(num_elts > 0);
#endif
#if DEBUG_PRINT
	printf("num elts remaining %u, num elts in dest %u\n", num_elts, dest->num_elts);
#endif

#if DEBUG
	if (std::is_same<T, uint32_t>::value) {
		for(int i = 1; i < num_elts; i++) {
			if (arr[i] < arr[i-1]) { print(); }
			assert(arr[i] > arr[i-1]);
		}

		for(int i = 1; i < dest->num_elts; i++) {
			if (dest->arr[i] < dest->arr[i-1]) { dest->print(); }
			assert(dest->arr[i] > dest->arr[i-1]);
		}
	}
#endif
	return num_elts_to_move;
}

template <class T>
uint64_t Array<T>::sum() {
	uint64_t result = 0;
	for(int i = 0; i < num_elts; i++) {
		result += arr[i];
	}
	return result;
}

template <class T>
void Array<T>::print() {
  std::cout << "Printing node: ";
	for(int i = 0; i < num_elts; i++) {
		std::cout << arr[i] << "\t";
	}
	std::cout << std::endl;
}


#endif
