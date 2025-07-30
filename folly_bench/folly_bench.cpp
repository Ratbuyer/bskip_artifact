#define GLOG_USE_GLOG_EXPORT

#include <fmt/format.h>
#include <folly/ConcurrentSkipList.h>
#include <iostream>
#include <memory>

using namespace folly;

int main(int argc, char *argv[]) {

  // Define the skip list node type with integer keys
  using SkipListType = ConcurrentSkipList<int>::Accessor;

  // Create a skip list with maximum level 10
  auto skipListHead = ConcurrentSkipList<int>::createInstance(10);
  SkipListType accessor(skipListHead);

  // Insert elements
  accessor.insert(10);
  accessor.insert(20);
  accessor.insert(30);
  accessor.insert(15);

  // Iterate over elements
  std::cout << "Skip List Elements: ";
  for (auto it = accessor.begin(); it != accessor.end(); ++it) {
    std::cout << *it << " ";
  }
  std::cout << std::endl;

  // Search for an element
  int searchKey = 20;
  auto it = accessor.find(searchKey);
  if (it != accessor.end()) {
    std::cout << "Found element: " << *it << std::endl;
  } else {
    std::cout << "Element not found!" << std::endl;
  }
  return 0;
}
