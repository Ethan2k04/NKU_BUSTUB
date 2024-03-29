#include "primer/orset.h"
#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this (finished)
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
  auto it = std::find_if(this->elements.begin(), this->elements.end(),
                         [&](const std::pair<T, uid_t> &pair) { return pair.first == elem; });
  return it != this->elements.end();
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this (finished)
  // throw NotImplementedException("ORSet<T>::Add is not implemented");

  std::pair<T, uid_t> pair(elem, uid);
  this->elements.push_back(pair);

  for (auto it = this->elements.begin(); it != this->elements.end();) {
    bool isInTombstones = false;
    for (auto jt = this->tombstones.begin(); jt != this->tombstones.end(); ++jt) {
      if (it->first == jt->first && it->second == jt->second) {
        isInTombstones = true;
        break;
      }
    }
    if (isInTombstones) {
      it = this->elements.erase(it);
    } else {
      ++it;
    }
  }
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this (finished)
  // throw NotImplementedException("ORSet<T>::Remove is not implemented");

  // COMMENT(student): First, remove the element which contains elem and add to a set called 'removed'
  std::vector<std::pair<T, uid_t>> removed;
  for (auto it = this->elements.begin(); it != this->elements.end();) {
    if (it->first == elem) {
      removed.push_back(*it);
      it = this->elements.erase(it);
    } else {
      ++it;
    }
  }

  // COMMENT(student): Second, add 'removed' to the back of tombstones
  std::copy(removed.begin(), removed.end(), std::back_inserter(this->tombstones));
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this (finished)
  // throw NotImplementedException("ORSet<T>::Merge is not implemented");

  std::copy(other.tombstones.begin(), other.tombstones.end(), std::back_inserter(this->tombstones));
  std::copy(other.elements.begin(), other.elements.end(), std::back_inserter(this->elements));

  std::set<std::pair<T, uid_t>> uniqueElements(this->elements.begin(), this->elements.end());
  std::set<std::pair<T, uid_t>> uniqueTombstones(this->tombstones.begin(), this->tombstones.end());

  this->elements.clear();
  this->tombstones.clear();

  this->elements.insert(elements.end(), uniqueElements.begin(), uniqueElements.end());
  this->tombstones.insert(tombstones.end(), uniqueTombstones.begin(), uniqueTombstones.end());

  for (auto it = this->elements.begin(); it != this->elements.end();) {
    bool isInTombstones = false;
    for (auto jt = this->tombstones.begin(); jt != this->tombstones.end(); ++jt) {
      if (it->first == jt->first && it->second == jt->second) {
        isInTombstones = true;
        break;
      }
    }
    if (isInTombstones) {
      it = this->elements.erase(it);
    } else {
      ++it;
    }
  }
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this (finished)
  // throw NotImplementedException("ORSet<T>::Elements is not implemented");
  std::vector<T> result;
  for (const auto &element : this->elements) {
    result.push_back(element.first);
  }
  return result;
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
