#ifndef UTILS_H
#define UTILS_H

#include <vector>

#include "constants.hpp"

//Entry getEmptyLogEntry();
unsigned int getElectionTimeout();
//std::vector<ID> getMemberIDs(const std::vector<std::string>&);
//ID getNullID();
//bool isANullID(const ID&);
bool areAMajorityGreaterThanOrEqual(std::vector<int>, int);

#endif
