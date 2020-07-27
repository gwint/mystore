#ifndef UTILS_H
#define UTILS_H

#include <vector>

#include "constants.hpp"

#include "gen-cpp/replicaservice_types.h"
#include "gen-cpp/ReplicaService.h"

Entry getEmptyLogEntry();
unsigned int getElectionTimeout();
std::vector<ID> getMemberIDs(const std::vector<std::string>&);
ID getNullID();
bool isANullID(const ID&);

#endif
