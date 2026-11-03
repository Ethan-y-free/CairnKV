#pragma once

#include <cstdint>
#include <string>

#include "cairnkv/status.h"

namespace cairnkv
{

struct HardState
{
    uint64_t current_term = 0;
    std::string voted_for;
};

Status SaveHardState(const std::string& path, const HardState& state);
StatusOr<HardState> LoadHardState(const std::string& path);

}
