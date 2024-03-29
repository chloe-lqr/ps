#pragma once

#include <cinttypes>
#include <sstream>

#include "base/magic.hpp"
#include "base/serialization.hpp"
#include "base/third_party/sarray.h"

namespace csci5570 {

struct Control {};

enum class Flag : char { kExit, kBarrier, kResetWorkerInModel, kClock, kAdd, kGet };
static const char* FlagName[] = {"kExit", "kBarrier", "kResetWorkerInModel", "kClock", "kAdd", "kGet"};

struct Meta {
  int sender;
  int recver;
  int model_id;
  Flag flag;  // {kExit, kBarrier, kResetWorkerInModel, kClock, kAdd, kGet}

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", model_id: " << model_id;
    ss << ", flag: " << FlagName[static_cast<int>(flag)];

    ss << "}";
    return ss.str();
  }
};

struct Message {
  Meta meta;
  std::vector<third_party::SArray<char>> data;

  template <typename V>
  void AddData(const third_party::SArray<V>& val) {
    data.push_back(third_party::SArray<char>(val));
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data)
        ss << " data_size=" << d.size() << " content is:" << d[0];
    }
    return ss.str();
  }
};

}  // namespace csci5570
