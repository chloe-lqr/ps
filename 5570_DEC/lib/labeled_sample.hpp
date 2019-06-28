#pragma once
#include <string>
#include <utility>
#include "base/third_party/sarray.h"

namespace csci5570 {
namespace lib {

// Consider both sparse and dense feature abstraction
// You may use Eigen::Vector and Eigen::SparseVector template
template <typename Feature, typename Label>
class LabeledSample {
 public:
  Feature x_;
  Label y_;
};  // class LabeledSample


class SVMSample : public LabeledSample<third_party::SArray< int >, int> {
public:
  std::string toString(){
    std::string result="Label: "+std::to_string(y_)+" Feature:";
    for (int i = 0; i < x_.size(); i++) {
      result=result+" "+/*std::to_string(x_[i].first)+":"+std::to_string(x_[i].second)*/std::to_string(x_[i]);
    }
    return result;
  }
  /*
  void test() {
    for (int i = 0; i < 5; i++) {
      x_.push_back(i);
    }
    y_ = 2;

  }
  */

  //  public:
  //   vector<int> x_;
  //   Label y_;
};  // class SVMSample
class KddSample : public LabeledSample<std::vector<std::pair<int, double>>, int> {
 public:
  std::string toString() {
    std::string result = "Label: " + std::to_string(y_) + " Feature:";
    for (int i = 0; i < x_.size(); i++) {
      result = result + " index:" + std::to_string(x_[i].first) + " value " + std::to_string(x_[i].second);
    }
    return result;
  }
  void test() {
    for (int i = 0; i < 5; i++) {
      x_.push_back(std::make_pair(1, 1.2));
    }
    y_ = 2;
  }
  //  public:
  //   vector<int> x_;
  //   Label y_;
};  // class kddSample

class NetflixSample : public LabeledSample<std::vector<std::pair<int, double>>, int> {
 public:
  std::string toString() {
    std::string result = "Label: " + std::to_string(y_) + " Feature:";
    for (int i = 0; i < x_.size(); i++) {
      result = result + " index:" + std::to_string(x_[i].first) + " value " + std::to_string(x_[i].second);
    }
    return result;
  }

};

template <typename Sample>
class DataStore {
 public:
  DataStore(int n_slots) { samples_.resize(n_slots); }

  void Push(int tid, Sample&& sample) { samples_[tid].push_back(std::move(sample)); }

  const std::vector<Sample>& Get(int slot_id) const {
    CHECK_LT(slot_id, samples_.size());
    return samples_[slot_id];
  }

  std::vector<Sample*> GetPtrs(int slot_id) {
    CHECK_LT(slot_id, samples_.size());
    std::vector<Sample*> ret;
    ret.reserve(samples_[slot_id].size());
    for (auto& sample : samples_[slot_id]) {
        ret.push_back(&sample);
    }
    return ret;
  }

  std::vector<Sample*> Get() {
    std::vector<Sample*> ret;
    for (auto& slot : samples_) {
      for (auto& sample : slot) {
        ret.push_back(&sample);
      }
    }
    return ret;
  }

 protected:
  std::vector<std::vector<Sample>> samples_;
};

}  // namespace lib
}  // namespace csci5570
