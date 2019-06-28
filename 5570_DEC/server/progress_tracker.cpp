#include "server/util/progress_tracker.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) 
{
  min_clock_=0;
  progresses_.clear();
  for (auto tid : tids) {
    LOG(INFO) << "Insert threadid: "<< tid;
    progresses_.insert(std::make_pair(tid, 0));
  }
  
}

int ProgressTracker::AdvanceAndGetChangedMinClock(int tid) {
  CHECK(CheckThreadValid(tid));
  if(this->IsUniqueMin(tid)) // if min_clock_ changes, return new value of min_clock_;
  {
  	min_clock_ += 1;
    progresses_[tid] += 1;
    return min_clock_;
  }
  else                      // if min_clock_ doesn't change, return -1;
  {
  	progresses_[tid] += 1;
    return -1;
  }
}

int ProgressTracker::GetNumThreads() const {
  return progresses_.size();
}

int ProgressTracker::GetProgress(int tid) const {
  CHECK(CheckThreadValid(tid));
  return progresses_.find(tid)->second;
}

int ProgressTracker::GetMinClock() const {
  return min_clock_;
}

bool ProgressTracker::IsUniqueMin(int tid) const {
  CHECK(CheckThreadValid(tid));
  auto it = progresses_.find(tid);
  if (it->second != min_clock_) {
    return false;
  }
  int min_count = 0;
  for (auto it : progresses_) {
    if (it.second == min_clock_)
      min_count += 1;
    if (min_count > 1)
      return false;
  }
  return true;
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  return progresses_.find(tid) != progresses_.end();
}

}  // namespace csci5570
