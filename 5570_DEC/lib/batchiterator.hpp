
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include <algorithm>

namespace csci5570{
    template <typename Sample>
class BatchIterator {
 public:
  BatchIterator(lib::DataStore<Sample>& data_store) : samples_(data_store.Get()) {}

  void random_start_point() { sample_idx_ = rand() % samples_.size(); }

  const std::vector<Sample*>& GetSamples() const { return samples_; }

  std::pair<std::vector<uint32_t>, std::vector<Sample*>> NextBatch(uint32_t batch_size) {
    std::pair<std::vector<uint32_t>, std::vector<Sample*>> ret;
    ret.second.reserve(batch_size);
    std::set<uint32_t> keys;

    for (size_t i = 0; i < batch_size; ++i) {
      ret.second.push_back(samples_[sample_idx_]);
      for (auto& field : samples_[sample_idx_]->x_) {
        keys.insert(field.first);
      }
      ++sample_idx_;

      sample_idx_ %= samples_.size();
    }

    ret.first = std::vector<uint32_t>(keys.begin(), keys.end());
    return ret;
  }
  std::pair<std::vector<uint32_t>, std::vector<Sample*>> AllBatch(){
    return this->NextBatch(samples_.size());
  }
  
 protected:
  int sample_idx_ = 0;
  std::vector<Sample*> samples_;
};
}
