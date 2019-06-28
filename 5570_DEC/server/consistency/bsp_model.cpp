#include "server/consistency/bsp_model.hpp"
#include "glog/logging.h"
#include <stdio.h>

namespace csci5570 {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue) {
  // TODO
	model_id_ = model_id;             // set this.model_id_
	storage_ = std::move(storage_ptr);// move the ownership of storage pointer to this class
	reply_queue_ = reply_queue;
}

void BSPModel::Clock(Message& msg) {
  // TODO
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  CHECK_LE(progress, progress_tracker_.GetMinClock() + 1);
  if (updated_min_clock != -1) {  // min clock updated
    for (auto add_req : add_buffer_) {
      storage_->Add(add_req);
    }
    add_buffer_.clear();

    for (auto get_req : get_buffer_) {
      reply_queue_->Push(storage_->Get(get_req));
    }
    get_buffer_.clear();

    storage_->FinishIter();
  }
}

void BSPModel::Add(Message& msg) {
  // TODO
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  if (progress == progress_tracker_.GetMinClock()) {
    LOG(INFO) << msg.DebugString();
    add_buffer_.push_back(msg);
  } else {
    CHECK(false) << "progress error in BSPModel::Add";
  }
}

void BSPModel::Get(Message& msg) {
  // TODO
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  if (progress == progress_tracker_.GetMinClock() + 1) { // if the progress if faster than current clock, let it wait;
    get_buffer_.push_back(msg);
  } else if (progress == progress_tracker_.GetMinClock()) { // if the progress is same as clock, do it immediately
    reply_queue_->Push(storage_->Get(msg));
  } else {
    CHECK(false) << "progress error in BSPModel::Get { get progress: " << progress << ", min clock: " << progress_tracker_.GetMinClock() << " }";
  }
}

int BSPModel::GetProgress(int tid) {
  // TODO
  return progress_tracker_.GetProgress(tid);
}

int BSPModel::GetGetPendingSize() {
  // TODO
  return get_buffer_.size();
}

int BSPModel::GetAddPendingSize() {
  // TODO
  return add_buffer_.size();
}
void BSPModel::ResetWorker(Message& msg) {
  // TODO
  CHECK_EQ(msg.data.size(), 1);
  third_party::SArray<uint32_t> tids;
  tids = msg.data[0];
  std::vector<uint32_t> tids_vec;
  for (auto tid : tids)
    tids_vec.push_back(tid);
  this->progress_tracker_.Init(tids_vec);
  Message reply_msg;
  reply_msg.meta.model_id = model_id_;
  reply_msg.meta.recver = msg.meta.sender;
  reply_msg.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(reply_msg);
}

}  // namespace csci5570
