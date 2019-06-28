#include "server/consistency/asp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

ASPModel::ASPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue) {
  // TODO Constructor
  model_id_ = model_id;             // set this.model_id_
  storage_ = std::move(storage_ptr);// move the ownership of storage pointer to this class
  reply_queue_ = reply_queue;
}

void ASPModel::Clock(Message& msg) {
  // TODO
  progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
}

void ASPModel::Add(Message& msg) {
  // TODO
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  storage_->Add(msg);
}

void ASPModel::Get(Message& msg) {
  // TODO
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  reply_queue_->Push(storage_->Get(msg));
}

int ASPModel::GetProgress(int tid) {
  // TODO
  return progress_tracker_.GetProgress(tid); 
}

void ASPModel::ResetWorker(Message& msg) {
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
