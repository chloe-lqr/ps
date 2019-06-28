#include "server/consistency/ssp_model.hpp"
#include "glog/logging.h"

#include <stdio.h>

namespace csci5570 {

SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr, int staleness,
                   ThreadsafeQueue<Message>* reply_queue) 
{
  // TODO Constructor
  model_id_ = model_id;
  storage_ = std::move(storage_ptr);
  staleness_ = staleness;
  reply_queue_ = reply_queue;
}

void SSPModel::Clock(Message& msg) {
  // TODO
  int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
  if (updated_min_clock != -1) {  // min clock updated
    auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
    for (auto req : reqs_blocked_at_this_min_clock) {
      reply_queue_->Push(storage_->Get(req));
    }
    storage_->FinishIter();
  }
}

void SSPModel::Add(Message& msg) {
  // TODO The add will not be block: this is difference between SSP staleness=0 and BSP
  storage_->Add(msg);
}

void SSPModel::Get(Message& msg) {
  // TODO
  CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));
  int progress = progress_tracker_.GetProgress(msg.meta.sender);
  int min_clock = progress_tracker_.GetMinClock();
  if (progress > min_clock + staleness_) {
    buffer_.Push(progress - staleness_, msg);
  } else {
    reply_queue_->Push(storage_->Get(msg));
  }
}

int SSPModel::GetProgress(int tid) {
  // TODO
  return progress_tracker_.GetProgress(tid); 
}

int SSPModel::GetPendingSize(int progress) {
  // TODO
  return buffer_.Size(progress);
}

void SSPModel::ResetWorker(Message& msg) 
{
  // TODO
  CHECK_EQ(msg.data.size(), 1);
  third_party::SArray<uint32_t> tids;
  tids = msg.data[0];
  std::vector<uint32_t> tids_vec;
  for (auto tid : tids){
    tids_vec.push_back(tid);
  }
  progress_tracker_.Init(tids_vec);
  Message reply_msg;
  reply_msg.meta.model_id = model_id_;
  reply_msg.meta.recver = msg.meta.sender;
  reply_msg.meta.sender = msg.meta.recver;
  reply_msg.meta.flag = Flag::kResetWorkerInModel;
  reply_queue_->Push(reply_msg);
}

}  // namespace csci5570
