#pragma once

#include <functional>

#include "base/message.hpp"

namespace csci5570 {

class AbstractCallbackRunner {
 public:
  /**
   * Register callbacks for receiving a message
   */
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) = 0;
  /**
   * Register callbacks for when all expected responses are received
   */
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) = 0;

  /**
   * Register a new request which expects to receive <expected_responses> responses
   */
  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) = 0;

  /**
   * Return when the request is completed
   */
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) = 0;

  /**
   * Used by the worker threads on receival of messages and to invoke callbacks
   */
  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) = 0;
};  // class AbstractCallbackRunner

class callbackRunner: public AbstractCallbackRunner {
 public:
  callbackRunner(){}

  void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) override{
      std::lock_guard<std::mutex> lk(mutex);
      if(recv_handle_map_[app_thread_id][model_id]){
         recv_handle_map_[app_thread_id].erase(model_id);
       }
      recv_handle_map_[app_thread_id][model_id]=recv_handle;
 }
  
  void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                const std::function<void()>& recv_finish_handle) override {
    std::lock_guard<std::mutex> lk(mutex);                                 
    if(recv_finish_handle_map_[app_thread_id][model_id]){
      recv_finish_handle_map_[app_thread_id].erase(model_id);
    }
    recv_finish_handle_map_[app_thread_id][model_id] = recv_finish_handle;
  }

  void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override {
    std::lock_guard<std::mutex> lk(mutex);     
    tracker_map_[app_thread_id][model_id] = {expected_responses, 0};
    if(!cond_map_[app_thread_id][model_id]){
    cond_map_[app_thread_id][model_id].reset(new std::condition_variable());
    }
  }
  void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override {
    std::unique_lock<std::mutex> lk(mutex);
    cond_map_[app_thread_id][model_id]->wait(lk, [this,app_thread_id,model_id] { return tracker_map_[app_thread_id][model_id].first == tracker_map_[app_thread_id][model_id].second; });
  }
  void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& m) override {
    bool recv_finish = false;
    {
      std::lock_guard<std::mutex> lk(mutex);
      recv_finish = tracker_map_[app_thread_id][model_id].first == tracker_map_[app_thread_id][model_id].second + 1 ? true : false;
    }
    recv_handle_map_[app_thread_id][model_id](m);
    if (recv_finish) {
     recv_finish_handle_map_[app_thread_id][model_id]();
    }
    {
      std::lock_guard<std::mutex> lk(mutex);
      tracker_map_[app_thread_id][model_id].second += 1;
      if (recv_finish) {
        if(cond_map_[app_thread_id][model_id]){
        cond_map_[app_thread_id][model_id]->notify_all();
        }
      }
    }
  }

 private:
  std::map<uint32_t,std::map<uint32_t,std::function<void(Message&)>>> recv_handle_map_;
  std::map<uint32_t,std::map<uint32_t,std::function<void()>>> recv_finish_handle_map_;

  std::mutex mutex;
  std::map<uint32_t,std::map<uint32_t,std::unique_ptr<std::condition_variable>>> cond_map_;
  std::map<uint32_t,std::map<uint32_t,std::pair<uint32_t, uint32_t>>> tracker_map_;
};  // class CallbackRunner




}  // namespace csci5570
