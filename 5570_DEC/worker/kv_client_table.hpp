#pragma once

#include "glog/logging.h"
#include <iostream>
#include "base/abstract_partition_manager.hpp"

#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_callback_runner.hpp"

#include <cinttypes>
#include <vector>

namespace csci5570 {
/*
 * Provides the API to users, and implements the worker-side abstraction of model
 * Each model in one application is uniquely handled by one KVClientTable
 *
 * @param Val type of model parameter values
 */
template <typename Val>
class KVClientTable {
 using KVPairs = std::pair<third_party::SArray<Key>, third_party::SArray<double>>;
 public:
  /**
   * @param app_thread_id       user thread id
   * @param model_id            model id
   * @param sender_queue        the work queue of a sender communication thread
   * @param partition_manager   model partition manager
   * @param callback_runner     callback runner to handle received replies from servers
   */
  KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const sender_queue,
                const AbstractPartitionManager* const partition_manager, AbstractCallbackRunner* const callback_runner)
      : app_thread_id_(app_thread_id),
        model_id_(model_id),
        sender_queue_(sender_queue),
        partition_manager_(partition_manager),
        callback_runner_(callback_runner){};
        
  // ========== API ========== //
  /* when Clock() is called the kv_client_table add a "Flag::kClock" message 
     for every server thread to sender queue
  */
  // tested by chongsu
  void Clock()
  {
    const std::vector<uint32_t>& server_thread_ids = partition_manager_->GetServerThreadIds();
    for(uint32_t i=0; i<server_thread_ids.size(); i++)
    {
      Message m;
      m.meta.sender = app_thread_id_;
      m.meta.recver = server_thread_ids[i];
      m.meta.flag = Flag::kClock;
      m.meta.model_id = model_id_;
      sender_queue_->Push(m);
    }
  }
  
  // vector version
  // when Add() is called the kv_client_table add a "Flag::kAdd" message to sender queue
  // tested by chongsu
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals) 
  {
    // prepare key Sarray and value Sarray for the message
    third_party::SArray<Key> key_tmp;
    third_party::SArray<double> val_tmp;
    for(size_t i=0; i < keys.size(); i++)
    {
       key_tmp.push_back(keys[i]);
    }
    for(size_t i=0; i < vals.size(); i++)
    {
       val_tmp.push_back(vals[i]);
    }
    
    // call range_partition to find corrresponding serverthread of each key-value pair
    std::vector<std::pair<int, KVPairs>> sliced;
    partition_manager_->Slice(std::make_pair(key_tmp, val_tmp), &sliced);
    
    // for each serverthread having new kvPair, add Flag::kAdd" message to sender queue
    for(size_t count=0; count < sliced.size(); count++)
    {
      Message m;
      third_party::SArray<char> key_char;
      third_party::SArray<Val> val_Val;
      key_char = sliced[count].second.first;
      for(size_t j=0; j < sliced[count].second.second.size(); j++)
      {
        val_Val.push_back((Val) sliced[count].second.second.data()[j]);
      }
     // third_party::SArray<char> val_char;
     //val_char = val_Val;
      m.AddData(key_char);
      m.AddData(val_Val);
      m.meta.sender = app_thread_id_;
      //LOG(INFO) << "In kvclient table: " << m.meta.sender;
      m.meta.recver = sliced[count].first;
      m.meta.flag = Flag::kAdd;
      m.meta.model_id = model_id_;
      sender_queue_->Push(m);
    }

  }
  // tested by chongsu
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals) 
  {
   // prepare key Sarray for the Get message
    third_party::SArray<Key> key_tmp;
    for(size_t i=0; i < keys.size(); i++)
    {
       key_tmp.push_back(keys[i]);
    }

    // call range_partition to find corrresponding serverthread of each key
    std::vector<std::pair<int, third_party::SArray<Key>>> sliced;
    partition_manager_->Slice(key_tmp, &sliced);
    
    // define recv_handle() and recv_finish_handle() functions
    third_party::SArray<Val> vtp;
    std::vector<Val> tmp;
    std::function<void(Message&)> recv_handle = [&](Message m)  // store the returned values to tmp
    {
      vtp = m.data[1];
      for (size_t i = 0; i < vtp.size(); i++) {
        tmp.push_back(vtp[i]);
      }
    };
    std::function<void()> recv_finish_handle = [&]()  // copy the returned values stored in tmp to *val (belongs to worker)
    { 
       vals->assign(tmp.begin(), tmp.end());
    };
    
    // register AbstractCallbackRunner functions
    callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, recv_finish_handle);
    callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, recv_handle);
    callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size()); // tell CallbackRunner number of Get Requests
    
    // for each serverthread having key, add Flag::kGet" message to sender queue
    for(size_t count=0; count < sliced.size(); count++)
    {
      Message m;
      third_party::SArray<char> key_char;
      key_char = sliced[count].second;
      m.AddData(key_char);
      m.meta.sender = app_thread_id_;
      m.meta.recver = sliced[count].first;
      m.meta.model_id = model_id_;
      m.meta.flag = Flag::kGet;
      sender_queue_->Push(m);
    }
    // wait until get the reply message from server thread
    callback_runner_->WaitRequest(app_thread_id_, model_id_);
  }

  // sarray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) 
  {
      //TODO
  }
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals)
  {
    //TODO
  }
  // ========== API ========== //

 private:
  uint32_t app_thread_id_;  // identifies the user thread
  uint32_t model_id_;       // identifies the model on servers

  ThreadsafeQueue<Message>* const sender_queue_;             // not owned
  AbstractCallbackRunner* const callback_runner_;            // not owned
  
  std::vector<Message> response_messages_vector_; 
  // see bug fix for abstract partition manager and its subclasses
  const AbstractPartitionManager* const partition_manager_;  // not owned

};  // class KVClientTable



}  // namespace csci5570



