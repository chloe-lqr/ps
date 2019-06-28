#include "driver/engine.hpp"

#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/node.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/server_thread.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/worker_thread.hpp"

namespace csci5570 {

void Engine::StartEverything(int num_server_threads_per_node) {
  // TODO
  // 1. Create an id_mapper and a mailbox
  CreateIdMapper(num_server_threads_per_node);
  CreateMailbox();
  // 2. Start Sender
  StartSender();
  // 3. Create serverThreads and workerThreads
  StartServerThreads();
  StartWorkerThreads();
  // 5. Start the communication threads, bind and connect to all otherthreads
  StartMailbox();
}
void Engine::CreateIdMapper(int num_server_threads_per_node) {
  // TODO
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  id_mapper_->Init(num_server_threads_per_node);
}
void Engine::CreateMailbox() {
  // TODO
  mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get()));
}
void Engine::StartServerThreads() {
  // TODO
  uint32_t node_id = node_.id;
  std::vector<uint32_t> server_threads_ids = id_mapper_->GetServerThreadsForId(node_id);
  for (size_t i = 0; i < server_threads_ids.size(); i++) 
  {
    server_thread_group_.emplace_back(new ServerThread(server_threads_ids[i]));
    mailbox_->RegisterQueue(server_thread_group_[i]->GetId(), server_thread_group_[i]->GetWorkQueue());
  }
  for (size_t i = 0; i < server_thread_group_.size(); i++) {
    server_thread_group_[i]->Start();
  }
}
void Engine::StartWorkerThreads() {
  // TODO
   // get worker thread id from id_mapper_
  uint32_t node_id = node_.id;
  uint32_t worker_thread_id = id_mapper_->GetWorkerHelperThreadsForId(node_id).front();

  // reset new callback runner
  callback_runner_.reset(new callbackRunner());

  // reset worker thread with new thread id from id_mapper
  worker_thread_.reset(new AbstractWorkerThread(worker_thread_id, callback_runner_.get()));

  mailbox_->RegisterQueue(worker_thread_->GetId(), worker_thread_->GetWorkQueue());

  worker_thread_->Start();
}
void Engine::StartMailbox() {
  // TODO
  mailbox_->Start();
}
void Engine::StartSender() {
  // TODO
  sender_.reset(new Sender(mailbox_.get()));
  sender_->Start();
}

void Engine::StopEverything() {
  // TODO
  // 1. Stop the Sender
  StopSender();    // LOG(INFO) << "Stopped sender";
  // 2. Stop the mailbox: by Barrier() and then exit
  Barrier();      //  LOG(INFO) << "Stopped barrier";
  // 3. The mailbox will stop the corresponding registered threads
  StopMailbox(); // LOG(INFO) <<    "Stopped Mailbox";
  // 4. Stop the ServerThreads and WorkerThreads
  StopServerThreads();   // LOG(INFO) << "Stopped serverthreads";
  StopWorkerThreads();   // LOG(INFO) << "Stopped workerthreads";
}
void Engine::StopServerThreads() {
  // TODO
  for(int i=0; i<server_thread_group_.size(); i++)
  {
    server_thread_group_[i]->Stop();
  }
}
void Engine::StopWorkerThreads() {
  // TODO
  worker_thread_->Stop();
}
void Engine::StopSender() {
  // TODO
  sender_->Stop();
}
void Engine::StopMailbox() {
  // TODO
  //call Mailbox::Stop
  mailbox_->Stop();
}

void Engine::Barrier() {
  // TODO
  //call Mailbox::Barrier
  mailbox_->Barrier();
}

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
  // construct WorkerSpec from vector of worker_spec structure
  WorkerSpec worker_spec(worker_alloc);

  for(size_t i = 0; i < worker_alloc.size(); i++)
  {
    uint32_t node_id = worker_alloc[i].node_id;
    auto workers_id = worker_spec.GetLocalWorkers(node_id);
    for (auto worker_id: workers_id) {
      uint32_t user_thread_id = id_mapper_->AllocateWorkerThread(node_id);
      worker_spec.InsertWorkerIdThreadId(worker_id, user_thread_id);
    }
  }

  return worker_spec;
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  // TODO
  std::vector<uint32_t> server_threads_id = id_mapper_->GetServerThreadsForId(node_.id);
  uint32_t worker_thread_id = id_mapper_->AllocateWorkerThread(node_.id);

  ThreadsafeQueue<Message> queue;
  mailbox_->RegisterQueue(worker_thread_id, &queue);
  Message msg;
  msg.meta.flag = Flag::kResetWorkerInModel;
  msg.meta.model_id = table_id;
  msg.meta.sender = worker_thread_id;
  msg.AddData(third_party::SArray<uint32_t>(worker_ids));
  
  // send message to each serverthread
  for (int i = 0; i < server_threads_id.size(); i++) {
    msg.meta.recver = server_threads_id[i];
    sender_->GetMessageQueue()->Push(msg);
  }
  Message reply;
  for (int i = 0; i < server_threads_id.size(); i++) {
    queue.WaitAndPop(&reply);
  }
  mailbox_->DeRegisterQueue(worker_thread_id);
  id_mapper_->DeallocateWorkerThread(node_.id, worker_thread_id);

}

void Engine::Run(const MLTask& task) {
  // TODO
  // get workeralloc infomation of the task
  WorkerSpec worker_spec = AllocateWorkers(task.GetWorkerAlloc());
  // get tables allocated to the task
  const std::vector<uint32_t>& tables = task.GetTables();
  for (auto& table: tables) {
    InitTable(table, worker_spec.GetAllThreadIds());
  }

  Barrier();

  if (worker_spec.HasLocalWorkers(node_.id)) {
    const auto& local_threads = worker_spec.GetLocalThreads(node_.id);
    const auto& local_workers = worker_spec.GetLocalWorkers(node_.id);

    std::vector<std::thread> thread_group(local_threads.size());
    std::map<uint32_t, AbstractPartitionManager*> partition_manager_map;
    for (auto& table: tables) {
      auto it = partition_manager_map_.find(table);
      partition_manager_map[table] = it->second.get();
    }

    for (size_t i = 0; i < thread_group.size(); i++) {
      mailbox_->RegisterQueue(local_threads[i], worker_thread_->GetWorkQueue());
      Info info;
      info.thread_id = local_threads[i];
      info.worker_id = local_workers[i];
      info.send_queue = sender_->GetMessageQueue();
      info.partition_manager_map = partition_manager_map;
      info.callback_runner = callback_runner_.get();
      thread_group[i] = std::thread([&task, info]() {task.RunLambda(info);});
    }
    
    for (auto& th : thread_group) {
        th.join();
    }
  }
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  // TODO
  std::map<uint32_t, std::unique_ptr<AbstractPartitionManager>>::iterator it;
  it = partition_manager_map_.find(table_id);
  if(it == partition_manager_map_.end())
  {
    partition_manager_map_.insert(std::make_pair(table_id, std::move(partition_manager)));
  }
}

}  // namespace csci5570
