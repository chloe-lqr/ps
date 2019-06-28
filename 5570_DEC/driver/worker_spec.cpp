#include "driver/worker_spec.hpp"
#include "glog/logging.h"

// worker_spec.cpp is verfied by chongsu

namespace csci5570 {

WorkerSpec::WorkerSpec(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
  // call the Init() function
  Init(worker_alloc);
}
bool WorkerSpec::HasLocalWorkers(uint32_t node_id) const {
  // TODO
  return node_to_workers_.find(node_id) != node_to_workers_.end();
}
const std::vector<uint32_t>& WorkerSpec::GetLocalWorkers(uint32_t node_id) const {
  // TODO
  std::map<uint32_t, std::vector<uint32_t>>::const_iterator it;
  it = node_to_workers_.find(node_id);
  CHECK(it != node_to_workers_.end()); // here CHECK() is better, since it will report err
  return it->second;	
}
const std::vector<uint32_t>& WorkerSpec::GetLocalThreads(uint32_t node_id) const {
  // TODO
   std::map<uint32_t, std::vector<uint32_t>>::const_iterator it;
   it = node_to_threads_.find(node_id);
   CHECK(it != node_to_threads_.end()); // here CHECK() is better, since it will report err
   return it->second;
}

std::map<uint32_t, std::vector<uint32_t>> WorkerSpec::GetNodeToWorkers() {
  // TODO
  return node_to_workers_;
}

std::vector<uint32_t> WorkerSpec::GetAllThreadIds() {
  // TODO
  std::vector<uint32_t> ret(thread_ids_.begin(), thread_ids_.end());
  return ret;
}

/**
 * Register worker id (specific to a task) along with the corresponding thread id
 */
void WorkerSpec::InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id) {
  // TODO
  // check the workerthreadid and serverthreadid is unoccupied
  CHECK(worker_to_thread_.find(worker_id) == worker_to_thread_.end());
  CHECK(thread_to_worker_.find(thread_id) == thread_to_worker_.end());
  CHECK(thread_ids_.find(thread_id) == thread_ids_.end());
  CHECK(worker_to_node_.find(worker_id) != worker_to_node_.end());

  // insert
  worker_to_thread_.insert(std::make_pair(worker_id, thread_id));
  thread_to_worker_.insert(std::make_pair(thread_id, worker_id));
  thread_ids_.insert(thread_id); 
  node_to_threads_[worker_to_node_[worker_id]].push_back(thread_id);
}

/**
 * Initiates the worker specification with the specified allocation
 * Update worker_to_node_, node_to_workers_ and num_workers_
 */
void WorkerSpec::Init(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
  uint32_t count_allocated_workers = 0;
  for(int i=0; i<worker_alloc.size(); i++)
  {
     uint32_t worker_node_id = worker_alloc[i].node_id;
     uint32_t worker_num_workers = worker_alloc[i].num_workers;
     std::vector<uint32_t> worker_ids;  
     for(int j = 0; j<worker_num_workers; j++)
     {
        // add std::make_pair(worker, node) to worker to node
        worker_to_node_.insert(std::make_pair(count_allocated_workers, worker_node_id));
	// add worker id to worker_ids for node to workers
        worker_ids.push_back(count_allocated_workers); 
	// update num_workers: how many workers have been allocated     
        count_allocated_workers++;  
     }
     node_to_workers_.insert(std::make_pair(worker_node_id, worker_ids));
  }
  num_workers_ = count_allocated_workers;
}
}  // namespace csci5570
