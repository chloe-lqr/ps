#include "driver/simple_id_mapper.hpp"

#include "glog/logging.h"

#include <cinttypes>
#include <vector>
#include <stdio.h>
#include "base/node.hpp"

namespace csci5570 {

SimpleIdMapper::SimpleIdMapper(Node node, const std::vector<Node>& nodes) {
  node_ = node;
  nodes_ = nodes;
  /*
  for(size_t i=0; i<nodes_.size(); i++)
  {
    //serverthreads
    std::vector<uint32_t> server_ids;
    node2server_.insert({nodes_[i].id, server_ids});
    //worker helper threads
    node2worker_helper_.insert({nodes_[i].id, {kMaxThreadsPerNode * node.id + kWorkerHelperThreadId}});
    // worker threads
    std::set<uint32_t> worker_ids;
    node2worker_.insert({nodes_[i].id,worker_ids});
   
  }
  */
}
  
  

uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) {
  // TODO 
  return tid / kMaxThreadsPerNode;
}

void SimpleIdMapper::Init(int num_server_threads_per_node) {
    // TODO    
  /*
    // first do some checkings
    CHECK_GT(num_server_threads_per_node, 0);
    CHECK_LE(num_server_threads_per_node, kWorkerHelperThreadId);
    // For each node of all available nodes
    for(const auto& node : nodes_)
    { 
       // node[i] should have serverthread id i*1000
       for (int i = 0; i < num_server_threads_per_node; ++ i) {
        node2server_[node.id].push_back(node.id * kMaxThreadsPerNode + i);
      }
       // node[i] should have workerhelperthreadid i*1000 + 50
       node2worker_helper_[node.id].push_back(node.id * kMaxThreadsPerNode + kWorkerHelperThreadId);
    }
    */
    CHECK_GT(num_server_threads_per_node, 0);
  CHECK_LE(num_server_threads_per_node, kWorkerHelperThreadId);
  // Suppose there are 1 server and 1 worker_helper_thread for each node
  for (const auto& node : nodes_) {
    CHECK_LT(node.id, kMaxNodeId);
    // {0, 1000, 2000, ...} are server threads if num_server_threads_per_node is 1
    for (int i = 0; i < num_server_threads_per_node; ++ i) {
      node2server_[node.id].push_back(node.id * kMaxThreadsPerNode + i);
    }
    // Only 1 worker helper thread now
    // {10, 1010, 2010, ...} are worker helper threads
    node2worker_helper_[node.id].push_back(node.id * kMaxThreadsPerNode + kWorkerHelperThreadId);
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
  // TODO
  /*
  CHECK_LE(node2worker_[node_id].size(), kMaxThreadsPerNode - kMaxBgThreadsPerNode);
  for (int i = kMaxBgThreadsPerNode; i < kMaxThreadsPerNode; ++ i) {
    int tid = i + node_id * kMaxThreadsPerNode;
    if (node2worker_[node_id].find(tid) == node2worker_[node_id].end()) {
      node2worker_[node_id].insert(tid);
      return tid;
    }
  }
  CHECK(false);
  return -1;
  */
  CHECK_LE(node2worker_[node_id].size(), kMaxThreadsPerNode - kMaxBgThreadsPerNode);
  for (int i = kMaxBgThreadsPerNode; i < kMaxThreadsPerNode; ++ i) {
    int tid = i + node_id * kMaxThreadsPerNode;
    if (node2worker_[node_id].find(tid) == node2worker_[node_id].end()) {
      node2worker_[node_id].insert(tid);
      return tid;
    }
  }
  CHECK(false);
  return -1;
}
void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
  // TODO
  CHECK(node2worker_[node_id].find(tid) != node2worker_[node_id].end());
  node2worker_[node_id].erase(tid);
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
  // TODO
  return node2server_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  // TODO
  return node2worker_helper_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  // TODO
  return {node2worker_[node_id].begin(), node2worker_[node_id].end()};
}
std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
  // TODO
  std::vector<uint32_t> ret;
  std::vector<uint32_t> tmp;

  for(size_t i=0; i<nodes_.size(); i++)
  {
     tmp = GetServerThreadsForId(nodes_[i].id);
     ret.insert(ret.end(), tmp.begin(), tmp.end());
  }
  return ret;
}

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
const uint32_t SimpleIdMapper::kWorkerHelperThreadId;

}  // namespace csci5570
