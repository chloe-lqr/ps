#pragma once
#include <functional>
#include <thread>
#include <vector>
#include <string>
#include "boost/utility/string_ref.hpp"
#include "base/serialization.hpp"
#include "io/coordinator.hpp"
#include "io/hdfs_assigner.hpp"
#include "io/hdfs_file_splitter.hpp"
#include "io/line_input_format.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include "lib/parser.hpp"
#include <ctime>
#include <chrono>

namespace csci5570 {
namespace lib {

template <typename Sample, typename DataStore>
class AbstractDataLoader {
 public:
  /**
   * Load samples from the url into datastore
   *
   * @param url          input file/directory
   * @param n_features   the number of features in the dataset
   * @param parse        a parsing function
   * @param datastore    a container for the samples / external in-memory storage abstraction
   */
  template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
  static void load(std::string url, int n_features, Parse parse, DataStore* datastore) {
  }

};  // class AbstractDataLoader

 template <typename Sample, typename DataStore>
 class DataLoader : public AbstractDataLoader<Sample, DataStore> {
 public:
  template <typename Parse> 
  static void load(std::string url, std::string hdfs_namenode, std::string master_host, std::string worker_host,
                   int hdfs_namenode_port, int master_port, int n_features, Parse parse, DataStore* datastore, 
                   uint32_t n_threads_per_node, uint32_t node_id, uint32_t num_nodes) {
    std::thread hdfs_main_thread;
    zmq::context_t zmq_context(1);
    
    if(worker_host == master_host){
    hdfs_main_thread =  std::thread([&zmq_context, master_port, hdfs_namenode_port, hdfs_namenode] {
      HDFSBlockAssigner hdfs_block_assigner(hdfs_namenode, hdfs_namenode_port, &zmq_context, master_port);
      hdfs_block_assigner.Serve();
    });
    LOG(INFO) << "HDFS Master started";
    }
    
    Coordinator coordinator(node_id, worker_host, &zmq_context, master_host, master_port);
    coordinator.serve();

    std::vector<std::thread> threads;
    threads.reserve(n_threads_per_node);
    
    auto start_time = std::chrono::steady_clock::now();
    for (size_t tid = 0; tid < n_threads_per_node; tid++) {
        // push a worker thread to the vector
        threads.push_back(std::thread([n_features, tid, url, n_threads_per_node, &datastore, &parse,
          worker_host, &coordinator,hdfs_namenode,hdfs_namenode_port,node_id, num_nodes] {
        int n_loading_threads = n_threads_per_node * num_nodes;
        LineInputFormat infmt(url, n_loading_threads, tid, &coordinator, worker_host, hdfs_namenode, hdfs_namenode_port);

        boost::string_ref record;
        bool success = false;
        int count = 0;
        // 2. Extract lines
        while (true) {
          success = infmt.next(record);
          if (success == false)
            break;
          // 3. Parse line and put samples into datastore
          datastore->Push(tid, parse(record, n_features));
          count ++;
          if(count > 2.3e6) break;
        }
        //LOG(INFO) << tid <<" finished loading, the number of lines:" << count;
        uint32_t generaltid = tid + node_id * n_loading_threads;
        BinStream finish_signal;
        finish_signal << worker_host << generaltid;
        coordinator.notify_master(finish_signal, HDFSBlockAssigner::kExit);
       }));
    }
    if(worker_host == master_host){
    //LOG(INFO) << node_id << "HDFS Master waiting";
    hdfs_main_thread.join();
    //LOG(INFO) << node_id << "HDFS Master joined";
    }
    // Make sure zmq_context and coordinator live long enough
    for (auto& t : threads)
    {
      t.join();
    }
   
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "total time: " << total_time << " ms on worker" << node_id;
  }
};
}  // namespace lib
}  // namespace csci5570
