#include <cmath>
#include <time.h>
#include <vector>
#include <iostream>
#include <random>
#include <thread>
#include <algorithm>
#include <numeric>
#include <ctime>
#include <chrono>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include "lib/parser.hpp"
#include "worker/kv_client_table.hpp"
#include "lib/batchiterator.hpp"


namespace csci5570{
DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(input, "", "The hdfs input url");
  
DEFINE_string(hdfs_namenode, "proj10", "The hdfs namenode hostname");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs namenode port");
DEFINE_int32(master_port, -1, "The master port");
DEFINE_int32(n_loaders_per_node, 100, "The number of loaders per node");
DEFINE_int32(batch_size, 2000, "batch size of each epoch");
DEFINE_int32(num_iters, 100, "number of iters");
DEFINE_int32(num_workers_per_node, 5, "num_workers_per_node");
DEFINE_double(alpha, 0.1, "learning rate");
  
DEFINE_int32(n_features, 10, "The number of feature in the dataset");  
//DEFINE_int32(hdfs_master_port, 23489, "A port number for the hdfs assigner host");  
  
std::vector<double> compute_gradients(const std::vector<lib::KddSample*>& samples, const std::vector<Key>& keys,
                                      const std::vector<double>& vals, double alpha) {
  std::vector<double> deltas(keys.size(), 0.);
  for (auto* sample : samples) {
    auto& x = sample->x_;
    double y = sample->y_;
    double predict = 0.;
    int idx = 0;
    for (auto& field : x) {
      while (keys[idx] < field.first)
        ++idx;
      predict += vals[idx] * field.second;
    }
    predict += vals.back();
    int predictLabel;
    if (predict >= 0) {
      predictLabel = 1;
    } else {
      predictLabel = -1;
    }

    idx = 0;
    for (auto& field : x) {
      while (keys[idx] < field.first)
        ++idx;
      deltas[idx] += alpha * field.second * (y - predictLabel);
    }
    deltas[deltas.size() - 1] += alpha * (y - predictLabel);
  }
  return deltas;
}

double correct_rate(const std::vector<lib::KddSample*>& samples, const std::vector<Key>& keys,
                    const std::vector<double>& vals) {
  int total = samples.size();
  double n = 0;
  for (auto* sample : samples) {
    auto& x = sample->x_;
    double y = sample->y_;
    double predict = 0.;
    int idx = 0;
    for (auto& field : x) {
      while (keys[idx] < field.first)
        ++idx;
      predict += vals[idx] * field.second;
    }
    predict += vals.back();
    int predict_;
    if (predict >= 0) {
      predict_ = 1;
    } else {
      predict_ = -1;
    }
    if (predict_ == y) {
      n++;
    }
  }
  double result = n / total;
  return result;
}
 std::vector<Node> ParseFile_(const std::string& filename) {
  std::vector<Node> nodes;
  std::ifstream input_file(filename.c_str());
  CHECK(input_file.is_open()) << "Error opening file: " << filename;
  std::string line;
  while (getline(input_file, line)) {
    size_t id_pos = line.find(":");
    CHECK_NE(id_pos, std::string::npos);
    std::string id = line.substr(0, id_pos);
    size_t host_pos = line.find(":", id_pos+1);
    CHECK_NE(host_pos, std::string::npos);
    std::string hostname = line.substr(id_pos+1, host_pos - id_pos - 1);
    std::string port = line.substr(host_pos+1, line.size() - host_pos - 1);
    try {
      Node node;
      node.id = std::stoi(id);
      node.hostname = std::move(hostname);
      node.port = std::stoi(port);
      nodes.push_back(std::move(node));
    }
    catch(const std::invalid_argument& ia) {
      LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
    }
  }
  return nodes;
}
  Node GetNodeById_(const std::vector<Node>& nodes, int id) {
  for (const auto& node : nodes) {
    if (id == node.id) {
      return node;
    }
  }
  CHECK(false) << "Node" << id << " is not in the given node list";
}


void SVMTest() {
  std::vector<Node> nodes = ParseFile_(FLAGS_config_file);
  Node my_node = GetNodeById_(nodes, FLAGS_my_id);
  LOG(INFO) << my_node.DebugString();
  uint32_t node_id = my_node.id;;
  uint32_t num_nodes = nodes.size();
  lib::DataStore<lib::KddSample> data_store(FLAGS_n_loaders_per_node);
  using Parser = lib::Parser<lib::KddSample, lib::DataStore<lib::KddSample>>;
  using Parse = std::function<lib::KddSample(boost::string_ref, int)>;
  lib::KddSample kdd_sample;
  auto kdd_parse = Parser::parse_kdd;

  std::string master_host = nodes.front().hostname;
  std::string worker_host = my_node.hostname;
  
  lib::DataLoader<lib::KddSample, lib::DataStore<lib::KddSample>> data_loader;
  data_loader.load<Parse>(FLAGS_input, FLAGS_hdfs_namenode, master_host, worker_host, FLAGS_hdfs_namenode_port, 
                          FLAGS_master_port, FLAGS_n_features, kdd_parse, &data_store, FLAGS_n_loaders_per_node, node_id, num_nodes);
  
  LOG(INFO) << "Node: " << node_id << " start engine";
  // start engine
  Engine engine(my_node, nodes);
  engine.StartEverything();

  // Create table on the server side
  const auto kTable = engine.CreateTable<double>(ModelType::SSP, StorageType::Map);

  // Specify task
  MLTask task;
  task.SetTables({kTable});
  std::vector<WorkerAlloc> worker_alloc;
  for (int i = 0; i < nodes.size(); i++) {
    worker_alloc.push_back({nodes[i].id, static_cast<uint32_t>(FLAGS_num_workers_per_node)});
  }
  task.SetWorkerAlloc(worker_alloc);
  
  task.SetLambda([kTable, &data_store, node_id](const Info& info) {
    //before learning
    BatchIterator<lib::KddSample> batch(data_store);
    auto keys_data_all = batch.NextBatch(1e5);
    
    //auto keys_data_1 = batch.NextBatch(FLAGS_batch_size);
    std::vector<lib::KddSample*> datasample_1 = keys_data_all.second;
    auto keys_1 = keys_data_all.first;
    std::vector<double> vals_1;
    KVClientTable<double> table(info.thread_id, kTable, info.send_queue,
                                info.partition_manager_map.find(kTable)->second, info.callback_runner);
    LOG(INFO) << "Before kGet Node: " << node_id; 
    table.Get(keys_1, &vals_1);
    LOG(INFO) << "After kGet Node: " << node_id;
    auto correctrate_before = correct_rate(datasample_1, keys_1, vals_1);
    LOG(INFO) << "Before learning, Node_id: " << node_id<<" Correctrate: "<< correctrate_before;
    table.Clock();
    
    
    
    auto start_time = std::chrono::steady_clock::now();
    //start learning
    for (int iter = 0; iter < FLAGS_num_iters; ++iter) {
      batch.random_start_point();
      auto keys_data_2 = batch.NextBatch(FLAGS_batch_size);
      std::vector<lib::KddSample*> datasample_2 = keys_data_2.second;
      auto keys_2 = keys_data_2.first;
      std::vector<double> vals_2;

      table.Get(keys_2, &vals_2);
      auto delta = compute_gradients(datasample_2, keys_2, vals_2, FLAGS_alpha);
      table.Add(keys_2, delta);
      table.Clock();
    }
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG(INFO) << "total time: " << total_time << " ms on worker" << node_id;
    
    
    
    //complete learning
    //auto keys_data_3 = batch.NextBatch(FLAGS_batch_size);
    std::vector<lib::KddSample*> datasample_3 = keys_data_all.second;
    auto keys_3 = keys_data_all.first;
    std::vector<double> vals_3;
    table.Get(keys_3, &vals_3);
    auto correctrate_after = correct_rate(datasample_3, keys_3, vals_3);
    LOG(INFO) << "After learning, Node_id: " << node_id << " Correctrate: " << correctrate_after;
    table.Clock();  
  });
  engine.Barrier();

  engine.Run(task);  
  engine.StopEverything();
  
  LOG(INFO) << "Node " << node_id << " complete";
}


}  // namespace csci5570
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_stderrthreshold = 0;
  FLAGS_colorlogtostderr = true;

  csci5570::SVMTest();
  return 0;
}

