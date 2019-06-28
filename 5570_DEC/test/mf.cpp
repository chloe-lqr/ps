#include <cmath>
#include <time.h>
#include <vector>
#include <iostream>
#include <random>
#include <thread>
#include <algorithm>
#include <numeric>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include "lib/parser.hpp"
#include "worker/kv_client_table.hpp"
#include "lib/batchiterator.hpp"
#include <time.h>
#include <math.h>


namespace csci5570
{
DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_string(input, "", "The hdfs input url");

DEFINE_string(hdfs_namenode, "proj10", "The hdfs namenode hostname");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs namenode port");
DEFINE_int32(master_port, -1, "The master port");
DEFINE_int32(n_loaders_per_node, 100, "The number of loaders per node");
DEFINE_int32(batch_size, 200, "batch size of each epoch");
DEFINE_int32(num_iters, 10, "number of iters");
DEFINE_int32(num_workers_per_node, 3, "num_workers_per_node");
DEFINE_double(alpha, 0.1, "learning rate");
DEFINE_double(lambda, 0.01, "regularization rate");

DEFINE_int32(n_features, 7, "The number of feature in the dataset");
//DEFINE_int32(hdfs_master_port, 23489, "A port number for the hdfs assigner host");

std::vector<double> gradients(const std::vector<lib::NetflixSample*>& samples, const std::vector<Key>& keys,
                              const std::vector<double>& vals, double alpha, double lambda, int batch_size, int n_features)
{
    std::vector<double> deltas(keys.size(), 0.0);

    for (auto* sample : samples)
    {
        auto& x = sample->x_;
        double y = sample->y_;
        double predict = 0.;
        std::vector<double> userFeature;
        std::vector<double> itemFeature;
        std::vector<int> userFeaturePos;
        std::vector<int> itemFeaturePos;

        // find user feature position
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            int tmpId = x[0].second*FLAGS_n_features+i;
            for(int j = 0; j < keys.size(); j++)
            {
                if (keys[j] == tmpId)
                {
                    userFeaturePos.push_back(j);
                    break;
                }
            }
        }
        // find item feature position
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            int tmpId = x[1].second*FLAGS_n_features+i;
            for(int j = 0; j < keys.size(); j++)
            {
                if (keys[j] == tmpId)
                {
                    itemFeaturePos.push_back(j);
                    break;
                }
            }
        }
        // fill user feature
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            userFeature.push_back(vals[userFeaturePos[i]]);
        }
        // fill item feature
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            itemFeature.push_back(vals[itemFeaturePos[i]]);
        }

        // compute predict
        for (int i = 0; i < FLAGS_n_features; i++)
        {
            predict += userFeature[i] * itemFeature[i];
        }

        // compute gradients
        for (int i = 0; i < FLAGS_n_features; i++)
        {
            double loss = y - predict;
            // update user gradients
            // reverse the sign
            deltas[userFeaturePos[i]] += alpha*((loss * itemFeature[itemFeaturePos[i]])/ batch_size - lambda * userFeature[userFeaturePos[i]]);
            // update item gradients
            deltas[itemFeaturePos[i]] += alpha*((loss * userFeature[userFeaturePos[i]])/ batch_size - lambda * itemFeature[itemFeaturePos[i]]);
        }
    }
    return deltas;
}



double rmse(const std::vector<lib::NetflixSample*>& samples, const std::vector<Key>& keys,
            const std::vector<double>& vals, int batch_size, int n_features)
{
    int total = samples.size();
    double rmse = 0.0;
    double error = 0.0;

    for (auto* sample : samples)
    {
        auto& x = sample->x_;
        double y = sample->y_;
        double predict = 0.0;

        std::vector<double> userFeature;
        std::vector<double> itemFeature;
        std::vector<int> userFeaturePos;
        std::vector<int> itemFeaturePos;

        // find user feature position
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            int tmpId = x[0].second*FLAGS_n_features+i;
            for(int j = 0; j < keys.size(); j++)
            {
                if (keys[j] == tmpId)
                {
                    userFeaturePos.push_back(j);
                    break;
                }
            }
        }
        // find item feature position
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            int tmpId = x[1].second*FLAGS_n_features+i;
            for(int j = 0; j < keys.size(); j++)
            {
                if (keys[j] == tmpId)
                {
                    itemFeaturePos.push_back(j);
                    break;
                }
            }
        }
        // fill user feature
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            userFeature.push_back(vals[userFeaturePos[i]]);
        }
        // fill item feature
        for(int i = 0; i < FLAGS_n_features; i++)
        {
            itemFeature.push_back(vals[itemFeaturePos[i]]);
        }

        // compute predict
        for (int i = 0; i < FLAGS_n_features; i++)
        {
            predict += userFeature[i] * itemFeature[i];
        }
        error += fabs(y - predict);
    }

    rmse = sqrt(error / batch_size);
    return rmse;
}



std::vector<Node> ParseFile_(const std::string& filename)
{
    std::vector<Node> nodes;
    std::ifstream input_file(filename.c_str());
    CHECK(input_file.is_open()) << "Error opening file: " << filename;
    std::string line;
    while (getline(input_file, line))
    {
        size_t id_pos = line.find(":");
        CHECK_NE(id_pos, std::string::npos);
        std::string id = line.substr(0, id_pos);
        size_t host_pos = line.find(":", id_pos+1);
        CHECK_NE(host_pos, std::string::npos);
        std::string hostname = line.substr(id_pos+1, host_pos - id_pos - 1);
        std::string port = line.substr(host_pos+1, line.size() - host_pos - 1);
        try
        {
            Node node;
            node.id = std::stoi(id);
            node.hostname = std::move(hostname);
            node.port = std::stoi(port);
            nodes.push_back(std::move(node));
        }
        catch(const std::invalid_argument& ia)
        {
            LOG(FATAL) << "Invalid argument: " << ia.what() << "\n";
        }
    }
    return nodes;
}



Node GetNodeById_(const std::vector<Node>& nodes, int id)
{
    for (const auto& node : nodes)
    {
        if (id == node.id)
        {
            return node;
        }
    }
    CHECK(false) << "Node" << id << " is not in the given node list";
}



void MFTest()
{
    std::vector<Node> nodes = ParseFile_(FLAGS_config_file);
    Node my_node = GetNodeById_(nodes, FLAGS_my_id);
    LOG(INFO) << my_node.DebugString();
    uint32_t node_id = my_node.id;;
    uint32_t num_nodes = nodes.size();

    lib::DataStore<lib::NetflixSample> data_store(FLAGS_n_loaders_per_node);
    using Parser = lib::Parser<lib::NetflixSample, lib::DataStore<lib::NetflixSample>>;
    using Parse = std::function<lib::NetflixSample(boost::string_ref, int)>;
    lib::NetflixSample netflix_sample;
    auto netflix_parse = Parser::parse_netflix;

    std::string master_host = nodes.front().hostname;
    std::string worker_host = my_node.hostname;

    lib::DataLoader<lib::NetflixSample, lib::DataStore<lib::NetflixSample>> data_loader;
    data_loader.load<Parse>(FLAGS_input, FLAGS_hdfs_namenode, master_host, worker_host, FLAGS_hdfs_namenode_port,
                            FLAGS_master_port, FLAGS_n_features, netflix_parse, &data_store, FLAGS_n_loaders_per_node, node_id, num_nodes);

    LOG(INFO) << "Node: " << node_id << "Can start engine";
    // start engine
    Engine engine(my_node, nodes);
    engine.StartEverything();

    // Create table on the server side
    const auto kTable = engine.CreateTable<double>(ModelType::BSP, StorageType::Map);
    // Specify task
    MLTask task;
    task.SetTables({kTable});
    std::vector<WorkerAlloc> worker_alloc;
    for (int i = 0; i < nodes.size(); i++)
    {
        worker_alloc.push_back({nodes[i].id, static_cast<uint32_t>(FLAGS_num_workers_per_node)});
    }
    task.SetWorkerAlloc(worker_alloc);


    task.SetLambda([kTable, &data_store, node_id](const Info& info)
    {
        // ***************************************************
        // create and init u & v
        LOG(INFO) << node_id << " before learning";
        KVClientTable<double> table_1(info.thread_id, kTable, info.send_queue,
                                      info.partition_manager_map.find(kTable)->second, info.callback_runner);
        std::vector<lib::NetflixSample*> samplePart = data_store.Get();

        std::vector<Key> all_keys;
        std::vector<double> initValue;
        std::set<Key> userkeys;
        std::set<Key> itemkeys;
          

        // get all users and items
        for(int i = 0; i < samplePart.size(); i++)
        {
            // all unique users
            int userId = 0;
            userId = int(samplePart[i]->x_[0].second);
            userkeys.insert(userId);

            // all unique items
            int itemId = 0;
            itemId = int(samplePart[i]->x_[1].second);
            itemkeys.insert(itemId);
        }
        // add user keys*feature into all_keys
        double tmp = 0.0;
	std::set<Key>::iterator ite1 = userkeys.begin();
	std::set<Key>::iterator ite2 = userkeys.end();
        for(; ite1!=ite2; ite1++)
        {
            for(int j = 0; j < FLAGS_n_features; j++)
            {
                srand(time(0));
                tmp = rand()%100/(double)101;
                initValue.push_back(tmp);
                // keyId = user/itemId * n + j
                all_keys.push_back(*ite1*FLAGS_n_features+j);
            }
        }
        // add item keys*feature into all keys
	std::set<Key>::iterator ite3 = itemkeys.begin();
	std::set<Key>::iterator ite4 = itemkeys.end();
        for(; ite3!=ite4; ite1++)
        {
            for(int j = 0; j < FLAGS_n_features; j++)
            {
                srand(time(0));
                tmp = rand()%100/(double)101;
                initValue.push_back(tmp);
                // keyId = user/itemId * n + j
                all_keys.push_back(*ite3*FLAGS_n_features+j);
            }
        }
        
	std::vector<double> vals_1;
        table_1.Get(all_keys, &vals_1);

        table_1.Add(all_keys, initValue);
   
        table_1.Clock();
//*****************************************************************

        //start learning
        LOG(INFO) << node_id << " start learning";
        BatchIterator<lib::NetflixSample> batch_2(data_store);
        for (int iter = 0; iter < FLAGS_num_iters; ++iter)
        {
            auto keys_data_2 = batch_2.NextBatch(FLAGS_batch_size);
            std::vector<lib::NetflixSample*> datasample_2 = keys_data_2.second;
            // all user id and movie id
            auto keys_2 = keys_data_2.first;

           // KVClientTable<double> table_2(info.thread_id, kTable, info.send_queue,
             //                             info.partition_manager_map.find(kTable)->second, info.callback_runner);

            // get all need userId/movieId' features
            std::vector<int> keys_need;
            std::vector<double> vals_2;
            for(int i = 0; i < keys_2.size(); i++)
            {
                for(int j = 0; j < FLAGS_n_features; j++)
                {
                    keys_need.push_back(keys_2[i]*FLAGS_n_features+j);
                }
            }
            table_1.Get(keys_2, &vals_2);

            auto delta = gradients(datasample_2, keys_2, vals_2, FLAGS_alpha, FLAGS_lambda, FLAGS_batch_size, FLAGS_n_features);

            table_1.Add(keys_2, delta);
            table_1.Clock();
        }

// *****************************************************************************

        //complete learning
        LOG(INFO) << node_id << " after learning";
        BatchIterator<lib::NetflixSample> batch_3(data_store);
        auto keys_data_3 = batch_3.NextBatch(FLAGS_batch_size);
        std::vector<lib::NetflixSample*> datasample_3 = keys_data_3.second;
        auto keys_3 = keys_data_3.first;
        std::vector<double> vals_3;
        //KVClientTable<double> table_3(info.thread_id, kTable, info.send_queue,
          //                            info.partition_manager_map.find(kTable)->second, info.callback_runner);

        LOG(INFO) << "Get key:" << node_id;
        // get all need userId/movieId' features
        std::vector<int> keys_need2;

        for(int i = 0; i < keys_3.size(); i++)
        {
            for(int j = 0; j < FLAGS_n_features; j++)
            {
                keys_need2.push_back(keys_3[i]*FLAGS_n_features+j);
            }
        }
        table_1.Get(keys_3, &vals_3);

        auto rmse_after = rmse(datasample_3, keys_3, vals_3, FLAGS_batch_size, FLAGS_n_features);

        LOG(INFO) << "After learning, Node_id: " << node_id << " RMSE: " << rmse_after;
        table_1.Clock();

    });

    engine.Barrier();
    engine.Run(task);

    LOG(INFO) << "Node " << node_id << " complete";
    engine.StopEverything();
    LOG(INFO) << "StopEverything, complete!";

}

}  // namespace csci5570


int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_stderrthreshold = 0;
    FLAGS_colorlogtostderr = true;

    csci5570::MFTest();
    return 0;
}

