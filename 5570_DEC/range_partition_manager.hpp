#pragma once

#include <cinttypes>
#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/third_party/sarray.h"

#include "glog/logging.h"

namespace csci5570 {

class RangePartitionManager : public AbstractPartitionManager {
public:
    RangePartitionManager(const std::vector<uint32_t>& server_thread_ids, const std::vector<third_party::Range>& ranges)
      : AbstractPartitionManager(server_thread_ids) 
    {
    	ranges_ = ranges;
    }

    void Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const override 
    {

    	for(auto it_server = server_thread_ids_.cbegin(); it_server!=server_thread_ids_.cend(); ++it_server)
    	{
    		std::vector<Key> server_keys;
            for(auto it_key=0; it_key<keys.size(); ++it_key)
            {
        	    Key current_key = keys[it_key];
                if(current_key>=ranges_[server_thread_ids_[*it_server]].begin() 
                	&& current_key<ranges_[server_thread_ids_[*it_server]].end())
                {
                    server_keys.push_back(current_key);
                }
            }
            if(!server_keys.empty()){
            	 Keys server_keys_sarray(server_keys);
                 sliced->push_back({*it_server,server_keys_sarray});
         	}

        }
       }

    void Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const override {

    }

private:
    std::vector<third_party::Range> ranges_;
};

}  // namespace csci5570


