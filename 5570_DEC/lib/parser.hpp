#pragma once
#include <string.h>
#include <boost/tokenizer.hpp>
#include <string>
#include <utility>
#include "boost/utility/string_ref.hpp"
#include "lib/labeled_sample.hpp"
#include <time.h>


namespace csci5570
{
namespace lib
{

template <typename Sample, typename DataStore>
class Parser
{
public:
    /**
     * Parsing logic for one line in file
     *
     * @param line    a line read from the input file
     */
    static Sample parse_libsvm(boost::string_ref line, int n_features)
    {
        // check the LibSVM format and complete the parsing
        // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
        // so far we tried all the tree and found std::strtok_r is fastest :)
        Sample temp_sample = SVMSample();

        boost::char_separator<char> sep(" :");
        boost::tokenizer<boost::char_separator<char>> tok(line, sep);

        int count = 0;
        for (boost::tokenizer<boost::char_separator<char>>::iterator beg = tok.begin(); beg != tok.end(); ++beg)
        {
            int index;
            double value;
            if (count == 0)
            {
                if (line.substr(0, 1) == "+")
                {
                    temp_sample.y_ = 1;
                }
                else
                {
                    temp_sample.y_ = -1;
                }
            }
            else if (count % 2 == 1)
            {
                index = stoi(*beg);
                temp_sample.x_.push_back(index);
            }
            else
            {
                value = stof(*beg);
            }
            count++;
        }
        return temp_sample;
    }
    static Sample parse_kdd(boost::string_ref line, int n_features)
    {
        // check the kdd format and complete the parsing
        Sample temp_sample = KddSample();
        boost::char_separator<char> sep(" :");
        boost::tokenizer<boost::char_separator<char>> tok(line, sep);

        int count = 0;
        int index = 666;
        double value = 0.66;
        for (boost::tokenizer<boost::char_separator<char>>::iterator beg = tok.begin(); beg != tok.end(); ++beg)
        {
            if (count == 0)
            {
                if (line.substr(0, 1) == "1")
                {
                    temp_sample.y_ = 1;
                }
                else
                {
                    temp_sample.y_ = -1;
                }
            }
            else if (count % 2 == 1)
            {
                index = stoi(*beg);
            }
            else
            {
                value = stod(*beg);

                temp_sample.x_.push_back(std::make_pair(index, value));
            }
            count++;
        }
        return temp_sample;
    }

/*
    static Sample parse_netflix(boost::string_ref line, int n_features)
    {

        Sample temp_sample = NetflixSample();

        vector<std::string> record;
        vector<double> init(n_features);
        char *p;
        char *s =(char*)line.data();
        const char *delim = "\t";
        // 1:user
        p = strtok(s, delim);

        for(int i=0; i<init.size(); i++)
        {
            double tmp=0.0;
            srand(time(0));
            tmp = rand()%100/(double)101;
            init[i] = tmp;
        }

        temp_sample.x_.push_back(std::make_pair("1:stoi(p)", init));
        // 2:item
        p = strtok(NULL, delim);

        for(int i=0; i<init.size(); i++)
        {
            double tmp=0.0;
            srand(time(0));
            tmp = rand()%100/(double)101;
            init[i] = tmp;
        }
        temp_sample.x_.push_back(std::make_pair("2:stoi(p)", init));
        // y:ranking
        p = strtok(NULL, delim);

        temp_sample.y_ = 0.0;

        return temp_sample;
    }
    */

        static Sample parse_netflix(boost::string_ref line, int n_features)
    {

        Sample temp_sample = NetflixSample();

	std::vector<std::string> record;
	/*
        char *p;
        char *s =(char*)line.data();
        const char *delim = "\t";
	
        // 1:user
        p = strtok(s, delim);
        temp_sample.x_.push_back(std::make_pair(stoi(std::string("10000")+std::string(p)), stod(std::string("10000")+std::string(p))));
        // 2:item
        p = strtok(NULL, delim);
        temp_sample.x_.push_back(std::make_pair(stoi(std::string("20000")+std::string(p)),  stod(std::string("20000")+std::string(p))));
        // y:ranking
        p = strtok(NULL, delim);
        temp_sample.y_ = stoi(std::string(p));
        */
        boost::char_separator<char> sep("\t");
	boost::tokenizer<boost::char_separator<char>> tok(line, sep);
	boost::tokenizer<boost::char_separator<char>>::iterator beg = tok.begin();
	temp_sample.x_.push_back(std::make_pair(stoi(std::string("1000")+std::string(*beg)), stod(std::string("1000")+std::string(*beg))));
        beg++;
	temp_sample.x_.push_back(std::make_pair(stoi(std::string(*beg)), stod(std::string(*beg))));
        beg++;
	temp_sample.y_ = stoi(std::string(*beg));
        return temp_sample;
    }

};  // class Parser

}  // namespace lib
}  // namespace csci5570
