#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

/**
 * CppMapper class handles performance-critical mapping operations
 * by processing input data and generating key-value pairs efficiently.
 */
class CppMapper {
private:
    int mapper_id;
    int num_reducers;
    std::unordered_map<int, std::unordered_map<std::string, std::vector<std::string>>> map_data;

public:
    /**
     * Constructor initializes the mapper with its ID and number of reducers
     */
    CppMapper(int id, int reducers) : mapper_id(id), num_reducers(reducers) {}

    /**
     * Process a line from the input file using a user-defined mapping function
     * 
     * @param line_idx Index of the line in the input file
     * @param line Content of the line
     * @param map_function Python function to call for mapping
     */
    void process_line(int line_idx, const std::string& line, py::function map_function) {
        // Call the Python mapping function with our C++ emit function
        map_function(line_idx, line, py::cpp_function([this](std::string key, std::string value) {
            emit_intermediate(key, value);
        }));
    }

    /**
     * Emit an intermediate key-value pair
     */
    void emit_intermediate(const std::string& key, const std::string& value) {
        // Calculate reducer ID using hash of key
        int reducer_id = std::hash<std::string>{}(key) % num_reducers;
        map_data[reducer_id][key].push_back(value);
    }

    /**
     * Write the intermediate data to output files
     */
    std::vector<int> write_data(const std::string& output_path) {
        std::vector<int> reducer_ids;
        
        // Create output directory if it doesn't exist
        std::string mkdir_cmd = "mkdir -p " + output_path;
        system(mkdir_cmd.c_str());
        
        for (const auto& reducer_pair : map_data) {
            int reducer_id = reducer_pair.first;
            reducer_ids.push_back(reducer_id);
            
            std::string out_file = output_path + "/m" + std::to_string(mapper_id) + 
                                  "r" + std::to_string(reducer_id) + ".txt";
            
            std::ofstream outfile(out_file);
            if (!outfile.is_open()) {
                throw std::runtime_error("Failed to open output file: " + out_file);
            }
            
            // Write JSON-formatted data
            outfile << "{";
            bool first_key = true;
            for (const auto& kv_pair : reducer_pair.second) {
                if (!first_key) outfile << ",";
                first_key = false;
                
                outfile << "\"" << kv_pair.first << "\":[";
                bool first_value = true;
                for (const auto& value : kv_pair.second) {
                    if (!first_value) outfile << ",";
                    first_value = false;
                    outfile << "\"" << value << "\"";
                }
                outfile << "]";
            }
            outfile << "}";
            outfile.close();
        }
        
        // Sort reducer IDs
        std::sort(reducer_ids.begin(), reducer_ids.end());
        return reducer_ids;
    }
};

// Pybind11 module definition
PYBIND11_MODULE(cpp_mapper, m) {
    py::class_<CppMapper>(m, "CppMapper")
        .def(py::init<int, int>())
        .def("process_line", &CppMapper::process_line)
        .def("write_data", &CppMapper::write_data);
}
