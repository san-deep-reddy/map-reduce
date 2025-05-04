#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <nlohmann/json.hpp>

namespace py = pybind11;
namespace fs = std::filesystem;
using json = nlohmann::json;

/**
 * CppReducer class handles performance-critical reducing operations
 * by efficiently processing intermediate data and generating final outputs.
 */
class CppReducer {
private:
    int reducer_id;
    int num_mappers;
    std::string intermediate_dir;
    std::unordered_map<std::string, std::vector<std::string>> final_dict;
    std::unordered_map<std::string, std::string> reduced_data;

public:
    /**
     * Constructor initializes the reducer with its configuration
     */
    CppReducer(const std::string& interm_dir, int id, int mappers) 
        : intermediate_dir(interm_dir), reducer_id(id), num_mappers(mappers) {
        load_intermediate_data();
    }

    /**
     * Load intermediate data from all mapper outputs for this reducer
     */
    void load_intermediate_data() {
        for (int mapper_id = 0; mapper_id < num_mappers; mapper_id++) {
            std::string file_path = intermediate_dir + "/m" + std::to_string(mapper_id) + 
                                   "r" + std::to_string(reducer_id) + ".txt";
            
            // Check if file exists
            if (fs::exists(file_path)) {
                std::ifstream file(file_path);
                if (!file.is_open()) {
                    throw std::runtime_error("Failed to open intermediate file: " + file_path);
                }
                
                try {
                    json data;
                    file >> data;
                    
                    // Merge the data into our final dictionary
                    for (auto& [key, values] : data.items()) {
                        for (const auto& value : values) {
                            final_dict[key].push_back(value);
                        }
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing JSON from " << file_path << ": " << e.what() << std::endl;
                }
                
                file.close();
            }
        }
    }

    /**
     * Run the reduce function on all key-value pairs
     */
    void reduce_all(py::function reduce_function) {
        for (const auto& [key, values] : final_dict) {
            reduce_function(key, values, py::cpp_function([this](std::string key, std::string value) {
                emit_final(key, value);
            }));
        }
    }

    /**
     * Emit a final key-value pair
     */
    void emit_final(const std::string& key, const std::string& value) {
        reduced_data[key] = value;
    }

    /**
     * Write the reduced data to the final output file
     */
    void write_data(const std::string& output_dir) {
        // Create output directory if it doesn't exist
        std::string mkdir_cmd = "mkdir -p " + output_dir;
        system(mkdir_cmd.c_str());
        
        std::string out_file = output_dir + "/" + std::to_string(reducer_id) + ".txt";
        std::ofstream outfile(out_file);
        if (!outfile.is_open()) {
            throw std::runtime_error("Failed to open output file: " + out_file);
        }
        
        // Write JSON-formatted data
        json output_json;
        for (const auto& [key, value] : reduced_data) {
            output_json[key] = value;
        }
        
        outfile << output_json.dump();
        outfile.close();
    }
    
    /**
     * Get the final dictionary (for testing)
     */
    std::unordered_map<std::string, std::vector<std::string>> get_final_dict() const {
        return final_dict;
    }
};

// Pybind11 module definition
PYBIND11_MODULE(cpp_reducer, m) {
    py::class_<CppReducer>(m, "CppReducer")
        .def(py::init<const std::string&, int, int>())
        .def("reduce_all", &CppReducer::reduce_all)
        .def("write_data", &CppReducer::write_data)
        .def("get_final_dict", &CppReducer::get_final_dict);
}
