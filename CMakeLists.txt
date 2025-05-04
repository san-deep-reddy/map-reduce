cmake_minimum_required(VERSION 3.10)
project(MapReduceCpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Python and pybind11
find_package(Python COMPONENTS Interpreter Development REQUIRED)
find_package(pybind11 REQUIRED)
find_package(nlohmann_json REQUIRED)

# Add the C++ components
pybind11_add_module(cpp_mapper mapper.cpp)
pybind11_add_module(cpp_reducer reducer.cpp)

# Link libraries
target_link_libraries(cpp_reducer PRIVATE nlohmann_json::nlohmann_json)
target_link_libraries(cpp_mapper PRIVATE nlohmann_json::nlohmann_json)

# Install targets
install(TARGETS cpp_mapper cpp_reducer
        LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})
