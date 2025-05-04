# Enhanced MapReduce Implementation

This project is a custom implementation of Google's MapReduce architecture, combining Python for orchestration with C++ for performance-critical components.

## Overview

MapReduce is a programming model designed for processing and generating large datasets on distributed systems. This implementation features:

- **Python core** for workflow orchestration, task distribution and fault tolerance
- **C++ performance modules** for data parsing, key-value pair generation and sorting
- **Multiprocessing** for parallel task execution and efficient resource utilization
- **Fault-tolerance mechanism** to handle process failures gracefully

## Architecture

The system consists of three main components:

1. **Master**: Orchestrates the entire MapReduce job, manages workers and handles fault tolerance
2. **Mappers**: Process input data and emit intermediate key-value pairs
3. **Reducers**: Process intermediate data and generate final output

## Performance-Critical C++ Components

Performance-sensitive operations are implemented in C++ and integrated with Python via pybind11:

- **Data parsing**: Fast parsing of input data in mappers
- **Key-value pair generation**: Efficient emission and collection of intermediate data
- **Sorting**: High-performance sorting of keys before reduction
- **JSON processing**: Optimized reading and writing of intermediate and output data

## Features

- **Fault tolerance**: Automatically detects and restarts failed mapper processes
- **Dynamic scaling**: Configurable number of mappers and reducers
- **Hybrid implementation**: Combines Python's ease of use with C++'s performance
- **Graceful fallback**: Uses Python implementation if C++ modules are unavailable

## Requirements

- Python 3.6+
- C++ compiler with C++17 support
- pybind11
- nlohmann-json C++ library

## Installation

```bash
# Install Python dependencies
pip install -r requirements.txt

# Build C++ components
python setup.py build_ext --inplace

# Alternative: Use CMake
mkdir build && cd build
cmake ..
make
```

## Usage

1. Create a configuration file (JSON format):

```json
{
  "input_file": "path/to/input.txt",
  "number_of_mapper": 4,
  "number_of_reducer": 2,
  "use_cpp": true
}
```

2. Define your map and reduce functions:

```python
def my_map_function(key, value, emit):
    # Process input and emit intermediate key-value pairs
    for word in value.split():
        emit(word, 1)

def my_reduce_function(key, values, emit):
    # Process intermediate data and emit final output
    emit(key, sum(int(v) for v in values))
```

3. Run the MapReduce job:

```python
from main import initialize_master

initialize_master(
    num_mappers=4,
    num_reducers=2,
    input_file="input.txt",
    user_defined_map=my_map_function,
    user_defined_reduce=my_reduce_function,
    kill_idx=-1  # Set to mapper index to test fault tolerance
)
```

## Project Structure

```
mapreduce/
│
├── src/
│   ├── python/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── map.py
│   │   └── reduce.py
│   │
│   └── cpp/
│       ├── mapper.cpp
│       ├── reducer.cpp
│       └── json_utils.hpp
|
├── setup.py
├── CMakeLists.txt
├── requirements.txt
└── README.md
```

## Performance Improvements

The C++ components provide significant performance improvements over the pure Python implementation:

- Faster data parsing and processing 
- More efficient memory usage
- Better sorting performance for large datasets
- Improved handling of large files

## Example Application: Word Count

A classic MapReduce example is counting word occurrences in a text corpus:

```python
def map_word_count(_, line, emit):
    for word in line.split():
        emit(word.lower(), 1)

def reduce_word_count(word, counts, emit):
    emit(word, str(sum(int(count) for count in counts)))
```

## Technical Highlights

- **Object-oriented design** with clear separation of concerns
- **Cross-language integration** using pybind11
- **Parallel processing** with Python's multiprocessing
- **Performance optimization** through C++ implementations
- **Fault tolerance** with process monitoring and recovery

## Future Enhancements

- Distributed execution across multiple machines
- Improved shuffling mechanism for better load balancing
- Additional built-in map and reduce functions
- Support for more complex data types and operations