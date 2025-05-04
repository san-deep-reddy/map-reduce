import json
import time
import os
from collections import defaultdict
import importlib.util

# Try to import C++ module
try:
    import cpp_mapper
    USE_CPP = True
    print("Using C++ mapper implementation for performance boost")
except ImportError:
    USE_CPP = False
    print("C++ mapper module not found, using Python implementation")

class Mapper:
    """
    Mapper class that processes input data and emits intermediate key-value pairs.
    Can use either Python or C++ implementation depending on availability.
    """

    def __init__(self, input_path, output_path, map_function, mapper_id, num_reducers):
        """
        Initializes the Mapper.

        Args:
            input_path (str): Path to the mapper's input file.
            output_path (str): Path to the directory for intermediate outputs.
            map_function (function): User-defined map function.
            mapper_id (int): Unique identifier for the mapper.
            num_reducers (int): Total number of reducers.
        """
        self.id = mapper_id
        self.input_path = input_path
        self.output_path = output_path
        self.map_function = map_function
        self.num_reducers = num_reducers
        self.status = 'I'  # Status: 'I' for In-progress, 'D' for Done
        self.reducer_ids = []
        
        # Read input data
        with open(self.input_path, 'r') as reader:
            self.input_data = reader.readlines()
            
        # Initialize C++ mapper if available
        if USE_CPP:
            self.cpp_mapper = cpp_mapper.CppMapper(mapper_id, num_reducers)

    def emit_intermediate(self, key, value):
        """
        Emits an intermediate key-value pair.

        Args:
            key: Intermediate key.
            value: Intermediate value.
        """
        reducer_id = hash(key) % self.num_reducers
        self.map_data[reducer_id][key].append(value)

    def write_data(self):
        """
        Writes the intermediate data to output files for reducers.
        """
        if USE_CPP:
            # C++ implementation handles writing
            return
            
        os.makedirs(self.output_path, exist_ok=True)
        for reducer_id in self.map_data:
            out_file = os.path.join(self.output_path, f'm{self.id}r{reducer_id}.txt')
            self.reducer_ids.append(reducer_id)
            with open(out_file, 'w') as outfile:
                json.dump(self.map_data[reducer_id], outfile)

    def start_mapper(self, active_reducers_queue, status_queue):
        """
        Starts the mapper process.

        Args:
            active_reducers_queue (multiprocessing.Queue): Queue to communicate active reducers.
            status_queue (multiprocessing.Queue): Queue to communicate status updates.
        """
        status_queue.put(['I', time.time()])
        
        if USE_CPP:
            # Use C++ implementation for performance-critical mapping
            for idx, line in enumerate(self.input_data):
                self.cpp_mapper.process_line(idx, line.rstrip('\n'), self.map_function)
                
            # Get reducer ids and write data using C++ implementation
            self.reducer_ids = self.cpp_mapper.write_data(self.output_path)
        else:
            # Use Python implementation
            self.map_data = defaultdict(lambda: defaultdict(list))
            for idx, line in enumerate(self.input_data):
                self.map_function(idx, line.rstrip('\n'), self.emit_intermediate)
                
            # Get reducer ids and sort them
            for reducer_id in self.map_data:
                self.reducer_ids.append(reducer_id)
            self.reducer_ids.sort()
            self.write_data()
            
        active_reducers_queue.put(self.reducer_ids)
        status_queue.put(['D', time.time()])
