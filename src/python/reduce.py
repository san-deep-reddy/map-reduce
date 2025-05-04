import json
import os
import time

# Try to import C++ module
try:
    import cpp_reducer
    USE_CPP = True
    print("Using C++ reducer implementation for performance boost")
except ImportError:
    USE_CPP = False
    print("C++ reducer module not found, using Python implementation")

class Reducer:
    """
    Reducer class that processes intermediate key-value pairs and produces final output.
    Can use either Python or C++ implementation depending on availability.
    """

    def __init__(self, intermediate_dir, output_dir, reduce_function, reducer_id, num_mappers):
        """
        Initializes the Reducer.

        Args:
            intermediate_dir (str): Directory containing intermediate files.
            output_dir (str): Directory to write final output.
            reduce_function (function): User-defined reduce function.
            reducer_id (int): Unique identifier for the reducer.
            num_mappers (int): Total number of mappers.
        """
        self.intermediate_dir = intermediate_dir
        self.output_dir = output_dir
        self.reduce_function = reduce_function
        self.id = reducer_id
        self.num_mappers = num_mappers
        
        if USE_CPP:
            # Initialize C++ reducer
            self.cpp_reducer = cpp_reducer.CppReducer(intermediate_dir, reducer_id, num_mappers)
        else:
            # Load intermediate data using Python implementation
            self.final_dict = {}
            self.load_intermediate_data()

    def load_intermediate_data(self):
        """
        Loads intermediate data from all mappers for this reducer.
        """
        if USE_CPP:
            # C++ implementation handles loading
            return
            
        for mapper_id in range(self.num_mappers):
            file_path = os.path.join(self.intermediate_dir, f'm{mapper_id}r{self.id}.txt')
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    for key, values in data.items():
                        self.final_dict.setdefault(key, []).extend(values)

    def emit_final(self, key, value):
        """
        Emits the final reduced key-value pair.

        Args:
            key: Final key.
            value: Final value.
        """
        self.reduced_data[key] = value

    def write_data(self):
        """
        Writes the reduced data to the final output file.
        """
        if USE_CPP:
            # C++ implementation handles writing
            return
            
        os.makedirs(self.output_dir, exist_ok=True)
        out_file = os.path.join(self.output_dir, f'{self.id}.txt')
        with open(out_file, 'w') as outfile:
            json.dump(self.reduced_data, outfile)

    def start_reducer(self, status_queue):
        """
        Starts the reducer process.

        Args:
            status_queue (multiprocessing.Queue): Queue to communicate status updates.
        """
        status_queue.put(['I', time.time()])
        
        if USE_CPP:
            # Use C++ implementation for performance-critical reducing
            self.cpp_reducer.reduce_all(self.reduce_function)
            self.cpp_reducer.write_data(self.output_dir)
        else:
            # Use Python implementation
            self.reduced_data = {}
            for key, values in self.final_dict.items():
                self.reduce_function(key, values, self.emit_final)
            self.write_data()
            
        status_queue.put(['D', time.time()])
