import json
import time
import os
from collections import defaultdict

class Mapper:
    """
    Mapper class that processes input data and emits intermediate key-value pairs.
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
        with open(self.input_path, 'r') as reader:
            self.input_data = reader.readlines()

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
        self.map_data = defaultdict(lambda: defaultdict(list))
        status_queue.put(['I', time.time()])

        for idx, line in enumerate(self.input_data):
            self.map_function(idx, line.rstrip('\n'), self.emit_intermediate)
            # Optional: Update status if needed

        self.reducer_ids.sort()
        active_reducers_queue.put(self.reducer_ids)
        status_queue.put(['D', time.time()])
        self.write_data()
