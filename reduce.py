import json
import os

class Reducer:
    """
    Reducer class that processes intermediate key-value pairs and produces final output.
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
        self.input_data = []

        # Load intermediate data from mappers
        self.load_intermediate_data()

    def load_intermediate_data(self):
        """
        Loads intermediate data from all mappers for this reducer.
        """
        self.final_dict = {}
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
        self.reduced_data = {}
        status_queue.put(['I', time.time()])

        for key, values in self.final_dict.items():
            self.reduce_function(key, values, self.emit_final)

        status_queue.put(['D', time.time()])
        self.write_data()
