import os
import sys
import json
import shutil
import multiprocessing as mp
import time
from pathlib import Path
from map import Mapper
from reduce import Reducer

def read_configs(file_path):
    """
    Reads configuration parameters from a JSON file.

    Args:
        file_path (str): Path to the configuration file.

    Returns:
        tuple: Contains input file name, number of mappers, and number of reducers.
    """
    try:
        with open(file_path, 'r') as conf_file:
            configs = json.load(conf_file)
            input_file = configs.get("input_file")
            num_mappers = int(configs.get("number_of_mapper"))
            num_reducers = int(configs.get("number_of_reducer"))
    except Exception as e:
        sys.exit(f'Fatal error: Unable to read the requested file. {e}')

    return input_file, num_mappers, num_reducers

def initialize_master(num_mappers, num_reducers, input_file, user_defined_map, user_defined_reduce, kill_idx):
    """
    Initializes the Master process and starts the MapReduce job.

    Args:
        num_mappers (int): Number of mapper processes.
        num_reducers (int): Number of reducer processes.
        input_file (str): Path to the input data file.
        user_defined_map (function): User-defined map function.
        user_defined_reduce (function): User-defined reduce function.
        kill_idx (int): Index of the mapper to simulate failure (for fault tolerance).
    """
    master_instance = Master(num_mappers, num_reducers, input_file, user_defined_map, user_defined_reduce, kill_idx)
    master_instance.start_process()

class Master:
    """
    Master class orchestrates the MapReduce job by managing mappers and reducers.
    """

    def __init__(self, num_mappers, num_reducers, input_file, user_defined_map, user_defined_reduce, kill_idx):
        """
        Initializes the Master with the necessary configuration.

        Args:
            num_mappers (int): Number of mappers.
            num_reducers (int): Number of reducers.
            input_file (str): Path to the input file.
            user_defined_map (function): User-defined map function.
            user_defined_reduce (function): User-defined reduce function.
            kill_idx (int): Index of the mapper to simulate failure.
        """
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.input_file = os.path.abspath(input_file)
        self.user_defined_map = user_defined_map
        self.user_defined_reduce = user_defined_reduce
        self.timeout = 3
        self.kill_idx = kill_idx

        # Generate a unique job ID
        self.job_id = f'{int(time.time())}'
        self.TMP_DIR = f'./tmp/{self.job_id}'
        self.OUT_DIR = f'./output/{self.job_id}'

        # Split input data for mappers
        self.split_input_data()

    def split_input_data(self):
        """
        Splits the input data among the mappers.
        """
        split_dir = f"{self.TMP_DIR}/input"
        os.makedirs(split_dir, exist_ok=True)

        with open(self.input_file, 'r') as reader:
            for idx, line in enumerate(reader):
                if not line.endswith('\n'):
                    line += '\n'
                mapper_id = idx % self.num_mappers
                mapper_file = os.path.join(split_dir, f'{mapper_id}.txt')
                with open(mapper_file, 'a') as writer:
                    writer.write(line)
        self.input_files = [os.path.join(split_dir, f'{i}.txt') for i in range(self.num_mappers)]

    def retry_mapper(self, idx):
        """
        Restarts a mapper process in case of failure.

        Args:
            idx (int): Index of the mapper to restart.
        """
        print(f"Mapper {idx} has crashed, restarting...")
        self.processes[idx].terminate()
        self.status_queues[idx] = mp.Queue()
        self.reducer_queues[idx] = mp.Queue()
        self.processes[idx] = mp.Process(target=self.mappers[idx].start_mapper, args=(self.reducer_queues[idx], self.status_queues[idx]))
        self.processes[idx].start()

    def start_process(self):
        """
        Starts mapper and reducer processes and monitors their execution.
        """
        print("Starting Mappers...")
        self.mappers = []
        self.processes = []
        self.reducer_queues = []
        self.status_queues = []
        self.mapper_status = [True] * self.num_mappers
        self.active_reducers = []

        # Initialize and start mapper processes
        for idx, input_file in enumerate(self.input_files):
            mapper = Mapper(input_file, f'{self.TMP_DIR}/intermediate', self.user_defined_map, idx, self.num_reducers)
            self.mappers.append(mapper)
            status_queue = mp.Queue()
            reducer_queue = mp.Queue()
            process = mp.Process(target=mapper.start_mapper, args=(reducer_queue, status_queue))
            self.status_queues.append(status_queue)
            self.reducer_queues.append(reducer_queue)
            self.processes.append(process)
            process.start()

            # Simulate failure for fault tolerance testing
            if self.kill_idx == idx:
                print(f"Simulating failure of Mapper {idx}")
                process.terminate()

        # Monitor mapper processes
        self.monitor_mappers()

        print("Starting Reducers...")
        # Initialize and start reducer processes
        self.reducers = []
        self.reducer_processes = []
        self.reducer_status_queues = []
        self.reducer_status = [True] * self.num_reducers

        for idx in range(self.num_reducers):
            reducer = Reducer(f'{self.TMP_DIR}/intermediate', self.OUT_DIR, self.user_defined_reduce, idx, self.num_mappers)
            self.reducers.append(reducer)
            status_queue = mp.Queue()
            process = mp.Process(target=reducer.start_reducer, args=(status_queue,))
            self.reducer_status_queues.append(status_queue)
            self.reducer_processes.append(process)
            process.start()

        # Monitor reducer processes
        self.monitor_reducers()

        print(f"MapReduce job completed. Output is available at '{self.OUT_DIR}'")
        # Clean up temporary files
        shutil.rmtree(self.TMP_DIR)

    def monitor_mappers(self):
        """
        Monitors mapper processes for completion or failure.
        """
        while any(self.mapper_status):
            for idx, status in enumerate(self.mapper_status):
                if status:
                    try:
                        curr_status, _ = self.status_queues[idx].get(timeout=self.timeout)
                        if curr_status == 'D':
                            self.mapper_status[idx] = False
                            self.active_reducers += self.reducer_queues[idx].get()
                            self.processes[idx].join()
                    except Exception:
                        self.retry_mapper(idx)

    def monitor_reducers(self):
        """
        Monitors reducer processes for completion.
        """
        while any(self.reducer_status):
            for idx, status in enumerate(self.reducer_status):
                if status:
                    try:
                        curr_status, _ = self.reducer_status_queues[idx].get(timeout=self.timeout)
                        if curr_status == 'D':
                            self.reducer_status[idx] = False
                            self.reducer_processes[idx].join()
                    except Exception:
                        pass  # In a real-world scenario, implement retry logic here

