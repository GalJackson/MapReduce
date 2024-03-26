#!/usr/bin/env python
# coding: utf-8

import multiprocessing as mp
import csv

# If using different file, please change:
input_file_path = r'./AComp_Passenger_data_no_error.csv'

class MapReduce:
    def __init__(self, file_path):
        self.file_path = file_path

    def map(self, line):
        # return a count of 1 as a value for each passenger in the CSV
        try:
            return (line[0], 1)
        except ValueError:
            return


    def shuffle(self, mapper_out): 
        """ Organise the mapped values by key """ 
        data = {} 
        for k, v in filter(None, mapper_out): 
            if k not in data: 
                data[k] = [v] 
            else: 
                data[k].append(v) 
        return data


    def reduce(self, kv):
        k, v = kv
        return k, sum(v)
    

    def run(self):
        map_in = []

        # open the CSV file
        with open(input_file_path, 'r', encoding='utf-8') as file:
            map_in = list(csv.reader(file, delimiter=','))

            # parallelise the MapReduce to the cpu cores available on current machine
            with mp.Pool(processes=mp.cpu_count()) as pool:
                map_out = pool.map(self.map, map_in)
                reduce_in = self.shuffle(map_out)
                reduce_out = pool.map(self.reduce, reduce_in.items())

        frequent_flyer = reduce_out[0]
        print(f"The passenger with the most flights is passenger {frequent_flyer[0]} with a total of {frequent_flyer[1]} flights.")


if __name__ == '__main__':
   map_reduce = MapReduce(input_file_path)
   map_reduce.run()