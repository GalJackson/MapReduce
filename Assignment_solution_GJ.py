#!/usr/bin/env python
# coding: utf-8

import multiprocessing as mp
import matplotlib.pyplot as plt
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

        # sort the data in descending order
        sorted_data = sorted(reduce_out, key=lambda x: x[1], reverse=True)

        frequent_flyer = sorted_data[0]
        print(f"The passenger with the most flights is passenger {frequent_flyer[0]} with a total of {frequent_flyer[1]}.")

        return sorted_data


class MapReduceDuration(MapReduce):
    def map(self, line):
        # override parent - return the flight duration as a value for each passenger in the CSV
        try:
            return (line[0], int(line[5]))
        except ValueError:
            return


if __name__ == '__main__':
    map_reduce = MapReduce(input_file_path)
    reduced_data = map_reduce.run()

    # pick out the top 3 most frequent flyers
    top_ids = [i[0] for i in reduced_data[:3]]
    counts = [i[1] for i in reduced_data[:3]]

    # plot top 3 frequent flyers on bar chart
    plt.figure()
    plt.bar(top_ids, counts)
    plt.title(f"Top three passengers. Most flights: {top_ids[0]} ({counts[0]} flights)")
    plt.show()

    map_reduce_duration = MapReduceDuration(input_file_path)
    reduced_data_duration = map_reduce_duration.run()

    # pick out the top 3 most frequent flyers
    top_ids = [i[0] for i in reduced_data_duration[:3]]
    counts = [i[1] for i in reduced_data_duration[:3]]

    # plot top 3 frequent flyers on bar chart
    plt.figure()
    plt.bar(top_ids, counts)
    plt.title(f"Top three passengers. Most airtime: {top_ids[0]} ({counts[0]} minutes)")
    plt.show()