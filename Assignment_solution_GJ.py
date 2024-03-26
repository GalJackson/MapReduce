#!/usr/bin/env python
# coding: utf-8

import multiprocessing as mp
import csv

input_file_path = r'./AComp_Passenger_data_no_error.csv'

def map(r):
    try:
        return (r[0], 1)
    except ValueError:
        return


def shuffle(mapper_out): 
    """ Organise the mapped values by key """ 
    data = {} 
    for k, v in filter(None, mapper_out): 
        if k not in data: 
            data[k] = [v] 
        else: 
            data[k].append(v) 
    return data


def reduce(kv):
    k, v = kv
    return k, sum(v)


if __name__ == '__main__':
    map_in = []

    # open the CSV file
    with open(input_file_path, 'r', encoding='utf-8') as file:
        map_in = list(csv.reader(file, delimiter=','))

        with mp.Pool(processes=mp.cpu_count()) as pool:
            map_out = pool.map(map, map_in)
            print(map_out)
            reduce_in = shuffle(map_out)
            print(reduce_in)
            reduce_out = pool.map(reduce, reduce_in.items())
    
    frequent_flyer = reduce_out[0]
    print(f"The passenger with the most flights is passenger {frequent_flyer[0]} with a total of {frequent_flyer[1]} flights.")
