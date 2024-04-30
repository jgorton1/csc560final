# a data structure to store statistics of a partition
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os
import pickle
class Partition:
    def __init__(self, parts, column_set, output_set, data, shell=False):
        # parts is a list of values that define the partition - e.g. ["R", [0,100]]
        # column_set is a list of column names
        # data is a pyarrow table
        self.parts = parts
        self.column_set = column_set
        self.output_set = output_set
        if shell:
            self.data = data
            return
        if data.shape[1] != len(column_set) + len(output_set):
            raise ValueError("Data does not have the correct number of columns")
            #data = data.select(column_set + output_set)
        print(parts)
        for i, part in enumerate(parts):
            print(type(part),part)
            if type(part) == str:  # Fix: Replace "string" with "str"
                print("filter on str")
                data = data[data[column_set[i]] == part]
            elif type(part) == list:
                data = data[(data[column_set[i]] >= part[0])][(data[column_set[i]] < part[1])]
        # get indexes of union of column_set and output_set
        self.data = data
    def sample(self, error_rate):
        # error_rate is the maximum error rate as a percentage of the sample mean of the output_set
        #print(self.output_set[0])
        data = self.data[self.output_set[0]].astype(float)
        #print(data.head())
        error = error_rate * data.mean()
        # for simplicity, 95% confidence interval (assuming normal distribution)
        S = data.std()
        #print(S)
        N = data.shape[0]
        n_0 = 1.96**2 * S**2 / error**2
        n = n_0 / (1 + n_0 / N)
        sample_size =  min(int(n), N)
        print(sample_size)
        # sample the data
        return self.data.sample(sample_size)
    def name(self):
        # return a string that represents the partition
        return "_".join([str(part) for part in self.parts])
    def row_count(self):
        return self.data.shape[0]

class PartitionCollection:
    def __init__(self):
        self.partitions = []
    
    def add_partition(self, partition):
        # make a copy and remove the data so we don't store it in the pickle file
        partition = Partition(partition.parts, partition.column_set, partition.output_set, pd.DataFrame(), shell=True)
        self.partitions.append(partition)
    
    def serialize(self, file_path):
        # if the file already exists, append to it
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                partitions = pickle.load(file)
            partitions.extend(self.partitions)
        with open(file_path, 'wb') as file:
            pickle.dump(self.partitions, file)
    
    @classmethod
    def deserialize(cls, file_path):
        with open(file_path, 'rb') as file:
            partitions = pickle.load(file)
        partition_collection = PartitionCollection()
        partition_collection.partitions = partitions
        return partition_collection