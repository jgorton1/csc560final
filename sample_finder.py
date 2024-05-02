'''
When we generate samples, we need to keep track of what we have so we know what we can use
We log the samples in pickle file of partitions

'''
import pickle
from Partition import *
def sample_finder(filename, where_cols, output_cols, where_predicates):
    # read the pickle file
    part_collection = PartitionCollection.deserialize(filename)
    for part in part_collection.partitions:
        print(part.parts)
    # find the partitions that can be used for the query
    # if the partition has the where columns and the output columns, then it can be used
    # what if the where columns/predicates are not in the same order as the partition columns?
    where_dict = dict(zip(where_cols, where_predicates))
    print(where_dict)
    sample_file_names = []
    metadatas = []
    for part in part_collection.partitions:
        if set_equal(part.column_set, where_cols) and set_equal(part.output_set, output_cols):
            for i, val in enumerate(part.parts):
                if type(val) == str:
                    if val != where_dict[part.column_set[i]]:
                        break
                elif type(val) == list:
                    if val[0] > where_dict[part.column_set[i]][1] or val[1] <= where_dict[part.column_set[i]][0]:
                        break
            else:
                # if the loop completes without breaking, then the partition satisfies the where predicate
                sample_file_names.append(part.name())
                metadatas.append([part.population_count, part.sample_count])

    return sample_file_names, metadatas # array of sample file names that can be used for the query

def set_equal(list1, list2):
    return set(list1) == set(list2)
def main():
    filename = "partitions.pkl"
    '''where_cols = ['l_returnflag','l_extendedprice']
    where_cols.reverse()
    output_cols = ['l_tax']

    where_predicates = ['R', [0,20000]] # TODO add support for multiple predicates
    where_predicates.reverse()'''
    where_cols = ['l_returnflag','l_shipmode']
    where_predicates = ['R', 'AIR']
    output_cols = ['l_extendedprice']
    sample_file_names = sample_finder(filename, where_cols, output_cols, where_predicates)
    print(sample_file_names)


if __name__ == "__main__":
    main()
    