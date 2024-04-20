'''
When we generate samples, we need to keep track of what we have so we know what we can use
We log the samples in a | separated value file, with the following columns:
table_name|sample_file_name|[col1,col2,...]|[range/value1, ...]|[output_col1, ...]
Then we load this file and use it to find samples usable for a given query

'''
def sample_finder(filename, where_cols, output_cols):
    with open(filename, 'r') as f:
        lines = f.readlines()
    samples = {}
    for line in lines:
        table_name, sample_file, cols, ranges, outputs = line.strip().split('|')
        cols = cols[1:-1].split(',')
        ranges = ranges[1:-1].split(',')
        outputs = outputs[1:-1].split(',')
        samples[table_name] = (sample_file, cols, ranges, outputs)
    