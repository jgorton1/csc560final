import sqlparse
import pyarrow.parquet as pq
def approx_query(query):
    # find the correct sample
    files = sample_finder('samples.txt', get_where_columns(query), get_output_columns(query), query)


    # execute the query on the sample
    # return the result with error bars



def main():
    query = "SELECT SUM(l_tax) FROM lineitem WHERE l_returnflag= 'R' and l_extendedprice < 1000"
    print(approx_query(query))