import sqlparse
import pyarrow.parquet as pq
def approx_query(query):
    # find the correct sample
    


    # execute the query on the sample
    # return the result with error bars



def main():
    query = "SELECT SUM(l_tax) FROM lineitem WHERE RETURNFLAG = 'R' and l_extendedprice < 1000"
    print(approx_query(query))