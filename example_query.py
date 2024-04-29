import pandasql as ps
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import time

def slow_query():
    lineitem = pq.read_table('db_parq/lineitem.parquet',columns=['l_tax','l_extendedprice','l_returnflag'])
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000"
    
    lineitem = lineitem.to_pandas()
    lineitem['l_tax'] = lineitem['l_tax'].astype(float)
    lineitem['l_extendedprice'] = lineitem['l_extendedprice'].astype(float)
    #print(lineitem.head())
    print(ps.sqldf(query, locals()))
def fast_query():
    lineitem = pq.read_table('samples/A_[Decimal(\'901.00\'), Decimal(\'11305.85\')].parquet',columns=['l_tax','l_extendedprice','l_returnflag'])
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000"
    lineitem = lineitem.to_pandas()
    lineitem['l_tax'] = lineitem['l_tax'].astype(float)
    lineitem['l_extendedprice'] = lineitem['l_extendedprice'].astype(float)
    #print(lineitem)
    print(ps.sqldf(query, locals()))

def main():
    start_time = time.time()
    slow_query()
    end_time = time.time()
    print("Execution time: ", end_time - start_time, "seconds")


if __name__ == "__main__":
    main()