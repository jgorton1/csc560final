import pandasql as ps
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import time
import duckdb
import pyarrow.dataset as ds
import sample_finder as sf
import parse

def slow_query():
    con = duckdb.connect()
    lineitem = ds.dataset('db_parq/lineitem.parquet', format='parquet')
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000"
    result = con.execute(query).arrow()
    print(result)



    '''
    lineitem = pq.read_table('db_parq/lineitem.parquet',columns=['l_tax','l_extendedprice','l_returnflag'])
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 20000"
    
    lineitem = lineitem.to_pandas()
    lineitem['l_tax'] = lineitem['l_tax'].astype(float)
    lineitem['l_extendedprice'] = lineitem['l_extendedprice'].astype(float)
    #print(lineitem.head())
    print(ps.sqldf(query, locals()))'''
def fast_query():
    con = duckdb.connect()
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 20000 "
    where_cols = parse.get_where_columns(query)
    output_cols = parse.get_output_columns(query)
    where_predicates = parse.get_where_predicates(query)
    query = parse.add_count_star(query)

    names, metadatas = sf.sample_finder("partitions.pkl", where_cols, output_cols, where_predicates)
    results = [None for i in range(len(names))]
    for i, name in enumerate(names):
        lineitem = ds.dataset('samples/' + name + '.parquet', format='parquet')
        #lineitem = ds.('samples/' + name + '.parquet')
        print(metadatas[i])
        results[i] = con.execute(query).arrow()
        print(results[i])

    weighted_avg = 0
    total_count = 0
    for i, result in enumerate(results):
        # get the weighted average
        avg = result["avg(l_tax)"][0].as_py()#.take([0]).to_pandas().iloc[0]
        count = result["count_star()"][0].as_py()#.take([0]).to_pandas().iloc[0]
        print(avg, count)
        weighted_avg += avg * count
        total_count += count
    print(weighted_avg / total_count)
    #lineitem = ds.dataset("samples/A_[Decimal('901.00'), Decimal('11305.85')].parquet", format='parquet')

    '''
    lineitem = pq.read_table('samples/A_[Decimal(\'901.00\'), Decimal(\'11305.85\')].parquet',columns=['l_tax','l_extendedprice','l_returnflag'])
    query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000"
    lineitem = lineitem.to_pandas()
    lineitem['l_tax'] = lineitem['l_tax'].astype(float)
    lineitem['l_extendedprice'] = lineitem['l_extendedprice'].astype(float)
    #print(lineitem)
    print(ps.sqldf(query, locals()))'''

def main():
    start_time = time.time()
    fast_query()
    end_time = time.time()
    print("Execution time: ", end_time - start_time, "seconds")


if __name__ == "__main__":
    main()