# use duckdb to calculate the variance of the l_tax column for the query:
# SELECT l_tax FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000
import duckdb
import sqlparse
from sqlparse.sql import Where, Comparison
import sample_finder as sf
import parse
import pyarrow.dataset as ds
con = duckdb.connect()
# variance query
query = "SELECT l_quantity, AVG(l_tax), VAR_SAMP(l_tax) AS variance FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 10000 GROUP BY l_quantity"


lineitem = ds.dataset('db_parq/lineitem.parquet', format='parquet')
result = con.execute(query).arrow()
print(result)
    