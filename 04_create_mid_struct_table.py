from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

path = '/home/gaoxiang/create-dataset-for-ir/outputs/02'

sc = SparkContext(appName="03_create_expir_table")
sqlContext = SQLContext(sc)

# process structures
lsalts  = sc.textFile(path + '/all.salts.smi')
lunique = sc.textFile(path + '/all.unique.smi')
lmix    = sc.textFile(path + '/all.mixtures.smi')

def line2row(line):
    l = line.split()
    return Row(mid=l[1],structure=l[0])

rows = sc.union([lsalts,lunique,lmix]).map(line2row)
df = sqlContext.createDataFrame(rows)

# process duplicates
ldup = sc.textFile(path + '/duplicates.log')

def dup_line2row(line):
    l = line.split()
    return Row(mid=l[1],dupof=l[6])

duprows = ldup.map(dup_line2row)
dupdf = sqlContext.createDataFrame(duprows)

# merge duplicates and normal
dup_mid_struct=dupdf.join(df,dupdf['dupof']==df['mid']).select(dupdf['mid'],df['structure'])
total_mid_struct = df.unionAll(dup_mid_struct)

# write to file
total_mid_struct.write.parquet('outputs/04/mid_structure')
total_mid_struct.show()
print('done')
