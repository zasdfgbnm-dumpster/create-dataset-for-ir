#!/usr/bin/env pyspark
from os import listdir
from tools.jdx2vec import jdx2vec
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

sc = SparkContext(appName="03_create_expir_table")

# read junk list
fnjunk = '/home/gaoxiang/create-dataset-for-ir/outputs/02/all.junk.smi'
junklist = [ i.split()[1] for i in open(fnjunk) ]

# process jdx files
inputdir = '/home/gaoxiang/create-dataset-for-ir/outputs/01/digitalized/'

def process(i):
    # get mid and index
    ii = i.split('.')[0].split('-')
    mid = ii[0]
    index = int(ii[1])
    # skip junk
    if mid in junklist:
        return None
    vec,state,state_info = jdx2vec(inputdir+i)
    # skip non-standardizable data
    if vec is None:
        return None
    row = Row(mid=mid,index=index,vec=vec,state=state,state_info=state_info)
    return row

filelist = sc.parallelize(listdir(inputdir),200)
rows = filelist.map(process).filter(lambda i: i is not None)

# generate table
sqlContext = SQLContext(sc)
expir = sqlContext.createDataFrame(rows)
expir.show()
expir.write.parquet('outputs/03/expir')
print('done')
