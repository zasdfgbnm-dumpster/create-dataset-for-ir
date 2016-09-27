# set up spark
import sys
from os import listdir
from os.path import join,exists
pyspark_home = '/opt/apache-spark/python/lib'
zipfiles = [join(pyspark_home, f) for f in listdir(pyspark_home) if f.endswith('.zip')]
sys.path += zipfiles
from pyspark import *
from pyspark.sql import *
spark = SparkSession.builder.getOrCreate()

path = '/home/gaoxiang/MEGA/tables'
tables = [ 'ExpIRAndState','MIDStruct','StructureUniverse','TheoreticalIR' ]
for i in tables:
	parquetpath = join(path,i)
	jsonpath = join(path,i+'.json')
	if exists(parquetpath) and not exists(jsonpath):
		spark.read.parquet(parquetpath).write.json(jsonpath)
