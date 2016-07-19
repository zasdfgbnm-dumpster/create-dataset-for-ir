from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

path = '/ufrc/roitberg/qasdfgtyuiop/05_create_struct_universe/outputs'
gdb = '/ufrc/roitberg/qasdfgtyuiop/gdb13'

sc = SparkContext(appName="05_create_struct_universe")
sqlContext = SQLContext(sc)

# get list of smiles
smiles_nist = sqlContext.read.parquet(path+'/04/mid_structure').rdd.map(lambda r:r.structure).distinct()
smiles_gdb = [ sc.textFile(gdb+'/{}.smi'.format(i)) for i in range(1,14) ]
smiles = sc.union(smiles_gdb+[smiles_nist]).distinct()
print('number of structures:',smiles.count())

# apply transformations to smiles to generate parquet
mass = smiles.pipe(path+'/../tools/calc-mass.py')
rows = smiles.zip(mass).map( lambda sm: Row(structure=sm[0],mass=sm[1]) )

df = sqlContext.createDataFrame(rows)
df.show()
df.write.parquet(path+'/05/universe')
