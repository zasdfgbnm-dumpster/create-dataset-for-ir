from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from rdkit.Chem import *

path = '/ufrc/roitberg/qasdfgtyuiop/05_create_struct_universe/outputs'
gdb = '/ufrc/roitberg/qasdfgtyuiop/gdb13'

sc = SparkContext(appName="03_create_expir_table")
sqlContext = SQLContext(sc)

# get list of smiles
smiles_nist = sqlContext.read.parquet(path+'/04/mid_structure').select('structure').distinct()
smiles_gdb = [ sc.textFile(gdb+'/{}.smi'.format(i)) for i in range(1,14) ]
smiles = sc.union(smiles_gdb+[smiles_nist]).distinct()

# apply transformations to smiles to generate parquet
def mass(smiles):
    m = MolFromSmiles(smiles)
    m = AddHs(m)
    return Descriptors.MolWt(m)

rows = smiles.map( lambda s: Row(structure=s,mass=mass(s)) )
print('total number of structures:',rows.count())
df = sqlContext.createDataFrame(rows)
df.show()
print('total number of structures:',rows.count())
df.write.parquet(path+'/05/universe')
