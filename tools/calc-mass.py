#!/ufrc/roitberg/qasdfgtyuiop/anaconda3/envs/my-rdkit-env/bin/python
from rdkit.Chem import *
from rdkit.Chem.Descriptors import *
import sys

def mass(smiles):
    m = MolFromSmiles(smiles)
    m = AddHs(m)
    w = MolWt(m)
    return round(w,3)

smileses = sys.stdin.read().split()
for s in smileses:
    m = 0
    try:  # for problematic structures, output a 0
        m = mass(s)
        print(s,m)
    except:
        pass
