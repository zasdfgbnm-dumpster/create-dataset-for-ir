from rdkit.Chem import *
import sys

def verify(smiles):
    m = MolFromSmiles(smiles)
    m = AddHs(m)
    return m

smileses = sys.stdin.read().split()
for s in smileses:
    m = 0
    try:
        m = verify(s)
        print(s)
    except:
        pass
