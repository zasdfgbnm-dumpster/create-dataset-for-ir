#!/usr/bin/env python
from rdkit.Chem import *
from rdkit.Chem.Descriptors import *

smiles = input()
m = MolFromSmiles(smiles)
m = AddHs(m)
w = MolWt(m)
print(round(w,3))
