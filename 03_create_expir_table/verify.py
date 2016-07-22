#!/usr/bin/env python
from rdkit.Chem import *
import sys

def verify(smiles):
    m = MolFromSmiles(smiles)
    m = AddHs(m)
    return smiles

smileses = sys.stdin.read().split()
for s in smileses:
    try:  # for problematic structures, skip
        print(verify(s))
    except:
        pass
