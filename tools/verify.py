#!/usr/bin/env python
from rdkit.Chem import *
from sys import *

if MolFromSmiles(argv[1]) is None:
    exit(1)
else:
    exit(0)
