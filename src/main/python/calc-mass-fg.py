from rdkit.Chem import *
from rdkit.Chem.Descriptors import *
import sys,os


class fg_matcher:

    def __init__(self, filename):
        self.parse(filename)

    def parse(self, filename):
        self.fgs = []
        lines = [l.strip() for l in open(filename)]

        def useful_lines():
            for i in lines:
                i = i.split('//')[0].strip()
                ii = i.split('\t')
                if len(ii) == 2:
                    yield (Chem.MolFromSmarts(ii[0]), ii[1])
        for i, j in useful_lines():
            self.fgs.append((i, j))

    def match(self, smiles):
        m = Chem.MolFromSmiles(smiles)
        return [m.HasSubstructMatch(patt) for patt, fgname in self.fgs]

hpgpath = '/ufrc/roitberg/qasdfgtyuiop/data'
pcpath = '/home/gaoxiang/MEGA/data'
ishpg = os.path.exists(hpgpath)
path = hpgpath if ishpg else pcpath
matcher = fg_matcher(path+'/FunctionalGroups.txt')


def mass(smiles):
    m = MolFromSmiles(smiles)
    m = AddHs(m)
    w = MolWt(m)
    return round(w, 3)

smileses = sys.stdin.read().split()
for s in smileses:
    try:
        m = mass(s)
        fgs = matcher.match(s)
        print(s, m, *fgs)
    except:
        pass
