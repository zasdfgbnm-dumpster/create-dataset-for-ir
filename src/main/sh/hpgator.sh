#!/usr/bin/env zsh

set -e

dest="qasdfgtyuiop@hpg2.rc.ufl.edu:/ufrc/roitberg/qasdfgtyuiop"

rsync -avL ~/MEGA/bin $dest
rsync -avL --delete --exclude="StructureUniverse" ~/MEGA/tables $dest
rsync -avL ~/MEGA/data $dest
rsync -av $1 $dest/bin
