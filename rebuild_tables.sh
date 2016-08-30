#!/usr/bin/env zsh

set -e

# copy files
for i in *([0-9])_*.scala;do
	scalac $i common.scala -d ~/MEGA/bin/$(basename $i .scala).jar
done
cp *.py ~/MEGA/bin
cp FunctionalGroups.txt ~/MEGA/data

cd ~/MEGA
rm -rf tables
mkdir tables

# build tables
spark-submit bin/03_create_mid_struct_table.jar		2> 03_create_mid_struct_table.log
spark-submit bin/04_create_expir_table.jar			2> 04_create_expir_table.log
spark-submit bin/05_create_struct_universe.jar		2> 05_create_struct_universe.log
spark-submit bin/06_create_theoretical_ir.jar		2> 06_create_theoretical_ir.log

# sync with hipergator
