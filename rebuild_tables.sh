#!/usr/bin/env bash
tup upd
rm -rf outputs/tables
mkdir outputs/tables
spark-submit 03_create_mid_struct_table.jar 2>03_create_mid_struct_table.log
spark-submit 04_create_expir_table.jar 2>04_create_expir_table.log
echo please run 05_create_struct_universe.jar on hipergator
spark-submit 06_create_theoretical_ir.jar 2>06_create_theoretical_ir.log
