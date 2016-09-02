#!/usr/bin/env zsh

set -e

for i in $PWD/src/main/python/{calc-mass-fg,verify}.py;do
	ln -sf $i ~/MEGA/bin
done
ln -sf $PWD/src/main/resources/FunctionalGroups.txt ~/MEGA/data
