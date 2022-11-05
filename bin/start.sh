#!/bin/bash

rm -f ../log/*

echo 'copy prod cfg'
cp ../cfg/icn.cfg.prod ../cfg/icn.cfg
#cp ../cfg/twap.csv.prod ../cfg/twap.csv

echo 'start icn_server'
nohup /home/keplercapital2/anaconda2/bin/python icn_server.py >>../log/icn.out 2>>../log/icn.err &


