#!/usr/bin/env python
"""
Produce a SQL create table query from a data file.

"""

import sys
import csv
import gzip
import codecs

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-d', help='column delimiter', default=',')
parser.add_argument('-i', help='input file')
parser.add_argument('-n', help='name of SQL table')
parser.add_argument('-c', help='flag to use csv module to read file', action='store_true')
parser.add_argument('-m', help='maximum number of rows to use to derive format. Use -1 for no limit.', type=int, default=10000)

args = parser.parse_args()

if args.i is None:
    raise Exception("Input file must be provided.")

if args.n is None:
    raise Exception("A name for the SQL table must be provided.")

info=dict()
delim = args.d
input_file = args.i
table_name = args.n

if delim == 't':
    delim = '\t'

fh = None
if input_file.endswith('.gz'):
    fh = gzip.open(input_file, 'rt')
else:
    fh = open(input_file)

if not args.c:
    header=fh.readline().strip().split(delim)
else:
    reader = csv.reader(fh, delimiter=delim)
    header = next(reader)

#print(header)

for column in header:
    info[column]={"maxlength":0,"intcount":0,"doublecount":0}

numrows=0

if not args.c:
    file_enumerator = enumerate(fh)
else:
    file_enumerator = enumerate(reader)

for nrow, line in file_enumerator:

    if not args.c:
        row = line.strip().split(delim)
    else:
        row = line

    if len(row) != len(header):
        raise Exception("Row length does not match header length:\n line: %s \n row(n=%d): \n %s \n header(n=%d): \n %s" % (line, len(row), row, len(header), header))

    if args.m > 0 and nrow > args.m:
        break
    numrows += 1
    #print len(row)
    for i,value in enumerate(row):
        try:
            a = int(value)
            info[header[i]]['intcount'] += 1
        except ValueError:
            try:
                a = float(value)
                info[header[i]]['doublecount'] += 1
            except ValueError:
                if len(value) > info[header[i]]['maxlength']:
                    info[header[i]]['maxlength'] = len(value)


print(f"CREATE TABLE `{table_name}` (")

colnames = []
for i,column in enumerate(header):
    coltype = 'varchar'
    lengthstr = f"({info[column]['maxlength']})"

    if info[column]['intcount']/float(numrows) > 0.9:
        coltype = 'int'
        lengthstr = '(11)'

    if info[column]['doublecount']/float(numrows) > 0.9:
        coltype = 'double'
        lengthstr = ''

    colname = column[:30].lower().replace(' ','_').replace('(','').replace(')','').replace('/','_').replace('-','_').replace('>','gt').replace('<','lt').replace('#','no').replace('%','perc')
    if colname == '':
        colname = f"col{i}"
    if colname in colnames:
        colname = colname + '_2'
    colnames.append(colname)
    print(f"`{colname}` {coltype}{lengthstr} DEFAULT NULL", end='')
    if not i == len(header)-1:
        print(",")
    else:
        print("")

print(") ENGINE=MyISAM DEFAULT CHARSET=latin1;")
