#!/usr/bin/env python
"""
Produce a SQL create table query from a data file.

"""

import sys
import csv
import gzip
import codecs
from StringIO import StringIO

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-d', help='column delimiter', default=',')
parser.add_argument('-i', help='input file')
parser.add_argument('-n', help='name of SQL table')
parser.add_argument('-c', help='flag to use csv module to read file', action='store_true')

args = parser.parse_args()

if args.i is None:
    raise Exception("Input file must be provided.")

if args.n is None:
    raise Exception("A name for the SQL table must be provided.")

info=dict()
delim = args.d
input_file = args.i
table_name = args.n

# read in a sample of the file, up to 100b
#x = open(input_file).read(100000)
file_stream = None
#if x.find('\r') != -1:
#    file_stream = StringIO(x.replace('\r','\n'))

#if file_stream is None:
#    reader = csv.reader(open(input_file),delimiter=delim)
#else:
#    reader = csv.reader(file_stream, delimiter=delim)

fh = None
if input_file.endswith('.gz'):
    fh = gzip.open(input_file)
else:
    fh = open(input_file)

if not args.c:
    header=fh.readline().strip().split(delim)
else:
    reader = csv.reader(fh, delimiter=delim)
    header = reader.next()

# print header

for column in header:
    info[column]={"maxlength":0,"intcount":0}

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
    
    if nrow > 10000:
        break
    numrows += 1
    #print len(row)
    for i,value in enumerate(row):
        try:
            a = int(value)
            info[header[i]]['intcount'] += 1
        except ValueError:
            if len(value) > info[header[i]]['maxlength']:
                info[header[i]]['maxlength'] = len(value)


print >> sys.stdout, "CREATE TABLE `%s` (" % table_name
colnames = []
for i,column in enumerate(header):
    coltype = 'varchar'
    length = info[column]['maxlength']
    if info[column]['intcount']/float(numrows) > 0.5:
        coltype = 'int'
        length = 11
    
    colname = column[:30].lower().replace(' ','_').replace('(','').replace(')','').replace('/','_').replace('-','_').replace('>','gt').replace('<','lt').replace('#','no').replace('%','perc')
    if colname == '' or colname in colnames:
        colname = colnames[-1] + '2'
    colnames.append(colname)
    print >> sys.stdout, "`%s` %s(%d) DEFAULT NULL" % (colname, coltype, length),
    if not i == len(header)-1:
        print >> sys.stdout, ","
    else:
        print >> sys.stdout, ""

print >> sys.stdout, ") ENGINE=MyISAM DEFAULT CHARSET=latin1;"
