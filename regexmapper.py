#!/usr/bin/env python

import sys
import re
import datetime
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
import happybase

host = "localhost"
port = "9090"

tablename = "apache_access_log"
colums = ["common:hostname","common:remotehost","common:remoteuser","common.eventtimestamp","http:requestmethod","http:requeststatus","http:responsebytes","misc:referrer","misc:agent"]

# HBase connection
connection = happybase.Connection('localhost')
# before first use:
connection.open()
table = connection.table(tablename)

# regular expression pattern
pattern = re.compile(r'^([0-9.]+)\s([\w.-]+)\s([\w.-]+)\s(\[[^\[\]]+\])\s"((?:[^"]|\")+)"\s(\d{3})\s(\d+|-)\s"((?:[^"]|\")+)"\s"((?:[^"]|\")+)"$')

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # match
    result = pattern.match(line)
    i = 0
    # print the result
    mutations = []
    for part in result.groups():
	rowkey = datetime.datetime.now().time()
	table.put(rowkey, {colums[i]: part}, datetime.datetime.now().time())
    	print '%s\t%s' % (part, 1)
	i+=1

connection.close()
