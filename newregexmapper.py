#!/usr/bin/env python

import sys
import re
import happybase
import time

cfdict = {'hostname':'common', 'remoteuser':'common', 'eventtimestamp':'common', 'requestmethod':'http','requeststatus':'http','responsebytes':'http','remotehost':'common','agent':'misc'}

parts = [
    r'(?P<hostname>\S+)',	                # host %h
    r'\S+',     	                        # indent %l (unused)
    r'(?P<remoteuser>\S+)',                     # user %u
    r'\[(?P<eventtimestamp>.+)\]',              # time %t
    r'"(?P<requestmethod>.+)"',                       # request "%r"
    r'(?P<requeststatus>[0-9]+)',               # status %>s
    r'(?P<responsebytes>\S+)',                  # size %b (careful, can be '-')
    r'"(?P<remotehost>.*)"',                    # referer "%{Referer}i"
    r'"(?P<agent>.*)"',                         # user agent "%{User-agent}i"
]
pattern = re.compile(r'\s+'.join(parts)+r'\s*\Z')

connection = happybase.Connection('localhost')
table = connection.table('apache_access_log')

# input comes from STDIN (standard input)
for line in sys.stdin:
    row_key = int(time.time())
    # remove leading and trailing whitespace
    line = line.strip()
    # match
    m  = pattern.match(line)
    result = m.groupdict()
    # increase counters
    b = table.batch()
    for key in result.keys():
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
	cfname = str(cfdict[key])+':'+key
	table.put(str(row_key), {cfname:result[key]})
        print '%s\t%s' % (result[key], 1)
    b.send()
