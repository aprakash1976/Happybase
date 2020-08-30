#!/usr/bin/env python

import time
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages/happybase')
import random
import contextlib
import re
import happybase

connection = happybase.Connection('localhost')
# regular expression pattern
pattern = re.compile(r'^([0-9.]+)\s([\w.-]+)\s([\w.-]+)\s(\[[^\[\]]+\])\s"((?:[^"]|\")+)"\s(\d{3})\s(\d+|-)\s"((?:[^"]|\")+)"\s"((?:[^"]|\")+)"$')

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    print line
    # match
    result = pattern.match(line)
    # print the result
    for part in result.groups():
        print '%s\t%s' % (part, 1)

