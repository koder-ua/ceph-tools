#!/usr/bin/env bash
set -x
set -e

lxc file push collect.py osd-0/tmp/collect.py
lxc exec osd-0 -- rm -f /tmp/res.db
lxc exec osd-0 -- python /tmp/collect.py collect -p -t 1000 -r 2 --db /tmp/res.db 0
rm -f /tmp/res.db
lxc file pull osd-0/tmp/res.db /tmp/res.db


