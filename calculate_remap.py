from __future__ import print_function

import re
import os
import sys
import json
import shutil
import logging
import argparse
import tempfile
import collections

from common import run, setup_loggers
from common import logger as clogger

logger = logging.getLogger("remap")


class PGInfo:
    def __init__(self, pgid, acting, size):
        self.pgid = pgid
        self.acting = acting
        self.size = size


class Pool:
    def __init__(self, name, pid, pg_count):
        self.name = name
        self.pid = pid
        self.pg_count = pg_count
        self.pg_map = {}

    def __str__(self):
        res = "pid = {0}\n".format(self.pid)
        for num, mapping in sorted(self.pg_map.items()):
            res += "    {0} => {1}\n".format(num, ",".join(mapping))
        return res


class OSDChanges:
    def __init__(self):
        self.pg_in = 0
        self.pg_out = 0
        self.bytes_in = 0
        self.bytes_out = 0


def calc_diff(p_old, p_new):
    assert p_old.pid == p_new.pid
    assert len(p_old.pg_map[0]) == len(p_new.pg_map[0])

    moved_to = collections.defaultdict(list)
    moved_from = collections.defaultdict(list)

    for pg_id, v1 in p_old.pg_map.items():
        v2 = p_new.pg_map[pg_id]
        if v1 != v2:
            for osd_id in v1 - v2:
                moved_from[osd_id].append(pg_id)
            for osd_id in v2 - v1:
                moved_to[osd_id].append(pg_id)

    return dict(moved_from.items()), dict(moved_to.items())


pool_start_line = re.compile(r"pool\s+(?P<pid>\d+)\s+pg_num\s+(?P<pg_num>\d+)\s*$")
pg_map_line = re.compile(r"(?P<pid>\d+)\." +
                         r"(?P<pg_id>[0-9a-f]+)\s+" +
                         r"\[(?P<osd_ids>[0-9a-f,]+)\]\s+\d+\s*$")


def parse(map_data):
    curr_pool = None
    for line in map_data.split("\n"):
        if curr_pool is not None:
            mline = pg_map_line.match(line)
            if mline is None:
                yield curr_pool
                curr_pool = None
            else:
                pg_num = int(mline.group('pg_id'), 16)
                osd_ids = {int(x) for x in mline.group('osd_ids').split(",")}
                curr_pool.pg_map[pg_num] = set(osd_ids)
                continue

        pline = pool_start_line.match(line)
        if pline:
            curr_pool = Pool(None,
                             int(pline.group('pid')),
                             int(pline.group('pg_num')))

    if curr_pool is not None:
        yield curr_pool


def get_pg_sizes(pg_dump_js=None):
    if pg_dump_js is None:
        pg_dump_js = run("ceph pg dump --format=json")

    pg_dump = json.loads(pg_dump_js)['pg_stats']
    res = {}

    for pg_dict in pg_dump:
        pool_id, pg_id = pg_dict['pgid'].split(".")  # type: str, str
        full_pg_id = (int(pool_id), int(pg_id, 16))
        res[full_pg_id] = pg_dict['stat_sum']['num_bytes']

    return res


def get_osd_diff(pool_pairs, pg_sizes):

    osd_changes = collections.defaultdict(OSDChanges)
    for pool_id, (old_pool, new_pool) in pool_pairs.items():
        frm, to = calc_diff(old_pool, new_pool)

        for osd_id, out_pgs in frm.items():
            osd_ch = osd_changes[osd_id]
            osd_ch.pg_out += len(out_pgs)
            for pg_id in out_pgs:
                osd_ch.bytes_out += pg_sizes[(pool_id, pg_id)]

        for osd_id, in_pgs in to.items():
            osd_ch = osd_changes[osd_id]
            osd_ch.pg_in += len(in_pgs)
            for pg_id in in_pgs:
                osd_ch.bytes_in += pg_sizes[(pool_id, pg_id)]

    return osd_changes


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="More logs")
    subparsers = parser.add_subparsers(dest='subparser_name')

    dump_parser = subparsers.add_parser('dump', help="Dump decompiled crush to FILE")
    dump_parser.add_argument("-o", "--osd-map", default=None, help="Use dumped OSD map")
    dump_parser.add_argument("out_file", help="File to store dump")

    apply_parser = subparsers.add_parser('apply', help="Apply new crush and calculate difference")
    apply_parser.add_argument("crush_file", help="File to load crush from")

    interactive_parser = subparsers.add_parser('interactive',
                                               help="Decompile crush from cluster, open it in editor, " +
                                                    "and calculate diff")
    interactive_parser.add_argument("-e", "--editor", default="subl", help="Editor name")

    for subparser in (apply_parser, interactive_parser):
        subparser.add_argument("-o", "--osd-map", default=None, help="Use dumped OSD map")
        subparser.add_argument("-p", "--per-osd", action="store_true",
                               help="Report per OSD stats")
        subparser.add_argument("-g", "--pg-dump", default=None,
                               help="Use dumped PG info (must be in json format)")

    return parser.parse_args(argv)


FILES_TO_REMOVE = []
def tmpnam():
    fd, name = tempfile.mkstemp()
    os.close(fd)
    FILES_TO_REMOVE.append(name)
    return name


RSMAP = [('K', 1024),
         ('M', 1024 ** 2),
         ('G', 1024 ** 3),
         ('T', 1024 ** 4)]


def b2ssize(value):
    value = int(value)
    if value < 1024:
        return str(value) + " "

    # make mypy happy
    scale = 1
    name = ""

    for name, scale in RSMAP:
        if value < 1024 * scale:
            if value % scale == 0:
                return "{0} {1}i".format(value // scale, name)
            else:
                return "{0:.1f} {1}i".format(float(value) / scale, name)

    return "{0}{1}i".format(value // scale, name)


def main(argv):
    opts = parse_args(argv[1:])

    default_level = logging.DEBUG if opts.verbose else logging.WARNING
    setup_loggers([clogger, logger], default_level=default_level)

    if opts.subparser_name == 'dump':
        crush_map_f = tmpnam()

        if opts.osd_map:
            run("osdmaptool --export-crush {0} {1}", crush_map_f, opts.osd_map)
        else:
            run("ceph osd getcrushmap -o {0}", crush_map_f)

        run("crushtool -d {0} -o {1}", crush_map_f, opts.out_file)
        return 0

    if opts.osd_map:
        osd_map_f = opts.osd_map
    else:
        osd_map_f = tmpnam()
        run("ceph osd getmap -o {0}", osd_map_f)

    crush_map_f = tmpnam()

    if opts.subparser_name == "apply":
        crush_map_txt_f = opts.crush_file
    else:
        assert opts.subparser_name == "interactive"
        run("osdmaptool --export-crush {0} {1}", crush_map_f, osd_map_f)
        crush_map_txt_f = tmpnam()
        run("crushtool -d {0} -o {1}", crush_map_f, crush_map_txt_f)
        run("{0} {1}", opts.editor, crush_map_txt_f)

        logger.info("Press enter, when done")
        sys.stdin.readline()

    run("crushtool -c {0} -o {1}", crush_map_txt_f, crush_map_f)

    if opts.osd_map:
        # don't change original osd map file
        osd_map_new_f = tmpnam()
        shutil.copy(osd_map_f, osd_map_new_f)
        logger.debug("Copy {0} => {1}".format(osd_map_f, osd_map_new_f))
    else:
        osd_map_new_f = osd_map_f

    curr_distr = run("osdmaptool --test-map-pgs-dump {0}", osd_map_f)
    curr_pools = {pool.pid: pool for pool in parse(curr_distr)}

    run("osdmaptool --import-crush {0} {1}", crush_map_f, osd_map_new_f)

    new_distr = run("osdmaptool --test-map-pgs-dump {0}", osd_map_new_f)
    new_pools = {pool.pid: pool for pool in parse(new_distr)}

    pool_pairs = {pool.pid: (curr_pools[pool.pid], pool) for pool in new_pools.values()}
    pg_sizes = get_pg_sizes(open(opts.pg_dump).read())

    osd_changes = get_osd_diff(pool_pairs, pg_sizes)

    total_send = 0
    total_moved_pg = 0

    for osd_id, osd_change in sorted(osd_changes.items()):
        if opts.per_osd:
            print("{0:>3d}: Send: {1:>6d}B".format(osd_id, b2ssize(osd_change.bytes_out)))
            print("     Recv: {0:>6d}B".format(b2ssize(osd_change.bytes_in)))
            print("     PG in:  {0:>4d}".format(osd_change.pg_in))
            print("     PG out: {0:>4d}".format(osd_change.pg_out))
        total_send += osd_change.bytes_in
        total_moved_pg += osd_change.pg_in

    print("Total moved :", b2ssize(total_send) + "B")
    print("Total PG moved  :", total_moved_pg)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    finally:
        map(os.unlink, FILES_TO_REMOVE)