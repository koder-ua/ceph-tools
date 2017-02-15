import re
import sys
import json
import shutil
import argparse
import tempfile
import subprocess
import contextlib
import collections
from typing import Optional, List, Set, Dict, Tuple, Iterator, Any


MORE_LOGS = False


class PGInfo:
    def __init__(self, pgid: int, acting: List[int], size: int) -> None:
        self.pgid = pgid
        self.acting = acting
        self.size = size


class Pool:
    def __init__(self, name: Optional[str], pid: int, pg_count: int) -> None:
        self.name = name
        self.pid = pid
        self.pg_count = pg_count
        self.pg_map = {}  # type: Dict[int, Set[int]]

    def __str__(self) -> str:
        res = "pid = {}\n".format(self.pid)
        for num, mapping in sorted(self.pg_map.items()):
            res += "    {} => {}\n".format(num, ",".join(mapping))
        return res


class OSDChanges:
    def __init__(self) -> None:
        self.pg_in = 0
        self.pg_out = 0
        self.bytes_in = 0
        self.bytes_out = 0


def run(cmd: str, *args, **kwargs) -> Tuple[str, str]:
    if args or kwargs:
        cmd = cmd.format(*args, **kwargs)

    if MORE_LOGS:
        print(">>>>", cmd)

    p = subprocess.Popen(cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    stdout, stderr = p.communicate()
    assert p.returncode == 0, "{!r} failed with code {}. Stdout\n{}\nstderr {}"\
        .format(cmd, p.returncode, stdout, stderr)

    return stdout.decode('utf8'), stderr.decode('utf8')


def calc_diff(p_old: Pool, p_new: Pool) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
    assert p_old.pid == p_new.pid
    assert len(p_old.pg_map[0]) == len(p_new.pg_map[0])

    moved_to = collections.defaultdict(list)  # type: Dict[int, List[int]]
    moved_from = collections.defaultdict(list)  # type: Dict[int, List[int]]

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


def parse(map_data: str) -> Iterator[Pool]:
    curr_pool = None  # type: Pool
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


def get_pg_sizes(pg_dump_js: str = None) -> Dict[Tuple[int, int], int]:
    if pg_dump_js is None:
        pg_dump_js, _ = run("ceph pg dump --format=json")

    pg_dump = json.loads(pg_dump_js)['pg_stats']  # type: List[Dict[str, Any]]
    res = {}  # type: Dict[Tuple[int, int], int]

    for pg_dict in pg_dump:
        pool_id, pg_id = pg_dict['pgid'].split(".")  # type: str, str
        full_pg_id = (int(pool_id), int(pg_id, 16))
        res[full_pg_id] = pg_dict['stat_sum']['num_bytes']

    return res


def get_osd_diff(pool_pairs: Dict[int, Tuple[Pool, Pool]],
                 pg_sizes: Dict[Tuple[int, int], int]) -> Dict[int, OSDChanges]:

    osd_changes = collections.defaultdict(OSDChanges)  # type: Dict[int, OSDChanges]
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


def parse_args(argv: List[str]) -> Any:
    p = argparse.ArgumentParser()

    p.add_argument("-v", "--verbose", action="store_true", help="More logs")
    p.add_argument("-p", "--per-osd", action="store_true", help="Report per OSD stats")

    p.add_argument("-o", "--osd-map", default=None, help="Use dumped OSD map")
    p.add_argument("-g", "--pg-dump", default=None, help="Use dumped PG info (must be in json format)")

    p.add_argument("-d", "--dump", metavar="FILE", default=None,
                   help="Dump decompiled crush to FILE")

    p.add_argument("-a", "--apply", metavar="FILE", default=None,
                   help="Calculate diff, using new crush from FILE")

    p.add_argument("-n", "--no-editor", action="store_true",
                   help="Don't open editor, just print file name")

    p.add_argument("-e", "--editor", default="subl", help="Editor name")

    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    opts = parse_args(argv[1:])

    global MORE_LOGS
    MORE_LOGS = opts.verbose

    if opts.dump and opts.apply:
        print("Use either -d/--dump or --apply")
        return 1

    with contextlib.ExitStack() as stack:
        if opts.osd_map:
            osd_map_f = opts.osd_map
        else:
            osd_map_f = stack.enter_context(tempfile.NamedTemporaryFile()).name

        crush_map_f = stack.enter_context(tempfile.NamedTemporaryFile()).name
        if opts.apply:
            crush_map_txt_f = opts.apply
        else:
            crush_map_txt_f = stack.enter_context(tempfile.NamedTemporaryFile()).name

        if not opts.osd_map:
            run("ceph osd getmap -o {}", osd_map_f)

        curr_distr, _ = run("osdmaptool --test-map-pgs-dump {}", osd_map_f)

        old_pools = None  # type: Dict[int, Pool]
        old_pools = {pool.pid: pool
                     for pool in parse(curr_distr)}

        if not opts.apply:
            run("osdmaptool --export-crush {} {}", crush_map_f, osd_map_f)

            if opts.dump:
                run("crushtool -d {} -o {}", crush_map_f, opts.dump)
                return 0
            else:
                run("crushtool -d {} -o {}", crush_map_f, crush_map_txt_f)

            if opts.no_editor:
                print("Crush stored in file", crush_map_txt_f)
            else:
                run("{} {}", opts.editor, crush_map_txt_f)

            print("Press enter, when done")
            sys.stdin.readline()

        run("crushtool -c {} -o {}", crush_map_txt_f, crush_map_f)

        if opts.osd_map:
            osd_map_new_f = stack.enter_context(tempfile.NamedTemporaryFile()).name
            shutil.copy(osd_map_f, osd_map_new_f)
        else:
            osd_map_new_f = osd_map_f

        run("osdmaptool --import-crush {} {}", crush_map_f, osd_map_new_f)
        new_distr, _ = run("osdmaptool --test-map-pgs-dump {}", osd_map_new_f)

        new_pools = None  # type: Dict[int, Pool]
        new_pools = {pool.pid: pool
                     for pool in parse(new_distr)}

        pool_pairs = {pool.pid: (old_pools[pool.pid], pool)
                      for pool in new_pools.values()}
        pg_sizes = get_pg_sizes(opts.pg_dump)

        osd_changes = get_osd_diff(pool_pairs, pg_sizes)

        total_send = 0
        total_moved_pg = 0

        for osd_id, osd_change in sorted(osd_changes.items()):
            if opts.per_osd:
                print("{:>3d}: Send: {:>6d} MiB".format(osd_id, osd_change.bytes_out // 1024 ** 2))
                print("     Recv: {:>6d} MiB".format(osd_change.bytes_in // 1024 ** 2))
                print("     PG in:  {:>4d}".format(osd_change.pg_in))
                print("     PG out: {:>4d}".format(osd_change.pg_out))
            total_send += osd_change.bytes_in
            total_moved_pg += osd_change.pg_in

        print("Total MiB moved :", total_send // 1024 ** 2)
        print("Total PG moved  :", total_moved_pg)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
