from __future__ import print_function

import sys
import json
import collections


def load_PG_distribution(pgdump_path, key='acting'):
    pg_dump = json.load(open(pgdump_path))
    osd_pool_pg_2d = collections.defaultdict(lambda: collections.Counter())

    for pg in pg_dump['pg_stats']:
        pool = int(pg['pgid'].split('.', 1)[0])
        for osd_num in pg[key]:
            osd_pool_pg_2d[osd_num][pool] += 1
    return osd_pool_pg_2d


def show_pg_distr(osd_pool_pg_2d):
    all_pools = set()
    for pools_per_osd in osd_pool_pg_2d.values():
        all_pools.update(pools_per_osd.keys())
    all_pools = sorted(all_pools)
    format = "%4s | " + "  ".join(["%4s"] * len(all_pools)) + " %6s \n"
    line_sz = len(format % (('',) * (len(all_pools) + 2))) - 1
    res = format % tuple([''] + all_pools + ['total'])
    res += '-' * line_sz + "\n"
    for osd, per_pool in sorted(osd_pool_pg_2d.items()):
        pgs = [per_pool.get(pool_id, 0) for pool_id in all_pools]
        res += format % tuple([osd] + pgs + [sum(pgs)])
    return res


def main(argv):
    distr = load_PG_distribution(argv[1])
    print(show_pg_distr(distr))
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
