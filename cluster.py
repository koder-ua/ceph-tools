import json
import collections

from common import run


OsdAddrs = collections.namedtuple('OsdAddrs', ("public", "cluster"))


def get_all_osds(osd_dump=None):
    if osd_dump is None:
        osd_dump = run("ceph osd dump --format=json")

    osd_addrs = {}
    for osd in json.loads(osd_dump)['osds']:
        osd_addrs[int(osd['osd'])] = OsdAddrs(osd['cluster_addr'].split('/')[0],
                                              osd['public_addr'].split('/')[0])

    return osd_addrs


