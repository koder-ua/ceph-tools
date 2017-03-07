from __future__ import print_function

import sys
import time
import json
import math
import shutil
import argparse


import yaml


from cephlib.common import check_output, b2ssize, tmpnam
from calculate_remap import calculate_remap, get_osd_curr


BE_QUIET = False


class CrushNode(object):
    def __init__(self, id, weight, name, type):
        self.id = id
        self.weight = weight
        self.name = name
        self.type = type
        self.childs = []
        self.full_path = None

    def __str__(self):
        return "{0}(name={1!r}, weight={2}, id={3})"\
            .format(self.type, self.name, self.weight, self.id)

    def __repr__(self):
        return str(self)


class Crush(object):
    def __init__(self, nodes_map, roots):
        self.nodes_map =  nodes_map
        self.roots = roots
        self.search_cache = None


def load_crush_tree(osd_tree):
    tree = json.loads(osd_tree)
    nodes_map = {}
    childs_ids = {}

    for node_js in tree['nodes']:
        weight = node_js.get('crush_weight', None)
        weight = float(weight) if weight is not None else weight
        node = CrushNode(
            id=node_js['id'],
            name=node_js['name'],
            type=node_js['type'],
            weight=weight
        )
        childs_ids[node.id] = node_js.get('children', [])
        nodes_map[node.id] = node

    all_childs = set()
    for parent_id, childs_ids in childs_ids.items():
        nodes_map[parent_id].childs = [nodes_map[node_id] for node_id in childs_ids]
        all_childs.update(childs_ids)

    crush = Crush(nodes_map,
                  [nodes_map[nid] for nid in (set(nodes_map) - all_childs)])

    return crush


def find_node(crush, path):
    nodes = find_nodes(crush, path)
    if not nodes:
        raise IndexError("Can't found any node with path {0!r}".format(path))
    if len(nodes) > 1:
        raise IndexError(
            "Found {0} nodes  for path {1!r} (should be only 1)".format(len(nodes), path))
    return nodes[0]


def build_search_idx(crush):
    if crush.search_cache is None:
        not_done = []
        for node in crush.roots:
            node.full_path = [(node.type, node.name)]
            not_done.append(node)

        done = []
        while not_done:
            new_not_done = []
            for node in not_done:
                if node.childs:
                    for cnode in node.childs:
                        cnode.full_path = node.full_path[:] + [(cnode.type, cnode.name)]
                        new_not_done.append(cnode)
                else:
                    done.append(node)
            not_done = new_not_done
        crush.search_cache = done


def find_nodes(crush, path):
    if not path:
        return crush.roots

    res = []
    for node in crush.search_cache:
        nfilter = dict(path)
        for tp, val in node.full_path:
            if tp in nfilter:
                if nfilter[tp] == val:
                    del nfilter[tp]
                else:
                    break
        else:
            if not nfilter:
                res.append(node)

    return res


default_zone_order = ["osd", "host", "chassis", "rack", "row", "pdu",
                      "pod", "room", "datacenter", "region", "root"]


def is_rebalance_complete():
    pg_stat = json.loads(check_output("ceph pg stat --format=json"))
    if 'num_pg_by_state' in pg_stat:
        for dct in pg_stat['num_pg_by_state']:
            if dct['name'] not in ("active+clean", "active+remapped") and dct["num"] != 0:
                return False
    else:
        for pg_stat_dict in json.loads(check_output("ceph -s --format=json"))['pgmap']['pgs_by_state']:
            if pg_stat_dict['state_name'] != "active+clean":
                if pg_stat_dict['count'] != 0:
                    return False
    return True


def request_weight_update(node, new_weight):
    cmd = "ceph osd crush set {name} {weight} {path}"
    path_s = " ".join("{0}={1}".format(tp, name) for tp, name in node.full_path)
    cmd = cmd.format(name=node.name, weight=new_weight, path=path_s)

    if not BE_QUIET:
        print(cmd)

    check_output(cmd.format(name=node.name, weight=new_weight, path=path_s))


def wait_rebalance_to_complete(any_updates):
    if not any_updates:
        if is_rebalance_complete():
            return

    if not BE_QUIET:
        print("Waiting for cluster to complete rebalance ", end="")
        sys.stdout.flush()

    if any_updates:
        time.sleep(5)

    while not is_rebalance_complete():
        if not BE_QUIET:
            print('.', end="")
            sys.stdout.flush()
        time.sleep(5)

    if not BE_QUIET:
        print("done")


def do_rebalance(config, args):
    osd_tree_js = check_output("ceph osd tree --format=json")
    crush = load_crush_tree(osd_tree_js)
    max_nodes_per_round = config.get('max_reweight', 4)
    max_weight_change = config.get('step', 0.5)
    min_weight_diff = config.get('min_weight_diff', 0.01)
    selection_algo = config.get('osd_selection', 'rround')

    rebalance_nodes = []
    total_weight_change = 0.0
    build_search_idx(crush)

    for node in config['osds']:
        node = node.copy()
        new_weight = node.pop('weight')
        path = list(node.items())
        path.sort(key=lambda x: -default_zone_order.index(x[0]))
        cnode = find_node(crush, path)
        rebalance_nodes.append((cnode, new_weight))

        if not BE_QUIET:
            print(repr(new_weight), repr(cnode.weight))
            print(cnode, "=>", new_weight)
            total_weight_change += abs(cnode.weight - new_weight)

    if not BE_QUIET:
        if total_weight_change < min_weight_diff:
            print("Nothing to change")
            return
        else:
            print("Total sum of all weight changes {:.1f}".format(total_weight_change))

    if not args.no_estimate:
        osd_map_f = tmpnam()
        check_output("ceph osd getmap -o {0}".format(osd_map_f))
        crush_map_f = tmpnam()
        check_output("osdmaptool --export-crush {0} {1}".format(crush_map_f, osd_map_f))

        cmd_templ = "crushtool -i {crush_map_f} -o {crush_map_f} --update-item {id} {weight} {name} {loc}"
        for node, new_weight in rebalance_nodes:
            loc = " ".join("--loc {0} {1}".format(tp, name) for tp, name in node.full_path)
            cmd = cmd_templ.format(crush_map_f=crush_map_f, id=node.id, weight=new_weight, name=node.name, loc=loc)
            check_output(cmd)

        osd_map_new_f = tmpnam()
        shutil.copy(osd_map_f, osd_map_new_f)
        check_output("osdmaptool --import-crush {0} {1}".format(crush_map_f, osd_map_new_f))
        osd_changes = calculate_remap(osd_map_f, osd_map_new_f)

        total_send = 0
        total_moved_pg = 0

        for osd_id, osd_change in sorted(osd_changes.items()):
            total_send += osd_change.bytes_in
            total_moved_pg += osd_change.pg_in

        print("Total bytes to be moved :", b2ssize(total_send) + "B")
        print("Total PG to be moved  :", total_moved_pg)

        if args.show_after:
            osd_curr = get_osd_curr()
            for osd_id, osd_change in sorted(osd_changes.items()):
                pg_diff = osd_change.pg_in - osd_change.pg_out
                bytes_diff = osd_change.bytes_in - osd_change.bytes_out
                print("OSD {0}, PG {1:>4d} => {2:>4d},  bytes {3:>6s} => {4:>6s}".format(
                    osd_id,
                    osd_curr[osd_id].pg, osd_curr[osd_id].pg + pg_diff,
                    b2ssize(osd_curr[osd_id].bytes), b2ssize(osd_curr[osd_id].bytes + bytes_diff)))

        if args.estimate_only:
            return

    if not BE_QUIET:
        already_changed = 0.0

    any_updates = False
    while rebalance_nodes:
        wait_rebalance_to_complete(any_updates)

        if not BE_QUIET:
            if already_changed > min_weight_diff:
                print("Done {0}%".format(int(already_changed * 100 // total_weight_change + 0.5)))

        next_nodes = rebalance_nodes[:max_nodes_per_round]

        if selection_algo == 'rround':
            # roll nodes
            rebalance_nodes = rebalance_nodes[max_nodes_per_round:] + next_nodes

        any_updates = False
        for node, required_weight in next_nodes:
            weight_change = required_weight - node.weight

            if abs(weight_change) > max_weight_change:
                weight_change = math.copysign(max_weight_change, weight_change)

            new_weight = node.weight + weight_change
            if abs(weight_change) > min_weight_diff:
                request_weight_update(node, new_weight)
                any_updates = True

            if abs(new_weight - required_weight) < min_weight_diff:
                rebalance_nodes = [(rnode, w) for (rnode, w) in rebalance_nodes if rnode is not node]

            node.weight = new_weight

            if not BE_QUIET:
                already_changed += abs(weight_change)

    wait_rebalance_to_complete(False)


help = """Gently change OSD's weight cluster in cluster.

Config file example(yaml):

# max weight change step
step: 0.1

# max OSD reweighted in parallel
max_reweight: 1

# minimal weight difference to be corrected
min_weight_diff: 0.01

# osd selection algorithm
osd_selection: rround

# list of all OSD to be rebalanced
osds:
  # OSD tree location: name, root, host
  - osd: osd.0
    root: default
    host: osd-0
    # required weight
    weight: 0.3

  # more OSD's
  - osd: osd.1
    root: default
    host: osd-2
    weight: 0.3
"""


def parse_args(argv):
    parser = argparse.ArgumentParser(usage=help)
    parser.add_argument("-q", "--quiet", help="Don't print any info/debug messages", action='store_true')

    parser.add_argument("-e", "--estimate-only", action='store_true',
                        help="Only estimate rebalance size (incompatible with -n/--no-estimate)")

    parser.add_argument("-s", "--show-after", action='store_true',
                        help="Show PGs and data distibution after rebalance complete" +
                             "(incompatible with -n/--no-estimate)")

    parser.add_argument("-n", "--no-estimate", action='store_true',
                        help="Don't estimate rebalance size (incompatible with -e/--estimate-only)")

    parser.add_argument("config", help="Yaml rebalance config file")
    args = parser.parse_args(argv[1:])

    if args.estimate_only and args.no_estimate:
        sys.stderr.write("-e/--estimate-only is incompatible with -n/--no-estimate\n")
        sys.stderr.flush()
        return None

    if args.show_after and args.no_estimate:
        sys.stderr.write("-s/--show-after is incompatible with -n/--no-estimate\n")
        sys.stderr.flush()
        return None

    return args

def main(argv):
    args = parse_args(argv)

    if not args:
        return 1

    global BE_QUIET
    if args.quiet:
        BE_QUIET = True

    cfg = yaml.load(open(args.config).read())
    do_rebalance(cfg, args)
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
