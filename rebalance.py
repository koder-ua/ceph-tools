from __future__ import print_function

import sys
import time
import json
import math
import argparse
import logging


import yaml


from common import run


BE_QUIET = False


class CrushNode(object):
    def __init__(self, id, weight, name, type):
        self.id = id
        self.weight = weight
        self.name = name
        self.type = type
        self.childs = []

    def __str__(self):
        return "{0}(name={1!r}, weight={2}, id={3})"\
            .format(self.type, self.name, self.weight, self.id)

    def __repr__(self):
        return str(self)


class Crush(object):
    def __init__(self, nodes_map, roots):
        self.nodes_map =  nodes_map
        self.roots = roots


def load_crush_tree(osd_tree):
    tree = json.loads(osd_tree)
    nodes_map = {}
    childs_ids = {}

    for node_js in tree['nodes']:
        node = CrushNode(
            id=node_js['id'],
            name=node_js['name'],
            type=node_js['type'],
            weight=node_js.get('crush_weight', None)
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


def find_nodes(crush, path):
    if not path:
        return crush.roots

    current_nodes = crush.roots
    for tp, name in path[:-1]:
        new_current_nodes = []
        for node in current_nodes:
            if node.type == tp and node.name == name:
                new_current_nodes.extend(node.childs)
        current_nodes = new_current_nodes

    res = []
    tp, name = path[-1]
    for node in current_nodes:
        if node.type == tp and node.name == name:
            res.append(node)

    return res


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
    parser.add_argument("-q", "--quiet", help="Don't print any info/debug messages")
    parser.add_argument("config", help="Yaml rebalance config file")
    return parser.parse_args(argv[1:])


default_zone_order = ["osd", "host", "chassis", "rack", "row", "pdu",
                      "pod", "room", "datacenter", "region", "root"]


def is_rebalance_complete():
    for dct in json.loads(run("ceph pg stat --format=json"))['num_pg_by_state']:
        if dct['name'] != "active+clean" and dct["num"] != 0:
            return False
    return True


def request_weight_update(node, path, new_weight):
    cmd = "ceph osd crush set {name} {weight} {path}"
    path_s = " ".join("{0}={1}".format(tp, name) for tp, name in path[:-1])
    cmd = cmd.format(name=node.name, weight=new_weight, path=path_s)
    print(cmd)
    run(cmd.format(name=node.name, weight=new_weight, path=path_s))


def wait_rebalance_to_complete():
    if is_rebalance_complete():
        return

    if not BE_QUIET:
        print("Waiting for cluster to complete rebalance ", end="")
        sys.stdout.flush()

    time.sleep(5)
    while not is_rebalance_complete():
        if not BE_QUIET:
            print('.', end="")
            sys.stdout.flush()
        time.sleep(5)

    if not BE_QUIET:
        print("done")


def do_rebalance(config):
    osd_tree_js = run("ceph osd tree --format=json")
    crush = load_crush_tree(osd_tree_js)
    max_nodes_per_round = config.get('max_reweight', 4)
    max_weight_change = config.get('step', 0.5)
    min_weight_diff = config.get('min_weight_diff', 0.01)
    selection_algo = config.get('osd_selection', 'rround')

    rebalance_nodes = []

    for node in config['osds']:
        node = node.copy()
        new_weight = node.pop('weight')
        path = list(node.items())
        path.sort(key=lambda x: -default_zone_order.index(x[0]))
        node = find_node(crush, path)
        rebalance_nodes.append((node, path, new_weight))

        if not BE_QUIET:
            print(node, "=>", new_weight)

    # TODO: estimate and show rebalance size
    # pprint.pprint(rebalance_nodes)

    while rebalance_nodes:
        wait_rebalance_to_complete()
        next_nodes = rebalance_nodes[:max_nodes_per_round]

        if selection_algo == 'rround':
            # roll nodes
            rebalance_nodes = rebalance_nodes[max_nodes_per_round:] + next_nodes

        any_updates = False
        for node, path, required_weight in next_nodes:
            weight_change = required_weight - node.weight

            if abs(weight_change) > max_weight_change:
                weight_change = math.copysign(max_weight_change, weight_change)

            new_weight = node.weight + weight_change
            if abs(weight_change) > min_weight_diff:
                request_weight_update(node, path, new_weight)
                any_updates = True

            if abs(new_weight - required_weight) < min_weight_diff:
                rebalance_nodes = [(rnode, path, w)
                                   for (rnode, path, w) in rebalance_nodes
                                   if rnode is not node]

            node.weight = new_weight

        if any_updates:
            time.sleep(5)

    wait_rebalance_to_complete()


def main(argv):
    args = parse_args(argv)

    global BE_QUIET
    if args.quiet:
        BE_QUIET = True

    cfg = yaml.load(open(args.config).read())
    do_rebalance(cfg)
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
