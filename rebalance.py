from __future__ import print_function

import re
import sys
import time
import json
import math
import shutil
import argparse
import collections


import yaml


from cephlib.common import check_output, b2ssize, tmpnam
from calculate_remap import calculate_remap, get_osd_curr


BE_QUIET = False


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class FakedNode(object):
    def __init__(self, **attrs):
        self.__dict__.update(attrs)


class Node(object):
    def __init__(self, id, name, type, weight):
        self.id = id
        self.name = name
        self.type = type
        self.childs = []
        self.full_path = None
        self.weight = weight

    def str_path(self):
        if self.full_path:
            return "/".join("{0}={1}".format(tp, name) for tp, name in self.full_path)
        return None

    def __str__(self):
        w = ", w={0.weight}".format(self) if self.weight is not None else ""
        fp = self.str_path() if self.full_path else ""
        return "{0.type}(name={0.name!r}, id={0.id}{1}{2})".format(self, w, fp)

    def __repr__(self):
        return str(self)

    def tree(self, tabs=0, tabstep=" " * 4):
        w = ", w={0.weight}".format(self) if self.weight is not None else ""
        yield tabstep * tabs + "{0.type}(name={0.name!r}, id={0.id}{1})".format(self, w)
        for cr_node in self.childs:
            for line in cr_node.tree(tabs=tabs + 1, tabstep=tabstep):
                yield line

    def copy(self):
        res = self.__class__(id=self.id, name=self.name, type=self.type, weight=self.weight)
        res.childs = self.childs
        return res


class Crush(object):
    def __init__(self, nodes_map, roots):
        self.nodes_map = nodes_map
        self.roots = roots
        self.search_cache = None

    def __str__(self):
        return "\n".join("\n".join(root.tree()) for root in self.roots)

    def build_search_idx(self):
        if self.search_cache is None:
            not_done = []
            for node in self.roots:
                node.full_path = [(node.type, node.name)]
                not_done.append(node)

            done = []
            while not_done:
                new_not_done = []
                for node in not_done:
                    if node.childs:
                        for chnode in node.childs:
                            chnode.full_path = node.full_path[:] + [(chnode.type, chnode.name)]
                            new_not_done.append(chnode)
                    else:
                        done.append(node)
                not_done = new_not_done
            self.search_cache = done

    def find_nodes(self, path):
        if not path:
            return self.roots

        res = []
        for node in self.search_cache:
            nfilter = dict(path)
            for tp, val in node.full_path:
                if tp in nfilter:
                    if nfilter[tp] == val:
                        del nfilter[tp]
                        if not nfilter:
                            break
                    else:
                        break

            if not nfilter:
                res.append(node)

        return res

    def find_node(self, path):
        nodes = self.find_nodes(path)
        if not nodes:
            raise IndexError("Can't found any node with path {0!r}".format(path))
        if len(nodes) > 1:
            raise IndexError(
                "Found {0} nodes  for path {1!r} (should be only 1)".format(len(nodes), path))
        return nodes[0]


def crush_prep_line(line):
    if "#" in line:
        line = line.split("#", 1)[0]
    return line.strip()


def iter_buckets(crushmap_txt_f):
    bucket_lines = None
    bucket_start_re = re.compile(r"(?P<type>[^ ]+)\s+(?P<name>[^ ]+)\s*\{$")

    bucket_type = None
    bucket_name = None
    in_bucket = False

    for line in open(crushmap_txt_f):
        line = crush_prep_line(line)
        if in_bucket:
            if line == '}':
                in_bucket = False
                yield bucket_name, bucket_type, bucket_lines
                bucket_lines = None
            else:
                bucket_lines.append(line)
        else:
            rr = bucket_start_re.match(line)
            if rr:
                in_bucket = True
                bucket_type = rr.group('type')
                bucket_name = rr.group('name')
                bucket_lines = []


def load_crushmap(crushmap_txt_f):
    roots = []
    osd_re = re.compile(r"osd\.\d+$")
    id_line_re = re.compile(r"id\s+-?\d+$")
    item_line_re = re.compile(r"item\s+(?P<name>[^ ]+)\s+weight\s+(?P<weight>[0-9.]+)$")
    nodes_map = {}

    for name, type, lines in iter_buckets(crushmap_txt_f):
        node = Node(None, name, type, None)

        for line in lines:
            if id_line_re.match(line):
                node.id = int(line.split()[1])
            else:
                item_rr = item_line_re.match(line)
                if item_rr:
                    node_name = item_rr.group('name')
                    weight = float(item_rr.group('weight'))

                    # append OSD child
                    if osd_re.match(node_name):
                        node_id = int(node_name.split(".")[1])
                        ch_node = Node(node_id, node_name, "osd", weight)
                        nodes_map[node_name] = ch_node
                    else:
                        # append child of other type (must be described already in file)
                        ch_node = nodes_map[node_name].copy()
                        ch_node.weight = weight

                    node.childs.append(ch_node)

        if type == 'root':
            roots.append(node)

        nodes_map[node.name] = node

    crush = Crush(nodes_map, roots)
    crush.build_search_idx()
    return crush


default_zone_order = ["osd", "host", "chassis", "rack", "row", "pdu",
                      "pod", "room", "datacenter", "region", "root"]


def is_rebalance_complete(allowed_states=("active+clean", "active+remapped")):
    pg_stat = json.loads(check_output("ceph pg stat --format=json"))
    if 'num_pg_by_state' in pg_stat:
        for dct in pg_stat['num_pg_by_state']:
            if dct['name'] not in allowed_states and dct["num"] != 0:
                return False
    else:
        ceph_state = json.loads(check_output("ceph -s --format=json"))
        for pg_stat_dict in ceph_state['pgmap']['pgs_by_state']:
            if pg_stat_dict['state_name'] not in allowed_states and pg_stat_dict['count'] != 0:
                return False
    return True


def request_weight_update(node, new_weight):
    cmd = "ceph osd crush set {name} {weight} {path}"
    path_s = " ".join("{0}={1}".format(tp, name) for tp, name in node.full_path)
    cmd = cmd.format(name=node.name, weight=new_weight, path=path_s)

    if not BE_QUIET:
        print(cmd)

    check_output(cmd)


def request_reweight_update(node, new_weight):
    osd_id = int(node.name.split('.')[1])
    cmd = "ceph osd reweight {0} {1}".format(osd_id, new_weight)

    if not BE_QUIET:
        print(cmd)

    check_output(cmd)


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


def load_all_data(args):
    if not args.osd_map:
        osd_map_f = tmpnam()
        check_output("ceph osd getmap -o {0}".format(osd_map_f))
    else:
        osd_map_f = args.osd_map

    crushmap_bin_f = tmpnam()
    check_output("osdmaptool --export-crush {0} {1}".format(crushmap_bin_f, osd_map_f))

    crushmap_txt_f = tmpnam()
    check_output("crushtool -d {0} -o {1}".format(crushmap_bin_f, crushmap_txt_f))

    if not args.osd_tree:
        if args.offline:
            print("Can't get osd's reweright in offline mode as --osd-tree is not passed from CLI")
            osd_tree_js = None
        else:
            osd_tree_js = check_output("ceph osd tree --format=json")
    else:
        osd_tree_js = open(args.osd_tree).read()

    curr_reweight = {}
    if osd_tree_js:
        for node in json.loads(osd_tree_js)['nodes']:
            if node['type'] == 'osd':
                curr_reweight[node['name']] = node.get('reweight', 1.0)

    crush = load_crushmap(crushmap_txt_f)
    return crush, curr_reweight, crushmap_bin_f, osd_map_f


class Config(object):
    def __init__(self):
        self.max_nodes_per_round = None
        self.max_weight_change = None
        self.max_reweight_change = None
        self.min_weight_diff = None
        self.min_reweight_diff = None
        self.rebalance_nodes = []
        self.reweight_nodes = []
        self.total_weight_change = 0.0
        self.total_reweight_change = 0.0


def prepare_update_config(config_dict, crush, curr_reweight):
    config = Config()

    config.max_nodes_per_round = config_dict.get('max_updated_nodes', 4)
    config.max_weight_change = config_dict.get('step', 0.5)
    config.max_reweight_change = config_dict.get('restep', 0.1)
    config.min_weight_diff = config_dict.get('min_weight_diff', 0.01)
    config.min_reweight_diff = config_dict.get('min_reweight_diff', 0.01)

    new_osd_reweights = {}

    for node in config_dict['osds']:
        node = node.copy()
        if 'weight' not in node and 'reweight' not in node:
            print("Node with osd {0!r} has neither weight no reweight. Fix config and restart".format(node))
            return None

        new_weight = node.pop('weight', None)
        new_reweight = node.pop('reweight', None)

        if new_weight is not None:
            path = list(node.items())
            path.sort(key=lambda x: -default_zone_order.index(x[0]))
            path_s = "/".join("{0}={1}".format(tp, name) for tp, name in path)
            try:
                cnode = crush.find_node(path)
            except IndexError as exc:
                print("Fail to find node {0}: {1}".format(path_s, exc))
                return None

            diff = abs(new_weight - cnode.weight)
            if diff < config.min_weight_diff:
                print(("Skip weight update for {0} as requested diff({1:.4f}) " +
                       "is less than min diff({2:.4f})").format(cnode, diff, config.min_weight_diff))
            else:
                config.rebalance_nodes.append((cnode, new_weight))
                config.total_weight_change += diff
                print(cnode.str_path(), "weight =", cnode.weight, "=>", new_weight)

        if new_reweight is not None:
            osd_name = node['osd']
            if osd_name not in curr_reweight:
                print(("No reweight coeficient available for {0}. " +
                       "Can't apply reweight parameter from config").format(osd_name))
                return None

            if new_osd_reweights.get(node['osd'], new_reweight) != new_reweight:
                print(("{0} has different reweight in different tree parts in config." +
                       "Impossible to apply this configuration").format(osd_name))
                return None

            diff = abs(new_reweight - curr_reweight[osd_name])
            if diff < config.min_reweight_diff:
                print(("Skip weight update for {0} as requested diff({1:.4f}) " +
                       "is less than min diff({2:.4f})").format(osd_name, diff, config.min_weight_diff))
            else:
                new_osd_reweights[osd_name] = new_reweight
                config.total_reweight_change += diff
                print(osd_name, "Reweigh =", curr_reweight[osd_name], "=>", new_reweight)

    config.reweight_nodes = [(FakedNode(name=osd_name, weight=curr_reweight[osd_name]), new_reweight)
                             for osd_name, new_reweight in new_osd_reweights.items()]
    return config


def do_rebalance(config_dict, args):
    crush, curr_reweight, crushmap_bin_f, osd_map_f = load_all_data(args)
    config = prepare_update_config(config_dict, crush, curr_reweight)

    if config is None:
        return 1

    if not (config.rebalance_nodes or config.reweight_nodes):
        print("Nothing to change")
        return 0
    elif not BE_QUIET:
        if config.total_weight_change > 0:
            print("Total sum of all weight changes = {0:.2f}".format(config.total_weight_change))
        if config.total_reweight_change > 0:
            print("Total sum of all reweight changes = {0:.2f}".format(config.total_reweight_change))

    # --------------------  DO ESTIMATION ------------------------------------------------------------------------------

    if not args.no_estimate:
        if config.total_reweight_change != 0.0:
            print("Warning: can't estimate reweight results! Estimation only includes weight changes!")

        if config.total_weight_change == 0:
            print("No weight is changes. No PG/data would be moved")
        else:
            cmd_templ = "crushtool -i {crush_map_f} -o {crush_map_f} --update-item {id} {weight} {name} {loc}"
            for node, new_weight in config.rebalance_nodes:
                loc = " ".join("--loc {0} {1}".format(tp, name) for tp, name in node.full_path)
                cmd = cmd_templ.format(crush_map_f=crushmap_bin_f, id=node.id, weight=new_weight,
                                       name=node.name, loc=loc)
                check_output(cmd)

            osd_map_new_f = tmpnam()
            shutil.copy(osd_map_f, osd_map_new_f)
            check_output("osdmaptool --import-crush {0} {1}".format(crushmap_bin_f, osd_map_new_f))

            if args.offline and not args.pg_dump:
                print("Can't calculate pg/data movement in offline mode if no pg dump provided")
            else:
                osd_changes = calculate_remap(osd_map_f, osd_map_new_f, pg_dump_f=args.pg_dump)

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
            return 0

    # --------------------  UPDATE CLUSTER -----------------------------------------------------------------------------

    if args.offline:
        print("No updates would be made in offline mode")
        return 1

    already_changed = 0.0
    requested_changes = config.total_weight_change + config.total_reweight_change

    wait_rebalance_to_complete(False)

    round = 0
    while config.rebalance_nodes or config.reweight_nodes:
        if not BE_QUIET:
            if already_changed > config.min_weight_diff:
                print("Done {0}%".format(int(already_changed * 100.0 / requested_changes + 0.5)))

        is_reweight_round = (round % 2 == 1 or not config.rebalance_nodes)
        if is_reweight_round:
            active_list = config.reweight_nodes
            max_change = config.max_reweight_change
            min_diff = config.min_reweight_diff
            upd_func = request_reweight_update
        else:
            active_list = config.rebalance_nodes
            max_change = config.max_weight_change
            min_diff = config.min_weight_diff
            upd_func = request_weight_update

        next_nodes = active_list[:config.max_nodes_per_round]
        del active_list[:config.max_nodes_per_round]

        for node, coef in next_nodes:
            change = coef - node.weight

            if abs(change) > max_change + min_diff:
                change = math.copysign(max_change, change)

            node.weight += change
            upd_func(node, node.weight)

            if abs(node.weight - coef) > min_diff:
                active_list.append((node, coef))

            already_changed += abs(change)

        wait_rebalance_to_complete(True)
        round += 1

    return 0


help = """Gently change OSD's weight cluster in cluster.

Config file example(yaml):

# max weight change step
step: 0.1

# max reweight change step
restep: 0.1

# max OSD reweighted in parallel
max_updated_nodes: 4

# minimal weight difference to be corrected
min_weight_diff: 0.01

# minimal reweight difference to be corrected
min_reweight_diff: 0.01

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
    weight: 0.3

  # for reweight need only OSD name
  - osd: osd.1
    reweight: 0.95
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

    parser.add_argument("-o", "--osd-map", metavar="FILE", help="Pass OSD map from CLI")
    parser.add_argument("-p", "--pg-dump", metavar="FILE", help="Pass PG map json file from CLI")
    parser.add_argument("-t", "--osd-tree", metavar="FILE", help="Pass osd tree json file from CLI")
    parser.add_argument("-f", "--offline", action='store_true', help="Don't ask ceph cluster for any data")

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
    return do_rebalance(cfg, args)


if __name__ == "__main__":
    exit(main(sys.argv))
