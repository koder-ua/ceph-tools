from __future__ import print_function

import os
import sys
import time
import json
import math
import shutil
import os.path
import logging
import argparse
import logging.config

import yaml


from cephlib.common import run_locally, tmpnam
from cephlib.units import b2ssize
from cephlib.crush import load_crushmap
from calculate_remap import calculate_remap, get_osd_curr


logger = logging.getLogger("ceph.rebalance")


class FakedNode(object):
    def __init__(self, **attrs):
        self.__dict__.update(attrs)


default_zone_order = ["osd", "host", "chassis", "rack", "row", "pdu",
                      "pod", "room", "datacenter", "region", "root"]


def is_rebalance_complete(allowed_states=("active+clean", "active+remapped")):
    pg_stat = json.loads(run_locally("ceph pg stat --format=json").decode('utf8'))
    if 'num_pg_by_state' in pg_stat:
        for dct in pg_stat['num_pg_by_state']:
            if dct['name'] not in allowed_states and dct["num"] != 0:
                return False
    else:
        ceph_state = json.loads(run_locally("ceph -s --format=json").decode('utf8'))
        for pg_stat_dict in ceph_state['pgmap']['pgs_by_state']:
            if pg_stat_dict['state_name'] not in allowed_states and pg_stat_dict['count'] != 0:
                return False
    return True


def request_weight_update(node, new_weight):
    path_s = " ".join("{0}={1}".format(tp, name) for tp, name in node.full_path)
    cmd = "ceph osd crush set {name} {weight} {path}".format(name=node.name, weight=new_weight, path=path_s)
    run_locally(cmd)


def request_reweight_update(node, new_weight):
    osd_id = int(node.name.split('.')[1])
    run_locally("ceph osd reweight {0} {1}".format(osd_id, new_weight))


def wait_rebalance_to_complete(any_updates, sleep_interwal=2):
    if not any_updates:
        if is_rebalance_complete():
            return

    logger.debug("Waiting for cluster to complete rebalance")

    if any_updates:
        time.sleep(sleep_interwal)

    while not is_rebalance_complete():
        sleep_interwal *= 1.5
        time.sleep(sleep_interwal)
        logger.debug("Waiting for cluster to complete rebalance")


def load_all_data(opts, no_cache=False):
    if no_cache or not opts.osd_map:
        if opts.offline:
            logger.error("Can't get osd's map in offline mode as --osd-map is not passed from CLI")
            return None, None, None, None
        else:
            osd_map_f = tmpnam()
            run_locally("ceph osd getmap -o {0}".format(osd_map_f))
    else:
        osd_map_f = opts.osd_map

    crushmap_bin_f = tmpnam()
    run_locally("osdmaptool --export-crush {0} {1}".format(crushmap_bin_f, osd_map_f))

    crushmap_txt_f = tmpnam()
    run_locally("crushtool -d {0} -o {1}".format(crushmap_bin_f, crushmap_txt_f))

    if no_cache or not opts.osd_tree:
        if opts.offline:
            logger.warning("Can't get osd's reweright in offline mode as --osd-tree is not passed from CLI")
            osd_tree_js = None
        else:
            osd_tree_js = run_locally("ceph osd tree --format=json").decode("utf8")
    else:
        osd_tree_js = open(opts.osd_tree).read()

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
            logger.error("Node with osd %r has neither weight no reweight. Fix config and restart", node)
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
                logger.error("Fail to find node %s: %s", path_s, exc)
                return None

            diff = abs(new_weight - cnode.weight)
            if diff < config.min_weight_diff:
                logger.info("Skip %s as requested weight diff %.2f is less than %.2f",
                            cnode.str_path(), diff, config.min_weight_diff)
            else:
                config.rebalance_nodes.append((cnode, new_weight))
                config.total_weight_change += diff
                logger.info("%s weight = %s => %s", cnode.str_path(), cnode.weight, new_weight)

        if new_reweight is not None:
            osd_name = node['osd']
            if osd_name not in curr_reweight:
                logger.error("No reweight coeficient available for %s. Can't apply reweight parameter from config",
                             osd_name)
                return None

            if new_osd_reweights.get(node['osd'], new_reweight) != new_reweight:
                logger.error("%s has different reweight in different tree parts in config." +
                             "Impossible to apply this configuration", osd_name)
                return None

            diff = abs(new_reweight - curr_reweight[osd_name])
            if diff < config.min_reweight_diff:
                logger.info("Skip reweighting %s as requested diff %.3f is less than %.3f",
                            osd_name, diff, config.min_reweight_diff)
            else:
                new_osd_reweights[osd_name] = new_reweight
                config.total_reweight_change += diff
                logger.info("%s Reweigh = %s => %s", osd_name, curr_reweight[osd_name], new_reweight)

    config.reweight_nodes = [(FakedNode(name=osd_name, weight=curr_reweight[osd_name]), new_reweight)
                             for osd_name, new_reweight in new_osd_reweights.items()]
    return config


def do_rebalance(config_dict, opts):
    crush, curr_reweight, crushmap_bin_f, osd_map_f = load_all_data(opts)
    if crush is None:
        return 1

    config = prepare_update_config(config_dict, crush, curr_reweight)

    if config is None:
        return 1

    expected_weight_results = dict((tuple(osd.full_path), osd.weight)
                                   for osd in crush.iter_nodes('osd'))
    expected_weight_results.update((tuple(node.full_path), new_weight)
                                   for node, new_weight in config.rebalance_nodes)

    expected_reweight_results = curr_reweight.copy()
    expected_reweight_results.update(config.reweight_nodes)

    if not (config.rebalance_nodes or config.reweight_nodes):
        logger.info("Nothing to change")
        return 0

    if config.total_weight_change > 0:
        logger.info("Total sum of all weight changes = %.2f", config.total_weight_change)
    if config.total_reweight_change > 0:
        logger.info("Total sum of all reweight changes = %.2f", config.total_reweight_change)

    # --------------------  DO ESTIMATION ------------------------------------------------------------------------------

    if not opts.no_estimate:
        if config.total_reweight_change != 0.0:
            logger.warning("Can't estimate reweight results! Estimation only includes weight changes!")

        if config.total_weight_change == 0:
            logger.info("No weight is changes. No PG/data would be moved")
        else:
            cmd_templ = "crushtool -i {crush_map_f} -o {crush_map_f} --update-item {id} {weight} {name} {loc}"
            for node, new_weight in config.rebalance_nodes:
                loc = " ".join("--loc {0} {1}".format(tp, name) for tp, name in node.full_path)
                cmd = cmd_templ.format(crush_map_f=crushmap_bin_f, id=node.id, weight=new_weight,
                                       name=node.name, loc=loc)
                run_locally(cmd)

            osd_map_new_f = tmpnam()
            shutil.copy(osd_map_f, osd_map_new_f)
            run_locally("osdmaptool --import-crush {0} {1}".format(crushmap_bin_f, osd_map_new_f))

            if opts.offline and not opts.pg_dump:
                logger.warning("Can't calculate pg/data movement in offline mode if no pg dump provided")
            else:
                osd_changes = calculate_remap(osd_map_f, osd_map_new_f, pg_dump_f=opts.pg_dump)

                total_send = 0
                total_moved_pg = 0

                for osd_id, osd_change in sorted(osd_changes.items()):
                    total_send += osd_change.bytes_in
                    total_moved_pg += osd_change.pg_in

                logger.info("Total bytes to be moved : %sB", b2ssize(total_send))
                logger.info("Total PG to be moved  : %s", total_moved_pg)

                if opts.show_after:
                    osd_curr = get_osd_curr()
                    for osd_id, osd_change in sorted(osd_changes.items()):
                        pg_diff = osd_change.pg_in - osd_change.pg_out
                        bytes_diff = osd_change.bytes_in - osd_change.bytes_out
                        logger.info("OSD {0}, PG {1:>4d} => {2:>4d},  bytes {3:>6s} => {4:>6s}".format(
                                    osd_id, osd_curr[osd_id].pg,
                                    osd_curr[osd_id].pg + pg_diff,
                                    b2ssize(osd_curr[osd_id].bytes),
                                    b2ssize(osd_curr[osd_id].bytes + bytes_diff)))

        if opts.estimate_only:
            return 0

    # --------------------  UPDATE CLUSTER -----------------------------------------------------------------------------

    if opts.offline:
        logger.error("No updates would be made in offline mode")
        return 1

    logger.info("Start updating the cluster")

    already_changed = 0.0
    requested_changes = config.total_weight_change / config.max_weight_change + \
                        config.total_reweight_change / config.max_reweight_change

    wait_rebalance_to_complete(False)

    round = 0
    while config.rebalance_nodes or config.reweight_nodes:
        if already_changed > config.min_weight_diff:
            logger.info("Done %s%%", int(already_changed * 100.0 / requested_changes + 0.5))

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

            already_changed += abs(change) / max_change

        wait_rebalance_to_complete(True)
        round += 1

    # ------------------- CHECK RESULTS --------------------------------------------------------------------------------

    if not opts.verify:
        return 0

    logger.info("Verifying results")
    crush, curr_reweight, crushmap_bin_f, osd_map_f = load_all_data(opts, no_cache=True)
    failed = False

    for node in crush.iter_nodes('osd'):
        path = tuple(node.full_path)
        if path not in expected_weight_results:
            logger.error("Can't found osd %s in expected results", node.str_path())
            failed = True
            continue

        if abs(expected_weight_results[path] - node.weight) > config.min_weight_diff:
            logger.error("osd %s has wrong weight %.4f expected is %.4f",
                         node.str_path(), node.weight, expected_weight_results[path])
            failed = True
        del expected_weight_results[path]

    for path in expected_weight_results:
        logger.error("osd %s missed in new crush", path)
        failed = True

    for osd_name, curr_rw in curr_reweight.items():
        if osd_name not in expected_reweight_results:
            logger.error("Can't found osd %s in reweight expected results", osd_name)
            failed = True
            continue

        if abs(expected_reweight_results[osd_name] - curr_rw) > config.min_reweight_diff:
            logger.error("osd %s has wrong reweight %.4f expected is %.4f",
                         osd_name, curr_rw, expected_reweight_results[osd_name])
            failed = True
        del expected_reweight_results[osd_name]

    for osd_name in expected_reweight_results:
        logger.error("osd %s missed in new crush", osd_name)
        failed = True

    if not failed:
        logger.info("Succesfully verifyed")
        return 0

    return 1


help = """python3 rebalance.py [OPTS] config_file.yaml

Gently change OSD's weight cluster in cluster.

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
    parser.add_argument("-v", "--verify", action='store_true', help="Check cluster state after rebalance")
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
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default=None, help="Console log level")

    parser.add_argument("config", help="Yaml rebalance config file")
    opts = parser.parse_args(argv[1:])

    lconf_path = os.path.join(os.path.dirname(__file__), 'logging.json')
    log_config = json.load(open(lconf_path))

    if opts.log_level is not None:
        log_config["handlers"]["console"]["level"] = opts.log_level

    logging.config.dictConfig(log_config)

    if opts.estimate_only and opts.no_estimate:
        logger.error("-e/--estimate-only is incompatible with -n/--no-estimate\n")
        return None

    if opts.show_after and opts.no_estimate:
        logger.error("-s/--show-after is incompatible with -n/--no-estimate\n")
        return None

    return opts


def main(argv):
    opts = parse_args(argv)

    if not opts:
        return 1

    cfg = yaml.load(open(opts.config).read())
    return do_rebalance(cfg, opts)


if __name__ == "__main__":
    exit(main(sys.argv))
