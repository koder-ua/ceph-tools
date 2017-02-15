from __future__ import print_function

import re
import os
import sys
import time
import json
import glob
import Queue
import pickle
import anydbm
import argparse
import traceback
import functools
import threading
# import subprocess
import contextlib
import collections
from datetime import datetime

from ceph_daemon import admin_socket


Stage = collections.namedtuple("Stage", ("name", "time"))

#
# digraph "ceph_request" {
#     initiated -> queued_for_pg  [label="data read from socket and put info queue"];
#     queued_for_pg -> reached_pg [label="op fetched from queue"];
#     reached_pg -> started [labe="op get all locks and processing started"];
#     started -> "waiting for subops from 1,2" [label="request to replica is send"];
#     "waiting for subops from 1,2" -> commit_queued_for_journal_write [label="???"];
#     "waiting for subops from 1,2" -> "sub_op_commit_rec from 1" [label="replica 1 done"];
#     "waiting for subops from 1,2" -> "sub_op_commit_rec from 2" [label="replica 2 done"];
#     write_thread_in_journal_buffer -> journaled_completion_queued;
#     journaled_completion_queued -> commit_sent -> journaled_completion_queued;
#     op_commit -> op_applied [label="data written to main storage buffer"];
#     op_applied -> done [label="data written(commited to disk?) to main storage?"];
# }


class OSDOp(object):
    expected_stages_order = [
              "initiated",
              "queued_for_pg",
              "reached_pg",
              "started",
              "waiting for subops from 1,2",
              "commit_queued_for_journal_write",
              "write_thread_in_journal_buffer",
              "op_commit",
              "op_applied",
              "done"]

    sub_op_key_map = {"sub_op_commit_rec from 1": "1st replica done",
                      "sub_op_commit_rec from 2": "2nd replica done"}

    extra_steps = {"commit_sent", "journaled_completion_queued"}

    skip_order = extra_steps | set(sub_op_key_map)


    result_order = [
        'pg_wait',
        'send_to_replica',
        'commit_queued_for_journal_write',
        'write_thread_in_journal_buffer',
        "op_commit",
        "1st replica done",
        "2nd replica done",
        "journaled_completion_queued",
        "commit_sent",
        "op_applied",
        "done"
    ]

    def __init__(self, client, object, op_type, start_time, stages):
        self.client = client
        self.object = object
        self.op_type = op_type
        self.start_time = start_time
        self.stages = stages

    def to_op_times(self):
        # main OSD stages
        main_osd_stages = [stage for stage in self.stages if stage.name not in self.skip_order]

        for stage, expected_name in zip(main_osd_stages, self.expected_stages_order):
            assert stage.name == expected_name, ",".join(stage.name for stage in main_osd_stages)

        if len(self.stages) < 3:
            return {}

        smap = {stage.name: stage.time for stage in self.stages}
        res = {'pg_wait': smap['started']}

        rep_send = smap.get("waiting for subops from 1,2")
        if rep_send is not None:
            res['send_to_replica'] = rep_send - smap['started']

            for name, res_key in self.sub_op_key_map.items():
                if name in smap:
                    res[res_key] = smap[name] - rep_send

            send_done_idx = self.expected_stages_order.index("waiting for subops from 1,2")
            ptime = smap["waiting for subops from 1,2"]
            for stage_name in self.expected_stages_order[send_done_idx + 1:]:
                if stage_name in smap:
                    res[stage_name] = smap[stage_name] - ptime
                    ptime = smap[stage_name]
                else:
                    break

            if 'commit_send' in smap:
                dtime = smap["op_commit"] - smap['commit_send']
                assert dtime >= 0
                res["send_commit_ack"] = dtime

            if "journaled_completion_queued" in smap:
                dtime = smap["journaled_completion_queued"] - smap['write_thread_in_journal_buffer']
                assert dtime >= 0
                res["journal_commit"] = dtime

        return res

    def __str__(self):
        res = "{0.__class__.__name__}({0.client}=>{0.object}, {1}):\n".format(self, ",".join(self.op_type))
        return res + "\n".join("  {0.time:>7d} {0.name}".format(stage) for stage in self.stages)

    def __repr__(self):
        return str(self)


def to_ctime_ms(time_str):
    dt_s, micro_sec = time_str.split('.')
    dt = datetime.strptime(dt_s, '%Y-%m-%d %H:%M:%S')
    return int(time.mktime(dt.timetuple()) * 1000000 + int(micro_sec))


rr = r"osd_op\((?P<client_id>client[^\t ]*?)\s+" + \
     r"(?P<pool>\d+).(?P<PG>[a-f0-9]+)" + \
     r"\s+(?P<object>.*?)\s+" + \
     r"\[[^\]]*\][^\[]*\[[^\]]*\]" + \
     r"\s+(?P<op_decr>[a-z+_]*)\s+.*?\)"

descr_rr = re.compile(rr)


def parse_op(op_js_data):
    descr = op_js_data['description'].encode("utf8")
    if not descr.startswith("osd_op"):
        return

    op_type_rr = descr_rr.match(descr)

    if op_type_rr is None:
        # print("Can't parse description line:\n{!r}".format(descr))
        return

    op_type = op_type_rr.group("op_decr").split("+")
    client = op_type_rr.group("client_id")
    object_name = op_type_rr.group("object")

    stages = []
    _, _, stages_json = op_js_data["type_data"]

    stime = to_ctime_ms(op_js_data['initiated_at'])

    for stage in stages_json:
        if stage['event'] != 'initiated_at':
            stages.append(Stage(stage['event'].encode("utf8"), to_ctime_ms(stage['time']) - stime))

    return OSDOp(client, object_name, op_type, stime, stages)


def collect_disks_usage():
    return open("/proc/diskstats").read()


def osd_exec(osd_id, args, cluster='ceph'):
    asok = "/var/run/ceph/{}-osd.{}.asok".format(cluster, osd_id)
    return admin_socket(asok, args.split(" "))
    # return subprocess.check_output("ceph daemon {} {}".format(asok(osd_id, cluster), args), shell=True)


def collect_historic_ops(osd_id, cluster='ceph'):
    return osd_exec(osd_id, "dump_historic_ops", cluster=cluster)
    # data = json.loads()
    # already_seen = set()
    #
    # for osd_op_js in data['Ops']:
    #     osd_op = parse_op(osd_op_js)
    #     if osd_op:
    #         op_id = (osd_op.client, osd_op.object, osd_op.start_time)
    #         if op_id not in already_seen:
    #             already_seen.add(op_id)
    #             yield osd_op


def collect_current_ops(osd_id, cluster='ceph'):
    return osd_exec(osd_id, "dump_ops_in_flight", cluster=cluster)

    # data = json.loads()
    #
    # for op in data['ops']:
    #     op_obj = parse_op(op)
    #     if op_obj:
    #         yield op_obj


def collect_perf(osd_id, cluster='ceph'):
    return osd_exec(osd_id, "perf dump", cluster=cluster)


def find_all_osd(cluster_name='ceph'):
    for fname in glob.glob("/var/run/ceph/{}-osd.*.asok".format(cluster_name)):
        yield os.path.basename(fname).split('.')[1]


def calc_stats(ops):
    time_per_stage = collections.defaultdict(int)
    stage_appears_count = collections.defaultdict(int)
    for op in ops:
        for name, op_time in op.to_op_times().items():
            time_per_stage[name] += op_time
            stage_appears_count[name] += 1

    for name in time_per_stage:
        time_per_stage[name] /= stage_appears_count[name]

    return time_per_stage


def show_stats(db_name, op_tp, osd_id=None):
    prefix_re = re.compile("{}.osd-{}::".format(op_tp, '.*' if osd_id is None else osd_id))
    db = anydbm.open(db_name, 'r')
    with contextlib.closing(db):
        osd_ops = [pickle.loads(val) for key, val in db.items() if prefix_re.match(key)]
        stats = calc_stats(osd_ops)
        for name in OSDOp.result_order:
            if name in stats:
                print("{:<40s}  {:>8d}".format(name, stats[name] / 1000))
    return 0


def worker(tag, func, timeout, oq, end_time):
    ctime = time.time()
    try:
        while ctime < end_time:
            oq.put((int(ctime * 1000), tag, func()))
            sleep_for = (ctime + timeout) - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            ctime = time.time()

        oq.put((None, True, None))
    except Exception:
        traceback.print_exc()
        oq.put((None, False, None))


def show_online(res_q, interval):
    pass


# can we store to DB directly from data collect threads?
def store_to_db(res_q, dbpath, th_count):
    db = anydbm.open(dbpath, "c")
    with contextlib.closing(db):
        while th_count != 0:
            ctime, tag, res = res_q.get()
            if ctime is None:  # mean that threads ends
                if not tag:  # mean that thread failed
                    return 1
                th_count -= 1
            else:
                db["{}::{}".format(tag, ctime)] = res
    return 0


def print_results(res_q, th_count):
    while th_count != 0:
        ctime, tag, res = res_q.get()
        if ctime is None:
            if not tag:
                return 1
            th_count -= 1
        else:
            print("{} - {} - {}".format(ctime, tag, res))
    return 0


def set_osd_historic(duration, keep, osd_id, cluster="ceph"):
    data = json.loads(osd_exec(osd_id, "dump_historic_ops", cluster=cluster))
    osd_exec(osd_id, "config set osd_op_history_duration {}".format(duration), cluster=cluster)
    osd_exec(osd_id, "config set osd_op_history_size {}".format(keep), cluster=cluster)
    return (data["duration to keep"], data["num to keep"])


def get_argparser():
    descr = "Collect ceph performance info"
    parser = argparse.ArgumentParser(prog='collect', description=descr)
    subparsers = parser.add_subparsers(dest='subparser_name')

    collect = subparsers.add_parser('collect', help='Collect data from running cluster')
    collect.add_argument("-c", "--cluster", default="ceph", help="Ceph cluster name")
    collect.add_argument("--db", default=None, help="Store into file in binary format")
    collect.add_argument("-r", "--run-time", type=int, default=60, help="Data collect inteval in seconds")
    collect.add_argument("-t", "--timeout", type=int, default=500, help="Collect timeout in ms")
    collect.add_argument("-p", "--prepare-for-historic", action="store_true",
                         help="Prepare OSD for reliable historic ops collection")
    collect.add_argument("osdids", nargs='*', help="OSD id's list or '*' to monitor all")

    info = subparsers.add_parser('info', help='Show basic db info')
    info.add_argument("db", help="Path to databse")

    stat = subparsers.add_parser('stat', help='Show stat for db')
    stat.add_argument("-i", "--osd-id", type=int, default=None, help="Show only event from selected osd")
    stat.add_argument("type", choices=("ops", "historic"), help="Event to stat")
    stat.add_argument("db", help="Path to databse")

    return parser


def collect(opts):
    if opts.osdids == ['*']:
        osd_ids = list(find_all_osd())
    else:
        if '*' in opts.osdids:
            print("* should be the only one osd id")
            return 1
        osd_ids = opts.osdids

    osd_historic_params = {}
    if opts.prepare_for_historic:
        for osd_id in osd_ids:
            osd_historic_params[osd_id] = set_osd_historic(2, 200, osd_id, opts.cluster)

    try:
        etime = time.time() + opts.run_time

        res_q = Queue.Queue()
        threads = []

        for osd_id in osd_ids:
            func = functools.partial(collect_current_ops, osd_id)
            th = threading.Thread(target=worker,
                                  args=('ops.osd-{}'.format(osd_id), func, opts.timeout / 1000.0, res_q, etime))
            threads.append(th)

        for osd_id in osd_ids:
            func = functools.partial(collect_historic_ops, osd_id)
            th = threading.Thread(target=worker,
                                  args=('historic.osd-{}'.format(osd_id), func, opts.timeout / 1000.0, res_q, etime))
            threads.append(th)

        th = threading.Thread(target=worker,
                              args=('diskstats', collect_disks_usage, opts.timeout / 1000.0, res_q, etime))
        threads.append(th)

        for osd_id in osd_ids:
            func = functools.partial(collect_perf, osd_id)
            th = threading.Thread(target=worker,
                                  args=('perf.osd-{}'.format(osd_id), func, opts.timeout / 1000.0, res_q, etime))
            threads.append(th)

        for th in threads:
            th.daemon = True
            th.start()

        if opts.db is not None:
            return store_to_db(res_q, opts.db, len(threads))
        else:
            return print_results(res_q, len(threads))
    finally:
        for osd_id, (duration, keep) in osd_historic_params.items():
            osd_historic_params[osd_id] = set_osd_historic(duration, keep, osd_id, opts.cluster)


def main(argv):
    parser = get_argparser()
    opts = parser.parse_args(argv[1:])

    if opts.subparser_name == 'collect':
        return collect(opts)
    elif opts.subparser_name == 'stat':
        return show_stats(opts.db, opts.type, opts.osd_id)
    else:
        raise NotImplementedError()


if __name__ == "__main__":
    exit(main(sys.argv))
