import os
import sys
import json
import socket
import select
import logging
import argparse


logger = logging.getLogger('net_checker')


class Daemonizator(object):
    def __init__(self, working_dir, stdout, stderr):
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr
        self.wpipe = None

    def two_fork(self):
        try:
            pid = os.fork()
            if pid > 0:
                # return to parent
                return False
        except OSError as e:
            sys.stderr.write("fork #1 failed: {0} ({1})\n".format(e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.setsid()

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from 1st children parent
                # use os._exit to aviod calling atexit functions
                os._exit(0)
        except OSError as e:
            sys.stderr.write("fork #2 failed: {0} ({1})\n".format(e.errno, e.strerror))
            sys.exit(1)

        os.chdir(self.working_dir)
        os.umask(0)
        return True

    def redirect_streams(self):
        # redirect standard file descriptors
        mode = os.O_CREAT | os.O_APPEND | os.O_WRONLY
        if self.stdout == self.stderr:
            stdout_fd = stderr_fd = os.open(self.stdout, mode)
        else:
            stdout_fd = os.open(self.stdout, mode)
            stderr_fd = os.open(self.stderr, mode)

        sys.stdout.flush()
        sys.stderr.flush()

        os.close(sys.stdin.fileno())
        os.close(sys.stdout.fileno())
        os.close(sys.stderr.fileno())

        os.dup2(stdout_fd, sys.stdout.fileno())
        os.dup2(stderr_fd, sys.stderr.fileno())

    def daemonize(self):
        rpipe, self.wpipe = os.pipe()

        if not self.two_fork():
            os.close(self.wpipe)

            data = b""

            while True:
                ndata = os.read(rpipe, 1024)
                if not ndata:
                    break
                data += ndata

            os.close(rpipe)
            return False, json.loads(data.decode("utf8"))

        os.close(rpipe)
        self.redirect_streams()
        return True, None

    def daemon_ready(self, server_data):
        os.write(self.wpipe, json.dumps(server_data).encode("utf8"))
        os.close(self.wpipe)

    def exit_parent(self):
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


# {
#     'nodes': [
#         {
#             'hostnames': ['x1', 'x2'],
#             'nets': [
#                 [[ip1, ip2, ...], mask, mtu],
#             ]
#         },
#     ]
# }


class ClientData(object):
    def __init__(self, addr):
        self.addr = addr
        self.data = ""

    def ready(self):
        return '\x00' in self.data


def server_main(opts):
    # open tcp socket
    # wait until all clients connect
    # gather info and compare it to config
    # send test config
    # wait for results
    # show results

    config = json.loads(open(opts.config, 'rt').read())

    clients = set(config['nodes'])
    total_clients = len(config['nodes'])

    ss = socket.socket()
    ss.bind((opts.ip, opts.port))
    ss.listen(10)
    ss.setblocking(False)
    client_data = {}
    read_socks = [ss]
    clients_ready = 0

    logger.info("Server start listening on %s:%s", opts.ip, opts.port)

    for i in range(opts.wait_for_client):
        rready, _, x = select.select(read_socks, [], [ss], timeout=1)
        for rdy_sock in rready:
            if rdy_sock is ss:
                new_client, addr = ss.accept()
                new_client.setblocking(False)
                client_data[new_client] = ClientData(addr)
                read_socks.append(new_client)
            else:
                client_data[rdy_sock].data += rdy_sock.recv()
                if client_data[rdy_sock].ready():
                    clients_ready += 1
                    if clients_ready == total_clients:
                        break

    good_clients = {}
    client_ips = []
    for sock, client in client_data.items():
        if not client.ready():
            logger.error("Client %s has not send config in time. Ignore it", client.addr)
            sock.close()
        else:
            good_clients[sock] = client
        client_ips.append(client.addr)

    if len(client_ips) != len(clients):
        all_nodes = config['nodes'].copy()
        ip2node = {}

        for node in all_nodes:
            for ips, _, _ in node['nets']:
                for ip in ips:
                    ip2node[ip] = node

        for ip in client_ips:
            if ip not in ip2node:
                logger.warning("Unexpected client %r", ip)
            else:
                del ip2node[ip]

        for node in set(ip2node.values()):
            logger.error("Client %s has not connected", node['hostnames'][0])

    return 0


def client_main(opts):
    # try to connect to server for opts.server_up_timeout seconds
    # send update about local network settings to server
    # get config for test
    # run tests
    # send report back
    # http://man7.org/linux/man-pages/man3/getifaddrs.3.html

    ss = socket.socket()
    ss.settimeout(opts.server_up_timeout)

    return 0


def ip_addr(value):
    err = argparse.ArgumentTypeError("{!r} is not match HOST:PORT".format(value))
    if not isinstance(value, str) or ':' not in value:
         raise err
    host, port = value.rsplit(':', 1)
    try:
        iport = int(port)
    except ValueError:
        raise err

    socket.gethostbyname(host)

    return host, iport


def parse_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    client = subparsers.add_parser('client', help='Run in client mode')
    client.add_argument('-d', '--daemon', action="store_true", help="Became a daemon")
    client.add_argument('-p', '--port', type=int, help="Port for network check", default=37144)
    client.add_argument('-t', '--server-up-timeout', type=int, help="Wait for server for X seconds",
                        default=120)
    client.add_argument('server', help="Server addr HOST:PORT", type=ip_addr)
    client.set_defaults(main_func=client_main)

    server = subparsers.add_parser('server', help='Run server')
    server.add_argument('config', help="json config file with cluster structure")
    server.add_argument('-p', '--port', help="Server port to listen on", default=37145)
    server.add_argument('-i', '--ip', help="Server ip to listen on", default='')
    server.add_argument('-l', '--log-level', default='INFO', choices=("DEBUG", "INFO", "WARNING", "ERROR", "NO_LOG"))
    server.add_argument('-r', '--report', help="Save yaml report to FILE", metavar='FILE')
    server.set_defaults(main_func=server_main)

    return parser.parse_args(argv)


def main(argv):
    opts = parse_args(argv[1:])
    return opts.main_func(opts)


if __name__ == "__main__":
    exit(main(sys.argv))