import sys
import logging
import subprocess


logger = logging.getLogger("cmd")


def run(cmd, *args, **kwargs):
    if args or kwargs:
        cmd = cmd.format(*args, **kwargs)

    logger.debug("%r", cmd)
    p = subprocess.Popen(cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    stdout, stderr = p.communicate()
    logger.debug("%r == %s", cmd, p.returncode)
    assert p.returncode == 0, "{0!r} failed with code {1}. Stdout\n{2}\nstderr {3}"\
        .format(cmd, p.returncode, stdout, stderr)

    if sys.version_info.major == 3:
        return stdout.decode('utf8'), stderr.decode('utf8')
    return stdout


def setup_loggers(loggers, default_level=logging.INFO, log_fname=None):
    sh = logging.StreamHandler()
    sh.setLevel(default_level)
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    colored_formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
    sh.setFormatter(colored_formatter)
    handlers = [sh]

    if log_fname is not None:
        fh = logging.FileHandler(log_fname)
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)

    for logger in loggers:
        logger.setLevel(logging.DEBUG)
        logger.handlers = []
        logger.addHandler(sh)

        for handler in handlers:
            logger.addHandler(handler)

    root_logger = logging.getLogger()
    root_logger.handlers = []

