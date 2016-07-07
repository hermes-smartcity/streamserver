from __future__ import unicode_literals, print_function, division

import glob
import os
import os.path
import re
import time
import logging
import argparse

from .. import utils


_clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

_cmdline_file = 'cmdline'
_proc_cmdline_pattern = '/proc/{{}}/{}'.format(_cmdline_file)


class ProcessChangedException(Exception):
    """The monitored PID seems to be a new process."""
    pass


class CPUMonitor(object):
    def __init__(self, pid, reset_now=True):
        self.pid = pid
        self.stat_file = '/proc/{}/stat'.format(pid)
        if reset_now:
            self.last_cpu_time = None
        else:
            self.last_cpu_time = 0.0
        self.status_ok = True
        self.update()

    def update(self):
        try:
            cpu_time = self._compute_cpu()
        except IOError:
            raise ProcessChangedException()
        if self.last_cpu_time is not None:
            diff = cpu_time - self.last_cpu_time
            if diff < 0:
                self.status_ok = False
                raise ProcessChangedException()
        else:
            diff = None
        self.last_cpu_time = cpu_time
        return diff

    def check_cmd(self, cmdline_re):
        correct = False
        if self.status_ok:
            try:
                if cmdline_re.match(process_name(self.pid)):
                    correct = True
            except IOError:
                # The process is not running anymore
                pass
        self.status_ok = correct
        return correct

    def check_running(self):
        try:
            with open(self.stat_file) as f:
                f.readline()
        except:
            running = False
        else:
            running = True
        return running

    def _compute_cpu(self):
        with open(self.stat_file) as f:
            line = f.readline()
        data = line.split(' ')
        cutime = int(data[13])
        cstime = int(data[14])
        total_jiffies = cutime + cstime
        return (1000 * total_jiffies // _clk_tck) / 1000


class CPUMultiMonitor(object):
    def __init__(self, cmdline_re=None, pids=None):
        if not cmdline_re and not pids:
            raise ValueError('Regular expression or PID list expected')
        self.cmdline_re = cmdline_re
        self.pids = pids
        self.monitors = {}
        self.update_monitors(reset_now=True)

    def update(self):
        times = {}
        try:
            for pid, monitor in self.monitors.iteritems():
                times[pid] = monitor.update()
        except ProcessChangedException:
            self.update_monitors()
            times = {}
        total_time = sum(t for t in times.itervalues())
        return total_time, times

    def update_monitors(self, reset_now=False):
        self.check_monitors()
        if self.cmdline_re:
            pids = get_pids(self.cmdline_re)
        if self.pids:
            pids = list(self.pids)
        for pid in pids:
            if not pid in self.monitors:
                try:
                    self.monitors[pid] = CPUMonitor(pid, reset_now=reset_now)
                except ProcessChangedException:
                    pass

    def check_monitors(self):
        to_be_removed = []
        for pid, monitor in self.monitors.iteritems():
            if not monitor.pid in self.pids:
                if not monitor.check_cmd(self.cmdline_re):
                    to_be_removed.append(pid)
            elif not monitor.check_running():
                to_be_removed.append(pid)
        for pid in to_be_removed:
            del self.monitors[pid]

    def __len__(self):
        return len(self.monitors)


def periodic_sleep():
    last_time = time.time()
    while True:
        time.sleep(60 - time.time() % 60)
        now = time.time()
        yield now - last_time
        last_time = now

def get_pids(cmdline_re):
    pids = []
    self_pid = os.getpid()
    for dirname in glob.iglob('/proc/[0-9]*'):
        with open(os.path.join(dirname, _cmdline_file)) as f:
            cmdline = f.read()
        if cmdline_re.match(cmdline):
            pid = int(dirname.split('/')[-1])
            if pid != self_pid:
                pids.append(pid)
    return pids

def get_cwd(pid):
    return os.readlink('/proc/{}/cwd'.format(pid))

def process_name(pid):
    with open(_proc_cmdline_pattern.format(pid)) as f:
        return f.read()

def _parse_args():
    parser = argparse.ArgumentParser(description='Monitor CPU use.')
    parser.add_argument('process',
                        help=('a single PID or a regular expression to match '
                              'the command name in the output of ps'))
    parser.add_argument('-l', '--label', dest='label',
                        default='cpu_monitor',
                        help='label to use in the logs, e.g. nginx-cpu')
    parser.add_argument('-d', '--log-dir', dest='log_dir',
                        default='.',
                        help='directory for the log file')
    return parser.parse_args()


def main():
    args = _parse_args()
    logname = utils.configure_logging(args.label, dirname=args.log_dir)
    if re.match(r'\d+', args.process):
        pids = [int(args.process)]
        command_regex = None
        logging.info('Monitor process id: {}'\
                     .format(pids[0]))
    else:
        pids = []
        command_regex = re.compile(args.process)
        logging.info('Monitor processes matching: {}'.format(args.process))
    logging.info('Logging to: {}'.format(logname))
    monitor = CPUMultiMonitor(cmdline_re=command_regex, pids=pids)
    if len(monitor):
        logging.info('{} matched PIDs: {}'.format(args.label,
                            ', '.join(str(p) for p in monitor.monitors)))
        try:
            last_time = time.time()
            for _ in periodic_sleep():
                current_time = time.time()
                duration = current_time - last_time
                last_time = current_time
                total_time, times = monitor.update()
                logging.info(('{} cpu: {:.03f}s / real: {:.03f}s / '
                              '{} processes {}')\
                        .format(args.label, total_time, duration, len(times),
                                '' if times else ' [reset]'))
        except KeyboardInterrupt:
            pass
    else:
        logging.error('No match.'.format(len(monitor)))

if __name__ == "__main__":
    main()
