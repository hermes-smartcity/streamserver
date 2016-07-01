from __future__ import unicode_literals, print_function, division

import glob
import os
import os.path
import re
import time
import logging

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
        cpu_time = self._compute_cpu()
        if self.last_cpu_time is not None:
            diff = cpu_time - self.last_cpu_time
            if diff < 0:
                self.status_ok = False
                raise ProcessChangedException()
        else:
            diff = None
        self.last_cpu_time = cpu_time
        return diff

    def check(self, cmdline_re):
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

    def _compute_cpu(self):
        with open(self.stat_file) as f:
            line = f.readline()
        data = line.split(' ')
        cutime = int(data[13])
        cstime = int(data[14])
        total_jiffies = cutime + cstime
        return (1000 * total_jiffies // _clk_tck) / 1000


class CPUMultiMonitor(object):
    def __init__(self, cmdline_re):
        self.cmdline_re = cmdline_re
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
        pids = get_pids(self.cmdline_re)
        for pid in pids:
            if not pid in self.monitors:
                self.monitors[pid] = CPUMonitor(pid, reset_now=reset_now)

    def check_monitors(self):
        to_be_removed = []
        for pid, monitor in self.monitors.iteritems():
            if not monitor.check(self.cmdline_re):
                to_be_removed.append(pid)
        for pid in to_be_removed:
            del self.monitor[pid]


def periodic_sleep():
    last_time = time.time()
    while True:
        time.sleep(60 - time.time() % 60)
        now = time.time()
        yield now - last_time
        last_time = now

def get_pids(cmdline_re):
    pids = []
    for dirname in glob.iglob('/proc/[0-9]*'):
        with open(os.path.join(dirname, _cmdline_file)) as f:
            cmdline = f.read()
        if cmdline_re.match(cmdline):
            pids.append(int(dirname.split('/')[-1]))
    return pids

def process_name(pid):
    with open(_proc_cmdline_pattern.format(pid)) as f:
        return f.read()

def main():
    utils.configure_logging('nginx-monitor')
    monitor = CPUMultiMonitor(re.compile('^nginx'))
    try:
        for duration in periodic_sleep():
            total_time, times = monitor.update()
            logging.info('nginx: {:.03f} / {:.03f}{}'\
                    .format(total_time, duration, '' if times else ' [reset]'))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
