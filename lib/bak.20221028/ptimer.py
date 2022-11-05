#!/usr/bin/env python

import time
import threading
from threading import Thread
import ctypes

libc = ctypes.CDLL('libc.so.6')

class StoppableThread(Thread):
    ''' Stoppable Thread '''
    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def is_stopped(self):
        return self._stop.isSet()


class  PeriodicTimer(StoppableThread):
    ''' Minute Timer '''
    def __init__(self, period, pos, callable, *args, **kwargs):
        super(PeriodicTimer, self).__init__()
        self.period = period
        self.pos = pos
        self.args = args
        self.callable = callable
        self.kwargs = kwargs
        self.daemon = True
        self.lasttime = None

    def run(self):
        while not self.is_stopped():
            if not self.lasttime:
                self.lasttime = int(time.time() / self.period) * self.period
                wait = self.pos + self.lasttime - time.time()
                while wait < 0.0001:
                    wait += self.period
                    self.lasttime += self.period
                time.sleep(wait)
            else:
                self.callable(*self.args, **self.kwargs)
                self.lasttime += self.period
                wait = self.pos + self.lasttime - time.time()
                #time.sleep(wait)
                libc.usleep(int(wait*1000000))
        self.lasttime = None




