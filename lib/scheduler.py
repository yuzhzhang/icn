import time
import datetime
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


class Scheduler(StoppableThread):
    def __init__(self, trigger_time, callable, *args, **kwargs):
        super(Scheduler, self).__init__()
        self.trigger_time = trigger_time
        self.args = args
        self.callable = callable
        self.kwargs = kwargs
        self.daemon = True
        self.wait = 0.1

    def run(self):
        while True:
            if datetime.datetime.now() >= self.trigger_time:
                if not self.is_stopped():
                    self.callable(*self.args, **self.kwargs)
                break
            libc.usleep(int(self.wait*1000000))
        return               

