import sys
import traceback

from datetime import datetime

class processor(object):
    
    def __init__(self, bin_size=600, chunk_size=100, stats={}, db=None, logger=None):
        self.bin = []
        self.bin_count = 0
        self.db = db
        self.stats = stats
        self.chunk_size = chunk_size
        self.bin_size = bin_size
        self.logger = logger

    def __call__(self, f, *args, **kwargs):
        try:
            def wrapped_f(*args, **kwargs):
                __is__ = False
                doc = args[0]
                if (len(self.bin) > 0):
                    st = self.bin[0].get('start', None)
                    ct = doc.get('start', None)
                    if (st is not None) and (ct is not None) and (ct > st):
                        delta = ct - st
                        secs = int(delta.total_seconds())
                        print(secs)
                        __is__ = (secs >= self.bin_size)
                self.bin.append(doc)
                if (__is__):
                    f(self.bin, self.bin_count, self.bin_size, stats=self.stats, db=self.db, chunk_size=self.chunk_size, logger=self.logger, **kwargs)
                    self.bin = []
            return wrapped_f
        except Exception as ex:
            extype, ex, tb = sys.exc_info()
            formatted = traceback.format_exception_only(extype, ex)[-1]
            if (self.logger):
                self.logger.error(formatted)
            else:
                print(formatted)
