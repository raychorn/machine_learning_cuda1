import sys
import traceback

from datetime import datetime

import datetime
from datetime import timezone

__ts = lambda t:datetime.datetime.fromtimestamp(datetime.datetime.fromtimestamp(t).timestamp(), tz=timezone.utc)
day_id = lambda t:int(int(((__ts(t).month-1) * (365.25/12)) + __ts(t).day) + (__ts(t).year * 365.25)) + (__ts(t).hour / 100)
__bin_id = lambda t:(int(t / 600))
_bin_id = lambda t:t - (int(t / 3600) * 3600)
bin_id = lambda t:(t - (int(t / 86400) * 86400))

BinID = lambda t:'{}.{}'.format(day_id(t), __bin_id(_bin_id(bin_id(t))))

class processor(object):
    
    def __init__(self, bin_size=600, stats={}, db=None, logger=None):
        self.bin = []
        self.bin_count = 1
        self.db = db
        self.stats = stats
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
                    f(self.bin, bin_count=self.bin_count, bin_size=self.bin_size, stats=self.stats, db=self.db, logger=self.logger, **kwargs)
                    self.bin = []
                    self.bin_count += 1
                    if (self.bin_count > 6):
                        self.bin_count = 1
            return wrapped_f
        except Exception as ex:
            extype, ex, tb = sys.exc_info()
            formatted = traceback.format_exception_only(extype, ex)[-1]
            if (self.logger):
                self.logger.error(formatted)
            else:
                print(formatted)
