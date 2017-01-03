# -*- coding: utf-8 -*-

import sys
import time


class ProgressBar:
    def __init__(self):
        self.start_time = 0
        self.stop_time = 0

    def start(self):
        self.start_time = time.time()

    def stop(self):
        self.stop_time = time.time()

    # Print iterations progress
    def print_progress(self, iteration, total, prefix='', suffix='', decimals=1, bar_length=100, termination_str=""):
        """
        Call in a loop to create terminal progress bar

        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            bar_length  - Optional  : character length of bar (Int)
        """
        str_format = "{0:." + str(decimals) + "f}"
        percents = str_format.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = '█' * filled_length + '-' * (bar_length - filled_length)

        sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

        if iteration == total:
            s = '\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)
            sys.stdout.write('\r' + ' ' * len(s))
            self.stop()
            if termination_str == "":
                sys.stdout.write('\rCompleted in %s secs' % (self.stop_time - self.start_time))
            sys.stdout.write('\n')
        sys.stdout.flush()

#
# Sample Usage
#

if __name__ == "__main__":
    from time import sleep

    # make a list
    items = list(range(0, 57))
    i = 0
    l = len(items)

    p = ProgressBar()

    # Initial call to print 0% progress
    p.start()
    p.print_progress(i, l, prefix = 'Progress:', suffix = 'Complete', bar_length = 50)
    for item in items:
        # Do stuff...
        sleep(0.1)
        # Update Progress Bar
        i += 1
        p.print_progress(i, l, prefix = 'Progress:', suffix = 'Complete', bar_length = 50)