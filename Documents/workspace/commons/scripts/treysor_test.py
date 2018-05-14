import datetime
import threading

from moengage.commons.loggers import Treysor


class TreysorTest(threading.Thread):
    def __init__(self, random_string, counter_start):
        super(TreysorTest, self).__init__()
        self.random_string = random_string
        self.counter_start = counter_start

    def run(self):
        import time
        import random
        Treysor('segmentation').setContext(rand=self.random_string)
        for i in range(self.counter_start, self.counter_start + 10):
            Treysor('segmentation').info(i=i)
            time.sleep(random.randint(1, 5))
            if i % 5 == 0:
                try:
                    x = 2 / (i % 5)
                except:
                    Treysor('segmentation').exception(i=i, log_tag='exception_checker')
                    Treysor('segmentation').updateContext(exception_occured=True)
        Treysor('random').info(time=datetime.datetime.now())
        Treysor('segmentation').clearContext()
        Treysor('segmentation').info(action='Finished execution')


def main():
    import random
    import string
    from moengage.commons.threadsafe import ThreadPoolExecutor
    print "Creating threads"
    args = []
    for x in range(0, 2):
        args.append((''.join(random.sample(string.letters * 5, 5)), random.randint(10, 1000)))
    executor = ThreadPoolExecutor(5, TreysorTest, args)
    executor.start()


if __name__ == '__main__':
    main()
