import threading
import time

threading.currentThread().__setattr__('data', None)


def foo():
    c = 50
    main_thread = None
    time.sleep(5)
    for t in threading.enumerate():
        if t.getName() == 'MainThread':
            main_thread = t
            break
    main_thread.__setattr__('data', c)


thread = threading.Thread(target=foo)
thread.start()

# time.sleep(2)
while True:
    if threading.currentThread().__getattribute__('data') is not None:
        print threading.currentThread().__getattribute__('data')
        break
