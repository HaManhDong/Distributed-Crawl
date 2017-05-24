import threading
import time
threading.currentThread().__setattr__('data',{'x':'y'})

def foo():
    for t in threading.enumerate():
        if t.getName()=='MainThread':
            data = t.__getattribute__('data')
            print data
            data['abc'] = 'def'
            t.__setattr__('data',data)
            break


t = threading.Thread(target=foo)
t.start()
time.sleep(5)
print threading.currentThread().__getattribute__('data')