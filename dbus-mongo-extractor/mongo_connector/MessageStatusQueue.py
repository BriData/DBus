# -*- coding: utf-8-*-

import threading

from mongo_connector.QueueElement import QueueElement


# 管理 QueueElement 类的队列， 引入了 同步锁，线程安全

#  at leatest once
#   list element state     |   None                         |   Init    |    OK    |  FAIL   |
#   add new                |   new, Init                    |   skip    |   skip   |  Init   |
#   mark as OK             |   impossible(OK been deleted)  |   OK      |   OK     |  OK     |
#   mark as failed         |   impossible(OK been deleted)  |   failed  |   skip   |  fail   |
class MessageStatusQueue:

    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()
        # self.fail_flag = False

    ##################################################################
    ''' implement locking for multi threading
    '''

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, type, value, traceback):
        self.release_lock()

    def get_queue(self):
        return self.queue

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()

    ###################################################################################

    # add kafka message to list, if have exist and it's status is not fail, it will not add again
    def append_or_update(self, timestamp):
        elem = self.get_queue_element(timestamp)
        if elem is not None:
            if elem.is_failed():
                elem.status = QueueElement.INIT
            # elem.set_message(timestamp)
        else:
            self.queue.append(QueueElement(timestamp))

    # find earliest failed element
    def seek_failed_point(self):
        for element in self.queue:
            if element.is_failed():
                return element
        return None

    # find lastest commit point
    def commit_point(self):
        elem = None
        for element in self.queue:
            if element.is_ok():
                elem = element
            else:
                break
        return elem

    # remove ok commint pointes
    def pop_ok_elements(self):
        # for element in self.queue:
        #     if element.is_ok() and element.emitCount == 0:
        #         # self.queue.pop(element)
        #         self.queue.remove(element)
        #     else:
        #         break
        for element in self.queue[:]:
            if element.is_ok():
                # self.queue.pop(element.key)
                self.queue.remove(element)
            else:
                break

    def is_empty(self):
        return len(self.queue) == 0

    def size(self):
        return len(self.queue)

    def get_queue_element(self, key):
        for element in self.queue:
            if key == element.key:
                return element
        return None
