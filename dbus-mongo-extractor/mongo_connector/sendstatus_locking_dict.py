# -*- coding: utf-8-*-

import threading


# 不在被使用 20180320
class SendStatusLockingDict(object):

    def __init__(self):
        self.dict = {}
        self.lock = threading.Lock()

    def __enter__(self):
        self.acquire_lock()
        return self

    def __exit__(self, type, value, traceback):
        self.release_lock()

    def get_dict(self):
        return self.dict

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()

    def get_latest_timestamp_done(self):
        min_false_timestamp = 0
        max_true_timestamp = 0
        for key in self.dict:
            if self.dict[key]["is_done"] is False:
                if min_false_timestamp is 0 or key < min_false_timestamp:
                    min_false_timestamp = key
        # find  min_true_timestamp  is_done is true and lower than min_false_timestamp
        for key in self.dict:
            if self.dict[key]["is_done"] is True:
                if max_true_timestamp is 0:
                    if min_false_timestamp is 0 or key < min_false_timestamp:
                        max_true_timestamp = key
                elif key > max_true_timestamp:
                    if min_false_timestamp is 0 or key < min_false_timestamp:
                        max_true_timestamp = key
        # return max_true_timestamp
        if max_true_timestamp is not 0:
            return max_true_timestamp

    def del_latest_timestamp_done(self, latest_timestamp):
        for key in list(self.dict):
            if key <= latest_timestamp:
                self.dict.pop(key)

    '''
    def set_send_status(self, id, is_sended, is_done):
        self.dict[id] = {"is_sended": is_sended, "is_done": is_done}
    '''
