# -*- coding: utf-8-*-

import logging

LOG = logging.getLogger(__name__)


# 记录kafka 发送是否成功的状态类
# 3个状态：
#       1 Init when message send,
#       2 Ok when message send success
#       3 Fail when message send fail
#  key： oplog timestamp 64bit
class QueueElement(object):
    INIT = 0
    OK = 1
    FAIL = -1

    def __init__(self, key=-1L):
        """ Init three status, Init when message send, Ok when message send success, Fail when message send fail
        """
        try:
            self.status = self.INIT
            # self.key = -1L
            self.key = key

        except Exception as ex:
            raise ex

    def fail(self):
        if self.is_ok() is False:
            self.status = self.FAIL
            LOG.info("message status was set to fail.")
        else:
            LOG.warn("skip to set fail. message")

    def ok(self):
        self.status = self.OK

    def is_failed(self):
        return self.status == self.FAIL

    def is_ok(self):
        return self.status is self.OK

    # whether the two objects are from common object
    # def __eq__(self, other):
    #     if self is other:
    #         return True
    #     if isinstance(other, QueueElement) is False:
    #         return False
    #     # oth = QueueElement(other)
    #     if self.key != other.key:
    #         return False
    #     return True

    def __eq__(self, other):
        if type(self) == type(other):
            return self.key == other.key
        else:
            # super(SBTNode, self).__eq__(other)
            return False

    def __hash__(self):
        return self.key ^ (self.key >> 32)
