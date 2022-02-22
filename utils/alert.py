from WorkWeixinRobot.work_weixin_robot import WWXRobot
import logging
import utils.logger
import time
from utils.config import Config

logger = logging.getLogger('Alert')
class WechatWork(object):
    def __init__(self, key):
        self.rbt = WWXRobot(key=key)

    def send_text(self, text, retry=3):
        _try = 0
        while(_try < retry):
            try:
                self.rbt.send_text(text)
                return
            except Exception as e:
                time.sleep(10)
                _try += 1
