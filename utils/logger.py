import logging
import sys, os
from utils.config import Config

if Config.log.mode == "console":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        datefmt='%Y/%m/%d %H:%M:%S',
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    )
else:
    if not os.path.exists(Config.log.logdir):
        os.mkdirs(Config.log.logdir)
    logging.basicConfig(
        level=logging.DEBUG,
        datefmt='%Y/%m/%d %H:%M:%S',
        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        filename=os.path.join(Config.log.logdir, 'server.log'),
        filemode='a'
    )
