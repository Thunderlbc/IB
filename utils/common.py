import sys
import os
import utils.logger
import asyncssh
import logging
from utils.network import async_post
import requests
import asyncio
import signal
import time
import utils
import threading
import os
import sys
import subprocess
import json
sys.path.insert(0, './')
logger = logging.getLogger('Worker')


async def async_remote_control(ip, port, method, *args, **kwargs):
    data = {
        "method": method
    }
    if len(args) > 0:
        data["args"] = ','.join([str(a) for a in args])
    if len(kwargs) > 0:
        data["kwargs"] = json.dumps(kwargs)
    _, res = await async_post("http://{}:{}/exec".format(ip, port), data, jsonify=True)
    return res


def remote_control(ip, port, method, *args, **kwargs):
    data = {
        "method": method
    }
    if len(args) > 0:
        data["args"] = ','.join([str(a) for a in args])
    if len(kwargs) > 0:
        data["kwargs"] = json.dumps(kwargs)
    res = requests.post("http://{}:{}/exec".format(ip, port), json=data)
    logger.debug("RemoteControl: got res[{}]".format(res.json()))
    return res.json()


async def async_run_cmd(cmd):
    cmd = ' '.join(cmd)
    proc = await asyncio.subprocess.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return str(stdout.decode()), str(stderr.decode())


def dict2obj(d):
    if isinstance(d, list):
        d = [dict2obj(x) for x in d]
    if not isinstance(d, dict):
        return d

    class C(object):
        pass
    o = C()
    for k in d:
        o.__dict__[k] = dict2obj(d[k])
    return o


def run_cmd(cmd, stdout, stderr):
    pkwargs = {
        "close_fds": True,
        "shell": False,
        "stdout": stdout,
        "stderr": stderr,
    }
    p = subprocess.Popen(cmd, **pkwargs)
    return p


def run_cmd_with_stdout_return(cmd):
    # 会阻塞等到结果返回
    p = run_cmd(cmd, subprocess.PIPE, None)
    stdout = []
    while(True):
        line = p.stdout.readline().decode()
        if not line:
            break
        stdout.append(line)
    return '\n'.join(stdout)


# 自定义超时异常
class TimeoutError(Exception):
    def __init__(self, msg):
        super(TimeoutError, self).__init__()
        self.msg = msg


def time_out(interval, callback=None):
    def decorator(func):
        def handler(signum, frame):
            raise TimeoutError("run func timeout")

        def wrapper(*args, **kwargs):
            try:
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(interval)       # interval秒后向进程发送SIGALRM信号
                result = func(*args, **kwargs)
                signal.alarm(0)              # 函数在规定时间执行完后关闭alarm闹钟
                return True, result
            except TimeoutError as e:
                return False, None
        return wrapper
    return decorator


async def upload_file(file_path, oss_path='oss://chia-test3/', fingerprint=0):
    file_name = os.path.basename(file_path)
    oss_basedir = oss_path.rstrip('/')+'/'+str(fingerprint)
    logger.debug("OSS make dir by fingerprint: [{}]".format(oss_basedir))
    await async_run_cmd(['/usr/bin/ossutil64', 'mkdir', oss_basedir])
    cmd = ['/usr/bin/ossutil64', 'cp', file_path, oss_basedir+'/'+file_name]
    stdout, _ = await async_run_cmd(cmd)
    logger.debug("UploadFile got stdout[{}]".format(stdout))
    return 'Succeed' in ''.join(stdout)  # 成功返回True，失败返回False

async def scp_file(file_path, remote_path):
    logger.debug("SCP found FilePath[{}] Remote[{}]".format(file_path, remote_path))
    try:
        await asyncssh.scp(file_path, remote_path+'/')
        return True
    except Exception as e:
        logger.error("Scp file[{}] to remote[{}] got return ERROR[{}]".format(file_path, remote_path + '/', str(e)))
        return False


if __name__ == "__main__":
    # with open('tmp.dat', 'w') as fd:
    #     run_cmd(['chia', 'keys', 'show'], fd, fd)

    # run_cmd_with_stdout_return(["ls"])
    # asyncio.run(async_remote_control("127.0.0.1", 5000, "post_process"))
    # res = remote_control("127.0.0.1", 5000, "post_process")
    # print(res.json())

    asyncio.run(scp_file(
        './test', remote_path="/plots/"))
    #asyncio.run(upload_file(
    #    '/chia-ssd/test2/plot-k25-2021-05-12-16-39-701a92040602854b4490e6d8e9254fc2ca6c4dc2d4f3e4e00caab032ea102a53.plot', oss_path='oss://my-coin/'))
