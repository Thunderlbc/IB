# -*- coding=utf8 -*-
import logging
import utils.logger
import os
import sys
import traceback
from server import init_server
import multiprocessing
import threading
import json
import time
import queue
import asyncio
from utils.config import Config
from copy import deepcopy
from utils.common import async_run_cmd, remote_control
from utils.create_plots import create_plots
from utils.network import get_host_ip
import asyncio
from utils.network import get_host_ip
from worker_posters import *
logger = logging.getLogger('Worker')

"""
Worker 功能定义如下：

1. 唤起plot进程
2. 判断是否可以新增p图task
3. p图后各种后续操作（上报、sync等）

Worker也是Flask server，初始化时需要初始化一个p图进程池以及一个线程用来监测是否有空闲进程池
"""
MASTER_IP = Config.master.ip
MASTER_PORT = Config.master.port


class Worker(object):

    def __init__(self, ):
        self.ip = get_host_ip()
        self.pid = os.getpid()
        self.master_ip = MASTER_IP
        self.master_port = MASTER_PORT
        self.update_time = time.time()
        self.notify_queue = multiprocessing.Queue()

        self.loop = None  # app创建时注入
        self.worker_id = None
        self.plot_pool = []

        self.crying()  # 初始化前与master节点通信获取worker_id, 此时还没有事件循环。

    def crying(self):
        """
        把自己的ip、process_id扔给master，要求master返回一个可用worker_id
        """
        logger.info("Worker: crying with IP[{}] and PID[{}]".format(
            self.ip, self.pid))
        res = remote_control(MASTER_IP, MASTER_PORT,
                             "worker_awaking", self.ip, self.pid)
        self.worker_id = res['data']
        logger.info(
            "Worker: Got WorkerId[{}] after cried".format(self.worker_id))

        check_thread = threading.Thread(
            target=self.check, args=(), name="Checker")
        callback_thread = threading.Thread(
            target=self.callback, args=(), name="Callbacker")

        check_thread.start()
        callback_thread.start()

    def check(self):
        """
        判断当前系统剩余资源，是否满足新增一个plot task
        如果满足，返回 可用cache 以及 dest目录
        """
        print("Thread-Checker Start...")
        while True:
            try:
                if len(self.plot_pool) == Config.worker.max_worker:
                    logger.debug("Checker: size of plot pool equal to max_worker plot_pool[{}]".format(self.plot_pool))
                    self.update_time = time.time()
                if len(self.plot_pool) < Config.worker.max_worker:
                    logger.debug(
                        "当前本机任务数小于最大p图数, 尝试启动一个新的p图程序 with PlotPool[{}]".format(self.plot_pool))
                    res = remote_control(self.master_ip, self.master_port, "query",
                                         self.worker_id, self.pid, int(time.time()-self.update_time))
                    logger.info("Worker[{}]: got response with request[{}] : [{}]".format(
                        self.worker_id, 'query', res))

                    if len(res['data']) == 0:
                        time.sleep(10)
                        continue
                    res = json.loads(res['data'])
                    if len(res) == 0:
                        logger.warning("Worker[{}]: don't get any thing from Master[{}:{}], waiting...".format(
                            self.worker_id, self.master_ip, self.master_port))
                        time.sleep(
                            Config.worker.sleep_secs_for_null_reply_from_master)
                        continue
                    task_id, meta_info = res["task_id"], res["meta"]
                    self.plot(task_id=task_id, meta_info=meta_info)
                    # self.plot_pool.append(p)
            except Exception as e:
                logger.error("CHECK Error[{}]".format(str(e)))
                traceback.print_exc()
            finally:
                time.sleep(Config.worker.sleep_secs_checker_for_one_iter)

    def callback(self):
        print("Thread-Callbacker Start...")
        while True:
            try:
                """
                json.dumps({
                    "dir": tmp_dir,
                    "filename": finished_filenames[0],
                    "pid": os.getpid(),
                    "task_id" : task_id,
                    "posters": posters
                }))
                """
                meta = json.loads(self.notify_queue.get_nowait())
                meta_info = deepcopy(json.loads(meta.get('meta_info')))
                logger.debug("Worker: got process callback [{}]".format(meta))
                # 开始执行posters里面的内容
                posters = meta.get('posters', [])
                asyncio.run_coroutine_threadsafe(self.post_process(
                    posters=posters, task_meta=meta, meta_info=meta_info), self.loop)
                child_pid = meta.get('child_pid')
                self.plot_pool.remove(child_pid)
                logger.debug("callback plot_pool [{}] remove child_pid [{}]".format(self.plot_pool, child_pid))
            except queue.Empty as e:
                time.sleep(10)

    async def post_process(self, posters=[], task_meta={}, meta_info={}):
        """
        此处需要从worker_posters.py中import来的所有 全局函数
        posters: 当初从 master那request来的信息中，会包含plot完成后操作序列
        return: post一系列操作是否成功
        """
        try:
            for funName, funArgs in posters:
                logger.debug("Worker[{}]: post-processing with FuncName[{}] and FuncArgs[{}]?".format(
                    self.worker_id, funName, funArgs))
                if funName[:5] != "post_":
                    logger.warning("Worker[{}]: found invalid funName[{}], not starts with 'post_', skipping...".format(
                        self.worker_id, funName))
                    continue
                if funArgs is None:
                    funArgs = []
                kwargs = task_meta
                kwargs['worker_id'] = self.worker_id
                kwargs['master_ip'] = self.master_ip
                kwargs['master_port'] = self.master_port
                kwargs['meta_info'] = meta_info
                #print ("XXX[{}]{} {} with GLOBAL[{}]".format(funName,funArgs,kwargs, globals()))

                is_succ = await globals()[funName](*funArgs, **kwargs)
                if not is_succ:
                    logger.error("Worker[{}]: Something Wrong with PostProcess[{}( {} )], please check...".format(
                        self.worker_id, funName, funArgs))
                logger.info("Worker[{}]: Done for PostProcess[{} ( {} )]".format(
                    self.worker_id, funName, funArgs))
        except Exception as e:
            logger.info("ERROR[{}]".format(str(e)))
            import traceback
            traceback.print_exc()

    def plot(self, meta_info, task_id):
        """
        plot_args: p图全部所需参数，包含ppk、fpk
        worker 自行决定 cache 以及dest
        return dest dir
        """
        fpk, ppk, posters = meta_info.get('fpk'), meta_info.get(
            'ppk'), meta_info.get('posters')

        p = multiprocessing.Process(target=create_plots, kwargs={
            "farmer_public_key": fpk,
            "pool_public_key": ppk,
            # "farmer_public_key": "8e32f7e637306464c51b9b0699aa855fcf00c1c9db322b9b6576ea23e2c405a3594fcdee26de24552ed6e652e8c8bf29",
            # "pool_public_key": "b1c711024cd6c5a43b77c51ee20e48ede51ea0389af79dc2ea811419b43c450f44ded9d6172b5c25f91f4d6a9b355d39",
            "tmp_dir": Config.plotting.tmpdir,
            "k": Config.plotting.k,
            "queue": self.notify_queue,
            "log_path": Config.plotting.logdir,
            "posters": posters,
            "task_id": task_id,
            "pid": os.getpid(),
            "meta_info": json.dumps(meta_info)
        })
        p.start()
        self.plot_pool.append(p.pid)

    async def exec_test(self, arg1, arg2, kwarg1=1, kwarg2=2):
        logger.debug("Remote control success. with arg1 [{}] arg2 [{}] kwarg1 [{}] kwarg2 [{}]".format(
            arg1, arg2, kwarg1, kwarg2))
        return 200, [], 'OK!'


if __name__ == "__main__":
    init_server(Worker())
    # import multiprocessing
    # from utils.create_plots import create_plots
    # create_plots(
    #     "8e32f7e637306464c51b9b0699aa855fcf00c1c9db322b9b6576ea23e2c405a3594fcdee26de24552ed6e652e8c8bf29",
    #     "b1c711024cd6c5a43b77c51ee20e48ede51ea0389af79dc2ea811419b43c450f44ded9d6172b5c25f91f4d6a9b355d39",
    #     "/chia-ssd/test2",
    #     25,
    #     multiprocessing.Queue()
    # )
