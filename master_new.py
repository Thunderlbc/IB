import os
import time
import json as ujson
import utils
from sanic import Sanic
from sanic.response import text, json
import asyncio
from utils.alert import WechatWork
from utils.common import scp_file
from utils.config import Config
from scripts.create_ecs import *
import asyncio, asyncssh, sys
from utils.network import *
import logging
import utils.logger
import threading, subprocess

logger = logging.getLogger('Master')
MINER_IP = '172.27.67.243'
app = Sanic(__name__)
app.alert = WechatWork(key=Config.notifier.key)

def alarm(text):
    def t(text):
        app.alert.send_text(text)
    tt = threading.Thread(target=t, args=(text,))
    tt.start()

key = 'LTAI5tSZQLwpkHaKs4k3BUxV'
secret = '27dm3mWMYCPhDEjwyb2nKMuf0Q5VL0'
region_id = 'cn-huhehaote'
cli = AliyunRunInstances()
app.cli = cli

#fpks = ['962de3a8707d0b3f8a92f129eac15358488982092a8f20b324f45a38c461647f1b59b66f6ce657603ea1c839a5e93011', 'aa626e133ee4aaf0ccb478c574dab14bf8589afdce164161461b66e5b61f08f7f97012b284a9f6e06b0dcc4065d65166', 'a33c55d3c6e61fad521219bfde5aa49504a582d5792a3a15851b40414faa9c3314837596c288127d58ecaa0943e88e1e', 'aad1125dc77e09abc316ef6912c939dab9341b4d5a6edc28ba1b9ba412559e0d6fdbd7c6197edeb9205f977fdd5fbb77']
#ppks = ['927c574fae23f1d5c4c9e65951a3139fa0c402667efcc23d05c0a2e521986d294f4c9b44397324ab37e701837b4d53d7', '8c62f867fb4a87f5da5ed287e5b65836a4f66fb87c924b62c7a579fc4836ce4388b264db1d93ac5987598be3edb9c7c2', '998077910fdb59603c5c10132ff9a559e37b6e22fc748ed418a44801898fd731767a8eedfd214f5560bae675df0d5c87', 'a26a38901388ecdcaa29d5a2a100cc0d147f3778b09482bd33d06661d75dd1b3a3d18e57dcb1c61e52479915ab4dac8a']

fpks = ['89056fa345ab12634e47be87eb53f2f587e3f9fe2bf5ca5fbd78d9421d3841867c470a1722b0bc8d9eeac328b6ba231a','9344f0d273cf902eefd180e8bf21cc45f023433e20ec714af7f4bbc0baabc510280682df05fd55ce277a3863fe1be617','8ca58fe8261d51c204c61e813578dc798105916de729be4129d3aef1527cdea8a3b178ebc6ff47aebac3478dfdfb6c80','860772a1dd0c568a28eba65a9cbd951e6f8bbb38aacd880e808883793e12c87f6a87666ab6cf90113da0b335ceed8e30', 'b713ae20c38ce42de7f407af593095ae235aae30cf40b3b32c058f51383b6f52328974a31a4eece0810378f2ce16ed81','8137dc2b169f92a8690caad4781526ebff8eb2d522c00f5e75a6004ea76d24fae68a4d4e6e3d7c77820c43149c958fa8','adc8f443f89e5076eb3bd19322887fcd6aaa220767477a033268e778182196913e5e4e20e48eebb05b6cb0cd773a809e','82ae1e5e8417e814f3ca3a72e3de8c33dba5cfc3a7a79b4e1610a29117c46ff1c25605899045794d982580dc6df7f5bf']
ppks = ['95ff4e6ef2cfd3f0807a8dbaebf0c8828f0a3d5bcb4fee69eaa8fd3693786d3122803a3322e75af7e86aef8a236a1743', 'a59b45a95c638b8841594c914f4c8f20d422d598b53a20421f06adb3e75ee901b363a1b161b4c4d51f1b38a572f9fdb5','b728c8ba6bfc12ed705c46f423c01c2e26ccb60f506282bc6214c20311c135911dbfd9bd347e51db64415860b33ef0e3','a78fcda6a966e238beea30306cd9485cee65f28407a17441c03d29ff740023be4a08106be768c7f63d8d18ad1b5028a9','aba7e4c5cb909ce50aa55ee5aa7be51aa70bab942b739dd4438793392b001cc7c811d34d7ace7e54e8ea43a86e16c668','97705faf9d80c3775bb6d88966be32d361a3f1206c32b2c4bb8214365ea5c7d3e830be38624b660328c12142ad0a55b8','8db90c38fd5bbbe403b7b93439899470f136ab7355529dda45ada8cb8dbc8f37fdfcd5ec3db548570738d89a4079d6ec','939818dda2c29ec44c23b52799554ab2164dcff1a0d0047e0e4bd6eabb33d471756090063520020cfc2ff311471fc284']

global group
global subgroup
group = 0
subgroup = 'a' # a, b 两组
subadirs = ['/datab', '/datac', '/datad', '/datae', '/dataf', '/datag', '/datah', '/datai']
subbdirs = ['/dataj', '/datak', '/datal', '/datam', '/datan', '/datao', '/datap', '/dataq']

class speeder(object):
    def __init__(self, max_job_nums=16):
        self.current_jobs = 0
        self.max_job_nums = max_job_nums

    def ask(self):
        if self.current_jobs >= self.max_job_nums:
            return False
        else:
            self.current_jobs += 1
            return True
    
    def leave(self):
        self.current_jobs -= 1

    def reset(self):
        self.current_jobs = 0

    def set_max(self, num):
        self.max_job_nums = num

app.speeder = speeder()

@app.route('/query/ask_to_start')
async def query_ask_to_start(request):
    if app.speeder.ask():
        logger.debug("/query/ask_to_start IP [{}] allow to start, current_jobs [{}]".format(request.ip, app.speeder.current_jobs))
        return text("go")
    else:
        logger.debug("/query/ask_to_start IP [{}] disallow to start, current_jobs [{}]".format(request.ip, app.speeder.current_jobs))
        return text("stop")

@app.route('/query/leave')
async def query_leave(request):
    app.speeder.leave()
    cli.release_instance(instance_id=get_instance_id_by_ip(request.ip))
    logger.debug("/query/leave IP [{}] ask to leave and ecs released automatically  ".format(request.ip))
    return text("OK")
@app.route('/query/reset')
async def query_reset(request):
    app.speeder.reset()
    return text("OK")
@app.route('/query/setmax')
async def query_reset(request):
    num = request.args['num']
    app.speeder.set_max(num)
    return text("OK")

@app.route('/query/check_plots')
async def query_check_plots(request):
    # ?plot_name=xxx&basedir=xxx
    data = request.args
    logger.debug("/query/check_plots got params [{}]".format(data))
    # async with asyncssh.connect(MINER_IP) as conn:
    #     result = await conn.run('du -sh {}'.format(os.path.join(data['basedir'], data['plot_name'])), check=True)
    #     print(result.stdout, end='')
    cmd = 'ssh {} "du -sh {}"'.format(MINER_IP, os.path.join(data['basedir'][0], os.path.basename((data['plot_name'][0]))))
    print(cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    try:
        res = p.stdout.readlines()[0].decode()
        size, _ = res.strip().split('\t')
        if int(size[:-1]) < 102 or size[-1] != 'G':
        #if int(size[:-1]) < 100 or size[-1] != 'G':
            alarm("check plot error with worker ip [{}] plot size [{}]".format(request.ip, size))
            return text("Wrong")
        return text("OK")
    except Exception as e:
        alarm("check plot error with exception [{}] worker ip [{}]".format(str(e), request.ip))
        return text("Wrong")

async def start_single_job(wip, fpk, ppk, remote_path, master_ip='172.27.67.252', master_port=5000, k=32):
    # async with asyncssh.connect(wip) as conn:
    #     result = await conn.run('nohup bash -x ~/run_worker.sh {} {} {} {} {} {} > ~/nohup.log 2>&1 < /dev/null &'.format(fpk, ppk, master_ip, master_port, remote_path, k), check=True)
    os.system('ssh {} "nohup bash -x ~/run_worker.sh {} {} {} {} {} {} > ~/nohup.log 2>&1 < /dev/null &"'.format(wip, fpk, ppk, master_ip, master_port, remote_path, k))
    return


async def controller():
    global group
    global subgroup
    for group in range(58, 104):
        job_info = []
        instances = await cli.create_instances(name='workerg{}{}'.format(group, subgroup), num=8)
        await asyncio.sleep(30) # 等待30s启动ssh
        job_info.append("Created {} instances: {} group {} subgroup {}".format(len(instances), instances, group, subgroup))
        for i, instance in enumerate(instances):
            ins_info = get_base_info_by_instance_id(instance)
            pip, iip = ins_info["PublicIpAddress"], ins_info["InnerIpAddress"]
            # await scp_file('/root/chia_arbitrary/singleton/run_worker.sh', "{}:{}".format(iip, '~/'))
            print("scp {} {}".format('/root/chia_arbitrary/singleton/run_worker.sh', "{}:{}".format(iip, '~/')))
            os.system("scp {} {}".format('/root/chia_arbitrary/singleton/run_worker.sh', "{}:{}".format(iip, '~/')))
            rp = '{}:{}'.format(MINER_IP, eval('sub{}dirs'.format(subgroup))[i])
            await start_single_job(iip, fpks[i], ppks[i], remote_path=rp)
            job_info.append("Start single job for Worker [{}] with remote path [{}]".format(iip, rp))
        app.alert.send_text("TEST:" + "\n".join(job_info))
        if subgroup == 'a':
            subgroup = 'b'
        elif subgroup == 'b':
            subgroup = 'a'
        else:
            pass
        # group += 1
        await asyncio.sleep(870)

if __name__ == "__main__":
    serv_coro = app.create_server(
        host="0.0.0.0", port=5000, debug=True, return_asyncio_server=True)
    loop = asyncio.get_event_loop()
    serv_task = asyncio.ensure_future(serv_coro, loop=loop)
    server = loop.run_until_complete(serv_task)
    server.after_start()
    try:
        loop.create_task(controller())
        loop.run_forever()
    except KeyboardInterrupt as e:
        loop.stop()
    finally:
        server.before_stop()
        close_task = server.close()
        loop.run_until_complete(close_task)
        for connection in server.connections:
            connection.close_if_idle()
        server.after_stop()

