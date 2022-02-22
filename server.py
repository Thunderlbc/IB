import time
import json as ujson
import utils
from sanic import Sanic
from sanic.response import text, json
import asyncio
from utils.alert import WechatWork
import logging
from utils.trade_common import logger


def init_server(obj):
    app = Sanic(__name__)
    app.obj = obj
    obj.app = app

    @app.route('/')
    async def root(request):
        app.alert.send_text('Hello, World!')
        return text("Hello!")

    @app.route('/exec', methods=['POST'])
    async def exec(request):
        params = {}
        params['mts'] = int(round(time.time() * 1000))
        response = {
            "code": "200",
            "data": [],
            "msg": "OK",
        }
        data = request.json
        logger.info("Got Data[{}]".format(data))
        logger.debug("Got DATA [{}] method [{}]".format(
            data, data.get('method')))
        check_flag = utils.check_param_parser([
            utils.parse_request_with_response(
                data, params, "method", 'str', None, response, is_required=True),
            utils.parse_request_with_response(
                data, params, "args", 'str', None, response, is_required=False),
            utils.parse_request_with_response(
                data, params, "kwargs", 'str', None, response, is_required=False),
        ])
        if not check_flag:
            return json(response)
        else:
            args = params["args"].split(',') if 'args' in params and params['args'] is not None else []
            kwargs = ujson.loads(params["kwargs"]) if 'kwargs' in params and params['kwargs'] is not None else {}
            code, data, msg = await getattr(app.obj, params["method"])(*args, **kwargs)
            response['code'] = code
            response['data'] = data
            response['msg'] = msg
            return json(response)

    serv_coro = app.create_server(
        host="0.0.0.0", port=25000, debug=True, return_asyncio_server=True)
    loop = asyncio.get_event_loop()
    obj.loop = loop
    serv_task = asyncio.ensure_future(serv_coro, loop=loop)
    server = loop.run_until_complete(serv_task)
    server.after_start()
    try:
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


if __name__ == "__main__":
    class test:
        async def work(self, a):
            print(a)
            return 200, [], "OK"
    init_server(test())
