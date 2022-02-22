import aiohttp
import asyncio
import json
import logging
import utils.logger
import socket

logger = logging.getLogger()


# params = {'key1': 'value1', 'key2': 'value2'}
async def async_get(url, params=None, jsonify=False, tag='DEFAULT'):
    try:
        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.get(url, params=params, timeout=600) as res:
                rtn_status = res.status
                if jsonify:
                    rtn = await res.json()
                else:
                    rtn = await res.text()
                return tag, rtn
    except Exception as e:
        logger.error("async get error: [{}]".format(str(e)))


# params = {'key1': 'value1', 'key2': 'value2'}
async def async_post(url, data=None, headers=None, jsonify=False, tag='DEFAULT'):
    try:
        print(data)
        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.post(url, data=json.dumps(data), headers=headers, timeout=600) as res:
                rtn_status = res.status
                if jsonify:
                    print(res)
                    rtn = await res.json()
                else:
                    rtn = await res.text()
                return tag, rtn
    except Exception as e:
        print("async post error: [{}]".format(str(e)))


def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


if __name__ == "__main__":
    async def test():
        tag, rtn = await async_post(url="http://localhost:19200/bot/test", data={"a": 1}, jsonify=False)
        print(tag, rtn)

    asyncio.run(test())
