import logging
from utils.trade_common import logger
import traceback
logger = logging.getLogger('parser')


def parse_request(pdict, params, key, _type, default):
    value = pdict.get(key, default)
    if value == None:
        return False
    if _type == "bool":
        if value == "0":
            params[key] = False
        else:
            params[key] = True
    elif _type == "str":
        params[key] = value
    elif _type == "int":
        params[key] = int(value)
    elif _type == "dict":
        params[key] = dict(value)
    elif _type == "json":
        params[key] = json.loads(value)
    else:
        params[key] = value
    return True


def parse_request_with_response(pdict, params, key, _type, default, response_, is_required=True):
    succ = False
    try:
        succ = parse_request(pdict, params, key, _type, default)

        if not succ and is_required:
            params[key] = default
            msg = response_.get('msg', '')
            if msg == 'OK':
                msg = ''
            msg += (key + " should exists with type:%s," % _type)
            response_['code'] = '110'
            response_['msg'] = msg
            return False
        else:
            return True

    except Exception as e:
        response_['code'] = '110'
        response_['msg'] = key + \
            " is not valid, please check its existence and type"
        logger.error("Parse Error: %s" % str(e))
        traceback.print_exc()
        return False


def check_param_parser(flag_list):
    return sum(flag_list) == len(flag_list)
