import yaml
import smart_open
from utils.common import dict2obj

with smart_open.open('./config.yaml') as fs:
    Config = dict2obj(yaml.load(fs, Loader=yaml.FullLoader))