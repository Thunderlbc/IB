import influxdb
import pandas as pd
import datetime
from influxdb import DataFrameClient
import traceback
import logging

_CLIENT = DataFrameClient(database='metrics')

def do_write_dataframe(data, measurement, tag_columns=None, **kwargs):
    if tag_columns is None or len(tag_columns) == 0:
        _CLIENT.write_points(data, measurement=measurement)
    else:
        value_columns = [col for col in data.columns if col not in tag_columns]
        for keys, part in data.groupby(tag_columns):
            if len(tag_columns) == 1:
                keys = [keys]
            tags = dict(zip(tag_columns, keys))
            logging.info('writing tags %s' % (tags))
            _CLIENT.write_points(part[value_columns],
                                measurement=measurement, tags=tags)


def write_dataframe(data, retry=3, **kwargs):
    for i in range(retry):
        try:
            do_write_dataframe(data, **kwargs)
            return True
        except Exception as e:
            traceback.print_exc()
    return False
if __name__ == '__main__':
    import pandas as pd
    #df = pd.DataFrame(data=list(range(30)),
    #                  index=pd.date_range(start='2021-05-10',
    #                                      periods=30, freq='H'), columns=['0'])
    #do_write_dataframe(data=df, measurement='test')

    #result = _CLIENT.query('SELECT  mean("diff")*100 FROM (SELECT (bitkub_ask / saxo_bid - 1) as diff from "THB_USD_ARBITRARY") WHERE time >= now() - 1d GROUP BY time(10s) ')
    #print (type(result['THB_USD_ARBITRARY']))

