# ====================================================================================
#                           Interpreter: Python 3.6.1
#                            Test platform: Mac OS X
#
#                                Author: Zhu Deng
#                           Website: http://zhudeng.top
#                         Contact Email: zhudeng94@gmail.com
#                       Created: 2020 09 13 13:28
# ====================================================================================

import requests
import json
import pandas as pd
from tqdm import tqdm


def main():
    # getMetaData()

    # get Data by location
    df = pd.read_csv('metadata/location.csv')
    df = df[df['enabled']]
    df.set_index(df['location_id'].apply(str), inplace=True)
    for location_id in tqdm(df['location_id']):
        name = df['name'][location_id]
        tqdm.write(name)
        result = getDataByLocation(location_id)
        result.to_csv('dataByLocation/%s_%s.csv' % (location_id, name), index=False)


def getMetaData():
    url = 'https://vizhub.healthdata.org/gbd-foresight/api/metadata?type=forecasting&lang=41'
    res = requests.get(url)
    js = json.loads(res.text)['data']
    for key in js.keys():
        df = pd.DataFrame(js[key]).T
        df.to_csv('metadata/%s.csv' % key, index=False)


def getDataByLocation(location):
    # All causes:
    df = pd.read_csv('metadata/cause.csv')
    cause = ".".join('%s' % i for i in df['cause_id'].to_list())

    # # Year 2020-2040:
    # df = pd.read_csv('metadata/year.csv')
    # year = ".".join('%s' % i for i in df.index[df.index >= 2020].to_list())

    # All ages:
    df = pd.read_csv('metadata/age.csv')
    age = ".".join('%s' % i for i in df['age_id'].to_list())

    result = pd.DataFrame()
    for year in range(2020, 2041):
        url = 'https://vizhub.healthdata.org/gbd-foresight/api/data?' \
              'data=cause' \
              '&cause=' + cause + \
              '&year=' + str(year) + \
              '&unit=3' \
              '&chart=top' \
              '&value=observed' \
              '&metric=1' \
              '&location=' + location + \
              '&age=' + age + \
              '&sex=3' \
              '&paf=0' \
              '&scenario=1.2' \
              '&version=165' \
              '&context=cause' \
              '&base=single'

        res = requests.get(url)
        df = pd.DataFrame(json.loads(res.text)['data'], columns=json.loads(res.text)['keys'])
        for key in df.columns:
            try:
                temp = pd.read_csv('metadata/%s.csv' % key)
                temp.set_index(temp['%s_id' % key].apply(str), inplace=True)
                df[key] = temp['name'][df[key].apply(str)].to_list()
            except:
                pass
        # df.reset_index(inplace=True)
        result = pd.concat([result, df])
    return result


if __name__ == '__main__':
    main()
