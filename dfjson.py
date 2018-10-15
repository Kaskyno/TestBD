# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 15:46:56 2018

@author: kaszo
"""
#import findspark
#findspark.init()


import json
import pandas as pd
from pandas.io.json import json_normalize


raw_data = []
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97%2Fcampaign%2F2017-08-21T05-15-57.json', 'r'):
    raw_data.append(json.loads(line))    


df = pd.DataFrame.from_dict(json_normalize(raw_data), orient='columns')
print (df.loc[1,:])