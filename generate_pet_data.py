#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 23:06:06 2023

@author: slepot
"""

import pandas as pd
import polars as pl
import random
import numpy as np
from random import shuffle

from pandas._testing import rands_array

for year in range(2020,2025):

    n_char = 40
    arr_length = int(np.floor(np.random.uniform(low=200000, 
                                   high=300000)))
    
    s_arr = rands_array(n_char, arr_length)
    
    random.choice(['EF','GH'])
    
    data_attributes = [
        dict(name='property_id',
             kind='str',
             options='random',
             length=40
             ),
        dict(name='source',
             kind='str',
             options=['IAZI','BAFU']
             ),
        dict(name='kanton',
             kind='str',
             options=pd.read_csv('georef-switzerland-kanton-millesime.csv',
                                 sep=';')['Official Name Kanton'].tolist()
             ),
        dict(name='segment',
             kind='str',
             options=['EF','EG','EH','STE'],
             ),
        dict(name='type_of_building',
             kind='str',
             options=['hotel','home','office','factory','uni']
             ),
        dict(name='gclass',
             kind='str',
             options=[str(int(i)) for i in np.linspace(1000,2000,51)],
             lb=1000,
             ub=9999
             ),
        dict(name='value',
             kind='float',
             options='random',
             lb=0,
             ub=np.random.choice([int(1e3),int(1e6),int(1e9),int(1e12)])
             ),
        dict(name='loan',
             kind='float',
             options='random',
             lb=0,
             ub=1e6
             ),
        dict(name='loan_to_value_ratio',
             kind='float',
             options='random',
             lb=0,
             ub=200
             ),
        dict(name='year_GVR',
             kind='int',
             options='random',
             lb=1950,
             ub=2022
             ),
        dict(name='year_LOFT',
             kind='int',
             options='random',
             lb=1950,
             ub=2022
             ),
        dict(name='year_WD',
             kind='int',
             options='random',
             lb=1950,
             ub=2022
             ),
        dict(name='validation_level',
             kind='int',
             options='random',
             lb=1000,
             ub=9999
             ),
        dict(name='EGID',
             kind='int',
             options='random',
             lb=1000,
             ub=9999
             ),
        dict(name='EGRID',
             kind='int',
             options='random',
             lb=1000,
             ub=9999
             ),
        dict(name='validation_status',
             kind='int',
             options='random',
             lb=1000,
             ub=9999
             ),
        dict(name='ratings_CO2_nbr',
             kind='float',
             options='random',
             lb=0,
             ub=1000
             ),
        dict(name='ratings_CO2_cls',
             kind='str',
             options=['A','B','C','D','E','F','G']
             ),
        dict(name='ratings_energy_nbr',
             kind='float',
             options='random',
             lb=0,
             ub=1000
             ),
        dict(name='ratings_energy_cls',
             kind='str',
             options=['A','B','C','D','E','F','G']
             ),
        ]
    
    df = pd.DataFrame()
    
    for col in data_attributes:
        if col['kind'] == 'str' and col['options'] != 'random':
            non_uniform_ints = np.arange(len(col['options'])+1)
            bigger_list = []
            for i,opt in enumerate(col['options']):
                bigger_list.extend([opt]*non_uniform_ints[-(i+1)])
            df[col['name']] = np.random.choice(bigger_list, size=arr_length)
        if col['kind'] == 'str' and col['options'] == 'random':
            df[col['name']] = rands_array(col['length'], arr_length)
        if col['kind'] == 'float' and col['options'] == 'random':
            df[col['name']] = np.random.uniform(low=col['lb'], 
                                                high=col['ub'], 
                                                size=(arr_length,))
        if col['kind'] == 'int' and col['options'] == 'random':
            df[col['name']] = np.random.randint(low=col['lb'], 
                                                high=col['ub'], 
                                                size=(arr_length,))
    print(year)
    df['EGID'] = 'CH'+df['EGID'].astype('str')
    df['EGRID'] = 'CH'+df['EGRID'].astype('str')

    df.to_parquet('pet_data_'+str(year)+'.parquet')
    df.to_csv('pet_data_'+str(year)+'.csv')
    # df.to('pet_data.csv')
