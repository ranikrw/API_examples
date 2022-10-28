import pandas as pd
import numpy as np

import eikon as ek
import time

# print(ek.__version__)
# ek.set_log_level(1) #This is for enabling log detail

# The newest version of the eikon package is not avaialable
# through anaconda, but pip. If you need to use pip in anaconda, 
# install it with: conda install -c anaconda pip

# To use the API, you must set the key with:
# ek.set_app_key("your_key_here")
# See the guides mentioned in the README.md file for how
# to get a key

# Note that you must be logged in to Eikon Desktop on your 
# computer and on NTNU VPN to be able to use this API

# For not sharing my key on GitHub, I have saved my key in an 
# unavailable file which I upload here:
eikon_keys = pd.read_csv('../eikon_keys.csv',sep=';',index_col=0)
eikon_key = 'academic2-1'
ek.set_app_key(eikon_keys.at[eikon_key,'key'])

#######################################
## Download all daily closing prices 
## of all stocks in an index
#######################################

RIC_index = '.OSEBX'

# Definition parameters for downloading from Eikon
fields = [
    'TR.PriceClose.date', # Closing day
    'TR.PriceClose' # Price at closing day
    ]
start_date  = '2021-01-01'
end_date    = '2022-10-01'

# Getting and saving daily closing price of the market index itself
ek_data,err = ek.get_data(RIC_index,fields,{'SDate':start_date,'EDate':end_date,'Frq':'D'})
ek_data['Date'] = pd.to_datetime(ek_data['Date'], format='%Y-%m-%d').dt.date # Formating to date format
ek_data.index = ek_data['Date'] # Setting date to index
ek_data.index.name = '' # Removing index title
ek_data = ek_data['Price Close']
ek_data.name = RIC_index
ek_data = ek_data.sort_index(ascending=True)
ek_data.to_csv('market.csv',sep=';',index=True) # Saving to file

# Get the index constituents 
# https://community.developers.refinitiv.com/questions/47079/how-can-i-get-historical-data-for-the-constituents.html
ek_data,err= ek.get_data('0#'+RIC_index,['TR.RIC','TR.TickerSymbol'])
RIC_list_index = list(ek_data['RIC'])

# Price data per instument in index
if len(RIC_list_index)>200: # Because it tend to fail if you download too much. Dividing into two downloads:
    ek_data = pd.DataFrame()

    ek_data_temp,err = ek.get_data(RIC_list_index[:200],fields,{'SDate':start_date,'EDate':end_date,'Frq':'D'})
    ek_data = pd.concat([ek_data,ek_data_temp],axis=0)

    ek_data_temp,err = ek.get_data(RIC_list_index[200:],fields,{'SDate':start_date,'EDate':end_date,'Frq':'D'})
    ek_data = pd.concat([ek_data,ek_data_temp],axis=0)
else:
    ek_data,err = ek.get_data(RIC_list_index,fields,{'SDate':start_date,'EDate':end_date,'Frq':'D'})
ek_data['Date'] = pd.to_datetime(ek_data['Date'], format='%Y-%m-%d').dt.date

# Preparing and saving daily closing prices 
# of all stocks in the index
data_instruments = pd.DataFrame()
RIC_removed = []
for RIC in RIC_list_index:
    data_tic = ek_data[ek_data['Instrument']==RIC]
    if np.sum(pd.isnull(data_tic['Date']))!=0: # stock not in index in the defined time interval
        RIC_removed = RIC_removed+[RIC]
    else:
        data_tic = data_tic.drop_duplicates(subset=['Instrument','Date','Price Close'], keep='first')
        if len(data_tic['Date'].unique())!=data_tic.shape[0]:
            print('WARNING: not all dates unique for RIC '.format(RIC))
        data_tic.index = data_tic['Date']
        data_tic.index.name = ''
        data_tic = data_tic['Price Close']
        data_tic.name = RIC
        data_instruments = pd.concat([data_instruments,data_tic],axis=1)
        np.sum(pd.isnull(data_tic))
data_instruments = data_instruments.sort_index(ascending=True)

if len(RIC_removed)>0:
    print(RIC_index+': Removed {} stocks, or {} percent, because of not available daily prices for the entire estimation period.'.format(len(RIC_removed),np.round(100*len(RIC_removed)/len(RIC_list_index),2)))

# Saving to file
pd.Series(RIC_removed,name='RIC_removed').to_csv(folder_name+'/RIC_removed.csv',sep=';',index=False)
data_instruments.to_csv('instruments.csv',sep=';',index=True)

