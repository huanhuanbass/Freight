import streamlit as st
st.set_page_config(layout="wide")

st.title('Baltic Exchange Dashboard')

from oceanbolt.sdk.client import APIClient
base_client = APIClient('eyJhbGciOiJSUzI1NiIsImtpZCI6ImUxNmUwNWI1NTZmNjVjYWEyNTg0ODU0N2FmYjNjZjI4IiwidHlwIjoiSldUIn0.eyJhdWQiOiJodHRwczovL2FwaS5vY2VhbmJvbHQuY29tIiwiZXhwIjoxNzMxNTk4NjIwLCJpYXQiOjE2OTk5NzYyNzEsImlzcyI6Imh0dHBzOi8vYXV0aC5vY2VhbmJvbHQuY29tLyIsImtpZCI6ImUxNmUwNWI1NTZmNjVjYWEyNTg0ODU0N2FmYjNjZjI4Iiwia3R5cGUiOiJhcGlrZXkiLCJvYmtpZCI6IlJ2VndVT2pZaGhXSjY3NU8iLCJvcmdpZCI6ImNvZmNvIiwic3ViIjoiYXV0aDB8NjU1MzUwNDEyOGQ4Nzg1M2JlZGY2NDdiIn0.UIsNEUGlFvKJyN4a-3kj6xcbbrygoC3B0xWZVSLdpEx7uSyr2ZD3i-lJ3PgiEoOOJettAcnuGjh0NwgaL_Qx4T-GAxvLqdAKJe_8c-hOzETmeHdeme8HfxIGg5i3bPY_PvRx1RGfAjKy9y9sFB3Qe1ARB-Q2KDwN55sVNQIX7Cdyvpxm2WbAEaGyqZzW4kik4FAYOqYVfUgfRWVNAtv372LQlnCF82PC6JeZKJifHu6WBWh3Y7ATVt1QZDe9yF7ITFGAHsQooVu9hEpJ-VvEcf_fEVGIJYJIN0bhJQtw_lOCW_VVMy2n4RolaGsmqpnNdDqh2PctZWVF3MoYO4j2YC8a8sZ_FvouchjrMs6YX81DIfm2EjFqQ_vZE4SBTEiKYZ-_T1UvwHB7MuqT1Ubhpk0pjkoH0hP1k04dBRZc5kxlvwEPDTvX_K2HAM2eYfY10iF-R_iLcO7eU7pR_-Ch0iwelt5xEabFzVIMqTxNoDZHAaaZR0YhR58Md3bD7Xr9kk3ZSvfBJ-ctbEd2uKj_GrdVyKE4bEZSoV_QZ4YoQGO6lVhR5hDwfrqNioWqj1JKlwJ-Q1V8L3396DQzE6gtHgccP951QzEBfe1SUMItyjeULa4FjQUFhm7PsQKqUxMg6kZumDYYKpM-_qI0g24PvhuwH_7lW3cRqe7v-018qhU')
from oceanbolt.sdk.data.trade_flows import TradeFlows

import streamlit as st
import warnings; warnings.simplefilter('ignore')
import pandas as pd
import numpy as np
from datetime import date
from calendar import monthrange
from pandas.tseries.offsets import BDay
import requests
import ftplib

pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)


st.write('Loading Data...')


st.text('----Getting Freight Data...')

#Getting Spot Freight Data
@st.cache_data()
def load_spot_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(10)
    params={'from':datefrom,'to':dateto}
    urlcape='https://api.balticexchange.com/api/v1.3/feed/FDS8Y7DVQJOFDYW6SZUZWMGJCVMEWBLI/data'
    urlpmx='https://api.balticexchange.com/api/v1.3/feed/FDS72H2FOQWJSDTJBVW55HJY1Z6W8ZJ0/data'
    urlsmx='https://api.balticexchange.com/api/v1.3/feed/FDSQZHFHC242QBA1M4OMIW89Q1GBJGCL/data'

    response = requests.get(urlcape, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    spotcape=pd.DataFrame(df.loc[0,'data'])
    spotcape.set_index('date',inplace=True)
    spotcape.rename(columns={'value':'C5TC'},inplace=True)

    response = requests.get(urlpmx, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    spotpmx=pd.DataFrame(df.loc[0,'data'])
    spotpmx.set_index('date',inplace=True)
    spotpmx.rename(columns={'value':'P4TC'},inplace=True)

    response = requests.get(urlsmx, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    spotsmx=pd.DataFrame(df.loc[0,'data'])
    spotsmx.set_index('date',inplace=True)
    spotsmx.rename(columns={'value':'S10TC'},inplace=True)

    spotnew=pd.merge(spotcape,spotpmx,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotsmx,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('spot.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates()
    spot.set_index('Date',inplace=True)
    spot=spot.dropna(subset=['P4TC'])

    spot.to_csv('spot.csv',index_label='Date')

    return spot

spot=load_spot_data()


#Getting Spot Freight Data if API doesn't work
@st.cache_data()
def load_spot_data_backup():
    spot=pd.read_csv('Baltic Exchange - Historic Data.csv')
    spot.set_index('Date',inplace=True)
    spot.index=pd.to_datetime(spot.index,dayfirst=True)
    #spot=spot[spot.index>=pd.to_datetime(date(2015,1,1))]

    return spot


#spot=load_spot_data_backup()


if 'spot' not in st.session_state:
    st.session_state['spot']=spot



#Getting PMX FFA Data
@st.cache_data()
def load_pmx_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(10)
    params={'from':datefrom,'to':dateto}
    urlpmxffa='https://api.balticexchange.com/api/v1.3/feed/FDSLG4CKMQ0QEYHE8NJ2DTGR2S6N5S7P/data'

    response = requests.get(urlpmxffa, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    ffapmx=pd.DataFrame(df.loc[0,'groupings'])

    ffapmx_=pd.DataFrame()
    for j in range(len(ffapmx.index)):
        ffapmx_0=ffapmx.loc[j,'date']
        ffapmx_n=pd.DataFrame(ffapmx.loc[j,'groups'])
        for i in range(len(ffapmx_n.index)):
            ffapmx_n_0=ffapmx_n.loc[i,'periodType']
            ffapmx_n_n=pd.DataFrame(ffapmx_n.loc[i,'projections'])
            ffapmx_n_n['periodType']=ffapmx_n_0
            ffapmx_n_n['date']=ffapmx_0
            ffapmx_=pd.concat([ffapmx_,ffapmx_n_n])
            
    ffapmx_[['Month','Year']]=ffapmx_['period'].str.split(' ',expand=True)
    ffapmx_['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                            'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                            'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffapmx_['Contract']='20'+ffapmx_['Year']+'_'+ffapmx_['Month']
    ffapmx_pt1=ffapmx_.pivot_table(index='archiveDate',columns='Contract',values='value',aggfunc='mean')
    ffapmx_pt1.index=pd.to_datetime(ffapmx_pt1.index)

    p4tcold=pd.read_csv('p4tc.csv')
    p4tcold=p4tcold.set_index('Date')
    p4tcold.index=pd.to_datetime(p4tcold.index)
    p4tc=pd.concat([p4tcold,ffapmx_pt1])
    p4tc.reset_index(inplace=True)
    p4tc.rename(columns={'index':'Date'},inplace=True)
    p4tc=p4tc.drop_duplicates()
    p4tc.set_index('Date',inplace=True)
    p4tc.to_csv('p4tc.csv',index_label='Date')

    ffapmx_pt2=ffapmx_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ffapmx_pt2.index=pd.to_datetime(ffapmx_pt2.index)
    ffapmx_pt2=ffapmx_pt2[['4TC_PCURMON','4TC_P+1MON','4TC_P+2MON','4TC_P+3MON','4TC_P+4MON','4TC_P+5MON', 
              '4TC_PCURQ','4TC_P+1Q','4TC_P+2Q','4TC_P+3Q','4TC_P+4Q','4TC_P+5Q','4TC_P+6Q',
              '4TC_P+1CAL','4TC_P+2CAL','4TC_P+3CAL','4TC_P+4CAL','4TC_P+5CAL','4TC_P+6CAL','4TC_P+7CAL']]

    p4tc_rold=pd.read_csv('p4tc_r.csv')
    p4tc_rold=p4tc_rold.set_index('Date')
    p4tc_rold.index=pd.to_datetime(p4tc_rold.index)
    p4tc_r=pd.concat([p4tc_rold,ffapmx_pt2])
    p4tc_r.reset_index(inplace=True)
    p4tc_r.rename(columns={'index':'Date'},inplace=True)
    p4tc_r=p4tc_r.drop_duplicates()
    p4tc_r.set_index('Date',inplace=True)
    p4tc_r.to_csv('p4tc_r.csv',index_label='Date')

    spotpmx=spot[['P4TC']]

    p4tc=pd.merge(spotpmx,p4tc,left_index=True,right_index=True,how='outer')

    p4tc.dropna(subset='P4TC',inplace=True)

    p4tc_r=pd.merge(spotpmx,p4tc_r,left_index=True,right_index=True,how='outer')
    p4tc_r.dropna(subset='P4TC',inplace=True)

    return p4tc, p4tc_r

p4tc=load_pmx_ffa_data()[0]
p4tc_r=load_pmx_ffa_data()[1]



#Getting PMX FFA Data if API doesn't work
@st.cache_data()
def load_pmx_ffa_data_backup():
    ffapmx=pd.read_csv('bfa_panamax_74.csv')
    ffapmx[['Month','Year']]=ffapmx['FFADescription'].str.split(' ',expand=True)
    ffapmx['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                         'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                          'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffapmx['NeedCorrection']=ffapmx['RouteIdentifier'].str.contains('CURQ')
    ffapmx['Month']=np.where(ffapmx['NeedCorrection']==True,ffapmx['Month'].replace({'M3':'Q1','M6':'Q2','M9':'Q3','M12':'Q4'}),ffapmx['Month'])
    ffapmx['Contract']='20'+ffapmx['Year']+'_'+ffapmx['Month']

    ffap4tc=ffapmx[ffapmx['RouteIdentifier'].str.contains('4TC_P')]
    ffap4tc_pt1=ffap4tc.pivot_table(index='ArchiveDate',columns='Contract',values='RouteAverage',aggfunc='mean')
    ffap4tc_pt1.index=pd.to_datetime(ffap4tc_pt1.index,dayfirst=True)
    ffap4tc_pt1.sort_index(inplace=True)

    ffapmx_pt2=ffapmx.pivot_table(index='ArchiveDate',columns='RouteIdentifier',values='RouteAverage',aggfunc='mean')
    ffapmx_pt2.index=pd.to_datetime(ffapmx_pt2.index,dayfirst=True)
    ffapmx_pt2.sort_index(inplace=True)
    ffap4tc_pt2=ffapmx_pt2[['4TC_PCURMON','4TC_P+1MON','4TC_P+2MON','4TC_P+3MON','4TC_P+4MON','4TC_P+5MON', 
              '4TC_PCURQ','4TC_P+1Q','4TC_P+2Q','4TC_P+3Q','4TC_P+4Q','4TC_P+5Q','4TC_P+6Q',
              '4TC_P+1CAL','4TC_P+2CAL','4TC_P+3CAL','4TC_P+4CAL','4TC_P+5CAL','4TC_P+6CAL','4TC_P+7CAL']]

    spotpmx=spot[['P4TC']]

    p4tc=pd.merge(spotpmx,ffap4tc_pt1,left_index=True,right_index=True,how='outer')
    p4tc.dropna(subset='P4TC',inplace=True)

    p4tc_r=pd.merge(spotpmx,ffap4tc_pt2,left_index=True,right_index=True,how='outer')
    p4tc_r.dropna(subset='P4TC',inplace=True)

    return p4tc, p4tc_r

#p4tc=load_pmx_ffa_data_backup()[0]
#p4tc_r=load_pmx_ffa_data_backup()[1]


if 'p4tc' not in st.session_state:
    st.session_state['p4tc']=p4tc
if 'p4tc_r' not in st.session_state:
    st.session_state['p4tc_r']=p4tc_r




#Getting Cape FFA Data
@st.cache_data()
def load_cape_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(10)
    params={'from':datefrom,'to':dateto}
    urlcapeffa='https://api.balticexchange.com/api/v1.3/feed/FDS2QE6Y0F4LPFOKC4YYVCM38NYVR5DU/data'

    response = requests.get(urlcapeffa, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    ffacape=pd.DataFrame(df.loc[0,'groupings'])

    ffacape_=pd.DataFrame()
    for j in range(len(ffacape.index)):
        ffacape_0=ffacape.loc[j,'date']
        ffacape_n=pd.DataFrame(ffacape.loc[j,'groups'])
        for i in range(len(ffacape_n.index)):
            ffacape_n_0=ffacape_n.loc[i,'periodType']
            ffacape_n_n=pd.DataFrame(ffacape_n.loc[i,'projections'])
            ffacape_n_n['periodType']=ffacape_n_0
            ffacape_n_n['date']=ffacape_0
            ffacape_=pd.concat([ffacape_,ffacape_n_n])
            
    ffacape_[['Month','Year']]=ffacape_['period'].str.split(' ',expand=True)
    ffacape_['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                            'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                            'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffacape_['Contract']='20'+ffacape_['Year']+'_'+ffacape_['Month']
    ffacape_pt1=ffacape_.pivot_table(index='archiveDate',columns='Contract',values='value',aggfunc='mean')
    ffacape_pt1.index=pd.to_datetime(ffacape_pt1.index)

    c5tcold=pd.read_csv('c5tc.csv')
    c5tcold=c5tcold.set_index('Date')
    c5tcold.index=pd.to_datetime(c5tcold.index)
    c5tc=pd.concat([c5tcold,ffacape_pt1])
    c5tc.reset_index(inplace=True)
    c5tc.rename(columns={'index':'Date'},inplace=True)
    c5tc=c5tc.drop_duplicates()
    c5tc.set_index('Date',inplace=True)
    c5tc.to_csv('c5tc.csv',index_label='Date')

    ffacape_pt2=ffacape_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ffacape_pt2.index=pd.to_datetime(ffacape_pt2.index)
    ffacape_pt2=ffacape_pt2[['5TC_CCURMON','5TC_C+1MON','5TC_C+2MON','5TC_C+3MON','5TC_C+4MON','5TC_C+5MON', 
              '5TC_CCURQ','5TC_C+1Q','5TC_C+2Q','5TC_C+3Q','5TC_C+4Q','5TC_C+5Q','5TC_C+6Q',
              '5TC_C+1CAL','5TC_C+2CAL','5TC_C+3CAL','5TC_C+4CAL','5TC_C+5CAL','5TC_C+6CAL','5TC_C+7CAL']]

    c5tc_rold=pd.read_csv('c5tc_r.csv')
    c5tc_rold=c5tc_rold.set_index('Date')
    c5tc_rold.index=pd.to_datetime(c5tc_rold.index)
    c5tc_r=pd.concat([c5tc_rold,ffacape_pt2])
    c5tc_r.reset_index(inplace=True)
    c5tc_r.rename(columns={'index':'Date'},inplace=True)
    c5tc_r=c5tc_r.drop_duplicates()
    c5tc_r.set_index('Date',inplace=True)
    c5tc_r.to_csv('c5tc_r.csv',index_label='Date')
    
    spotcape=spot[['C5TC']]

    c5tc=pd.merge(spotcape,c5tc,left_index=True,right_index=True,how='outer')
    c5tc.dropna(subset='C5TC',inplace=True)

    c5tc_r=pd.merge(spotcape,c5tc_r,left_index=True,right_index=True,how='outer')
    c5tc_r.dropna(subset='C5TC',inplace=True)

    return c5tc, c5tc_r

c5tc=load_cape_ffa_data()[0]
c5tc_r=load_cape_ffa_data()[1]

#Getting Cape FFA Data if API doesn't work
@st.cache_data()
def load_cape_ffa_data_backup():
    ffacape=pd.read_csv('bfa_cape.csv')
    ffacape[['Month','Year']]=ffacape['FFADescription'].str.split(' ',expand=True)
    ffacape['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                         'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                          'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffacape['NeedCorrection']=ffacape['RouteIdentifier'].str.contains('CURQ')
    ffacape['Month']=np.where(ffacape['NeedCorrection']==True,ffacape['Month'].replace({'M3':'Q1','M6':'Q2','M9':'Q3','M12':'Q4'}),ffacape['Month'])
    ffacape['Contract']='20'+ffacape['Year']+'_'+ffacape['Month']

    ffac5tc=ffacape[ffacape['RouteIdentifier'].str.contains('5TC_C')]
    ffac5tc_pt1=ffac5tc.pivot_table(index='ArchiveDate',columns='Contract',values='RouteAverage',aggfunc='mean')
    ffac5tc_pt1.index=pd.to_datetime(ffac5tc_pt1.index,dayfirst=True)
    ffac5tc_pt1.sort_index(inplace=True)

    ffacape_pt2=ffacape.pivot_table(index='ArchiveDate',columns='RouteIdentifier',values='RouteAverage',aggfunc='mean')
    ffacape_pt2.index=pd.to_datetime(ffacape_pt2.index,dayfirst=True)
    ffacape_pt2.sort_index(inplace=True)
    ffac5tc_pt2=ffacape_pt2[['5TC_CCURMON','5TC_C+1MON','5TC_C+2MON','5TC_C+3MON','5TC_C+4MON','5TC_C+5MON', 
              '5TC_CCURQ','5TC_C+1Q','5TC_C+2Q','5TC_C+3Q','5TC_C+4Q','5TC_C+5Q','5TC_C+6Q',
              '5TC_C+1CAL','5TC_C+2CAL','5TC_C+3CAL','5TC_C+4CAL','5TC_C+5CAL','5TC_C+6CAL','5TC_C+7CAL','5TC_C+8CAL','5TC_C+9CAL']]

    spotcape=spot[['C5TC']]

    c5tc=pd.merge(spotcape,ffac5tc_pt1,left_index=True,right_index=True,how='outer')
    c5tc.dropna(subset='C5TC',inplace=True)

    c5tc_r=pd.merge(spotcape,ffac5tc_pt2,left_index=True,right_index=True,how='outer')
    c5tc_r.dropna(subset='C5TC',inplace=True)

    return c5tc, c5tc_r

#c5tc=load_cape_ffa_data_backup()[0]
#c5tc_r=load_cape_ffa_data_backup()[1]


if 'c5tc' not in st.session_state:
    st.session_state['c5tc']=c5tc
if 'c5tc_r' not in st.session_state:
    st.session_state['c5tc_r']=c5tc_r




#Getting SMX FFA Data
@st.cache_data()
def load_smx_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(10)
    params={'from':datefrom,'to':dateto}
    urlsmxffa='https://api.balticexchange.com/api/v1.3/feed/FDSGGYH6236OC931DOFJ7O4RJ0CK0A8B/data'

    response = requests.get(urlsmxffa, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    ffasmx=pd.DataFrame(df.loc[0,'groupings'])

    ffasmx_=pd.DataFrame()
    for j in range(len(ffasmx.index)):
        ffasmx_0=ffasmx.loc[j,'date']
        ffasmx_n=pd.DataFrame(ffasmx.loc[j,'groups'])
        for i in range(len(ffasmx_n.index)):
            ffasmx_n_0=ffasmx_n.loc[i,'periodType']
            ffasmx_n_n=pd.DataFrame(ffasmx_n.loc[i,'projections'])
            ffasmx_n_n['periodType']=ffasmx_n_0
            ffasmx_n_n['date']=ffasmx_0
            ffasmx_=pd.concat([ffasmx_,ffasmx_n_n])
            
    ffasmx_[['Month','Year']]=ffasmx_['period'].str.split(' ',expand=True)
    ffasmx_['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                            'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                            'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffasmx_['Contract']='20'+ffasmx_['Year']+'_'+ffasmx_['Month']
    ffasmx_pt1=ffasmx_.pivot_table(index='archiveDate',columns='Contract',values='value',aggfunc='mean')
    ffasmx_pt1.index=pd.to_datetime(ffasmx_pt1.index)

    s10tcold=pd.read_csv('s10tc.csv')
    s10tcold=s10tcold.set_index('Date')
    s10tcold.index=pd.to_datetime(s10tcold.index)
    s10tc=pd.concat([s10tcold,ffasmx_pt1])
    s10tc.reset_index(inplace=True)
    s10tc.rename(columns={'index':'Date'},inplace=True)
    s10tc=s10tc.drop_duplicates()
    s10tc.set_index('Date',inplace=True)
    s10tc.to_csv('s10tc.csv',index_label='Date')

    ffasmx_pt2=ffasmx_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ffasmx_pt2.index=pd.to_datetime(ffasmx_pt2.index)
    ffasmx_pt2=ffasmx_pt2[['10TC_SCURMON','10TC_S+1MON','10TC_S+2MON','10TC_S+3MON','10TC_S+4MON','10TC_S+5MON', 
              '10TC_SCURQ','10TC_S+1Q','10TC_S+2Q','10TC_S+3Q','10TC_S+4Q',
              '10TC_S+1CAL','10TC_S+2CAL','10TC_S+3CAL','10TC_S+4CAL','10TC_S+5CAL','10TC_S+6CAL','10TC_S+7CAL']]

    s10tc_rold=pd.read_csv('s10tc_r.csv')
    s10tc_rold=s10tc_rold.set_index('Date')
    s10tc_rold.index=pd.to_datetime(s10tc_rold.index)
    s10tc_r=pd.concat([s10tc_rold,ffasmx_pt2])
    s10tc_r.reset_index(inplace=True)
    s10tc_r.rename(columns={'index':'Date'},inplace=True)
    s10tc_r=s10tc_r.drop_duplicates()
    s10tc_r.set_index('Date',inplace=True)
    s10tc_r.to_csv('s10tc_r.csv',index_label='Date')

    spotsmx=spot[['S10TC']]

    s10tc=pd.merge(spotsmx,s10tc,left_index=True,right_index=True,how='outer')
    s10tc.dropna(subset='S10TC',inplace=True)

    s10tc_r=pd.merge(spotsmx,s10tc_r,left_index=True,right_index=True,how='outer')
    s10tc_r.dropna(subset='S10TC',inplace=True)

    return s10tc, s10tc_r

s10tc=load_smx_ffa_data()[0]
s10tc_r=load_smx_ffa_data()[1]





#Getting SMX FFA Data if API doesn't work
@st.cache_data()
def load_smx_ffa_data_backup():
    ffasmx=pd.read_csv('bfa_supramax.csv')
    ffasmx[['Month','Year','others']]=ffasmx['FFADescription'].str.split(' ',expand=True)
    ffasmx['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                         'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                          'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffasmx['NeedCorrection']=ffasmx['RouteIdentifier'].str.contains('CURQ')
    ffasmx['Month']=np.where(ffasmx['NeedCorrection']==True,ffasmx['Month'].replace({'M3':'Q1','M6':'Q2','M9':'Q3','M12':'Q4'}),ffasmx['Month'])
    ffasmx['Contract']='20'+ffasmx['Year']+'_'+ffasmx['Month']

    ffas10tc=ffasmx[ffasmx['RouteIdentifier'].str.contains('10TC_S')]
    ffas10tc_pt1=ffas10tc.pivot_table(index='ArchiveDate',columns='Contract',values='RouteAverage',aggfunc='mean')
    ffas10tc_pt1.index=pd.to_datetime(ffas10tc_pt1.index,dayfirst=True)
    ffas10tc_pt1.sort_index(inplace=True)

    ffasmx_pt2=ffasmx.pivot_table(index='ArchiveDate',columns='RouteIdentifier',values='RouteAverage',aggfunc='mean')
    ffasmx_pt2.index=pd.to_datetime(ffasmx_pt2.index,dayfirst=True)
    ffasmx_pt2.sort_index(inplace=True)
    ffas10tc_pt2=ffasmx_pt2[['10TC_SCURMON','10TC_S+1MON','10TC_S+2MON','10TC_S+3MON','10TC_S+4MON','10TC_S+5MON', 
              '10TC_SCURQ','10TC_S+1Q','10TC_S+2Q','10TC_S+3Q','10TC_S+4Q',
              '10TC_S+1CAL','10TC_S+2CAL','10TC_S+3CAL','10TC_S+4CAL','10TC_S+5CAL','10TC_S+6CAL','10TC_S+7CAL']]

    spotsmx=spot[['S10TC']]

    s10tc=pd.merge(spotsmx,ffas10tc_pt1,left_index=True,right_index=True,how='outer')
    s10tc.dropna(subset='S10TC',inplace=True)

    s10tc_r=pd.merge(spotsmx,ffas10tc_pt2,left_index=True,right_index=True,how='outer')
    s10tc_r.dropna(subset='S10TC',inplace=True)

    return s10tc, s10tc_r

#s10tc=load_smx_ffa_data_backup()[0]
#s10tc_r=load_smx_ffa_data_backup()[1]


if 's10tc' not in st.session_state:
    st.session_state['s10tc']=s10tc
if 's10tc_r' not in st.session_state:
    st.session_state['s10tc_r']=s10tc_r



#Getting PMX Option IV Data
@st.cache_data()
def load_pmx_iv_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(10)
    params={'from':datefrom,'to':dateto}
    urlpmxiv='https://api.balticexchange.com/api/v1.3/feed/FDSJP7F4SNVXW99J687O9JIINKHB8MQ7/data'

    response = requests.get(urlpmxiv, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    ivpmx=pd.DataFrame(df.loc[0,'groupings'])

    ivpmx_=pd.DataFrame()
    for j in range(len(ivpmx.index)):
        ivpmx_0=ivpmx.loc[j,'date']
        ivpmx_n=pd.DataFrame(ivpmx.loc[j,'groups'])
        for i in range(len(ivpmx_n.index)):
            ivpmx_n_0=ivpmx_n.loc[i,'periodType']
            ivpmx_n_n=pd.DataFrame(ivpmx_n.loc[i,'projections'])
            ivpmx_n_n['periodType']=ivpmx_n_0
            ivpmx_n_n['date']=ivpmx_0
            ivpmx_=pd.concat([ivpmx_,ivpmx_n_n])
            
    ivpmx_[['Month','Year']]=ivpmx_['period'].str.split(' ',expand=True)
    ivpmx_['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                            'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                            'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ivpmx_['Contract']='20'+ivpmx_['Year']+'_'+ivpmx_['Month']
    ivpmx_pt1=ivpmx_.pivot_table(index='archiveDate',columns='Contract',values='value',aggfunc='mean')
    ivpmx_pt1.index=pd.to_datetime(ivpmx_pt1.index)

    ivold=pd.read_csv('p4tciv.csv')
    ivold=ivold.set_index('Date')
    ivold.index=pd.to_datetime(ivold.index)
    iv=pd.concat([ivold,ivpmx_pt1])
    iv.reset_index(inplace=True)
    iv.rename(columns={'index':'Date'},inplace=True)
    iv=iv.drop_duplicates()
    iv.set_index('Date',inplace=True)
    iv.to_csv('p4tciv.csv',index_label='Date')

    ivpmx_pt2=ivpmx_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ivpmx_pt2.index=pd.to_datetime(ivpmx_pt2.index)

    iv_rold=pd.read_csv('p4tciv_r.csv')
    iv_rold=iv_rold.set_index('Date')
    iv_rold.index=pd.to_datetime(iv_rold.index)
    iv_r=pd.concat([iv_rold,ivpmx_pt2])
    iv_r.reset_index(inplace=True)
    iv_r.rename(columns={'index':'Date'},inplace=True)
    iv_r=iv_r.drop_duplicates()
    iv_r.set_index('Date',inplace=True)
    iv_r.to_csv('p4tciv_r.csv',index_label='Date')

    iv_r=iv_r[['IV_PTC_CURMON','IV_PTC_+1MON','IV_PTC_+2MON','IV_PTC_+1Q','IV_PTC_+2Q','IV_PTC_+3Q','IV_PTC_+4Q','IV_PTC_+1CAL','IV_PTC_+2CAL','IV_PTC_+3CAL','IV_PTC_+4CAL']]

    p4tciv=iv.copy()
    p4tciv_r=iv_r.copy()

    return p4tciv, p4tciv_r

p4tciv=load_pmx_iv_data()[0]
p4tciv_r=load_pmx_iv_data()[1]



#Getting PMX Option IV Data if API doesn't work
@st.cache_data()
def load_pmx_iv_data_backup():
    ivpmx=pd.read_csv('boa_panamax_74.csv')

    ivpmx[['Month','Year']]=ivpmx['FFADescription'].str.split(' ',expand=True)
    ivpmx['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                         'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                          'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ivpmx['NeedCorrection']=ivpmx['RouteIdentifier'].str.contains('CURQ')
    ivpmx['Month']=np.where(ivpmx['NeedCorrection']==True,ivpmx['Month'].replace({'M3':'Q1','M6':'Q2','M9':'Q3','M12':'Q4'}),ivpmx['Month'])
    ivpmx['Contract']='20'+ivpmx['Year']+'_'+ivpmx['Month']

    ivp4tc=ivpmx[ivpmx['RouteIdentifier'].str.contains('IV_PTC')]
    ivp4tc_pt1=ivp4tc.pivot_table(index='ArchiveDate',columns='Contract',values='RouteAverage',aggfunc='mean')
    ivp4tc_pt1.index=pd.to_datetime(ivp4tc_pt1.index,dayfirst=True)
    ivp4tc_pt1.sort_index(inplace=True)

    ivpmx_pt2=ivpmx.pivot_table(index='ArchiveDate',columns='RouteIdentifier',values='RouteAverage',aggfunc='mean')
    ivpmx_pt2.index=pd.to_datetime(ivpmx_pt2.index,dayfirst=True)
    ivpmx_pt2.sort_index(inplace=True)

    ivp4tc_pt2=ivpmx_pt2[['IV_PTC_CURMON','IV_PTC_+1MON','IV_PTC_+2MON','IV_PTC_+1Q','IV_PTC_+2Q','IV_PTC_+3Q','IV_PTC_+4Q','IV_PTC_+1CAL','IV_PTC_+2CAL','IV_PTC_+3CAL','IV_PTC_+4CAL']]

    p4tciv=ivp4tc_pt1.copy()

    p4tciv_r=ivp4tc_pt2.copy()

    return p4tciv, p4tciv_r

#p4tciv=load_pmx_iv_data_backup()[0]
#p4tciv_r=load_pmx_iv_data_backup()[1]


if 'p4tciv' not in st.session_state:
    st.session_state['p4tciv']=p4tciv
if 'p4tciv_r' not in st.session_state:
    st.session_state['p4tciv_r']=p4tciv_r


st.text('Freight Data Done')

#Getting Bunker Data
st.text('----Getting Bunker Data...')

@st.cache_data()
def load_bunker_data():
    bunker=pd.read_csv('Historical data - Bunker Prices.csv')

    bunker_s=bunker[bunker['Period']=='SPOT']
    bunker_f=bunker[bunker['Period']!='SPOT']

    bunker_f[['Month','Year']]=bunker_f['Period'].str.split('-',expand=True)
    bunker_f['Month'].replace({'JAN':'1','FEB':'2','MAR':'3','APR':'4','MAY':'5','JUN':'6',
                         'JUL':'7','AUG':'8','SEP':'9','OCT':'10','NOV':'11','DEC':'12'},inplace=True)

    bunker_f['Fixed Contract']=bunker_f['Year']+'_M'+bunker_f['Month']
    bunker_f['Month']=pd.to_numeric(bunker_f['Month'])
    bunker_f['Year']=pd.to_numeric(bunker_f['Year'])

    bunker_f['Archive Month']=pd.to_datetime(bunker_f['Date']).dt.month
    bunker_f['Archive Year']=pd.to_datetime(bunker_f['Date']).dt.year
    bunker_f['Rolling Month Gap']=(bunker_f['Year']-bunker_f['Archive Year'])*12+(bunker_f['Month']-bunker_f['Archive Month'])


    bunker_s['Amount']=bunker_s['Amount'].astype(str)
    bunker_f['Amount']=bunker_f['Amount'].astype(str)
    bunker_s['Amount']=bunker_s['Amount'].str.replace(',', '')
    bunker_f['Amount']=bunker_f['Amount'].str.replace(',', '')  
    bunker_s['Amount']=bunker_s['Amount'].astype(float)
    bunker_f['Amount']=bunker_f['Amount'].astype(float)
    bunker_s['Date']=pd.to_datetime(bunker_s['Date'])
    bunker_f['Date']=pd.to_datetime(bunker_f['Date'])

    return bunker_s, bunker_f

bunker_s=load_bunker_data()[0]
bunker_f=load_bunker_data()[1]



if 'bunker_s' not in st.session_state:
    st.session_state['bunker_s']=bunker_s

if 'bunker_f' not in st.session_state:
    st.session_state['bunker_f']=bunker_f


st.text('Bunker Data Done!...')

#Getting Fleet Data
st.text('----Getting Fleet Data...')

@st.cache_data()
def load_fleet_data():

    FTP_HOST='transfer.clarksons.com'
    FTP_USER='Noble.data'
    FTP_PASS='z026GXFB'

    ftp=ftplib.FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASS, encoding='utf-8')

    latestfile=None
    for f in ftp.mlsd(facts=['type','modify','size']):
        filetype=f[1]['type']
        if not filetype=='file':
            continue
        if latestfile==None:
            latestfile=f
        else:
            if latestfile[1]['modify']<=f[1]['modify']:
                latestfile=f
                
    filename=latestfile[0]
    txt='RETR '+filename

    with open(filename,'wb') as file:
        ftp.retrbinary(txt,file.write)
        
    ftp.quit()
    raw = pd.read_csv(filename,compression='zip',encoding='unicode_escape')
    #raw=pd.read_csv('Cofco_Dataset.csv',encoding='ISO-8859-1')
    fleet=raw[raw['P36_VESSEL_TYPE'].isin(['Bulk Carrier','Ore Carrier'])]
    fleet['Delivery Year']=fleet['A12_YEAR_BUILT']
    fleet['Delivery Month']=fleet['A13_MONTH_BUILT']
    fleet['Delivery Date']=pd.to_datetime(dict(year=fleet['Delivery Year'],month=fleet['Delivery Month'],day=1))
    fleet['Dwt']=fleet['A04_DWT_tonnes']
    fleet['Segment']=pd.cut(fleet['Dwt'],bins=[0,9999,42999,67999,109999,999999],labels=['Others','Handysize','Supramax','Panamax','Capesize'])
    fleet['Subsegment']=pd.cut(fleet['Dwt'],bins=[0,9999,26999,42999,49999,59999,67999,79999,89999,109999,139999,179999,249999,999999],labels=['Others','Small Handysize','Large Handysize','Handymax','Supramax','Ultramax','Panamax','Kamsarmax','Postpanamax','Babycape','Capesize','Large Capesize','VLOC'])
    fleet['Demolition Date']=pd.to_datetime(fleet['REMOVAL_DATE'],dayfirst=True,format='mixed')
    fleet['Demolition Year']=fleet['Demolition Date'].dt.year
    fleet['Demolition Month']=fleet['Demolition Date'].dt.month
    fleet['Status Group']=np.where(fleet['Demolition Date'].isnull(),'Current','Demolished')
    fleet['Age']=pd.to_datetime('today').year-fleet['Delivery Year']
    fleet['Demo Age']=fleet['Demolition Year']-fleet['Delivery Year']
    fleet['Order Date']=pd.to_datetime(fleet['E22_CONTRACT_DATE'],dayfirst=True,format='mixed')
    fleet['Order Year']=fleet['Order Date'].dt.year
    fleet['Order Month']=fleet['Order Date'].dt.month

    return fleet

fleet=load_fleet_data()

if 'fleet' not in st.session_state:
    st.session_state['fleet']=fleet


st.text('Fleet Data Done!')

#Getting Trade Flow Data 
st.text('----Getting Trade Flow Data...')

from oceanbolt.sdk.client import APIClient
from oceanbolt.sdk.data.trade_flows import TradeFlows

@st.cache_data
def load_trade_flows_data():
    load_country_list=['Australia','Brazil','Canada','South Africa','India','Indonesia','Russian Federation','United States','Colombia','China','Guinea']
    segment_cat=['Capesize','Panamax','Supramax','Handysize','Shortsea']    
    base_client = APIClient('eyJhbGciOiJSUzI1NiIsImtpZCI6ImUxNmUwNWI1NTZmNjVjYWEyNTg0ODU0N2FmYjNjZjI4IiwidHlwIjoiSldUIn0.eyJhdWQiOiJodHRwczovL2FwaS5vY2VhbmJvbHQuY29tIiwiZXhwIjoxNzMxNjAxNTAwLCJpYXQiOjE2OTk5NzkxNDksImlzcyI6Imh0dHBzOi8vYXV0aC5vY2VhbmJvbHQuY29tLyIsImtpZCI6ImUxNmUwNWI1NTZmNjVjYWEyNTg0ODU0N2FmYjNjZjI4Iiwia3R5cGUiOiJhcGlrZXkiLCJvYmtpZCI6IjFLUDhrNHBtU1E5UjRSUXoiLCJvcmdpZCI6ImNvZmNvIiwic3ViIjoiYXV0aDB8NjU1MzUwNDEyOGQ4Nzg1M2JlZGY2NDdiIn0.2EnbxcTv5InLrTvo3ikuKkZiazm7thHAIm55yAawwsfv8Vg092V0uZjA71m0WoAtSyIq8rVLbevH-l0gZU666M5qOyHeF-oCEM5Nc0e1_pIDKcS2L169s_nsPKynsz-69MMS7lRuCoSi7Cis1ZVCUh9gyvSSe-DI_rcZiHWHoV5h57Ig8J_jAf2vS6aaFGaya1gHMuJYNHORlbMec3k5_CtYDM-1qMvy4A7ZRC2l13kzM2zXx02p_6L7bALh6PCXjHcNwZR-BzNqIoLeGjKwJ7H6an_k9pdwFWzVsYRjG80_pL5h-S_94T2CGzJjX7l5hk8Fpm6Wuu8C9tVq2wMwaVHTLFiJJZp-fuqAkjqpR03fEMLe7h0XvoLQm4CSAtyiDSxkiVBn3Q5mhekWTItNyuyHmS4w3C5MrFjHIWqmGi36qcva3yxvYF3KNoe9uvAYIErD1hmmnUpm-xqPt_DSPTW_AAo-ryqw0L78kC0Zew4F2Sz-J0EOA6olRSK8OrERZT1VgO6NStisbeSGghWfJEKjwz4uu_NTMmjZRpjwSOXZ3lRTzm8ziyQ7B1LI4dlXcoU-5lp1XkFOHhEt6FLng_8rOyV6bLvxNl27fruFmY7rdHDa36QthQ-bX-guilQrSL33NRv8iovRknEpPkEtQ7SFuZPKQ3aBgzvR09AQuMg')
    from oceanbolt.sdk.data.trade_flows import TradeFlows
    rows=[]
    tradelist=pd.DataFrame()
    for yr in [2015,2016,2017,2018,2019,2020,2021,2022,2023]:
        print('Getting Data for Year '+str(yr))
        save=pd.DataFrame()
        for m in range(1,13):
            df = TradeFlows(base_client).get(
                start_date=date(yr,m,1),
                end_date=date(yr,m,monthrange(yr,m)[1]),
                flow_direction='export',
                max_results=20000
            )
            print('----'+str(len(df))+' rows downloaded for month '+str(m))
            rows.append(len(df))
            tradelist=pd.concat([df,tradelist])
            save=pd.concat([df,save])

        print('Total rows for year '+str(yr)+': '+str(len(save)))

    print('Max rows per individual download: '+str(max(rows)))
    print(len(tradelist.index))
    print(len(tradelist['flow_id'].unique()))

    tradeflow=tradelist.copy()
    tradeflow['Year']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.year
    tradeflow['Month']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.month
    tradeflow['Date']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.date
    tradeflow['load_group']=np.where(tradeflow['load_country'].isin(load_country_list),tradeflow['load_country'],'Others')

    EUlist=['Austria','Belgium','Bulgaria','Croatia','Cyprus','Czechia','Denmark','Estonia','Finland','France','Germany','Greece',
        'Hungary','Ireland','Italy','Latvia','Lithuania','Luxembourg','Malta','Netherlands','Poland','Portugal','Romania',
        'Slovakia','Slovenia','Spain','Sweden']

    tradeflow['discharge_group']=np.where(tradeflow['discharge_country'].isin(EUlist),'EU',tradeflow['discharge_country'])
    tradeflow['discharge_group']=np.where(tradeflow['discharge_group'].isin(['EU','China','India','Turkey','Unknown Country','Japan','Korea, Republic of','Hong Kong','Taiwan, Province of China']),tradeflow['discharge_group'],'Others')
    tradeflow['discharge_group']=np.where(tradeflow['discharge_group'].isin(['EU','China','India','Turkey','Unknown Country','Others']),tradeflow['discharge_group'],'JapanKoreaTWHK')
    tradeflow['discharge_group']=np.where(tradeflow['discharge_region'].isin(['SEA']),'SEA',tradeflow['discharge_group'])
    
    tradeflow['segment']=pd.Categorical(tradeflow['segment'],categories=segment_cat)

    return tradeflow

#tf=load_trade_flows_data()


#Getting Trade Flow Data if API is not working

@st.cache_data
def load_trade_flows_data_backup():
    load_country_list=['Australia','Brazil','Canada','South Africa','India','Indonesia','Russian Federation','United States','Colombia','China','Guinea']
    segment_cat=['Capesize','Panamax','Supramax','Handysize','Shortsea']
    rows=[]
    tradelist=pd.DataFrame()
    for yr in [2015,2016,2017,2018,2019,2020,2021,2022,2023]:    
        df = pd.read_excel(f'{yr}.xlsx')
        rows.append(len(df))
        tradelist=pd.concat([df,tradelist])

        print('Total rows for year '+str(yr)+': '+str(len(df)))


    tradeflow=tradelist.copy()
    tradeflow['Year']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.year
    tradeflow['Month']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.month
    tradeflow['Date']=pd.to_datetime(tradeflow['load_port_departed_at']).dt.date
    tradeflow['load_group']=np.where(tradeflow['load_country'].isin(load_country_list),tradeflow['load_country'],'Others')

    EUlist=['Austria','Belgium','Bulgaria','Croatia','Cyprus','Czechia','Denmark','Estonia','Finland','France','Germany','Greece',
        'Hungary','Ireland','Italy','Latvia','Lithuania','Luxembourg','Malta','Netherlands','Poland','Portugal','Romania',
        'Slovakia','Slovenia','Spain','Sweden']

    tradeflow['discharge_group']=np.where(tradeflow['discharge_country'].isin(EUlist),'EU',tradeflow['discharge_country'])
    tradeflow['discharge_group']=np.where(tradeflow['discharge_group'].isin(['EU','China','India','Turkey','Unknown Country','Japan','Korea, Republic of','Hong Kong','Taiwan, Province of China']),tradeflow['discharge_group'],'Others')
    tradeflow['discharge_group']=np.where(tradeflow['discharge_group'].isin(['EU','China','India','Turkey','Unknown Country','Others']),tradeflow['discharge_group'],'JapanKoreaTWHK')
    tradeflow['discharge_group']=np.where(tradeflow['discharge_region'].isin(['SEA']),'SEA',tradeflow['discharge_group'])
    
    tradeflow['segment']=pd.Categorical(tradeflow['segment'],categories=segment_cat)

    return tradeflow

tf=load_trade_flows_data_backup()


print('Total Trade Flow Records: '+str(len(tf['flow_id'].unique())))

if 'tradeflow' not in st.session_state:
    st.session_state['tradeflow']=tf


st.text('Trade Flow Data Done!')

st.write('All Data Loaded!!')