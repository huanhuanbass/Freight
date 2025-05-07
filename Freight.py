import streamlit as st
st.set_page_config(layout="wide")
#st.text('updated')

st.title('Baltic Exchange Dashboard')



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
#@st.cache_data()
def load_spot_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlcape='https://api.balticexchange.com/api/v1.3/feed/FDS041FOL8AMWM6CHZEXDRAG9P33TT5W/data'
    urlpmx='https://api.balticexchange.com/api/v1.3/feed/FDS72H2FOQWJSDTJBVW55HJY1Z6W8ZJ0/data'
    urlsmx='https://api.balticexchange.com/api/v1.3/feed/FDSQZHFHC242QBA1M4OMIW89Q1GBJGCL/data'
    urlhandy='https://api.balticexchange.com/api/v1.3/feed/FDSPMJYK538ET37RIGOY12PFFAXXYUIY/data'

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

    response = requests.get(urlhandy, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    spothandy=pd.DataFrame(df.loc[0,'data'])
    spothandy.set_index('date',inplace=True)
    spothandy.rename(columns={'value':'HS7TC'},inplace=True)

    spotnew=pd.merge(spotcape,spotpmx,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotsmx,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spothandy,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('spot.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    st.text('Spot Data Before Update: '+str(spotold.index.date[-1]))

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates()
    spot.set_index('Date',inplace=True)
    spot=spot.dropna(subset=['P4TC'])

    st.text('Spot Data After Update: '+str(spot.index.date[-1]))

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


#Getting Capesize Route Data
@st.cache_data()
def load_caperoute_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlcape='https://api.balticexchange.com/api/v1.3/feed/FDS041FOL8AMWM6CHZEXDRAG9P33TT5W/data'
    urlcaperoute='https://api.balticexchange.com/api/v1.3/feed/FDSIR2LD7ZH28DVT07YZDO77YD4K5T3J/data'

    response = requests.get(urlcape, headers=headers,params=params)
    df=pd.DataFrame(response.json())  
    spotcape=pd.DataFrame(df.loc[0,'data'])
    spotcape.set_index('date',inplace=True)
    spotcape.rename(columns={'value':'C5TC'},inplace=True)

    response = requests.get(urlcaperoute, headers=headers,params=params)
    df=pd.DataFrame(response.json())

    spotc2=pd.DataFrame(df.loc[0,'data'])
    spotc2.set_index('date',inplace=True)
    spotc2.rename(columns={'value':'C2'},inplace=True)

    spotc3=pd.DataFrame(df.loc[1,'data'])
    spotc3.set_index('date',inplace=True)
    spotc3.rename(columns={'value':'C3'},inplace=True)

    spotc5=pd.DataFrame(df.loc[2,'data'])
    spotc5.set_index('date',inplace=True)
    spotc5.rename(columns={'value':'C5'},inplace=True)

    spotc7=pd.DataFrame(df.loc[3,'data'])
    spotc7.set_index('date',inplace=True)
    spotc7.rename(columns={'value':'C7'},inplace=True)

    spotc8=pd.DataFrame(df.loc[4,'data'])
    spotc8.set_index('date',inplace=True)
    spotc8.rename(columns={'value':'C8'},inplace=True)

    spotc9=pd.DataFrame(df.loc[5,'data'])
    spotc9.set_index('date',inplace=True)
    spotc9.rename(columns={'value':'C9'},inplace=True)

    spotc10=pd.DataFrame(df.loc[6,'data'])
    spotc10.set_index('date',inplace=True)
    spotc10.rename(columns={'value':'C10'},inplace=True)

    spotc14=pd.DataFrame(df.loc[7,'data'])
    spotc14.set_index('date',inplace=True)
    spotc14.rename(columns={'value':'C14'},inplace=True)

    spotc16=pd.DataFrame(df.loc[8,'data'])
    spotc16.set_index('date',inplace=True)
    spotc16.rename(columns={'value':'C16'},inplace=True)

    spotc17=pd.DataFrame(df.loc[9,'data'])
    spotc17.set_index('date',inplace=True)
    spotc17.rename(columns={'value':'C17'},inplace=True)

    spotnew=pd.merge(spotcape,spotc2,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc3,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc5,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc7,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc8,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc9,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc10,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc14,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc16,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotc17,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('caperoute.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    st.text('Capesize Route Data Before Update: '+str(spotold.index.date[-1]))

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates(subset='Date',keep='last')
    spot.set_index('Date',inplace=True)

    st.text('Capesize Route Data After Update: '+str(spot.index.date[-1]))

    spot.to_csv('caperoute.csv',index_label='Date')

    return spot

caperoute=load_caperoute_data()

if 'caperoute' not in st.session_state:
    st.session_state['caperoute']=caperoute

#Getting Panamax Route Data
@st.cache_data()
def load_pmxroute_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlpmx='https://api.balticexchange.com/api/v1.3/feed/FDS72H2FOQWJSDTJBVW55HJY1Z6W8ZJ0/data'
    urlpmxroute='https://api.balticexchange.com/api/v1.3/feed/FDSMSBFH191FZVM5NJ4NK51YY6QXCTO7/data'

    response = requests.get(urlpmx, headers=headers,params=params)
    df=pd.DataFrame(response.json())  
    spotpmx=pd.DataFrame(df.loc[0,'data'])
    spotpmx.set_index('date',inplace=True)
    spotpmx.rename(columns={'value':'P4TC'},inplace=True)

    response = requests.get(urlpmxroute, headers=headers,params=params)
    df=pd.DataFrame(response.json())

    spotp1a=pd.DataFrame(df.loc[0,'data'])
    spotp1a.set_index('date',inplace=True)
    spotp1a.rename(columns={'value':'P1A'},inplace=True)

    spotp2a=pd.DataFrame(df.loc[1,'data'])
    spotp2a.set_index('date',inplace=True)
    spotp2a.rename(columns={'value':'P2A'},inplace=True)

    spotp3a=pd.DataFrame(df.loc[2,'data'])
    spotp3a.set_index('date',inplace=True)
    spotp3a.rename(columns={'value':'P3A'},inplace=True)

    spotp4=pd.DataFrame(df.loc[3,'data'])
    spotp4.set_index('date',inplace=True)
    spotp4.rename(columns={'value':'P4'},inplace=True)

    spotp5=pd.DataFrame(df.loc[4,'data'])
    spotp5.set_index('date',inplace=True)
    spotp5.rename(columns={'value':'P5'},inplace=True)

    spotp6=pd.DataFrame(df.loc[5,'data'])
    spotp6.set_index('date',inplace=True)
    spotp6.rename(columns={'value':'P6'},inplace=True)

    spotp7=pd.DataFrame(df.loc[6,'data'])
    spotp7.set_index('date',inplace=True)
    spotp7.rename(columns={'value':'P7'},inplace=True)

    spotp8=pd.DataFrame(df.loc[7,'data'])
    spotp8.set_index('date',inplace=True)
    spotp8.rename(columns={'value':'P8'},inplace=True)

    spotnew=pd.merge(spotpmx,spotp1a,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp2a,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp3a,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp4,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp5,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp6,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp7,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spotp8,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('pmxroute.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    st.text('Panamax Route Data Before Update: '+str(spotold.index.date[-1]))

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates(subset='Date',keep='last')
    spot.set_index('Date',inplace=True)
    

    st.text('Panamax Route Data After Update: '+str(spot.index.date[-1]))

    spot.to_csv('pmxroute.csv',index_label='Date')

    return spot

pmxroute=load_pmxroute_data()

if 'pmxroute' not in st.session_state:
    st.session_state['pmxroute']=pmxroute

#Getting Supramax Route Data
@st.cache_data()
def load_smxroute_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlsmx='https://api.balticexchange.com/api/v1.3/feed/FDSQZHFHC242QBA1M4OMIW89Q1GBJGCL/data'
    urlsmxroute='https://api.balticexchange.com/api/v1.3/feed/FDSAIN68PQBQM977TO3VCL397UXBVYWV/data'

    response = requests.get(urlsmx, headers=headers,params=params)
    df=pd.DataFrame(response.json())  
    spotsmx=pd.DataFrame(df.loc[0,'data'])
    spotsmx.set_index('date',inplace=True)
    spotsmx.rename(columns={'value':'S10TC'},inplace=True)

    response = requests.get(urlsmxroute, headers=headers,params=params)
    df=pd.DataFrame(response.json())

    spots1b=pd.DataFrame(df.loc[0,'data'])
    spots1b.set_index('date',inplace=True)
    spots1b.rename(columns={'value':'S1B'},inplace=True)

    spots1c=pd.DataFrame(df.loc[1,'data'])
    spots1c.set_index('date',inplace=True)
    spots1c.rename(columns={'value':'S1C'},inplace=True)

    spots2=pd.DataFrame(df.loc[2,'data'])
    spots2.set_index('date',inplace=True)
    spots2.rename(columns={'value':'S2'},inplace=True)

    spots3=pd.DataFrame(df.loc[3,'data'])
    spots3.set_index('date',inplace=True)
    spots3.rename(columns={'value':'S3'},inplace=True)

    spots4a=pd.DataFrame(df.loc[4,'data'])
    spots4a.set_index('date',inplace=True)
    spots4a.rename(columns={'value':'S4A'},inplace=True)

    spots4b=pd.DataFrame(df.loc[5,'data'])
    spots4b.set_index('date',inplace=True)
    spots4b.rename(columns={'value':'S4B'},inplace=True)

    spots5=pd.DataFrame(df.loc[6,'data'])
    spots5.set_index('date',inplace=True)
    spots5.rename(columns={'value':'S5'},inplace=True)

    spots8=pd.DataFrame(df.loc[7,'data'])
    spots8.set_index('date',inplace=True)
    spots8.rename(columns={'value':'S8'},inplace=True)

    spots9=pd.DataFrame(df.loc[8,'data'])
    spots9.set_index('date',inplace=True)
    spots9.rename(columns={'value':'S9'},inplace=True)

    spots10=pd.DataFrame(df.loc[9,'data'])
    spots10.set_index('date',inplace=True)
    spots10.rename(columns={'value':'S10'},inplace=True)

    spots15=pd.DataFrame(df.loc[10,'data'])
    spots15.set_index('date',inplace=True)
    spots15.rename(columns={'value':'S15'},inplace=True)

    spotnew=pd.merge(spotsmx,spots1b,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots1c,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots2,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots3,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots4a,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots4b,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots5,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots8,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots9,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots10,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spots15,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('smxroute.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    st.text('Supramax Route Data Before Update: '+str(spotold.index.date[-1]))

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates(subset='Date',keep='last')
    spot.set_index('Date',inplace=True)

    st.text('Supramax Route Data After Update: '+str(spot.index.date[-1]))

    spot.to_csv('smxroute.csv',index_label='Date')

    return spot

smxroute=load_smxroute_data()

if 'smxroute' not in st.session_state:
    st.session_state['smxroute']=smxroute


#Getting Handysize Route Data
@st.cache_data()
def load_handyroute_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlhandy='https://api.balticexchange.com/api/v1.3/feed/FDSPMJYK538ET37RIGOY12PFFAXXYUIY/data'
    urlhandyroute='https://api.balticexchange.com/api/v1.3/feed/FDSREHV3FRHP773368630ERWCAIU7CX0/data'

    response = requests.get(urlhandy, headers=headers,params=params)
    df=pd.DataFrame(response.json())  
    spothandy=pd.DataFrame(df.loc[0,'data'])
    spothandy.set_index('date',inplace=True)
    spothandy.rename(columns={'value':'HS7TC'},inplace=True)

    response = requests.get(urlhandyroute, headers=headers,params=params)
    df=pd.DataFrame(response.json())

    spoths1=pd.DataFrame(df.loc[0,'data'])
    spoths1.set_index('date',inplace=True)
    spoths1.rename(columns={'value':'HS1'},inplace=True)

    spoths2=pd.DataFrame(df.loc[1,'data'])
    spoths2.set_index('date',inplace=True)
    spoths2.rename(columns={'value':'HS2'},inplace=True)

    spoths3=pd.DataFrame(df.loc[2,'data'])
    spoths3.set_index('date',inplace=True)
    spoths3.rename(columns={'value':'HS3'},inplace=True)

    spoths4=pd.DataFrame(df.loc[3,'data'])
    spoths4.set_index('date',inplace=True)
    spoths4.rename(columns={'value':'HS4'},inplace=True)

    spoths5=pd.DataFrame(df.loc[4,'data'])
    spoths5.set_index('date',inplace=True)
    spoths5.rename(columns={'value':'HS5'},inplace=True)

    spoths6=pd.DataFrame(df.loc[5,'data'])
    spoths6.set_index('date',inplace=True)
    spoths6.rename(columns={'value':'HS6'},inplace=True)

    spoths7=pd.DataFrame(df.loc[6,'data'])
    spoths7.set_index('date',inplace=True)
    spoths7.rename(columns={'value':'HS7'},inplace=True)


    spotnew=pd.merge(spothandy,spoths1,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths2,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths3,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths4,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths5,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths6,left_index=True,right_index=True,how='outer')
    spotnew=pd.merge(spotnew,spoths7,left_index=True,right_index=True,how='outer')
    spotnew.index=pd.to_datetime(spotnew.index)

    spot=pd.read_csv('handyroute.csv')
    spotold=spot.set_index('Date')
    spotold.index=pd.to_datetime(spotold.index)

    st.text('Handysize Route Data Before Update: '+str(spotold.index.date[-1]))

    spot=pd.concat([spotold,spotnew])
    spot.reset_index(inplace=True)
    spot.rename(columns={'index':'Date'},inplace=True)
    spot=spot.drop_duplicates(subset='Date',keep='last')
    spot.set_index('Date',inplace=True)

    st.text('Handysize Route Data After Update: '+str(spot.index.date[-1]))

    spot.to_csv('handyroute.csv',index_label='Date')
    
    return spot

handyroute=load_handyroute_data()

if 'handyroute' not in st.session_state:
    st.session_state['handyroute']=handyroute

#Getting PMX FFA Data
#@st.cache_data(ttl='12h')
def load_pmx_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
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

    st.text('FFA Data Before Update: '+str(p4tcold.index.date[-1]))

    p4tc=pd.concat([p4tcold,ffapmx_pt1])
    p4tc.reset_index(inplace=True)
    p4tc.rename(columns={'index':'Date'},inplace=True)
    p4tc=p4tc.drop_duplicates()
    p4tc.set_index('Date',inplace=True)

    st.text('FFA Data After Update: '+str(p4tc.index.date[-1]))

    p4tc.to_csv('p4tc.csv',index_label='Date')

    ffapmx_pt2=ffapmx_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ffapmx_pt2.index=pd.to_datetime(ffapmx_pt2.index)
    ffapmx_pt2=ffapmx_pt2[['4TC_PCURMON','4TC_P+1MON','4TC_P+2MON','4TC_P+3MON','4TC_P+4MON','4TC_P+5MON', 
              '4TC_PCURQ','4TC_P+1Q','4TC_P+2Q','4TC_P+3Q','4TC_P+4Q','4TC_P+5Q',
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

p4tc,p4tc_r=load_pmx_ffa_data()




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
              '4TC_PCURQ','4TC_P+1Q','4TC_P+2Q','4TC_P+3Q','4TC_P+4Q','4TC_P+5Q',
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
#@st.cache_data(ttl='12h')
def load_cape_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
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
              '5TC_CCURQ','5TC_C+1Q','5TC_C+2Q','5TC_C+3Q','5TC_C+4Q','5TC_C+5Q',
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

c5tc,c5tc_r=load_cape_ffa_data()


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
#@st.cache_data(ttl='12h')
def load_smx_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
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

s10tc,s10tc_r=load_smx_ffa_data()





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





#Getting Handy FFA Data
@st.cache_data()
def load_handy_ffa_data():
    headers = {'x-apikey': 'FMNNXJKJMSV6PE4YA36EOAAJXX1WAH84KSWNU8PEUFGRHUPJZA3QTG1FLE09SXJF'}
    dateto=pd.to_datetime('today')
    datefrom=dateto-BDay(15)
    params={'from':datefrom,'to':dateto}
    urlhandyffa='https://api.balticexchange.com/api/v1.3/feed/FDSPIQYIH9UUI56BL6U83DUECJNMQKMW/data'

    response = requests.get(urlhandyffa, headers=headers,params=params)
    df=pd.DataFrame(response.json())
    ffahandy=pd.DataFrame(df.loc[0,'groupings'])

    ffahandy_=pd.DataFrame()
    for j in range(len(ffahandy.index)):
        ffahandy_0=ffahandy.loc[j,'date']
        ffahandy_n=pd.DataFrame(ffahandy.loc[j,'groups'])
        for i in range(len(ffahandy_n.index)):
            ffahandy_n_0=ffahandy_n.loc[i,'periodType']
            ffahandy_n_n=pd.DataFrame(ffahandy_n.loc[i,'projections'])
            ffahandy_n_n['periodType']=ffahandy_n_0
            ffahandy_n_n['date']=ffahandy_0
            ffahandy_=pd.concat([ffahandy_,ffahandy_n_n])
            
    ffahandy_[['Month','Year']]=ffahandy_['period'].str.split(' ',expand=True)
    ffahandy_['Month'].replace({'Jan':'M1','Feb':'M2','Mar':'M3','Apr':'M4','May':'M5','Jun':'M6',
                            'Jul':'M7','Aug':'M8','Sep':'M9','Oct':'M10','Nov':'M11','Dec':'M12',
                            'Feb/Mar':'Q1','May/Jun':'Q2','Aug/Sep':'Q3','Nov/Dec':'Q4','Cal':'Y'},inplace=True)
    ffahandy_['Contract']='20'+ffahandy_['Year']+'_'+ffahandy_['Month']
    ffahandy_pt1=ffahandy_.pivot_table(index='archiveDate',columns='Contract',values='value',aggfunc='mean')
    ffahandy_pt1.index=pd.to_datetime(ffahandy_pt1.index)



    hs7tcold=pd.read_csv('hs7tc.csv')
    hs7tcold=hs7tcold.set_index('Date')
    hs7tcold.index=pd.to_datetime(hs7tcold.index)
    hs7tc=pd.concat([hs7tcold,ffahandy_pt1])  

    hs7tc.reset_index(inplace=True)
    hs7tc.rename(columns={'index':'Date'},inplace=True)
    hs7tc=hs7tc.drop_duplicates()
    hs7tc.set_index('Date',inplace=True)
    hs7tc.to_csv('hs7tc.csv',index_label='Date')

    ffahandy_pt2=ffahandy_.pivot_table(index='archiveDate',columns='identifier',values='value',aggfunc='mean')
    ffahandy_pt2.index=pd.to_datetime(ffahandy_pt2.index)
    ffahandy_pt2=ffahandy_pt2[['TC_H38CURMON','TC_H38+1MON','TC_H38+2MON','TC_H38+3MON','TC_H38+4MON','TC_H38+5MON', 
              'TC_H38CURQ','TC_H38+1Q','TC_H38+2Q','TC_H38+3Q','TC_H38+4Q',
              'TC_H38+1CAL','TC_H38+2CAL','TC_H38+3CAL','TC_H38+4CAL','TC_H38+5CAL','TC_H38+6CAL','TC_H38+7CAL']]

    hs7tc_rold=pd.read_csv('hs7tc_r.csv')
    hs7tc_rold=hs7tc_rold.set_index('Date')
    hs7tc_rold.index=pd.to_datetime(hs7tc_rold.index)
    hs7tc_r=pd.concat([hs7tc_rold,ffahandy_pt2])
    hs7tc_r.reset_index(inplace=True)
    hs7tc_r.rename(columns={'index':'Date'},inplace=True)
    hs7tc_r=hs7tc_r.drop_duplicates()
    hs7tc_r.set_index('Date',inplace=True)
    hs7tc_r.to_csv('hs7tc_r.csv',index_label='Date')

    spothandy=spot[['HS7TC']]

    hs7tc=pd.merge(spothandy,hs7tc,left_index=True,right_index=True,how='outer')
    hs7tc.dropna(subset='HS7TC',inplace=True)

    hs7tc_r=pd.merge(spothandy,hs7tc_r,left_index=True,right_index=True,how='outer')
    hs7tc_r.dropna(subset='HS7TC',inplace=True)

    return hs7tc, hs7tc_r

hs7tc,hs7tc_r=load_handy_ffa_data()



if 'hs7tc' not in st.session_state:
    st.session_state['hs7tc']=hs7tc
if 'hs7tc_r' not in st.session_state:
    st.session_state['hs7tc_r']=hs7tc_r








st.text('Freight Data Done')


st.write('All Data Loaded!!')

def update_data():
    for key in st.session_state.keys():
        del st.session_state[key]
    st.cache_data.clear()

st.button('Update Data',on_click=update_data)
st.text('Data is automatically reloaded for potential updates every 12 hours.')
st.text('If you would like to trigger the reload right now, please click on the above "Update Data" button.')



