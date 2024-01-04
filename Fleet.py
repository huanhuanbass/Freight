import streamlit as st
st.set_page_config(layout="wide")
#st.write('project updated on 20240103')

import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
import ftplib
from pandas.tseries.offsets import BDay
import requests

cutoff = pd.to_datetime('today')
curryear=cutoff.year

plot_ticks='inside'
plot_tickwidth=2
plot_ticklen=10
plot_title_font_color='dodgerblue'
plot_title_font_size=25
plot_legend_font_size=15
plot_axis=dict(tickfont = dict(size=15))


st.title('Fleet Data')
st.text('Dry Bulk Freight (Fleet Data) Interactive Dashboard')



#Getting Fleet Data
st.text('----Getting Fleet Data...')

@st.cache_data(ttl='1d')
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
    st.text(filename)
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

#Getting Spot Freight Data
st.text('----Getting Spot Data...')
@st.cache_data(ttl='1h')
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

    spot.to_csv('../spot.csv',index_label='Date')

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

st.text('Spot Data Done!')


st.button('Update Data',on_click=st.cache_data.clear)
st.text('Data is automatically reloaded for potential updates every 24 hours.')
st.text('If you would like to trigger the reload now, please click on the above "Update Data" button.')


#import fleet data
fleet=st.session_state['fleet']

#import spot data
spot=st.session_state['spot']


freight=spot[['C5TC','P4TC','S10TC']]
freight.rename(columns={'C5TC':'Capesize','P4TC':'Panamax','S10TC':'Supramax'},inplace=True)

st.markdown('## **Delivery**')

yr11=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr11')
yr21=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr21')
deliv=fleet.pivot_table(index=['Delivery Year','Delivery Month'],columns='Segment',values='Dwt',aggfunc='count')
deliv.sort_index(ascending=False,axis=1,inplace=True)
deliv.drop(columns=['Others'],inplace=True)
deliv.reset_index(inplace=True)
deliv['Delivery Month']=deliv['Delivery Month'].astype(int)
deliv['Delivery Year_Month']=deliv['Delivery Year'].astype(str)+'_'+deliv['Delivery Month'].astype(str)
deliv=deliv[(deliv['Delivery Year']>=yr11)&(deliv['Delivery Year']<=yr21)]
deliv.drop(columns=['Delivery Year','Delivery Month'],inplace=True)
deliv.set_index('Delivery Year_Month',inplace=True)
devplot=px.bar(deliv,width=1000,height=500,title='Delivery by Size')
devplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
devplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(devplot)

st.markdown('#### **----Subsegments**')
yr12=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr12')
yr22=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr22')
size2=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz2')
delivsz=fleet[fleet['Segment']==size2]
delivsz=delivsz.pivot_table(index=['Delivery Year','Delivery Month'],columns='Subsegment',values='Dwt',aggfunc='count')
delivsz=delivsz.loc[:, (delivsz!= 0).any(axis=0)]
delivsz.sort_index(ascending=False,axis=1,inplace=True)
delivsz.reset_index(inplace=True)
delivsz['Delivery Month']=delivsz['Delivery Month'].astype(int)
delivsz['Delivery Year_Month']=delivsz['Delivery Year'].astype(str)+'_'+delivsz['Delivery Month'].astype(str)
delivsz=delivsz[(delivsz['Delivery Year']>=yr12)&(delivsz['Delivery Year']<=yr22)]
delivsz.drop(columns=['Delivery Year','Delivery Month'],inplace=True)
delivsz.set_index('Delivery Year_Month',inplace=True)
devszplot=px.bar(delivsz,width=1000,height=500,title=size2+' Delivery by Subsegment')
devszplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
devszplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(devszplot)

st.markdown('## **Demolition**')

yr13=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr13')
yr23=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr23')
demo=fleet.pivot_table(index=['Demolition Year','Demolition Month'],columns='Segment',values='Dwt',aggfunc='count')
demo.sort_index(ascending=False,axis=1,inplace=True)
demo.drop(columns=['Others'],inplace=True)
demo.reset_index(inplace=True)
demo['Demolition Year']=demo['Demolition Year'].astype(int)
demo['Demolition Month']=demo['Demolition Month'].astype(int)
demo['Demolition Year_Month']=demo['Demolition Year'].astype(str)+'_'+demo['Demolition Month'].astype(str)
demo=demo[(demo['Demolition Year']>=yr13)&(demo['Demolition Year']<=yr23)]
demo.drop(columns=['Demolition Year','Demolition Month'],inplace=True)
demo.set_index('Demolition Year_Month',inplace=True)
demoplot=px.bar(demo,width=1000,height=500,title='Demolition by Size')
demoplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
demoplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(demoplot)

st.markdown('#### **----Subsegments**')
yr14=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr14')
yr24=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr24')
size3=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz3')
demosz=fleet[fleet['Segment']==size3]
demosz=demosz.pivot_table(index=['Demolition Year','Demolition Month'],columns='Subsegment',values='Dwt',aggfunc='count')
demosz=demosz.loc[:, (demosz!= 0).any(axis=0)]
demosz.sort_index(ascending=False,axis=1,inplace=True)
demosz.reset_index(inplace=True)
demosz['Demolition Year']=demosz['Demolition Year'].astype(int)
demosz['Demolition Month']=demosz['Demolition Month'].astype(int)
demosz['Demolition Year_Month']=demosz['Demolition Year'].astype(str)+'_'+demosz['Demolition Month'].astype(str)
demosz=demosz[(demosz['Demolition Year']>=yr14)&(demosz['Demolition Year']<=yr24)]
demosz.drop(columns=['Demolition Year','Demolition Month'],inplace=True)
demosz.set_index('Demolition Year_Month',inplace=True)
demoszplot=px.bar(demosz,width=1000,height=500,title=size3+' Demolition by Subsegment')
demoszplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
demoszplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(demoszplot)


st.markdown('## **----Order Book**')

yr15=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr15')
yr25=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr25')
order=fleet.pivot_table(index=['Order Year','Order Month'],columns='Segment',values='Dwt',aggfunc='count')
order.sort_index(ascending=False,axis=1,inplace=True)
order.drop(columns=['Others'],inplace=True)
order.reset_index(inplace=True)
order['Order Year']=order['Order Year'].astype(int)
order['Order Month']=order['Order Month'].astype(int)
order['Order Year_Month']=order['Order Year'].astype(str)+'_'+order['Order Month'].astype(str)
order=order[(order['Order Year']>=yr15)&(order['Order Year']<=yr25)]
order.drop(columns=['Order Year','Order Month'],inplace=True)
order.set_index('Order Year_Month',inplace=True)
orderplot=px.bar(order,width=1000,height=500,title='Order Book by Size')
orderplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
orderplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(orderplot)

st.markdown('#### **----Subsegments**')
yr16=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr16')
yr26=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr26')
size4=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz4')
ordersz=fleet[fleet['Segment']==size4]
ordersz=ordersz.pivot_table(index=['Order Year','Order Month'],columns='Subsegment',values='Dwt',aggfunc='count')
ordersz=ordersz.loc[:, (ordersz!= 0).any(axis=0)]
ordersz.sort_index(ascending=False,axis=1,inplace=True)
ordersz.reset_index(inplace=True)
ordersz['Order Year']=ordersz['Order Year'].astype(int)
ordersz['Order Month']=ordersz['Order Month'].astype(int)
ordersz['Order Year_Month']=ordersz['Order Year'].astype(str)+'_'+ordersz['Order Month'].astype(str)
ordersz=ordersz[(ordersz['Order Year']>=yr16)&(ordersz['Order Year']<=yr26)]
ordersz.drop(columns=['Order Year','Order Month'],inplace=True)
ordersz.set_index('Order Year_Month',inplace=True)
orderszplot=px.bar(ordersz,width=1000,height=500,title=size4+' Order Book by Subsegment')
orderszplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
orderszplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(orderszplot)

st.markdown('## **Order vs Delivery Pattern**')
size1=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz')
ordev=fleet[fleet['Segment']==size1]
ordev=ordev.pivot_table(index='Order Year',columns='Delivery Year',values='Dwt',aggfunc='count')
ordev=ordev[ordev.index>2014]
ordev=ordev.transpose()
ordev=ordev[ordev.index>2017]
ordev['Total']=ordev.sum(axis=1)
ordev=ordev.transpose()
ordev['Total']=ordev.sum(axis=1)
ordev=ordev.astype('Int64')
st.write(ordev)

st.markdown('## **Order Pace vs Freight Rate Correlation**')
size8=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax'],key='sz8')
freq1=st.selectbox('Select Frequency',options=['Q','Y','M'],key='fq1')
capefleet=fleet[fleet['Segment']==size8]
capeorder=capefleet.pivot_table(index='Order Date',values='Dwt',aggfunc='count')
capeorder=capeorder.resample(freq1).sum()
capeorder.rename(columns={'Dwt':'Order'},inplace=True)
capespot=freight[[size8]]
capespot=capespot.resample(freq1).mean()
capeordercorr=pd.merge(capeorder,capespot,left_index=True,right_index=True,how='inner')

cor=capeordercorr[['Order',size8]].corr().iloc[0,1].round(2)
subplot_fig = make_subplots(specs=[[{"secondary_y": True}]])
fig1=px.line(capeordercorr['Order'])
fig2=px.line(capeordercorr[size8])
fig2.update_traces(yaxis='y2')
subplot_fig.add_traces(fig1.data + fig2.data)
subplot_fig.update_layout(title=size8+' Order vs Spot TC Rate (corr: '+str(cor)+')',width=1000,height=500)
subplot_fig.layout.xaxis.title='Date'
subplot_fig.layout.yaxis.title='Count'
subplot_fig.layout.yaxis2.title='C5TC'
subplot_fig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
subplot_fig.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
subplot_fig.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(subplot_fig)



st.markdown('## **Demolition Pace vs Freight Rate Correlation**')

size7=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax'],key='sz7')
freq2=st.selectbox('Select Frequency',options=['Q','Y','M'],key='fq2')
segfleet=fleet[fleet['Segment']==size7]
segdemo=segfleet.pivot_table(index='Demolition Date',values='Dwt',aggfunc='count')
segdemo=segdemo.resample(freq2).sum()
segdemo.rename(columns={'Dwt':'Demolition'},inplace=True)
segspot=freight[[size7]]
segspot=segspot.resample(freq2).mean()
segdemocorr=pd.merge(segdemo,segspot,left_index=True,right_index=True,how='inner')
#segdemocorr.to_excel('pmxcorr.xlsx')

cor=segdemocorr[['Demolition',size7]].corr().iloc[0,1].round(2)
subplot_fig = make_subplots(specs=[[{"secondary_y": True}]])
fig1=px.line(segdemocorr['Demolition'])
fig2=px.line(segdemocorr[size7])
fig2.update_traces(yaxis='y2')
subplot_fig.add_traces(fig1.data + fig2.data)
subplot_fig.update_layout(title=str(size7)+' Demolition vs Spot TC Rate (corr: '+str(cor)+')',width=1000,height=500)
subplot_fig.layout.xaxis.title='Date'
subplot_fig.layout.yaxis.title='Count'
subplot_fig.layout.yaxis2.title='Freight'
subplot_fig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
subplot_fig.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
subplot_fig.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(subplot_fig)


st.markdown('## **Demolition Pace vs Overage Count Correlation**')

size6=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz6')
freq3=st.selectbox('Select Frequency',options=['Q','Y','M'],key='fq3')
overage=st.number_input('Input Threshold for Overage',value=19)
segfleet=fleet[fleet['Segment']==size6]
fleethistory=pd.DataFrame()

for prevyear in [2015,2016,2017,2018,2019,2020,2021,2022,2023]:
    for prevmonth in range(1,13):        
        prevfleet=segfleet[pd.to_datetime(segfleet['Delivery Date'])<pd.to_datetime(date(prevyear,prevmonth,1))]
        prevfleet=prevfleet[(pd.to_datetime(prevfleet['Demolition Date'])>pd.to_datetime(date(prevyear,prevmonth,1)+timedelta(days=-1)))|(prevfleet['Demolition Date'].isna())]
        prevfleet['Prev Age']=prevyear-prevfleet['Delivery Year']
        prevoverage=prevfleet[prevfleet['Prev Age']>overage]
        prevsize=len(prevfleet.index)
        prevoveragecount=len(prevoverage.index)
        toadd=pd.DataFrame({'Year':[prevyear],'Month':[prevmonth],'Total Count':[prevsize],'Overage Count':[prevoveragecount]})
        fleethistory=pd.concat([fleethistory,toadd])
fleethistory.set_index(['Year','Month'],inplace=True)


segdemo=segfleet.pivot_table(index=['Demolition Year','Demolition Month'],values='Dwt',aggfunc='count')
segdemo.rename(columns={'Dwt':'Demolition'},inplace=True)
segdemo.index.rename(['Year','Month'],inplace=True)
fleethistory=pd.merge(fleethistory,segdemo,left_index=True,right_index=True,how='inner')
segdeliv=segfleet.pivot_table(index=['Delivery Year','Delivery Month'],values='Dwt',aggfunc='count')
segdeliv.rename(columns={'Dwt':'Delivery'},inplace=True)
segdeliv.index.rename(['Year','Month'],inplace=True)
fleethistory=pd.merge(fleethistory,segdeliv,left_index=True,right_index=True,how='inner')
fleethistory.fillna(0,inplace=True)
fleethistory=fleethistory.astype('Int64')


overage_demo=fleethistory[['Overage Count','Demolition']]
overage_demo.reset_index(inplace=True)
overage_demo['Date']=pd.to_datetime(dict(year=overage_demo['Year'], month=overage_demo['Month'], day=1))
overage_demo.set_index('Date',inplace=True)
overage_demo.drop(columns=['Year','Month'],inplace=True)
overage_demo.index=pd.to_datetime(overage_demo.index)
overage_demo=overage_demo.resample(freq3).agg({'Overage Count':'mean','Demolition':'sum'})

cor=overage_demo[['Overage Count','Demolition']].corr().iloc[0,1].round(2)
subplot_fig = make_subplots(specs=[[{"secondary_y": True}]])
fig1=px.line(overage_demo['Overage Count'])
fig2=px.line(overage_demo['Demolition'])
fig2.update_traces(yaxis='y2')
subplot_fig.add_traces(fig1.data + fig2.data)
subplot_fig.update_layout(title=str(size6)+' Demolition vs Overage Count (corr: '+str(cor)+')',width=1000,height=500)
subplot_fig.layout.xaxis.title='Date'
subplot_fig.layout.yaxis.title='Overage Count'
subplot_fig.layout.yaxis2.title='Demoltion'
subplot_fig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
subplot_fig.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
subplot_fig.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(subplot_fig)


st.markdown('## **Fleet Age Profile**')
current=fleet[fleet['Status Group']=='Current']
age=current.pivot_table(index='Age',columns='Segment',values='Dwt',aggfunc='count')
age.sort_index(ascending=False,axis=1,inplace=True)
age.drop(columns=['Others'],inplace=True)
ageplot=px.bar(age,width=1000,height=500,title='Fleet Age Profile by Size')
ageplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
ageplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(ageplot)

st.markdown('#### **----Subsegment**')
size5=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz5')
currentsz=current[current['Segment']==size5]
agesz=currentsz.pivot_table(index='Age',columns='Subsegment',values='Dwt',aggfunc='count')
#agesz=agesz.loc[:, (agesz!= 0).any(axis=0)]
agesz.sort_index(ascending=False,axis=1,inplace=True)
agesz.drop(columns=['Others'],inplace=True)
ageszplot=px.bar(agesz,width=1000,height=500,title=size5+' Age Profile by Subsegment')
ageszplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
ageszplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(ageszplot)

st.markdown('## **Fleet Summary**')

yr17=st.number_input('Input Start Year',min_value=2005,max_value=curryear,value=curryear-5,step=1,key='yr17')
yr27=st.number_input('Input End Year',min_value=curryear,max_value=curryear+5,value=curryear,step=1,key='yr27')
size99=st.selectbox('Select Size',options=['Capesize','Panamax','Supramax','Handysize'],key='sz99')
segfleet=fleet[fleet['Segment']==size99]
fleethistory=pd.DataFrame()

for prevyear in range(yr17,yr27+1):
    for prevmonth in range(1,13):        
        prevfleet=segfleet[pd.to_datetime(segfleet['Delivery Date'])<pd.to_datetime(date(prevyear,prevmonth,1))]
        prevfleet=prevfleet[(pd.to_datetime(prevfleet['Demolition Date'])>pd.to_datetime(date(prevyear,prevmonth,1)+timedelta(days=-1)))|(prevfleet['Demolition Date'].isna())]
        prevsize=len(prevfleet.index)
        toadd=pd.DataFrame({'Year':[prevyear],'Month':[prevmonth],'Total Count':[prevsize]})
        fleethistory=pd.concat([fleethistory,toadd])
fleethistory.set_index(['Year','Month'],inplace=True)


segdemo=segfleet.pivot_table(index=['Demolition Year','Demolition Month'],values='Dwt',aggfunc='count')
segdemo.rename(columns={'Dwt':'Demolition'},inplace=True)
segdemo.index.rename(['Year','Month'],inplace=True)
fleethistory=pd.merge(fleethistory,segdemo,left_index=True,right_index=True,how='outer')
segdeliv=segfleet.pivot_table(index=['Delivery Year','Delivery Month'],values='Dwt',aggfunc='count')
segdeliv.rename(columns={'Dwt':'Delivery'},inplace=True)
segdeliv.index.rename(['Year','Month'],inplace=True)
fleethistory=pd.merge(fleethistory,segdeliv,left_index=True,right_index=True,how='outer')
fleethistory.fillna(0,inplace=True)
fleethistory=fleethistory.astype('Int64')
fleethistory.reset_index(inplace=True)
fleethistory['Date']=pd.to_datetime(dict(year=fleethistory['Year'], month=fleethistory['Month'], day=1))
fleethistory.set_index('Date',inplace=True)
fleetoverview=fleethistory[(fleethistory['Year']>=yr17)&(fleethistory['Year']<=yr27)]

fleetoverview['Demolition']=0-fleetoverview['Demolition']
fleetoverview['Net Change']=fleetoverview['Delivery']+fleetoverview['Demolition']

#import plotly.graph_objects as go
subplot_fig = make_subplots(specs=[[{"secondary_y": True}]])
#fig1=go.Figure()
#fig1.add_trace(go.Bar(x=fleetoverview.index,y=fleetoverview['Demolition'],name='Demolition'))
#fig1.add_trace(go.Bar(x=fleetoverview.index,y=fleetoverview['Delivery'],name='Delivery'))
fig1=px.bar(fleetoverview[['Delivery','Demolition']],barmode='relative')
fig2=px.line(fleetoverview['Total Count'])
fig2.update_traces(yaxis='y2')
subplot_fig.add_traces(fig1.data+fig2.data)
subplot_fig.update_layout(title=size99+' Fleet Overview',width=1000,height=500)
subplot_fig.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
subplot_fig.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(subplot_fig)

#fleethistory.to_excel('pmx fleet.xlsx')