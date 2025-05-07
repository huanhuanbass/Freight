import streamlit as st
import plotly.express as px

import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
from dateutil import relativedelta
from pandas.tseries.offsets import BDay
from ta.volatility import BollingerBands
from ta.trend import SMAIndicator

import plotly.graph_objects as go

draft_template = go.layout.Template()
draft_template.layout.annotations = [
    dict(
        name="draft watermark",
        text="COFCO Internal Use Only",
        textangle=0,
        opacity=0.1,
        font=dict(color="black", size=70),
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
    )
]


cutoff = pd.to_datetime('today')
curryear=cutoff.year

plot_ticks='inside'
plot_tickwidth=2
plot_ticklen=10
plot_title_font_color='dodgerblue'
plot_title_font_size=25
plot_legend_font_size=15
plot_axis=dict(tickfont = dict(size=15))

#st.text('updated')
st.title('Baltic Exchange Capesize 5TC')
st.text('Dry Bulk Freight (Capesize) Interactive Dashboard')




#import freight data
if 'spot' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
spot=st.session_state['spot']
if 'p4tc' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
p4tc=st.session_state['p4tc']
if 'p4tc_r' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
p4tc_r=st.session_state['p4tc_r']
if 'c5tc' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
c5tc=st.session_state['c5tc']
if 'c5tc_r' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
c5tc_r=st.session_state['c5tc_r']
if 's10tc' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
s10tc=st.session_state['s10tc']
if 's10tc_r' not in st.session_state:
    st.markdown('# **:red[！ERROR]**')
    st.markdown('## **:red[Please reload data by clicking on the first tab Freight]**')
s10tc_r=st.session_state['s10tc_r']

caperoute=st.session_state['caperoute']
pmxroute=st.session_state['pmxroute']
smxroute=st.session_state['smxroute']
handyroute=st.session_state['handyroute']

p4tc_roll=p4tc_r.copy()
c5tc_roll=c5tc_r.copy()
s10tc_roll=s10tc_r.copy()


tooday=spot.index.max()
y1=str(tooday.year+2)+'_Y'
check=c5tc[y1]
check.dropna(inplace=True)



tday=check.index.max()
lday=tday-BDay(1)
l2day=tday-BDay(2)
l3day=tday-BDay(3)
l4day=tday-BDay(4)
lweek=tday-BDay(5)
l2week=tday-BDay(10)
l3week=tday-BDay(15)
lmonth=tday-BDay(20)
l2month=tday-BDay(45)


s0='C5TC'
m0=str(tday.year)+'_M'+str(tday.month)
q0=str(tday.year)+'_Q'+str(tday.quarter)
y0=str(tday.year)+'_Y'

m1=str((tday + relativedelta.relativedelta(months=1)).year)+'_M'+str((tday + relativedelta.relativedelta(months=1)).month)
m2=str((tday + relativedelta.relativedelta(months=2)).year)+'_M'+str((tday + relativedelta.relativedelta(months=2)).month)
m3=str((tday + relativedelta.relativedelta(months=3)).year)+'_M'+str((tday + relativedelta.relativedelta(months=3)).month)
m4=str((tday + relativedelta.relativedelta(months=4)).year)+'_M'+str((tday + relativedelta.relativedelta(months=4)).month)

q1=str((tday + relativedelta.relativedelta(months=3*1)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*1)).quarter)
q2=str((tday + relativedelta.relativedelta(months=3*2)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*2)).quarter)
q3=str((tday + relativedelta.relativedelta(months=3*3)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*3)).quarter)
q4=str((tday + relativedelta.relativedelta(months=3*4)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*4)).quarter)
q5=str((tday + relativedelta.relativedelta(months=3*5)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*5)).quarter)
q6=str((tday + relativedelta.relativedelta(months=3*6)).year)+'_Q'+str((tday + relativedelta.relativedelta(months=3*6)).quarter)

y1=str(tday.year+1)+'_Y'
y2=str(tday.year+2)+'_Y'
y3=str(tday.year+3)+'_Y'
y4=str(tday.year+4)+'_Y'
y5=str(tday.year+5)+'_Y'




c5tc_pt=c5tc.copy()
c5tc_pt.index=c5tc_pt.index.date
c5tc_pt.sort_index(ascending=False,inplace=True)


idx=pd.bdate_range(start='1/1/2014', end=tooday)
idx2=pd.bdate_range(start='1/1/2014', end=tday)
c5tc_pt=c5tc_pt.reindex(idx,method='bfill')
c5tc_pt.sort_index(ascending=False,inplace=True)
c5tc_pt.index=c5tc_pt.index.date

st.markdown('## **Spot by Route**')
route=caperoute.copy()

rangelist=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Last Week to Date','Month to Date','All'],key='880')
sllist=st.multiselect('Select Contracts',options=route.columns,default=['C5TC','C3','C5'],key='990')
route_sl=route[sllist]

today = pd.to_datetime('today')
if rangelist=='Last Week to Date':
    rangestart=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist=='Month to Date':
    rangestart=date(today.year,today.month,1)
elif rangelist=='Year to Date':
    rangestart=date(today.year,1,1)
elif rangelist=='Last Year to Date':
    rangestart=date(today.year-1,1,1)
else:
    rangestart=date(2014,1,1)

route_sl=route_sl[pd.to_datetime(route_sl.index)>=pd.to_datetime(rangestart)]
lplot=px.line(route_sl,width=1000,height=500,title='Capesize Routes Line Chart')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)

st.markdown('#### **----Ratio of Routes**')

rtr1=st.selectbox('Select Route 1',options=['C3']+list(route.columns))
rtr2=st.selectbox('Select Route 2',options=list(route.columns))
if rtr1!=rtr2:
    rtr=route[[rtr1,rtr2]]
    rtr.dropna(inplace=True)
    rtr['Ratio']=rtr[rtr1]/rtr[rtr2]
    tspplot=px.line(rtr[['Ratio']],width=1000,height=500,title='Capesize Routes Ratio: '+str(rtr1)+' over '+str(rtr2))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)

st.markdown('#### **----Spread of Routes**')

rtsp1=st.selectbox('Select Route 1',options=['C3']+list(route.columns),key='882')
rtsp2=st.selectbox('Select Route 2',options=list(route.columns),key='883')
if rtsp1!=rtsp2:
    rtsp=route[[rtsp1,rtsp2]]
    rtsp.dropna(inplace=True)
    rtsp['Spread']=rtsp[rtsp1]-rtsp[rtsp2]
    tspplot=px.line(rtsp[['Spread']],width=1000,height=500,title='Capesize Routes Spread: '+str(rtsp1)+' minus '+str(rtsp2))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)



st.markdown('## **Spot and Forward Contracts Line Chart**')
st.markdown('#### **----Fixed Contracts**')
rangelist=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Last Week to Date','Month to Date','All'])
sllist=st.multiselect('Select Contracts',options=c5tc_pt.columns,default=[s0,m1,m2,m3,q1,q2,q3,q4,y1,y2],key='1')
c5tc_sl=c5tc_pt[sllist]

today = pd.to_datetime('today')
if rangelist=='Last Week to Date':
    rangestart=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist=='Month to Date':
    rangestart=date(today.year,today.month,1)
elif rangelist=='Year to Date':
    rangestart=date(today.year,1,1)
elif rangelist=='Last Year to Date':
    rangestart=date(today.year-1,1,1)
else:
    rangestart=date(2014,1,1)

c5tc_sl=c5tc_sl[pd.to_datetime(c5tc_sl.index)>=pd.to_datetime(rangestart)]
lplot=px.line(c5tc_sl,width=1000,height=500,title='C5TC Spot and Fixed Forward Contracts Historical Price')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)

st.markdown('#### **----Rolling Contracts**')

rangelist_r=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Month to Date','Last Week to Date','All'],key='101')
sllist_r=st.multiselect('Select Contracts',options=c5tc_roll.columns,default=['C5TC','5TC_C+1MON','5TC_C+2MON','5TC_C+3MON','5TC_C+1Q','5TC_C+2Q','5TC_C+3Q','5TC_C+4Q','5TC_C+1CAL'],key='102')
c5tc_sl=c5tc_roll[sllist_r]

today = pd.to_datetime('today')
if rangelist_r=='Last Week to Date':
    rangestart_r=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist_r=='Month to Date':
    rangestart_r=date(today.year,today.month,1)
elif rangelist_r=='Year to Date':
    rangestart_r=date(today.year,1,1)
elif rangelist_r=='Last Year to Date':
    rangestart_r=date(today.year-1,1,1)
else:
    rangestart_r=date(2014,1,1)

c5tc_sl=c5tc_sl[pd.to_datetime(c5tc_sl.index)>=pd.to_datetime(rangestart_r)]
lplot=px.line(c5tc_sl,width=1000,height=500,title='C5TC Spot and Rolling Forward Contracts Historical Price')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)


st.markdown('## **Technical Analysis**')
st.markdown('#### **----Fixed Contracts**')

contractlist=st.selectbox('Select Spot or Forward Contract',options=[q1]+list(c5tc_pt.columns))
bb=st.number_input('Bollinger Bands Window',value=20)
ma1=st.number_input('Short Term Moving Average Window',value=20)
ma2=st.number_input('Long Term Moving Average Window',value=50)


c5tc_contract=c5tc_pt[[contractlist]]
c5tc_contract.dropna(inplace=True)

c5tc_contract.sort_index(inplace=True)
indicator_mast = SMAIndicator(close=c5tc_contract[contractlist], window=ma1)
indicator_malt = SMAIndicator(close=c5tc_contract[contractlist], window=ma2)
indicator_bb = BollingerBands(close=c5tc_contract[contractlist], window=bb, window_dev=2)
c5tc_contract['ma_st'] = indicator_mast.sma_indicator()
c5tc_contract['ma_lt'] = indicator_malt.sma_indicator()
c5tc_contract['bb_m'] = indicator_bb.bollinger_mavg()
c5tc_contract['bb_h'] = indicator_bb.bollinger_hband()
c5tc_contract['bb_l'] = indicator_bb.bollinger_lband()


contractplot=px.line(c5tc_contract,width=1000,height=500,title='C5TC Fixed Contract Bollinger Bands and Moving Average')
contractplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
contractplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
contractplot.update_layout(template=draft_template)
st.plotly_chart(contractplot)

st.markdown('#### **----Rolling Contracts**')

rangelist_r=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Month to Date','Last Week to Date','All'],key='205')
contractlist_r=st.selectbox('Select Spot or Forward Contract',options=['5TC_C+1MON']+list(c5tc_roll.columns),key='201')
bb_r=st.number_input('Bollinger Bands Window',value=20,key='202')
ma1_r=st.number_input('Short Term Moving Average Window',value=20,key='203')
ma2_r=st.number_input('Long Term Moving Average Window',value=50,key='204')

if rangelist_r=='Last Week to Date':
    rangestart_r=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist_r=='Month to Date':
    rangestart_r=date(today.year,today.month,1)
elif rangelist_r=='Year to Date':
    rangestart_r=date(today.year,1,1)
elif rangelist_r=='Last Year to Date':
    rangestart=date(today.year-1,1,1)
else:
    rangestart_r=date(2014,1,1)


c5tc_contract=c5tc_roll[[contractlist_r]]
c5tc_contract.dropna(inplace=True)
c5tc_contract=c5tc_contract[pd.to_datetime(c5tc_contract.index)>=pd.to_datetime(rangestart_r)]

c5tc_contract.sort_index(inplace=True)
indicator_mast = SMAIndicator(close=c5tc_contract[contractlist_r], window=ma1_r)
indicator_malt = SMAIndicator(close=c5tc_contract[contractlist_r], window=ma2_r)
indicator_bb = BollingerBands(close=c5tc_contract[contractlist_r], window=bb_r, window_dev=2)
c5tc_contract['ma_st'] = indicator_mast.sma_indicator()
c5tc_contract['ma_lt'] = indicator_malt.sma_indicator()
c5tc_contract['bb_m'] = indicator_bb.bollinger_mavg()
c5tc_contract['bb_h'] = indicator_bb.bollinger_hband()
c5tc_contract['bb_l'] = indicator_bb.bollinger_lband()

contractplot=px.line(c5tc_contract,width=1000,height=500,title='C5TC Rolling Contract Bollinger Bands and Moving Average')
contractplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
contractplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
contractplot.update_layout(template=draft_template)
st.plotly_chart(contractplot)





st.markdown('## **C5TC Spot and Rolling FFA Contracts Seasonality**')
contractlist_r=st.selectbox('Select Spot or Forward Contract',options=list(c5tc_roll.columns),key='211')
freq=st.radio('Select Frequency',options=['Daily','Weekly','Monthly','Quarterly'],key='spotfreq')
c5tc_sp=c5tc_roll[[contractlist_r]]
c5tc_sp.index=pd.to_datetime(c5tc_sp.index)

avgly=c5tc_sp[c5tc_sp.index.year==today.year-1][contractlist_r].mean()
avgty=c5tc_sp[c5tc_sp.index.year==today.year][contractlist_r].mean()
day_ytd=c5tc_sp[c5tc_sp.index.year==today.year].index.day_of_year.max()
avglyytd=c5tc_sp[(c5tc_sp.index.year==today.year-1)&(c5tc_sp.index.day_of_year<=day_ytd)][contractlist_r].mean()

if freq=='Weekly':
    c5tc_sp['Year']=c5tc_sp.index.year
    c5tc_sp['Week']=c5tc_sp.index.isocalendar().week
    c5tc_sp.loc[c5tc_sp[c5tc_sp.index.date==date(2016,1,2)].index,'Week']=0
    c5tc_sp.loc[c5tc_sp[c5tc_sp.index.date==date(2021,1,2)].index,'Week']=0
    c5tc_sp.loc[c5tc_sp[c5tc_sp.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(c5tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear1')
    c5tc_sp=c5tc_sp[c5tc_sp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_sp.pivot_table(index='Week',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=c5tc_sppt.max().max()
    spotplot=px.line(c5tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Weekly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=8, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=8, y=maxi*8.5/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=8, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=8, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Monthly':
    c5tc_sp['Year']=c5tc_sp.index.year
    c5tc_sp['Month']=c5tc_sp.index.month
    yrlist=list(c5tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear2')
    c5tc_sp=c5tc_sp[c5tc_sp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_sp.pivot_table(index='Month',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=c5tc_sppt.max().max()
    spotplot=px.line(c5tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Monthly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=2.5, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=2.5, y=maxi*8.5/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=2.5, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=2.5, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Quarterly':
    c5tc_sp['Year']=c5tc_sp.index.year
    c5tc_sp['Quarter']=c5tc_sp.index.quarter
    yrlist=list(c5tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear3')
    c5tc_sp=c5tc_sp[c5tc_sp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_sp.pivot_table(index='Quarter',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=c5tc_sppt.max().max()
    spotplot=px.line(c5tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Quarterly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=1.4, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=1.4, y=maxi*8.5/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=1.4, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=1.4, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Daily':
    c5tc_sp['Year']=c5tc_sp.index.year
    c5tc_sp['Day']=c5tc_sp.index.day_of_year
    yrlist=list(c5tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-3,curryear+1),key='spotyear3')
    c5tc_sp=c5tc_sp[c5tc_sp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_sp.pivot_table(index='Day',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=c5tc_sppt.max().max()
    spotplot=px.line(c5tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Daily Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='red'
    if len(spotplot['data'])>3:
        spotplot['data'][2]['line']['color']='black'
    spotplot.update_traces(connectgaps=True)
    spotplot.add_annotation(x=50, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=50, y=maxi*8.5/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=50, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=50, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)





st.markdown('## **Forward Curve**')
sllist2=st.multiselect('Select Contracts',options=c5tc_pt.columns,default=[s0,m0,m1,m2,m3,m4,q1,q2,q3,q4,q5,q6,y1,y2],key='2')
c5tc_fc=c5tc_pt[sllist2]
c5tc_fct=c5tc_fc.transpose()


tday=tday.date()
lday=lday.date()
l2day=l2day.date()
l3day=l3day.date()
l4day=l4day.date()
lweek=lweek.date()
l2week=l2week.date()
l3week=l3week.date()
lmonth=lmonth.date()
l2month=l2month.date()

sllist3=st.multiselect('Select Dates',options=c5tc_fct.columns,default=[tday,lday,lweek,lmonth],key='3')
c5tc_fctsl=c5tc_fct[sllist3]
fctplot=px.line(c5tc_fctsl,width=1000,height=500,title='C5TC Forward Curve')
fctplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
fctplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
fctplot.update_layout(template=draft_template)
st.plotly_chart(fctplot)


st.markdown('## **Time Spread**')
st.markdown('#### **----Fixed Contracts**')
tsp1=st.selectbox('Select Contract 1',options=[q1]+list(c5tc_pt.columns))
tsp2=st.selectbox('Select Contract 2',options=[q2]+list(c5tc_pt.columns))
if tsp1!=tsp2:
    c5tc_tsp=c5tc_pt[[tsp1,tsp2]]
    c5tc_tsp.dropna(inplace=True)
    c5tc_tsp['Spread']=c5tc_tsp[tsp1]-c5tc_tsp[tsp2]
    tspplot=px.line(c5tc_tsp[['Spread']],width=1000,height=500,title='C5TC Fixed Contract Time Spread: '+str(tsp1)+' minus '+str(tsp2))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)

st.markdown('#### **----Rolling Contracts**')
tsp1_r=st.selectbox('Select Contract 1',options=['5TC_C+1MON']+list(c5tc_roll.columns))
tsp2_r=st.selectbox('Select Contract 2',options=['5TC_C+2MON']+list(c5tc_roll.columns))

if tsp1_r!=tsp2_r:
    c5tc_tsp=c5tc_roll[[tsp1_r,tsp2_r]]
    c5tc_tsp.dropna(inplace=True)
    c5tc_tsp['Spread']=c5tc_tsp[tsp1_r]-c5tc_tsp[tsp2_r]
    tspplot=px.line(c5tc_tsp[['Spread']],width=1000,height=500,title='C5TC Rolling Contract Time Spread: '+str(tsp1_r)+' minus '+str(tsp2_r))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)


freq_r=st.radio('Select Frequency',options=['Weekly','Monthly','Quarterly'],key='freq_r')
if freq_r=='Weekly':
    c5tc_tsp['Year']=c5tc_tsp.index.year
    c5tc_tsp['Week']=c5tc_tsp.index.isocalendar().week
    c5tc_tsp.loc[c5tc_tsp[c5tc_tsp.index.date==date(2016,1,2)].index,'Week']=0
    c5tc_tsp.loc[c5tc_tsp[c5tc_tsp.index.date==date(2021,1,2)].index,'Week']=0
    c5tc_tsp.loc[c5tc_tsp[c5tc_tsp.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(c5tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear11')
    c5tc_tsp=c5tc_tsp[c5tc_tsp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_tsp.pivot_table(index='Week',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(c5tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Weekly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Monthly':
    c5tc_tsp['Year']=c5tc_tsp.index.year
    c5tc_tsp['Month']=c5tc_tsp.index.month
    yrlist=list(c5tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear22')
    c5tc_tsp=c5tc_tsp[c5tc_tsp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_tsp.pivot_table(index='Month',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(c5tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Monthly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Quarterly':
    c5tc_tsp['Year']=c5tc_tsp.index.year
    c5tc_tsp['Quarter']=c5tc_tsp.index.quarter
    yrlist=list(c5tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear),key='spotyear33')
    c5tc_tsp=c5tc_tsp[c5tc_tsp['Year'].isin(yrsl)]
    c5tc_sppt=c5tc_tsp.pivot_table(index='Quarter',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(c5tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Quarterly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)




st.markdown('## **Size Spread**')

p4tc_pt=p4tc.copy()
p4tc_pt=p4tc_pt.reindex(idx2,method='bfill')
p4tc_pt.sort_index(ascending=False,inplace=True)
p4tc_pt.index=p4tc_pt.index.date

c5tc_pt.rename(columns={'C5TC':'Spot'},inplace=True)
p4tc_pt.rename(columns={'P4TC':'Spot'},inplace=True)

ssp_opt=list(set(p4tc_pt.columns)&set(c5tc_pt.columns))
ssp_opt.sort()

fcssp_multiopt=st.multiselect('Select Contracts for Forward Curve',options=ssp_opt,default=['Spot',m0,m1,m2,m3,m4,q1,q2,q3,q4,q5,q6,y1,y2],key='10')
c5tc_fcssp=c5tc_pt[fcssp_multiopt]
p4tc_fcssp=p4tc_pt[fcssp_multiopt]
fcssp_opt=list(set(p4tc_fcssp.index)&set(c5tc_fcssp.index))
fcssp_opt.sort(reverse=True)
fcssp=st.selectbox('Select Date for Forward Curve',options=fcssp_opt)
c5tc_fcssp=c5tc_fcssp.filter(items=[fcssp],axis=0)
c5tc_fcssp=c5tc_fcssp.transpose()
c5tc_fcssp.columns=['Capesize']
p4tc_fcssp=p4tc_fcssp.filter(items=[fcssp],axis=0)
p4tc_fcssp=p4tc_fcssp.transpose()
p4tc_fcssp.columns=['Panamax']
cp_fcssp=pd.merge(c5tc_fcssp,p4tc_fcssp,how='outer',left_index=True,right_index=True)

fcsspplot=px.line(cp_fcssp,width=1000,height=500,title='C5TC Forward Curve Size Spread')
fcsspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
fcsspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
fcsspplot.update_layout(template=draft_template)
st.plotly_chart(fcsspplot)

st.markdown('#### **----Fixed Contracts**')

ssp=st.multiselect('Choose Contract',options=ssp_opt,default=[m1],key='11')
ssp_chart=pd.DataFrame()
for i in ssp:
    c5tc_ssp=c5tc_pt[[i]]
    c5tc_ssp.columns=['Capesize']
    p4tc_ssp=p4tc_pt[[i]]
    p4tc_ssp.columns=['Panamax']
    ssp_mg=pd.merge(c5tc_ssp,p4tc_ssp,how='inner',left_index=True,right_index=True)
    ssp_mg[str(i)+' Size Spread']=ssp_mg['Capesize']-ssp_mg['Panamax']
    ssp_mg.dropna(inplace=True)
    ssp_chart=pd.merge(ssp_chart,ssp_mg[[str(i)+' Size Spread']],left_index=True,right_index=True,how='outer')

sspplot=px.line(ssp_chart,width=1000,height=500,title='Fixed Contract Size Spread: C5TC minus P4TC')
sspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
sspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
sspplot.update_layout(template=draft_template)
st.plotly_chart(sspplot)

st.markdown('#### **----Rolling Contracts**')

p4tc_roll.rename(columns={'P4TC':'4TC_PSpot'},inplace=True)
c5tc_roll.rename(columns={'C5TC':'5TC_CSpot'},inplace=True)

rsp_opt=pd.Series(p4tc_roll.columns.values)
rsp_opt=rsp_opt.str.removeprefix('4TC_P')


rsp=st.selectbox('Choose Contract',options=['+1MON']+list(rsp_opt),key='300')

rsp_c='5TC_C'+rsp
rsp_p='4TC_P'+rsp
rsp_sp=rsp+' Spread'

rsp_chart=pd.merge(c5tc_roll[rsp_c],p4tc_roll[rsp_p],left_index=True,right_index=True,how='inner')

rsp_chart[rsp_sp]=rsp_chart[rsp_c]-rsp_chart[rsp_p]

rspplot=px.line(rsp_chart[rsp_sp],width=1000,height=500,title=str(rsp)+' Rolling Contract Size Spread: C5TC Minus P4TC')
rspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
rspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
rspplot.update_layout(template=draft_template)
st.plotly_chart(rspplot)


freq_r=st.radio('Select Frequency',options=['Weekly','Monthly','Quarterly'],key='freq_rsp')
if freq_r=='Weekly':
    rsp_chart['Year']=rsp_chart.index.year
    rsp_chart['Week']=rsp_chart.index.isocalendar().week
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2016,1,2)].index,'Week']=0
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2021,1,2)].index,'Week']=0
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(rsp_chart['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear11r')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Week',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract C5TC Minus P4TC Size Spread Weekly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Monthly':
    rsp_chart['Year']=rsp_chart.index.year
    rsp_chart['Month']=rsp_chart.index.month
    yrlist=list(rsp_chart['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear22r')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Month',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract C5TC Minus P4TC Size Spread Monthly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Quarterly':
    rsp_chart['Year']=rsp_chart.index.year
    rsp_chart['Quarter']=rsp_chart.index.quarter
    yrlist=list(rsp_chart['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear),key='spotyear33r')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Quarter',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract C5TC Minus P4TC Size Spread Quarterly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)


tday=check.index.max()
lday=tday-BDay(1)
l2day=tday-BDay(2)
l3day=tday-BDay(3)
l4day=tday-BDay(4)
lweek=tday-BDay(5)
l2week=tday-BDay(10)
l3week=tday-BDay(15)
lmonth=tday-BDay(20)
l2month=tday-BDay(45)

s0='Spot'

st.markdown('## **Table Summary**')
c5tc_df=c5tc_pt[[s0,m0,m1,m2,m3,m4,q0,q1,q2,q3,q4,q5,q6,y1,y2,y3,y4]]
c5tc_=pd.concat([c5tc_df.loc[[tday.date()]],c5tc_df.loc[[lday.date()]],c5tc_df.loc[[lweek.date()]],c5tc_df.loc[[lmonth.date()]]])
st.write(c5tc_.style.format('{:,.0f}'))

st.markdown('#### **----Change**')
c5tc_.loc['DoD Chg']=c5tc_df.loc[tday.date()]-c5tc_df.loc[lday.date()]
c5tc_.loc['WoW Chg']=c5tc_df.loc[tday.date()]-c5tc_df.loc[lweek.date()]
c5tc_.loc['MoM Chg']=c5tc_df.loc[tday.date()]-c5tc_df.loc[lmonth.date()]
c5tc_chg=pd.concat([c5tc_.loc[['DoD Chg']],c5tc_.loc[['WoW Chg']],c5tc_.loc[['MoM Chg']]])
st.write(c5tc_chg.style.format('{:,.0f}'))

st.markdown('#### **----Change in Percentage**')
c5tc_.loc['DoD Chg %']=c5tc_.loc['DoD Chg']/c5tc_df.loc[lday.date()]
c5tc_.loc['WoW Chg %']=c5tc_.loc['WoW Chg']/c5tc_df.loc[lweek.date()]
c5tc_.loc['MoM Chg %']=c5tc_.loc['MoM Chg']/c5tc_df.loc[lmonth.date()]
c5tc_chgpct=pd.concat([c5tc_.loc[['DoD Chg %']],c5tc_.loc[['WoW Chg %']],c5tc_.loc[['MoM Chg %']]])
st.write(c5tc_chgpct.style.format('{:,.2%}'))