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

st.title('Baltic Exchange Panamax 4TC')
st.text('Dry Bulk Freight (Panamax) Interactive Dashboard')



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
check=p4tc[y1]
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



s0='P4TC'
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




p4tc_pt=p4tc.copy()
p4tc_pt.index=p4tc_pt.index.date
p4tc_pt.sort_index(ascending=False,inplace=True)

idx=pd.bdate_range(start='1/1/1998', end=tooday)
idx2=pd.bdate_range(start='1/1/1998', end=tday)
p4tc_pt=p4tc_pt.reindex(idx,method='bfill')
p4tc_pt.sort_index(ascending=False,inplace=True)
p4tc_pt.index=p4tc_pt.index.date

st.markdown('## **Spot by Route**')
route=pmxroute.copy()

rangelist=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Last Week to Date','Month to Date','All'],key='8802')
sllist=st.multiselect('Select Contracts',options=route.columns,default=['P4TC','P6'],key='9902')
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
lplot=px.line(route_sl,width=1000,height=500,title='Panamax Routes Line Chart')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)

st.markdown('#### **----Ratio of Routes**')

rtr1=st.selectbox('Select Route 1',options=['P6']+list(route.columns),key='884')
rtr2=st.selectbox('Select Route 2',options=list(route.columns),key='885')
if rtr1!=rtr2:
    rtr=route[[rtr1,rtr2]]
    rtr.dropna(inplace=True)
    rtr['Ratio']=rtr[rtr1]/rtr[rtr2]
    rtrplot=px.line(rtr[['Ratio']],width=1000,height=500,title='Panamax Routes Ratio: '+str(rtr1)+' over '+str(rtr2))
    rtrplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    rtrplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    rtrplot.update_layout(template=draft_template)
    st.plotly_chart(rtrplot)

st.markdown('#### **----Spread of Routes**')

rtsp1=st.selectbox('Select Route 1',options=['P6']+list(route.columns),key='886')
rtsp2=st.selectbox('Select Route 2',options=list(route.columns),key='887')
if rtsp1!=rtsp2:
    rtsp=route[[rtsp1,rtsp2]]
    rtsp.dropna(inplace=True)
    rtsp['Spread']=rtsp[rtsp1]-rtsp[rtsp2]
    tspplot=px.line(rtsp[['Spread']],width=1000,height=500,title='Panamax Routes Spread: '+str(rtsp1)+' minus '+str(rtsp2))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)

st.markdown('## **Spot and Forward Contracts Line Chart**')
st.markdown('#### **----Fixed Contracts**')
rangelist1=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Month to Date','Last Week to Date','All'],key='rg1')
sllist1=st.multiselect('Select Contracts',options=p4tc_pt.columns,default=[s0,m1,q1,q2,y1,y2],key='sl1')
p4tc_sl=p4tc_pt[sllist1]

today = pd.to_datetime('today')
if rangelist1=='Last Week to Date':
    rangestart1=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist1=='Month to Date':
    rangestart1=date(today.year,today.month,1)
elif rangelist1=='Year to Date':
    rangestart1=date(today.year,1,1)
elif rangelist1=='Last Year to Date':
    rangestart1=date(today.year-1,1,1)
else:
    rangestart1=date(1998,1,1)

p4tc_sl=p4tc_sl[pd.to_datetime(p4tc_sl.index)>=pd.to_datetime(rangestart1)]
lplot=px.line(p4tc_sl,width=1000,height=500,title='P4TC Spot and Fixed Forward Contracts Historical Price')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)

st.markdown('#### **----Rolling Contracts**')

rangelist_r=st.selectbox('Select Range',options=['Last 2 Years to Date','Last Year to Date','Year to Date','Month to Date','Last Week to Date','All'],key='101')
sllist_r=st.multiselect('Select Contracts',options=p4tc_roll.columns,default=['P4TC','4TC_P+1MON','4TC_P+1Q','4TC_P+2Q','4TC_P+1CAL','4TC_P+2CAL'],key='102')
p4tc_sl=p4tc_roll[sllist_r]

today = pd.to_datetime('today')
if rangelist_r=='Last Week to Date':
    rangestart_r=today - timedelta(days=today.weekday()) + timedelta(days=6, weeks=-2)
elif rangelist_r=='Month to Date':
    rangestart_r=date(today.year,today.month,1)
elif rangelist_r=='Year to Date':
    rangestart_r=date(today.year,1,1)
elif rangelist_r=='Last 2 Years to Date':
    rangestart_r=date(today.year-2,1,1)
elif rangelist_r=='Last Year to Date':
    rangestart_r=date(today.year-1,1,1)
else:
    rangestart_r=date(1998,1,1)

p4tc_sl=p4tc_sl[pd.to_datetime(p4tc_sl.index)>=pd.to_datetime(rangestart_r)]
lplot=px.line(p4tc_sl,width=1000,height=500,title='P4TC Spot and Rolling Forward Contracts Historical Price')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
lplot.update_traces(connectgaps=True)
lplot.update_layout(template=draft_template)
st.plotly_chart(lplot)

st.markdown('## **Technical Analysis**')
st.markdown('#### **----Fixed Contracts**')

contractlist=st.selectbox('Select Spot or Forward Contract',options=[q1]+list(p4tc_pt.columns))
bb=st.number_input('Bollinger Bands Window',value=20)
ma1=st.number_input('Short Term Moving Average Window',value=20)
ma2=st.number_input('Long Term Moving Average Window',value=50)


p4tc_contract=p4tc_pt[[contractlist]]
p4tc_contract.dropna(inplace=True)

p4tc_contract.sort_index(inplace=True)
indicator_mast = SMAIndicator(close=p4tc_contract[contractlist], window=ma1)
indicator_malt = SMAIndicator(close=p4tc_contract[contractlist], window=ma2)
indicator_bb = BollingerBands(close=p4tc_contract[contractlist], window=bb, window_dev=2)
p4tc_contract['ma_st'] = indicator_mast.sma_indicator()
p4tc_contract['ma_lt'] = indicator_malt.sma_indicator()
p4tc_contract['bb_m'] = indicator_bb.bollinger_mavg()
p4tc_contract['bb_h'] = indicator_bb.bollinger_hband()
p4tc_contract['bb_l'] = indicator_bb.bollinger_lband()


contractplot=px.line(p4tc_contract,width=1000,height=500,title='P4TC Fixed Contract Bollinger Bands and Moving Average')
contractplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
contractplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
contractplot.update_layout(template=draft_template)
st.plotly_chart(contractplot)

st.markdown('#### **----Rolling Contracts**')

rangelist_r=st.selectbox('Select Range',options=['Last Year to Date','Year to Date','Month to Date','Last Week to Date','All'],key='205')
contractlist_r=st.selectbox('Select Spot or Forward Contract',options=['4TC_P+1MON']+list(p4tc_roll.columns),key='201')
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
    rangestart_r=date(today.year-1,1,1)
else:
    rangestart_r=date(1998,1,1)


p4tc_contract=p4tc_roll[[contractlist_r]]
p4tc_contract.dropna(inplace=True)
p4tc_contract=p4tc_contract[pd.to_datetime(p4tc_contract.index)>=pd.to_datetime(rangestart_r)]

p4tc_contract.sort_index(inplace=True)
indicator_mast = SMAIndicator(close=p4tc_contract[contractlist_r], window=ma1_r)
indicator_malt = SMAIndicator(close=p4tc_contract[contractlist_r], window=ma2_r)
indicator_bb = BollingerBands(close=p4tc_contract[contractlist_r], window=bb_r, window_dev=2)
p4tc_contract['ma_st'] = indicator_mast.sma_indicator()
p4tc_contract['ma_lt'] = indicator_malt.sma_indicator()
p4tc_contract['bb_m'] = indicator_bb.bollinger_mavg()
p4tc_contract['bb_h'] = indicator_bb.bollinger_hband()
p4tc_contract['bb_l'] = indicator_bb.bollinger_lband()

contractplot=px.line(p4tc_contract,width=1000,height=500,title='P4TC Rolling Contract Bollinger Bands and Moving Average')
contractplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
contractplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
contractplot.update_layout(template=draft_template)
st.plotly_chart(contractplot)

st.markdown('## **P4TC Spot and Rolling FFA Contracts Seasonality**')
contractlist_r=st.selectbox('Select Spot or Forward Contract',options=list(p4tc_roll.columns),key='211')
freq=st.radio('Select Frequency',options=['Daily','Weekly','Monthly','Quarterly'],key='spotfreq')
p4tc_sp=p4tc_roll[[contractlist_r]]
p4tc_sp.index=pd.to_datetime(p4tc_sp.index)

avgly=p4tc_sp[p4tc_sp.index.year==today.year-1][contractlist_r].mean()
avgty=p4tc_sp[p4tc_sp.index.year==today.year][contractlist_r].mean()
day_ytd=p4tc_sp[p4tc_sp.index.year==today.year].index.day_of_year.max()
avglyytd=p4tc_sp[(p4tc_sp.index.year==today.year-1)&(p4tc_sp.index.day_of_year<=day_ytd)][contractlist_r].mean()

if freq=='Weekly':
    p4tc_sp['Year']=p4tc_sp.index.year
    p4tc_sp['Week']=p4tc_sp.index.isocalendar().week
    p4tc_sp.loc[p4tc_sp[p4tc_sp.index.date==date(2016,1,2)].index,'Week']=0
    p4tc_sp.loc[p4tc_sp[p4tc_sp.index.date==date(2021,1,2)].index,'Week']=0
    p4tc_sp.loc[p4tc_sp[p4tc_sp.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(p4tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear1')
    p4tc_sp=p4tc_sp[p4tc_sp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_sp.pivot_table(index='Week',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=p4tc_sppt.max().max()
    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Weekly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=8, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=8, y=maxi*9/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=8, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=8, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Monthly':
    p4tc_sp['Year']=p4tc_sp.index.year
    p4tc_sp['Month']=p4tc_sp.index.month
    yrlist=list(p4tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear2')
    p4tc_sp=p4tc_sp[p4tc_sp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_sp.pivot_table(index='Month',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=p4tc_sppt.max().max()
    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Monthly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=2.5, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=2.5, y=maxi*9/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=2.5, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=2.5, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Quarterly':
    p4tc_sp['Year']=p4tc_sp.index.year
    p4tc_sp['Quarter']=p4tc_sp.index.quarter
    yrlist=list(p4tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear),key='spotyear3')
    p4tc_sp=p4tc_sp[p4tc_sp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_sp.pivot_table(index='Quarter',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=p4tc_sppt.max().max()
    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Quarterly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.add_annotation(x=1.4, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=1.4, y=maxi*9/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=1.4, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=1.4, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq=='Daily':
    p4tc_sp['Year']=p4tc_sp.index.year
    p4tc_sp['Day']=p4tc_sp.index.day_of_year
    yrlist=list(p4tc_sp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-3,curryear+1),key='spotyear3')
    p4tc_sp=p4tc_sp[p4tc_sp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_sp.pivot_table(index='Day',columns='Year',values=contractlist_r,aggfunc='mean')
    maxi=p4tc_sppt.max().max()
    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(contractlist_r)+' Daily Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='red'
    if len(spotplot['data'])>3:
        spotplot['data'][2]['line']['color']='black'
    spotplot.update_traces(connectgaps=True)
    spotplot.add_annotation(x=50, y=maxi*10/10,text=str(today.year-1)+' Avg: '+str(int(avgly)),showarrow=False,font=dict(size=18))
    #spotplot.add_annotation(x=50, y=maxi*9/10,text='2022 YTD Avg: '+str(int(avg2022ytd)),showarrow=False,font=dict(size=20))
    spotplot.add_annotation(x=50, y=maxi*9.2/10,text=str(today.year)+' YTD Avg: '+str(int(avgty)),showarrow=False,font=dict(size=18))
    spotplot.add_annotation(x=50, y=maxi*8.4/10,text='YTD YoY: '+str('{:.1%}'.format(avgty/avglyytd-1)),showarrow=False,font=dict(size=18,color=['red','lightgreen'][avgty/avglyytd-1>0]))
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)


st.markdown('## **Forward Curve**')
sllist2=st.multiselect('Select Contracts',options=p4tc_pt.columns,default=[s0,m0,m1,m2,m3,m4,q1,q2,q3,q4,q5,q6,y1,y2],key='2')
p4tc_fc=p4tc_pt[sllist2]
p4tc_fct=p4tc_fc.transpose()


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

sllist3=st.multiselect('Select Dates',options=p4tc_fct.columns,default=[tday,lday,lweek,lmonth],key='3')
p4tc_fctsl=p4tc_fct[sllist3]
fctplot=px.line(p4tc_fctsl,width=1000,height=500,title='P4TC Forward Curve')
fctplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
fctplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
fctplot.update_layout(template=draft_template)
st.plotly_chart(fctplot)

st.markdown('## **Time Spread**')
st.markdown('#### **----Fixed Contracts**')
tsp1=st.selectbox('Select Contract 1',options=[q1]+list(p4tc_pt.columns))
tsp2=st.selectbox('Select Contract 2',options=[q2]+list(p4tc_pt.columns))

if tsp1!=tsp2:
    p4tc_tsp=p4tc_pt[[tsp1,tsp2]]
    p4tc_tsp.dropna(inplace=True)
    p4tc_tsp['Spread']=p4tc_tsp[tsp1]-p4tc_tsp[tsp2]
    tspplot=px.line(p4tc_tsp[['Spread']],width=1000,height=500,title='P4TC Fixed Contract Time Spread: '+str(tsp1)+' minus '+str(tsp2))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)

st.markdown('#### **----Rolling Contracts**')
tsp1_r=st.selectbox('Select Contract 1',options=['4TC_P+1MON']+list(p4tc_roll.columns))
tsp2_r=st.selectbox('Select Contract 2',options=['4TC_P+2MON']+list(p4tc_roll.columns))

if tsp1_r!=tsp2_r:
    p4tc_tsp=p4tc_roll[[tsp1_r,tsp2_r]]
    p4tc_tsp.dropna(inplace=True)
    p4tc_tsp['Spread']=p4tc_tsp[tsp1_r]-p4tc_tsp[tsp2_r]
    tspplot=px.line(p4tc_tsp[['Spread']],width=1000,height=500,title='P4TC Rolling Contract Time Spread: '+str(tsp1_r)+' minus '+str(tsp2_r))
    tspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    tspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    tspplot.update_layout(template=draft_template)
    st.plotly_chart(tspplot)


freq_r=st.radio('Select Frequency',options=['Weekly','Monthly','Quarterly'],key='freq_r')
if freq_r=='Weekly':
    p4tc_tsp['Year']=p4tc_tsp.index.year
    p4tc_tsp['Week']=p4tc_tsp.index.isocalendar().week
    p4tc_tsp.loc[p4tc_tsp[p4tc_tsp.index.date==date(2016,1,2)].index,'Week']=0
    p4tc_tsp.loc[p4tc_tsp[p4tc_tsp.index.date==date(2021,1,2)].index,'Week']=0
    p4tc_tsp.loc[p4tc_tsp[p4tc_tsp.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(p4tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear11')
    p4tc_tsp=p4tc_tsp[p4tc_tsp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_tsp.pivot_table(index='Week',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Weekly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Monthly':
    p4tc_tsp['Year']=p4tc_tsp.index.year
    p4tc_tsp['Month']=p4tc_tsp.index.month
    yrlist=list(p4tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear22')
    p4tc_tsp=p4tc_tsp[p4tc_tsp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_tsp.pivot_table(index='Month',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Weekly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)

elif freq_r=='Quarterly':
    p4tc_tsp['Year']=p4tc_tsp.index.year
    p4tc_tsp['Quarter']=p4tc_tsp.index.quarter
    yrlist=list(p4tc_tsp['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear),key='spotyear33')
    p4tc_tsp=p4tc_tsp[p4tc_tsp['Year'].isin(yrsl)]
    p4tc_sppt=p4tc_tsp.pivot_table(index='Quarter',columns='Year',values='Spread',aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title='Rolling Contract Time Spread Weekly Seasonality '+str(tsp1_r)+' minus '+str(tsp2_r))
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)



st.markdown('## **Size Spread**')

c5tc_pt=c5tc.copy()
c5tc_pt=c5tc_pt.reindex(idx2,method='bfill')
c5tc_pt.sort_index(ascending=False,inplace=True)
c5tc_pt.index=c5tc_pt.index.date

s10tc_pt=s10tc.copy()
s10tc_pt=s10tc_pt.reindex(idx2,method='bfill')
s10tc_pt.sort_index(ascending=False,inplace=True)
s10tc_pt.index=s10tc_pt.index.date

p4tc_pt.rename(columns={'P4TC':'Spot'},inplace=True)
s10tc_pt.rename(columns={'S10TC':'Spot'},inplace=True)
c5tc_pt.rename(columns={'C5TC':'Spot'},inplace=True)

ssp_opt=list(set(s10tc_pt.columns)&set(p4tc_pt.columns)&set(c5tc_pt.columns))
ssp_opt.sort()

fcssp_multiopt=st.multiselect('Select Contracts for Forward Curve',options=ssp_opt,default=['Spot',m0,m1,m2,m3,m4,q1,q2,q3,q4,y1,y2],key='10')
p4tc_fcssp=p4tc_pt[fcssp_multiopt]
s10tc_fcssp=s10tc_pt[fcssp_multiopt]
c5tc_fcssp=c5tc_pt[fcssp_multiopt]
fcssp_opt=list(set(s10tc_fcssp.index)&set(p4tc_fcssp.index)&set(c5tc_fcssp.index))
fcssp_opt.sort(reverse=True)
fcssp=st.selectbox('Select Date for Forward Curve',options=fcssp_opt)
p4tc_fcssp=p4tc_fcssp.filter(items=[fcssp],axis=0)
p4tc_fcssp=p4tc_fcssp.transpose()
p4tc_fcssp.columns=['Panamax']
s10tc_fcssp=s10tc_fcssp.filter(items=[fcssp],axis=0)
s10tc_fcssp=s10tc_fcssp.transpose()
s10tc_fcssp.columns=['Supramax']
c5tc_fcssp=c5tc_fcssp.filter(items=[fcssp],axis=0)
c5tc_fcssp=c5tc_fcssp.transpose()
c5tc_fcssp.columns=['Capesize']
cp_fcssp=pd.merge(p4tc_fcssp,s10tc_fcssp,how='outer',left_index=True,right_index=True)
cp_fcssp=pd.merge(c5tc_fcssp,cp_fcssp,how='outer',left_index=True,right_index=True)

fcsspplot=px.line(cp_fcssp,width=1000,height=500,title='P4TC Forward Curve Size Spread')
fcsspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
fcsspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
fcsspplot.update_layout(template=draft_template)
st.plotly_chart(fcsspplot)

st.markdown('#### **----Fixed Contracts**')

ssp1=st.multiselect('Choose Contract',options=ssp_opt,default=[m1],key='2011')
ssp_chart1=pd.DataFrame()
for i in ssp1:
    c5tc_ssp1=c5tc_pt[[i]]
    c5tc_ssp1.columns=['Capesize']
    p4tc_ssp1=p4tc_pt[[i]]
    p4tc_ssp1.columns=['Panamax']
    ssp_mg1=pd.merge(c5tc_ssp1,p4tc_ssp1,how='inner',left_index=True,right_index=True)
    ssp_mg1[str(i)+' Size Spread']=ssp_mg1['Capesize']-ssp_mg1['Panamax']
    ssp_mg1.dropna(inplace=True)
    ssp_chart1=pd.merge(ssp_chart1,ssp_mg1[[str(i)+' Size Spread']],left_index=True,right_index=True,how='outer')

sspplot1=px.line(ssp_chart1,width=1000,height=500,title='Fixed Contract Size Spread: C5TC minus P4TC')
sspplot1.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
sspplot1.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
sspplot1.update_layout(template=draft_template)
st.plotly_chart(sspplot1)

ssp2=st.multiselect('Choose Contract',options=ssp_opt,default=[m1],key='2012')
ssp_chart2=pd.DataFrame()
for j in ssp2:
    p4tc_ssp2=p4tc_pt[[j]]
    p4tc_ssp2.columns=['Panamax']
    s10tc_ssp2=s10tc_pt[[j]]
    s10tc_ssp2.columns=['Supramax']
    ssp_mg2=pd.merge(p4tc_ssp2,s10tc_ssp2,how='inner',left_index=True,right_index=True)
    ssp_mg2[str(j)+' Size Spread']=ssp_mg2['Panamax']-ssp_mg2['Supramax']
    ssp_mg2.dropna(inplace=True)
    ssp_chart2=pd.merge(ssp_chart2,ssp_mg2[[str(j)+' Size Spread']],left_index=True,right_index=True,how='outer')

sspplot2=px.line(ssp_chart2,width=1000,height=500,title='Fixed Contract Size Spread: P4TC minus S10TC')
sspplot2.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
sspplot2.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
sspplot2.update_layout(template=draft_template)
st.plotly_chart(sspplot2)

st.markdown('#### **----Rolling Contracts**')

p4tc_roll.rename(columns={'P4TC':'4TC_PSpot'},inplace=True)
s10tc_roll.rename(columns={'S10TC':'10TC_SSpot'},inplace=True)
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


rsp=st.selectbox('Choose Contract',options=['+1MON']+list(rsp_opt),key='301')

rsp_s='10TC_S'+rsp
rsp_p='4TC_P'+rsp
rsp_sp=rsp+' Spread'

rsp_chart=pd.merge(s10tc_roll[rsp_s],p4tc_roll[rsp_p],left_index=True,right_index=True,how='inner')

rsp_chart[rsp_sp]=rsp_chart[rsp_p]-rsp_chart[rsp_s]

rspplot=px.line(rsp_chart[rsp_sp],width=1000,height=500,title=str(rsp)+' Rolling Contract Size Spread: P4TC Minus S10TC')
rspplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
rspplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
rspplot.update_layout(template=draft_template)
st.plotly_chart(rspplot)


freq_r=st.radio('Select Frequency',options=['Weekly','Monthly','Quarterly'],key='freq_rsp2')
if freq_r=='Weekly':
    rsp_chart['Year']=rsp_chart.index.year
    rsp_chart['Week']=rsp_chart.index.isocalendar().week
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2016,1,2)].index,'Week']=0
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2021,1,2)].index,'Week']=0
    rsp_chart.loc[rsp_chart[rsp_chart.index.date==date(2022,1,1)].index,'Week']=0
    yrlist=list(rsp_chart['Year'].unique())
    yrlist.sort(reverse=True)
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-4,curryear+1),key='spotyear11rr')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Week',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract P4TC Minus S10TC Size Spread Weekly Seasonality')
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
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear+1),key='spotyear22rr')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Month',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract P4TC Minus S10TC Size Spread Monthly Seasonality')
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
    yrsl=st.multiselect('Select Years',options=yrlist,default=np.arange(curryear-6,curryear),key='spotyear33rr')
    rsp_chart=rsp_chart[rsp_chart['Year'].isin(yrsl)]
    p4tc_sppt=rsp_chart.pivot_table(index='Quarter',columns='Year',values=rsp_sp,aggfunc='mean')

    spotplot=px.line(p4tc_sppt,width=1000,height=500,title=str(rsp)+' Rolling Contract P4TC Minus S10TC Size Spread Quarterly Seasonality')
    spotplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
    spotplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
    spotplot['data'][-1]['line']['width']=5
    spotplot['data'][-1]['line']['color']='black'
    spotplot.update_layout(template=draft_template)
    st.plotly_chart(spotplot)


s0='Spot'
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

st.markdown('## **Table Summary**')
p4tc_df=p4tc_pt[[s0,m0,m1,m2,m3,m4,q0,q1,q2,q3,q4,q5,q6,y1,y2,y3,y4]]
p4tc_=pd.concat([p4tc_df.loc[[tday.date()]],p4tc_df.loc[[lday.date()]],p4tc_df.loc[[lweek.date()]],p4tc_df.loc[[lmonth.date()]]])
st.write(p4tc_.style.format('{:,.0f}'))

st.markdown('#### **Change**')
p4tc_.loc['DoD Chg']=p4tc_df.loc[tday.date()]-p4tc_df.loc[lday.date()]
p4tc_.loc['WoW Chg']=p4tc_df.loc[tday.date()]-p4tc_df.loc[lweek.date()]
p4tc_.loc['MoM Chg']=p4tc_df.loc[tday.date()]-p4tc_df.loc[lmonth.date()]
p4tc_chg=pd.concat([p4tc_.loc[['DoD Chg']],p4tc_.loc[['WoW Chg']],p4tc_.loc[['MoM Chg']]])
st.write(p4tc_chg.style.format('{:,.0f}'))

st.markdown('#### **Change in Percentage**')
p4tc_.loc['DoD Chg %']=p4tc_.loc['DoD Chg']/p4tc_df.loc[lday.date()]
p4tc_.loc['WoW Chg %']=p4tc_.loc['WoW Chg']/p4tc_df.loc[lweek.date()]
p4tc_.loc['MoM Chg %']=p4tc_.loc['MoM Chg']/p4tc_df.loc[lmonth.date()]
p4tc_chgpct=pd.concat([p4tc_.loc[['DoD Chg %']],p4tc_.loc[['WoW Chg %']],p4tc_.loc[['MoM Chg %']]])
st.write(p4tc_chgpct.style.format('{:,.2%}'))