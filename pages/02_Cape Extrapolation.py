import streamlit as st
import plotly.express as px

import pandas as pd
from datetime import date
import calendar
st.set_page_config(layout="wide")


st.title('Capesize C5TC Extrapolation')

cutoff = pd.to_datetime('today')
curryear=cutoff.year
currquarter=cutoff.quarter
currmonth=cutoff.month

plot_ticks='inside'
plot_tickwidth=2
plot_ticklen=10
plot_title_font_color='dodgerblue'
plot_title_font_size=25
plot_legend_font_size=15
plot_axis=dict(tickfont = dict(size=15))


#import freight data
spot=st.session_state['spot']

spotcape=spot[['C5TC']]
spotcape.sort_index(inplace=True)
spotcape.dropna(inplace=True)

ffacape=st.session_state['c5tc_r']
currmffa=ffacape[['5TC_CCURMON']]
currmffa.dropna(inplace=True)
currmffa.index=pd.to_datetime(currmffa.index)
currmffa.sort_index(inplace=True)
ldffa=currmffa['5TC_CCURMON'][-1]

st.markdown('## **Current Month Extrapolation**')
st.markdown('#### **----Extrapolation from FFA to Spot**')

spotcape['Year']=spotcape.index.year
spotcape['Quarter']=spotcape.index.quarter
spotcape['Month']=spotcape.index.month

mtd=spotcape[(spotcape['Year']==curryear) & (spotcape['Month']==currmonth)]
mtdavg=mtd['C5TC'].mean()
ldspot=spotcape.iloc[-1,0]


currffa = st.number_input('Insert Current Month FFA',step=50,value=int(ldffa))

bzday=list(pd.bdate_range(date(curryear,currmonth,1),date(curryear,currmonth,calendar.monthrange(curryear, currmonth)[1])))

prvday=list(mtd.index)

futday=list(set(bzday)-set(prvday))

dprev=len(prvday)
dfut=len(futday)
dttl=dprev+dfut

extpl=(currffa*dttl-mtdavg*dprev)/dfut

mdict={'Latest Spot':[ldspot],'MtD Avg':[mtdavg],'Current Month FFA Input':[currffa],'Implied Avg for Days Left':[extpl],'Trading Days Passed':[dprev],'Total Trading Days':[dttl],'Trading Days Left':[dfut],'Latest FFA':[ldffa]}

mdf=pd.DataFrame(data=mdict,index=['Value'])

st.write(mdf.style.format('{:,.0f}'))

incre=2*(extpl-ldspot)/(1+dfut)

mrest=pd.DataFrame(data={'Date':futday,'Implied C5TC Average':extpl})
mrest.set_index('Date',inplace=True)
mrest.sort_index(inplace=True)
mrest['Year']=mrest.index.year
mrest['Quarter']=mrest.index.quarter
mrest['Month']=mrest.index.month

mrest.reset_index(inplace=True)
mrest['Row']=mrest.index+1
mrest.set_index('Date',inplace=True)
mrest['Simulation']=ldspot+mrest['Row']*incre

m=pd.concat([mtd,mrest])
m['Current Month FFA Input']=currffa

mchart=m[['C5TC','Current Month FFA Input','Implied C5TC Average','Simulation']]

lplot=px.line(mchart,width=1000,height=500,title='C5TC Current Month Extrapolation: From FFA to Spot')
lplot.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(lplot)

st.markdown('##### :blue[Daily Increment in Simulation:]'+' '+str(int(incre)))

st.markdown('#### **----Extrapolation from Spot to FFA**')

estspot = st.number_input('Insert Your Estimate of Average Spot Level for the Rest of the Trading Days',step=50,value=int(ldffa))

impffa=(mtdavg*dprev+estspot*dfut)/dttl

mdict2={'Latest Spot':[ldspot],'MtD Avg':[mtdavg],'Estimated Spot Left':[estspot],'Implied FFA':[impffa],'Trading Days Passed':[dprev],'Trading Days Left':[dfut],'Total Trading Days':[dttl],'Latest FFA':[ldffa]}
mdf2=pd.DataFrame(data=mdict2,index=['Value'])
st.write(mdf2.style.format('{:,.0f}'))

incre2=2*(estspot-ldspot)/(1+dfut)

mrest2=pd.DataFrame(data={'Date':futday,'Estimated Spot Left':estspot})
mrest2.set_index('Date',inplace=True)
mrest2.sort_index(inplace=True)
mrest2['Year']=mrest2.index.year
mrest2['Quarter']=mrest2.index.quarter
mrest2['Month']=mrest2.index.month

mrest2.reset_index(inplace=True)
mrest2['Row']=mrest2.index+1
mrest2.set_index('Date',inplace=True)
mrest2['Simulation']=ldspot+mrest2['Row']*incre2

m2=pd.concat([mtd,mrest2])
m2['Implied FFA']=impffa
mchart2=m2[['C5TC','Estimated Spot Left','Implied FFA','Simulation']]

lplot2=px.line(mchart2,width=1000,height=500,title='C5TC Current Month Extrapolation: From Spot to FFA')
lplot2.update_xaxes(ticks=plot_ticks, tickwidth=plot_tickwidth,  ticklen=plot_ticklen)
lplot2.update_layout(title_font_color=plot_title_font_color,title_font_size=plot_title_font_size,legend_font_size=plot_legend_font_size,xaxis=plot_axis,yaxis=plot_axis)
st.plotly_chart(lplot2)

st.markdown('##### :blue[Daily Increment in Simulation:]'+' '+str(int(incre2)))


st.markdown('## **Current Quarter Extrapolation**')
st.markdown('#### **----Extrapolation from Month to Quarter**')

if currmonth%3==1:
    qm1=st.number_input('Insert Current FFA for Month '+str(currmonth),step=50)
    qm2=st.number_input('Insert Current FFA for Month '+str(currmonth+1),step=50)
    qm3=st.number_input('Insert Current FFA for Month '+str(currmonth+2),step=50)

    st.text('Implied FFA for Quarter '+str(currquarter)+': '+str(int((qm1+qm2+qm3)/3)))
    st.text('Implied FFA for Month '+str(currmonth+1)+' and Month '+str(currmonth+2)+' Average: '+str(int((qm2+qm3)/2)))

elif currmonth%3==2:
    qm2=st.number_input('Insert Current FFA for Month '+str(currmonth+1),step=50)
    qm3=st.number_input('Insert Current FFA for Month '+str(currmonth+2),step=50)
    
    