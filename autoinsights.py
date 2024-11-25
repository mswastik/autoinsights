import polars as pl
import altair as alt
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from theme import theme
from jinja2 import Template

alt.data_transformers.disable_max_rows()

today=datetime.today().replace(day=1)
#period = datetime.strptime('01-12-2023','%d-%m-%Y')
lmon=date(today.year, today.month, 1)-relativedelta(months=1)

sk= 900
class ss():
    l2fc='L2 Stat Final Rev'
    l0fc='`Fcst Stat Final Rev'
a=ss()
a.l2fc='L2 DF Final Rev'
a.l0fc='`Fcst DF Final Rev'

def genai(loch,loc,prodh,prod,df,uom,shead,lhead,rl):
    #UNCOMMENT on VM
    #df = pl.read_csv("F:\\COE Projects\\Swastik\\data.csv",dtypes={'CatalogNumber':pl.String,'UOM':pl.Float64,'`L0 ASP Final Rev':pl.Float64,'`Act Orders Rev':pl.Float64,'Act Orders Rev Val':pl.Float64,'L2 DF Final Rev':pl.Float64,'L1 DF Final Rev':pl.Float64,'L0 DF Final Rev':pl.Float64,'L2 Stat Final Rev':pl.Float64,'`Fcst DF Final Rev':pl.Float64,'`Fcst Stat Final Rev':pl.Float64,'`Fcst Stat Prelim Rev':pl.Float64,'Fcst DF Final Rev Val':pl.Float64} )
    df = df.with_columns(df['`Fcst DF Final Rev'].cast(pl.Float32()))
    df = df.with_columns(df['`Fcst Stat Final Rev'].cast(pl.Float32()))
    df = df.with_columns(df['`Fcst Stat Prelim Rev'].cast(pl.Float32()))
    df=df.filter(pl.col(prodh).is_in(prod.split(',')))
    #df=df.filter(pl.col(loch)==loc)
    df=df.filter(pl.col(loch).is_in(loc.split(',')))
    if not(rl):
        rl='CatalogNumber'
    if uom==1:
        df=df.with_columns(pl.col('`Act Orders Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('L2 DF Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('L2 Stat Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('L0 DF Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('L1 DF Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('`Fcst DF Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('`Fcst Stat Final Rev')*pl.col('UOM'))
        df=df.with_columns(pl.col('`Fcst Stat Prelim Rev')*pl.col('UOM'))
    if (len(df['IBP Level 5'].unique())<10) | ((df['Country'].unique()[0]=='UNITED STATES') & (df['Business Unit'].unique()[0]=='Upper Extremities')):
        ph='IBP Level 6'
        #df=df.drop(columns=["IBP Level 5"])
    else:
        ph='IBP Level 5'
        #df=df.drop(columns=["IBP Level 6"])
    #print(df.columns)
    df=df.drop(['Selling Division','Business Sector','Business Unit','UOM','Act Orders Rev Val','`L0 ASP Final Rev'])
    #df=df.group_by([loch,'Franchise','CatalogNumber','Product Line',ph,'SALES_DATE']).sum()
    df=df.group_by(['Franchise',rl,'IBP Level 7',ph,'SALES_DATE']).sum()
    df=df.with_columns((pl.when(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(pl.col('`Fcst DF Final Rev'))).alias('ActwFC'))
    df1=df.filter((df['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    cc=list(df1.group_by(rl).sum().sort(by='ActwFC',descending=True)[rl][:sk])
    df1=df1.filter(df1[rl].is_in(cc))
    
    # CALCULATIONS
    fdf=df.clone().filter((df['SALES_DATE']>=datetime(today.year,today.month,1)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=6)))
    fdf=fdf.with_columns((pl.col('L1 DF Final Rev')-pl.col('L0 DF Final Rev')).abs().alias('Fidelity L0L1'))
    fdf=fdf.with_columns(pl.col('Fidelity L0L1').cast(pl.Int32))
    tf=fdf.group_by(rl).sum().top_k(10,by='Fidelity L0L1')
    fdf=fdf.filter(pl.col(rl).is_in(tf[rl].unique()))
    fdf=fdf.with_columns((pl.col('SALES_DATE').dt.date()).alias('SALES_DATE'))
    fdf=fdf.pivot(index=[rl,'IBP Level 7'],columns='SALES_DATE',values='Fidelity L0L1',aggregate_function='sum')
    fdf = fdf[fdf.columns[:2]+sorted(fdf.columns[2:])]
    fdf=fdf.with_columns(sum=pl.sum_horizontal(fdf.columns[2:]))
    fdf=fdf.filter(pl.col('sum')>70).sort('sum',descending=True).drop('sum')
    #fdf=fdf.sort(pl.col(rl).replace({val: idx for idx, val in enumerate(tf[rl].to_list())},default=None))
    
    gdf=df.clone().sort([ph,'SALES_DATE'],descending=False)
    gdf=gdf.group_by(ph,pl.col('SALES_DATE').dt.year()).sum().sort([ph,'SALES_DATE'],descending=False)
    gdf=gdf.with_columns(pl.col('ActwFC').pct_change().over(ph).alias("YoY growth"))[[ph,'SALES_DATE','ActwFC','YoY growth']]
    gdf=gdf.with_columns(pl.col('YoY growth').diff().over(ph).alias("LY YoY"))

    # Brands with steep growth compared to this year
    gdf=gdf.with_columns(abs(pl.col('YoY growth')-pl.col('LY YoY')).alias('diff'))
    gdf=gdf.sort(by=pl.col('LY YoY').abs(),descending=True)
    gdf1=gdf.filter(pl.col('SALES_DATE')==today.year+1).filter(pl.col('LY YoY').is_not_nan()).filter(pl.col('LY YoY').is_finite()).filter(pl.col('ActwFC')>300).drop_nulls().sort(pl.col('LY YoY').abs(),descending=True)
    tmdf=gdf.filter(pl.col(ph).is_in(gdf1[ph][:10])).to_pandas().drop(columns=['LY YoY','diff'])
    tmdf['YoY growth']=tmdf['YoY growth'].map('{:.1%}'.format)
    tmdf['ActwFC']=tmdf['ActwFC'].round(0).map('{:,.0f}'.format)
    tmdf=tmdf.melt([ph,'SALES_DATE'])
    tmdf=tmdf.rename(columns={'variable':'Measure'})
    tmdf=tmdf[(tmdf['SALES_DATE']>today.year-3) & (tmdf['SALES_DATE']<today.year+2)]
    tmdf1=tmdf.pivot(index=[ph,'Measure'],columns='SALES_DATE',values='value')
 
    df5=df1.clone()
    df5=df5.with_columns(abs(df5['`Act Orders Rev']-df5['L2 Stat Final Rev']).alias('L2 Stat Abs Err'))
    df5=df5.with_columns((df5['`Act Orders Rev']-df5['L2 Stat Final Rev']).alias('L2 Stat Err'))
    df5=df5.with_columns(((1-df5['L2 Stat Abs Err']/df5['`Act Orders Rev'])).alias('L2 Stat Acc'))
    df5=df5.with_columns(abs(df5['`Act Orders Rev']-df5['L2 DF Final Rev']).alias('L2 DF Abs Err'))
    df5=df5.with_columns((df5['`Act Orders Rev']-df5['L2 DF Final Rev']).alias('L2 DF Err'))
    df5=df5.with_columns(((1-df5['L2 DF Abs Err']/df5['`Act Orders Rev'])).alias('L2 DF Acc'))
    df5=df5.with_columns(pl.when(df5['`Act Orders Rev']==0).then(1).otherwise(df5['L2 Stat Acc']))
    #print(df5)
    df5=df5.with_columns(df5['L2 Stat Acc'].clip(0,df5['L2 Stat Acc'].max()).alias('L2 Stat Acc'))
    #df5=df5.with_columns(pl.col('L2 Stat Acc').cast(pl.Float64).clip(0,pl.col('L2 Stat Acc').max()).alias('L2 Stat Acc'))
    df5=df5.with_columns(pl.when(df5['`Act Orders Rev']==0).then(1).otherwise(df5['L2 DF Acc']))
    df5=df5.with_columns(df5['L2 DF Acc'].clip(0,df5['L2 DF Acc'].max()).alias('L2 DF Acc'))
    #df5=df5.with_columns(pl.col('L2 DF Acc').cast(pl.Float64).clip(0,pl.col('L2 DF Acc').max()).alias('L2 DF Acc'))
    df5=df5.with_columns((df5['`Act Orders Rev']-df5['L2 DF Final Rev']).alias('Bias'))
    df5=df5.with_columns((df5['L2 DF Acc']-df5['L2 Stat Acc']).alias('FVA'))
    #print(df5['SALES_DATE'].unique())
    tmd=df5.filter((df5['SALES_DATE']>today-relativedelta(months=12)) &  (df5['SALES_DATE']<=today))
    tmd=tmd.sort('SALES_DATE')
    tmd=tmd.with_columns(pl.col('`Act Orders Rev').fill_null(0))
    tmd=tmd.with_columns(pl.col('`Act Orders Rev').std().over(['CatalogNumber']).alias('std'))
    tmd=tmd.with_columns(pl.col('`Act Orders Rev').mean().over(['CatalogNumber']).alias('avg'))
    tmd=tmd.with_columns((tmd['std']/tmd['avg']).alias('cvar'))
    tmd=tmd.with_columns(pl.when(pl.col('cvar')<=.4).then(pl.lit('X')).when(pl.col('cvar')<=1).then(pl.lit("Y")).otherwise(pl.lit("Z")).alias("Segment"))
    df5=df5.join(tmd[['CatalogNumber','Segment']].unique(),on=['CatalogNumber'],how='left')

    tmdf=df5.filter((df5['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=3)) & (df5['SALES_DATE']<=datetime(today.year,today.month,1)-relativedelta(months=1)))
    tmdf=tmdf.join(tmdf.group_by(rl).agg([pl.sum('`Act Orders Rev').alias("3M orders")]),on=[rl],how='left')
    tmdf=tmdf.join(tmdf.group_by(rl).agg([pl.sum('L2 DF Abs Err').alias("3M L2DF err")]),on=[rl],how='left')
    tmdf=tmdf.join(tmdf.group_by(rl).agg([pl.sum('L2 Stat Abs Err').alias("3M L2Stat err")]),on=[rl],how='left')
    tmdf=tmdf.join(tmdf.group_by(rl).agg([pl.sum('L2 Stat Final Rev').alias("3M L2 stat")]),on=[rl],how='left')
    tmdf=tmdf.join(tmdf.group_by(rl).agg([pl.sum('L2 DF Final Rev').alias("3M L2 df")]),on=[rl],how='left')

    tmdf=tmdf.with_columns((tmdf['3M orders']/tmdf['`Act Orders Rev'].sum()).alias('3M Orders Cont'))
    tmdf=tmdf.with_columns((tmdf['3M L2DF err']/tmdf['L2 DF Abs Err'].sum()).alias('3M DF Err Cont'))
    tmdf=tmdf.with_columns((tmdf['3M L2Stat err']/tmdf['L2 Stat Abs Err'].sum()).alias('3M Stat Err Cont'))
    tmdf=tmdf.with_columns((tmdf['3M DF Err Cont']/tmdf['3M Orders Cont']).alias('DF err ratio'))
    tmdf=tmdf.with_columns(((tmdf['3M orders']-tmdf['3M L2 stat'])/tmdf['3M orders']).alias('3M StatBias'))
    tmdf=tmdf.with_columns(((tmdf['3M orders']-tmdf['3M L2 df'])/tmdf['3M orders']).alias('3M DFBias'))
    tmdf=tmdf.with_columns(((1-tmdf['3M L2DF err']/tmdf['3M orders'])).alias('3M L2DF Acc'))
    tmdf=tmdf.with_columns(tmdf['3M L2DF Acc'].clip(0,tmdf['3M L2DF Acc'].max()).alias('3M L2DF Acc'))
    tmdf=tmdf.with_columns(((1-tmdf['3M L2Stat err']/tmdf['3M orders'])).alias('3M L2Stat Acc'))
    tmdf=tmdf.with_columns(tmdf['3M L2Stat Acc'].clip(0,tmdf['3M L2Stat Acc'].max()).alias('3M L2Stat Acc'))
    tmdf=tmdf.with_columns((tmdf['3M L2DF Acc']-tmdf['3M L2Stat Acc']).alias('3M FVA'))
    tmdf=tmdf[[ph,rl,'SALES_DATE','3M Orders Cont','3M DF Err Cont','3M Stat Err Cont','L2 DF Acc','L2 Stat Acc','3M StatBias','3M DFBias','3M FVA','3M L2DF Acc','3M L2Stat Acc','DF err ratio']]
    df5=df5.join(tmdf,on=[rl,'SALES_DATE'],how='left')
    df5=df5.with_columns((df5['L2 DF Final Rev']-df5['`Fcst DF Final Rev']).alias('FC change'))
    df5=df5.with_columns(abs(df5['FC change']).alias('FC change'))
    df5=df5.sort('SALES_DATE',descending=False)
    df5=df5.with_columns(pl.col('L2 Stat Acc').diff().over([rl]).alias('Stat Decrease'))
    df5=df5.with_columns(pl.col('L2 DF Acc').diff().over([rl]).alias('DF Decrease'))
    df5=df5.with_columns(pl.col('L2 DF Abs Err').diff().over([rl]).alias('L2 DF Err Inc'))
    df5=df5.with_columns(pl.col('L2 Stat Abs Err').diff().over([rl]).alias('L2 Stat Err Inc'))
    
    # Bias
    df6=df5.melt([ph,rl,'SALES_DATE','3M Orders Cont','3M DF Err Cont','3M Stat Err Cont','L2 Stat Acc','L2 DF Acc','FC change','3M DFBias','3M StatBias','3M FVA','Stat Decrease','DF Decrease'])
    df6=df6.rename({'variable':'type'})
    ddf2=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).top_k(80,by='3M DF Err Cont').filter(pl.col('3M DFBias')<-.05).to_pandas()[[ph,'IBP Level 7',rl,'3M DFBias','3M DF Err Cont']][:10]
    ddf3=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).top_k(80,by='3M DF Err Cont').filter(pl.col('3M DFBias')>.05).to_pandas()[[ph,'IBP Level 7',rl,'3M DFBias','3M DF Err Cont']][:10]

    #FVA
    fdf1=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).filter(pl.col('3M FVA')<-0.005).to_pandas().sort_values('3M DF Err Cont',ascending=False)[[ph,'IBP Level 7',rl,'3M L2DF Acc','3M L2Stat Acc','3M FVA','3M DF Err Cont']][:10]
    fdf1['3M L2DF Acc']=fdf1['3M L2DF Acc'].map('{:.1%}'.format)
    fdf1['3M L2Stat Acc']=fdf1['3M L2Stat Acc'].map('{:.1%}'.format)
    fdf1['3M DF Err Cont']=fdf1['3M DF Err Cont'].map('{:.1%}'.format)
    fdf1['3M FVA']=fdf1['3M FVA'].map('{:.1%}'.format)
 
    # Items with Maximum Error Increase
    edf=df5.filter(pl.col('SALES_DATE').is_in([datetime(today.year,today.month,1)-relativedelta(months=2),datetime(today.year,today.month,1)-relativedelta(months=1)]))
    tes=edf.filter(pl.col('ActwFC')>50).filter(pl.col("SALES_DATE")==datetime(today.year,today.month,1)-relativedelta(months=1)).filter(pl.col('L2 DF Err Inc')>0).drop_nulls('L2 DF Err Inc').sort('L2 DF Err Inc',descending=True)[[rl]][:10]
    if len(edf.filter(pl.col(rl).is_in(list(tes)[0])))>0:
        edf=edf.filter(pl.col(rl).is_in(list(tes)[0]))
    else:
        #pass
        print(prod+" Passed")
        #edf=edf[:10]
    edf=edf.to_pandas()
    edf['SALES_DATE']=edf['SALES_DATE'].dt.date

    edf2=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).filter(pl.col('DF err ratio')>1)
    tes=edf2.drop_nulls('L2 DF Err Inc').sort('3M DF Err Cont',descending=True)[[rl]][:20]
    edf2=df5.filter(pl.col(rl).is_in(list(tes)[0])).filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1))
    edf2=edf2.to_pandas()
    edf2['SALES_DATE']=edf2['SALES_DATE'].dt.date

    # Brand level working for mail body
    ibdf = df5.filter(pl.any_horizontal(pl.col('`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err').fill_nan(0))).filter(pl.all_horizontal(
        pl.col('`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err').is_finite())).group_by(ph,'SALES_DATE').sum()
    ibdf=ibdf[[ph,'SALES_DATE','`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err']]
    ibdf=ibdf.with_columns(((1-ibdf['L2 Stat Abs Err']/ibdf['`Act Orders Rev'])).alias('L2 Stat Acc'))
    ibdf=ibdf.with_columns(((1-ibdf['L2 DF Abs Err']/ibdf['`Act Orders Rev'])).alias('L2 DF Acc'))
    
    ib1=ibdf.filter(pl.col('SALES_DATE')==today.date()-relativedelta(months=1))
    ib1=ib1.with_columns((ib1['`Act Orders Rev']/ib1['`Act Orders Rev'].sum()).alias('Orders Cont'))
    ib1=ib1.with_columns((ib1['L2 DF Abs Err']/ib1['L2 DF Abs Err'].sum()).alias('DF Err Cont'))
    ib1=ib1.with_columns((ib1['L2 DF Acc']-ib1['L2 Stat Acc']).alias('FVA'))   #LM FVA change to 3M
    ib1=ib1.with_columns(((ib1['`Act Orders Rev']-ib1['L2 DF Final Rev'])/ib1['`Act Orders Rev']).alias('Bias'))
    ib1=ib1.sort('DF Err Cont',descending=True)
    #ib1.write_excel(f"C:\\Users\\smishra14\\OneDrive - Stryker\\python\\autoinsights\\{prod}.xlsx")
   
    tmi=ibdf.filter((pl.col('SALES_DATE')>=datetime(today.year,today.month,1)-relativedelta(months=3)) & (pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=1)))
    tmi=tmi.group_by(ph).sum()
    tmi=tmi.with_columns((pl.col('`Act Orders Rev')/pl.col('`Act Orders Rev').sum()).alias('Orders Cont'))
    tmi=tmi.with_columns((pl.col('L2 DF Abs Err')/pl.col('L2 DF Abs Err').sum()).alias('DF Err Cont'))
    tmi=tmi.with_columns((pl.col('DF Err Cont')/pl.col('Orders Cont')).alias('cont ratio'))
    tmi=tmi.filter(pl.col('cont ratio')>1)[[ph,'SALES_DATE','DF Err Cont','Orders Cont']]
    tmi=tmi.sort('DF Err Cont')[:5].melt([ph,'SALES_DATE'])
    tmi=tmi.rename({'value':'% Contribution'})

    gdf=gdf.with_columns((pl.col('ActwFC')/pl.col('ActwFC').sum()*100).alias('vol cont'))
    gdfc=gdf.filter(pl.col('SALES_DATE')==today.year+1).filter(pl.col('LY YoY').is_not_nan()).filter(pl.col('LY YoY').is_finite()).filter((pl.col('LY YoY').abs()>.09)).sort('vol cont',descending=True)
    cd=gdf.filter(pl.col(ph).is_in(gdfc[[ph]][:5])).filter(pl.col('SALES_DATE').is_in([today.year-1,today.year,today.year+1]))

    #cd=cd.with_columns(SALES_DATE=pl.col('SALES_DATE').cast(pl.String))
    #print(cd)
    bb=alt.Chart(cd).encode(x=alt.X('ActwFC:Q'),y=ph,yOffset="SALES_DATE:N",text=alt.Text('YoY growth:N',format=".0%"),color=alt.Color('SALES_DATE:N',title=''))
    bb1=(bb.mark_bar()+bb.mark_text(fontSize=10,dx=17)).configure(**theme).configure_axis(titleColor= "#555", titleFontSize=13).configure_axisX(labelFontSize=11).properties(width=230,height=215)   #Growth
    bb3b=alt.Chart(ib1.filter(pl.col('FVA')<-0.009)[:5]).encode(x=alt.X('FVA:Q',axis=alt.Axis(format=".0%")),y=alt.Y(ph,axis=alt.Axis(orient='right')),text=alt.Text('FVA',format='.0%')).properties(width=260,height=160)    #FVA
    bb3=(bb3b.mark_bar()+bb3b.mark_text(fontSize=10,dx=-18)).configure(**theme).configure_axis(titleColor= "#555", titleFontSize=13).configure_axisX(labelFontSize=11)
    bb2a=alt.Chart(ib1.filter(pl.col('Bias')<=-.1)[:5]).encode(x=alt.X('Bias:Q',axis=alt.Axis(format=".0%")),y=alt.Y(ph,axis=alt.Axis(orient='right')),text=alt.Text('Bias',format='.0%')).properties(width=210,height=160)  #-ve Bias
    bb2=(bb2a.mark_bar()+bb2a.mark_text(fontSize=10,dx=-18)).configure(**theme).configure_axis(titleColor= "#555", titleFontSize=12).configure_axisX(labelFontSize=11)
    bb2c=alt.Chart(ib1.filter(pl.col('Bias')>=.1)[:5]).encode(x=alt.X('Bias:Q',axis=alt.Axis(format=".0%")),y=ph,text=alt.Text('Bias',format='.0%')).properties(width=210,height=160)   #+ve Bias
    bb2b=(bb2c.mark_bar()+bb2c.mark_text(fontSize=10,dx=18)).configure(**theme).configure_axis(titleColor= "#555", titleFontSize=12).configure_axisX(labelFontSize=11)
    bb4b=alt.Chart(tmi).mark_bar().encode(x=alt.X('% Contribution:Q',axis=alt.Axis(format=".2%")),y=alt.Y(ph).sort('-x'),text=alt.Text('% Contribution:Q',format=".2%"),yOffset='variable').properties(width=230,height=200)
    bb4=(bb4b.mark_bar().encode(color=alt.Color('variable',title=''))+bb4b.mark_text(fontSize=10,dx=18)).configure(**theme).configure_axis(titleColor= "#555", titleFontSize=12).configure_axisX(labelFontSize=11)    #Contribution

    jinja2_template_string = open("C:\\Users\\smishra14\\OneDrive - Stryker\\python\\autoinsights\\test-supa.html", 'r').read()
    template = Template(jinja2_template_string)

    edf2['L2 DF Acc']=edf2['L2 DF Acc'].map('{:,.1%}'.format)
    edf2['3M DFBias']=edf2['3M DFBias'].map('{:,.1%}'.format)
    edf2['3M FVA']=edf2['3M FVA'].map('{:,.1%}'.format)
    edf2['3M Orders Cont']=edf2['3M Orders Cont'].map('{:,.1%}'.format)
    edf2['3M DF Err Cont']=edf2['3M DF Err Cont'].map('{:,.1%}'.format)
    ddf2['3M DFBias']=ddf2['3M DFBias'].map('{:,.1%}'.format)
    ddf2['3M DF Err Cont'] = ddf2['3M DF Err Cont'].map('{:,.1%}'.format)
    ddf3['3M DFBias']=ddf3['3M DFBias'].map('{:,.1%}'.format)
    ddf3['3M DF Err Cont'] = ddf3['3M DF Err Cont'].map('{:,.1%}'.format)

    # Max Error Increase
    eidf=edf.pivot(index=[ph,rl,'IBP Level 7'],columns='SALES_DATE',values='L2 DF Abs Err').sort_values(lmon,ascending=False).reset_index()
    eidf.columns=list(map(str,eidf.columns))
    eidf[eidf.columns[3]]=eidf[eidf.columns[3]].map('{:,.0f}'.format)
    eidf[eidf.columns[4]] = eidf[eidf.columns[4]].map('{:,.0f}'.format)
    if len(eidf)>10:
        eidf=eidf[:3]
    #else:
    #    eidf=edf.copy()
    df6=df5.to_pandas()
    # Growth - tmdf2 tmdf1
    # FVA - fdf1
    # +Bias - ddf3
    # -Bias - ddf2
    # Error Cont - fdf1 edf2
    # Max Err Inc - eidf
    #pad=pl.read_excel(f"C:\\Users\\smishra14\\OneDrive - Stryker\\data\\padata.xlsx",infer_schema_length=5000,schema_overrides={'SALES_DATE':pl.Datetime,'CatalogNumber':pl.String,'UOM':pl.Float64,'`L0 ASP Final Rev':pl.Float64,'`Act Orders Rev':pl.Float64,'Act Orders Rev Val':pl.Float64,'L2 DF Final Rev':pl.Float64,'L1 DF Final Rev':pl.Float64,'L0 DF Final Rev':pl.Float64,'L2 Stat Final Rev':pl.Float64,'`Fcst DF Final Rev':pl.Float64,'`Fcst Stat Final Rev':pl.Float64,'`Fcst Stat Prelim Rev':pl.Float64,'Fcst DF Final Rev Val':pl.Float64})
    pad=pl.read_parquet(f"C:\\Users\\smishra14\\OneDrive - Stryker\\data\\padata.parquet")
    try:
        pad=pad.filter(pl.all_horizontal(~pl.col(prodh).is_in(prod.split(',')), (~pl.col(loch).is_in(loc.split(','))), pl.col('SALES_DATE')!=datetime(today.year,today.month,1)-relativedelta(months=1)))
    except:
        pass
    #print([i for i in zip(pad.dtypes,df5.dtypes)])
    pl.concat([pad,df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1))], how="diagonal_relaxed").write_parquet(f"C:\\Users\\smishra14\\OneDrive - Stryker\\data\\padata.parquet")
    #df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).write_parquet(f"C:\\Users\\smishra14\\OneDrive - Stryker\\data\\padata.parquet")

    html = template.render(jdf=df6[[rl,ph,'SALES_DATE','`Act Orders Rev','`Fcst Stat Final Rev','`Fcst DF Final Rev','L2 DF Final Rev','L2 Stat Final Rev']].to_json(orient='records',date_format='iso'),
                           jdf2=tmdf1.reset_index().to_json(orient='records'),
                           sc2=list(tmdf1.reset_index().columns),
                           jdf3=fdf1.sort_values('3M DF Err Cont',ascending=False).to_json(orient='records'),
                           sc3=list(fdf1.columns),
                           jdf4=ddf2.to_json(orient='records'),
                           sc4=list(ddf2.columns),
                           jdf5=ddf3.to_json(orient='records'),
                           sc5=list(ddf3.columns),
                           jdf6=eidf.to_json(orient='records'),
                           sc6=list(map(str,eidf.columns)),
                           jdf7=edf2[[ph,rl,'IBP Level 7','L2 DF Acc', '3M DFBias','3M FVA','3M Orders Cont','3M DF Err Cont']].sort_values('3M DF Err Cont',ascending=False).to_json(orient='records'),
                           sc7=[ph,rl,'IBP Level 7','L2 DF Acc', '3M DFBias','3M FVA','3M Orders Cont','3M DF Err Cont'],
                           fdf=fdf.to_pandas().to_json(orient='records'),
                           fcol=list(map(str,fdf.columns)),
                           loc=loc,
                           prod=prod,
                           ph1=ph,
                           rl=rl,
                           today=str(datetime.today()))
    with open(f"reports//{lhead}-{shead}.html", "w") as fh:
        fh.write(html)
    return bb1,bb2,bb3,bb4,bb2b