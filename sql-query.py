import polars as pl
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
#import altair as alt
from arrow_odbc import read_arrow_batches_from_odbc

#alt.data_transformers.disable_max_rows()
today=datetime.today()
with open('config.json') as json_file:
    data = json.load(json_file)
    un = data.get('username')
    pp = data.get('password')

#loc='StrykerGroupRegion'
#loc='Region'
loc='Country'
reg='UNITED STATES'
ss="gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"
fn='Instruments'

#df=pl.read_parquet(f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
df=pl.read_parquet(f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\{fn}.parquet')
#df=df.with_columns(pl.col('SALES_DATE').str.to_datetime('%Y-%m-%d'))

# Demand Review
query=f'''
SELECT 
    [SellingDivision] as [Selling Division],[COUNTRY_GROUP] 'Area',[StrykerGroupRegion] as [Stryker Group Region],[Region],[Country],p.[CatalogNumber],
    p.[BusinessSector] as [Business Sector],p.[BusinessUnit] as [Business Unit],p.[Franchise],p.[ProductLine] as [Product Line],p.[IBPLevel5] as [IBP Level 5],p.[IBPLevel6] as [IBP Level 6],[SALES_DATE],p.[xx_uom_conversion] as UOM ,
    SUM([L0_ASP_Final_Rev]) [`L0 ASP Final Rev], SUM([Act_Orders_Rev]) "`Act Orders Rev",
    SUM([Act_Orders_Rev_Val]) "Act Orders Rev Val", SUM(s.[L2_DF_Final_Rev]) as [L2 DF Final Rev],
    SUM(s."L1_DF_Final_Rev") as [L1 DF Final Rev], SUM(s."L0_DF_Final_Rev") as [L0 DF Final Rev],
    SUM(s.[L2_Stat_Final_Rev]) as [L2 Stat Final Rev], SUM(Fcst_DF_Final_Rev) as [`Fcst DF Final Rev], SUM(Fcst_Stat_Final_Rev) as [`Fcst Stat Final Rev],
    SUM(Fcst_Stat_Prelim_Rev) as [`Fcst Stat Prelim Rev], SUM(Fcst_DF_Final_Rev_Val) as [Fcst DF Final Rev Val]
       
FROM [Envision].[Demantra_CLD_Fact_Sales] s

JOIN [Envision].[DIM_Demantra_CLD_demantraproducts] p
ON s.item_skey = p.item_skey
        
JOIN [Envision].[DIM_Demantra_CLD_DemantraLocation] l
ON s.Location_sKey = l.Location_skey

JOIN [Envision].[Dim_DEMANTRA_CLD_MDP_Matrix] m
ON s.MDP_Key = m.MDP_Key

WHERE
     --([SALES_DATE] BETWEEN DATEADD(month, -24, GETDATE()) AND DATEADD(month, 24, GETDATE())) AND
     [SALES_DATE] BETWEEN '2021-01-01' AND '2025-12-31' AND
     --[COUNTRY_GROUP] in ('East Asia') AND
     [{loc.replace(' ','')}] in ('{reg}') AND
     --p.Franchise IN ('CMF','Endoscopy','Medical','Instruments','Joint Replacement','Spine','Trauma and Extremities')
     p.Franchise IN ('Instruments')
          
GROUP BY
    [SellingDivision],[COUNTRY_GROUP],[StrykerGroupRegion],[Region],[Country],p.[BusinessSector],p.[BusinessUnit],p.[Franchise],
    p.[IBPLevel5],p.[IBPLevel6],p.[ProductLine],[SALES_DATE],p.[CatalogNumber],p.[Item_id],p.[gim_itemid],p.[xx_uom_conversion]  '''

connection_string=f'Driver={{ODBC Driver 17 for SQL Server}};Server={ss};database=gda_glbsyndb;trusted_connection=Yes;'
reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string,user=un,password=pp)

df1=pl.DataFrame()
df=df.filter(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=2))
for batch in reader:
    #df1=pl.from_arrow(batch)
    df1=pl.concat([df1,pl.from_arrow(batch)])
df1=df1.with_columns(pl.col('SALES_DATE').cast(pl.Datetime).dt.cast_time_unit('us'))
df=pl.concat([df,df1])
df.write_parquet(f'C:\\Users\\smishra14\\OneDrive - Stryker\\data\\{fn}.parquet')