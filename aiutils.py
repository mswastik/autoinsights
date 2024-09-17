import polars as pl
from autoinsights import genai
import win32com.client as win32
import os
import io
import altair as alt
import base64
from datetime import datetime #, date

today=datetime.today().replace(day=1)
alt.themes.enable('powerbi')
#maf=pl.read_excel("F:\\COE Projects\\Swastik\\mapping.xlsx",sheet_name='Sheet2')   #UNCOMMENT on VM
#maf=pl.read_excel("C:\\Users\\smishra14\\OneDrive - Stryker\\python\\autoinsights\\archive\\mapping1.xlsx",sheet_name='Sheet1')
maf=pl.read_excel("C:\\Users\\smishra14\\OneDrive - Stryker\\python\\autoinsights\\map.xlsx",sheet_name='Sheet2')
for sfn in maf['Filename'].unique():
   sfdf=maf.filter(pl.col('Filename')==sfn)
   print(sfn)
   df=pl.read_parquet(f"C:\\Users\\smishra14\\OneDrive - Stryker\\data\\{sfn}.parquet")
   for i in sfdf.iter_rows(named=True):
      if len(i['Product Value'])>55:
         shead=i['Product Value'].split(',')
         shead=shead[:4]
      else:
         shead=i['Product Value']
      if len(i['Location Value'])>55:
         lhead=i['Location Value'].split(',')
         lhead=lhead[:4]
      else:
         lhead=i['Location Value']
      buf1,buf2,buf3,buf4,buf5 = io.BytesIO(),io.BytesIO(),io.BytesIO(),io.BytesIO(),io.BytesIO()
      bb1,bb2,bb3,bb4,bb2b=genai(i['Location Hierarchy'],i['Location Value'],i['Product Hierarchy'],i['Product Value'],df,i['UOM'],shead,lhead,i['Report Level'])
      #print(shead)
      try:
         bb1.save(buf1,format='png', ppi=120)
      except:
         alt.Chart(buf1,format='png', ppi=120)
      try:
         bb2.save(buf2,format='png', ppi=120)
      except:
         alt.Chart(buf2,format='png', ppi=120)
      try:
         bb3.save(buf3,format='png', ppi=120)
      except:
         alt.Chart(buf3,format='png', ppi=120)
      try:
         bb4.save(buf4,format='png', ppi=120)
      except:
         alt.Chart(buf4,format='png', ppi=120)
      try:
         bb2b.save(buf5,format='png', ppi=120)
      except:
         alt.Chart(buf5,format='png', ppi=120)

      # Brand Color FFCC33, F44336, 9bf1f7, background-color:rgb(227,225,225);
      ht=f'''<!DOCTYPE html>
         <html>
         <body style="font-family: 'Lato','Calibri','Arial', Times, serif,'Courier New', Courier, monospace;">
               <div style="display: flex; background-color: #85458A; height: 55px; text-align:center; margin-bottom:0px; padding-bottom:0px;"><h1 style="color: white;font-family:Arial;">Planning Advisor for</br>
                <p style="color:#FFCC33; margin-bottom:0px;">{i['Location Value']} {i['Product Value']}</p></h1>
               <table style="width:100%" align="center" cellspacing="0" cellpadding="0" ; margin-top:0px; padding-top:-2px;>
               <colgroup>
                     <col span="1" style="width: 3%;">
                     <col span="1" style="width: 30%;">
                     <col span="1" style="width: 67%;"></colgroup>
               <tr style="width:100%; padding-bottom:5px;">
               <td style="background-color:#FFCC33;width: .4%;"></td>
               <td style="padding-left: 15px;width:25%; font:1.3em;"> Brands with high difference in next year's growth compared to this year's growth </td>
               <td style="padding-left: 15px;" colspan="1"><img src="data:image/png;base64,{base64.b64encode(buf1.getvalue()).decode()} style="max-width:480px;"</img></td>
               </tr>
               <tr style="background-color:#85458A;height:3px; margin-top:9px;"></tr>
               <tr>
               <td style="background-color:#B2B4AE;"></td>
               <td style="padding-left: 15px; font:1.3em;">Negative FVA (Stat Accu. > DF Accu) brands with high error contribution</td>
               <td style="padding-left: 15px;" colspan="1"><img src="data:image/png;base64,{base64.b64encode(buf3.getvalue()).decode()} style="max-width:480px;" </img></td>
               </tr>
               <tr style="background-color:#85458A;height:3px; margin-top:9px;"></tr>
               <tr>
               <td style="background-color:#FFCC33; padding-bottom:5px;"></td>
               <td style="padding-left: 15px;  font:1.3em;">Brands for which error contribution is higher than volume contribution in last 3 months</td>
               <td style="padding-left: 15px;" colspan="1"><img src="data:image/png;base64,{base64.b64encode(buf4.getvalue()).decode()} style="max-width:580px;"</img></td>
               </tr>
               <tr style="background-color:#85458A;height:3px; margin-top:9px;"></tr>
               <tr>
               <td style="background-color:#B2B4AE;padding-bottom:5px; "></td>
               <td style="padding-left: 15px; font:1.3em;">High negative BIAS brands </td>
               <td style="padding-left: 15px;"><img src="data:image/png;base64,{base64.b64encode(buf2.getvalue()).decode()} style="max-width:510px;" </img></td>
               </tr>
               <tr style="background-color:#85458A;height:3px; margin-top:9px;"></tr>
               <tr>
               <td style="background-color:#FFCC33;"></td>
               <td style="padding-left: 15px; font:1.3em;">High positive BIAS brands </td>
               <td style="padding-left: 15px;"><img src="data:image/png;base64,{base64.b64encode(buf5.getvalue()).decode()} style="max-width:510px;" </img></td>
               </tr>
               <tr style="background-color:#85458A;height:4px; margin-top:9px;"></tr>
         </table>
         </div>
         </body>
      </html>'''
      outlook=win32.Dispatch('outlook.application')
      mail=outlook.CreateItem(0)
      if i['Planner']:
         mail.To=i['Planner']
      if i['Modeller']:
         mail.CC=i['Modeller']
      mail.SentOnBehalfOfName="apa_demandcoe@stryker.com"
      mail.Subject=f"[Planning Advisor]-[{i['Location Value']} {shead}]-{today.strftime('%b-%y')} "
      mail.HTMLbody=ht
      mail.Attachments.Add(os.getcwd()+"\\reports"+f"\\{lhead}-{shead}.html")
      mail.Send()
      print(f'Sent for  {i['Location Value']} - {i['Product Value']}')