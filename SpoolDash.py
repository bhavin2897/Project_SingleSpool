import multiprocessing as mp
import time
import datetime
from multiprocessing import Pool
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import pymysql
from pandas import DataFrame
from plotly.subplots import make_subplots
from reportlab.lib import colors
from reportlab.lib import utils
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch, cm
# import reportlab
from reportlab.pdfgen import canvas
from reportlab.platypus import Table
import shutil
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import  TTFont


customerDB = "CustomerDB_999330"
user = 'student'
password = 'student'


def dellbottom(timetsv, timefsv, previousstart, previousstop, Mnumber):
 db = pymysql.connect(db=customerDB, user=user,passwd=password,host="86.109.253.36",port=55500)
 cursor = db.cursor()
 cursor.execute(f"""
 SELECT 
 Value,`Timestamp`,Alias
 FROM {customerDB}.tProcessData
 INNER JOIN {customerDB}.tDatapoint ON CustomerDB_999330.tProcessData.idDatapoint={customerDB}.tDatapoint.id 
 INNER JOIN {customerDB}.tMachine ON CustomerDB_999330.tDatapoint.idMachine={customerDB}.tMachine.id
 WHERE `Timestamp` BETWEEN {timefsv} AND {timetsv} AND Type IN ('BMV 16/12','BMV 24 Z')
 AND MCMachine IN ('{Mnumber}')""")


 # Separate code for utilization of the machine
 cursor2 = db.cursor()
 cursor2.execute(f""" SELECT 
 TsEnd as Timestamp , ValAvg as Utilization 
 FROM CustomerDB_999330.tCalcValues
 INNER JOIN CustomerDB_999330.tDatapoint ON CustomerDB_999330.tCalcValues.idDatapoint = CustomerDB_999330.tDatapoint.id 
 INNER JOIN CustomerDB_999330.tMachine ON CustomerDB_999330.tDatapoint.idMachine=CustomerDB_999330.tMachine.id
 WHERE TsEnd BETWEEN {timefsv} AND {timetsv} AND Alias IN ('Utilization')
 AND MCMachine IN ('{Mnumber}') 
 """)


 # SQL script for Product Code
 cursor3 = db.cursor()
 cursor3.execute(f"""SELECT 
 `Timestamp`, Value
 FROM {customerDB}.tProcessDataStrings 
 INNER JOIN {customerDB}.tDatapoint ON {customerDB}.tProcessDataStrings.idDatapoint = {customerDB}.tDatapoint.id 
 INNER JOIN {customerDB}.tMachine ON {customerDB}.tDatapoint.idMachine={customerDB}.tMachine.id 
 WHERE MCMachine IN ('{Mnumber}') 
 """)

 prevdata = db.cursor()
 prevdata.execute(f"""
 SELECT 
 Value,`Timestamp`,Alias
 FROM {customerDB}.tProcessData
 INNER JOIN {customerDB}.tDatapoint ON {customerDB}.tProcessData.idDatapoint={customerDB}.tDatapoint.id 
 INNER JOIN {customerDB}.tMachine ON {customerDB}.tDatapoint.idMachine={customerDB}.tMachine.id
 WHERE `Timestamp` BETWEEN {previousstart} AND {previousstop} AND Type IN ('BMV 16/12','BMV 24 Z')
 AND Alias IN ('SpeedAct', 'LengthcounterAct', 'LengthcounterNom')  
 AND MCMachine IN ('{Mnumber}')
""")

 cursor4 = db.cursor()
 cursor4.execute(f"""
 SELECT
 `Timestamp`,Value,Alias
 FROM {customerDB}.tProcessData
 INNER JOIN {customerDB}.tDatapoint ON {customerDB}.tProcessData.idDatapoint={customerDB}.tDatapoint.id
 INNER JOIN {customerDB}.tMachine ON {customerDB}.tDatapoint.idMachine={customerDB}.tMachine.id
 AND Alias IN ('LubricatingOffTime')
 AND MCMachine IN ('{Mnumber}')
 """)

 MCname = db.cursor()
 MCname.execute(f"""
 SELECT 
 Alias
 FROM {customerDB}.tHeadstation
 WHERE MCHeadstation IN ('{Mnumber}')""")


 results = cursor.fetchall()
 results2 = cursor2.fetchall()
 results3 = cursor3.fetchall()
 result4 = prevdata.fetchall()
 result5 = cursor4.fetchall()
 MCname_result = DataFrame(data= MCname.fetchall(),columns= ['Alias'])
 db.commit()
 db.close()

 # -------------------------------------------------------------------------------
 # ------------------------------------------------------------------------------

 # To save the csv file from database as Dataframe
 db_data = DataFrame(results, columns= ['Value', 'Timestamp', 'Alias'])
 prev = DataFrame(result4, columns= ['Value', 'Timestamp', 'Alias']) # Extracting Previous Spool Data



 # Pivoting Dataframe for making each parameter an column
 machine_data = db_data.pivot(values='Value', index='Timestamp', columns='Alias').reset_index().rename_axis('index', axis=1)
 # Above dataframe would be used for all plots


 # Only Machine Status table
 machine_status = machine_data[['Timestamp','MachineStatus']].dropna().reset_index()

 machinespeedLength= machine_data[['Timestamp', 'SpeedAct', 'LengthcounterAct']].dropna(thresh=2).reset_index()


 SpeedActual = machine_data['SpeedAct'].replace(0,np.nan)
 Total_wireLength = machine_data['LengthcounterNom'].dropna().iloc[-1]
 Actual_wireLength = machine_data['LengthcounterAct']
 lengthcsv= machine_data[['Timestamp','LengthcounterNom','LengthcounterAct']].dropna(thresh = 2)
 speedcsv = machine_data[['Timestamp','SpeedAct']].dropna()


 db_utili = DataFrame(results2, columns=['Timestamp', 'Utilization'])
 db_product_code_test = DataFrame(results3, columns=['Timestamp', 'ProductCode'])
 product_code = db_product_code_test.iloc[-1]['ProductCode']

 Spoolbeforedata = prev.pivot(values='Value', index='Timestamp', columns='Alias').reset_index().rename_axis('index', axis=1)
 previousDf = Spoolbeforedata[['Timestamp', 'SpeedAct', 'LengthcounterAct']]


 # -------------------------------------------------------------------------------
 # ------------------------------------------------------------------------------

 machine_status['totalseconds'] = machine_status['Timestamp'] - datetime.datetime(1900, 1, 1)
 machine_status['totalseconds'].dt.total_seconds()


 def total_time(test_series, indexvalue):
     total = datetime.timedelta(0)
     a = datetime.datetime(1900, 1, 1)
     test_series['newc'] = test_series['Timestamp'] - a
     test_series['newc'].dt.total_seconds()
     for i in indexvalue:
         try:
             total = ((machine_status.loc[i + 1, 'totalseconds']) - (test_series.loc[i, 'newc'])) + total
         except KeyError:
             pass
     return total


 def convert_timedelta(duration):
     days, seconds = duration.days, duration.seconds
     hours = seconds // 3600
     minutes = (seconds % 3600) // 60
     seconds = (seconds % 60)
     dp = '{}Days   {}:{}:{}'.format(days,hours,minutes,seconds)
     return dp

 mostrecent_dt = machine_data['Timestamp'].max()
 mostlate_dt = machine_data['Timestamp'].min()
 shiftime = convert_timedelta(mostrecent_dt - mostlate_dt)


 if db_data['Alias'].str.contains('LubricatingOffTime').any():
     lubricationdb = machine_data[['Timestamp', 'LubricationPulses', 'LubricatingOffTime']].dropna(thresh=2)
     lubricationdb['Timestamp'] = pd.to_datetime(lubricationdb['Timestamp']).dt.strftime('%d-%b-%y %H:%M:%S')
     Lubr = lubricationdb[['Timestamp', 'LubricatingOffTime']].dropna()

 else:
      lubricationdb = machine_data[['Timestamp', 'LubricationPulses']].dropna(thresh=2)
      test_lub = DataFrame(result5, columns=['Timestamp', 'LubricatingOffTime', 'Alias'])
      # test_lub['Timestamp'] = pd.to_datetime(test_lub['Timestamp']).dt.strftime('%d-%b-%y %H:%M:%S')
      frame1 = test_lub.loc[test_lub['Timestamp'] <= mostlate_dt, ['Timestamp', 'LubricatingOffTime']].tail(3)
      frame2 = DataFrame([[mostlate_dt, test_lub.iloc[-1]['LubricatingOffTime']]],
                         columns=['Timestamp', 'LubricatingOffTime'])
      Lubr = frame1.append(frame2)
      Lubr['Timestamp'] = pd.to_datetime(Lubr['Timestamp']).dt.strftime('%d-%b-%y %H:%M:%S')


 # most recent and first data and time from the requested report duration
 spoolbeforestart = prev['Timestamp'].min()
 spoolbeforestop = prev['Timestamp'].max()
 previousspoolduration = convert_timedelta(spoolbeforestop - spoolbeforestart)


 # separating Runtime
 runtime_test = DataFrame(machine_status.loc[machine_status['MachineStatus'] == 30, 'Timestamp'])
 Runtime = total_time(runtime_test, runtime_test.index)
 dis_runtime = convert_timedelta(Runtime)

 # 0
 no_order_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 0), 'Timestamp'])
 No_Order = total_time(no_order_test, no_order_test.index)
 dis_No_Order = convert_timedelta(No_Order)  # Function ot display time as Day:Hours:Minutes:Seconds

 # 3
 standstill_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 3), 'Timestamp'])
 Standstill = total_time(standstill_test, standstill_test.index)
 dis_standstill = convert_timedelta(Standstill)

 # 6
 no_comm_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 6), 'Timestamp'])
 No_Comm = total_time(no_comm_test, no_comm_test.index)
 dis_no_comm = convert_timedelta(No_Comm)

 # 9
 fault_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 9), 'Timestamp'])
 Fault_Machine = total_time(fault_test, fault_test.index)
 dis_FM = convert_timedelta(Fault_Machine)

 # 12
 product_msg_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 12), 'Timestamp'])
 Product_message = total_time(product_msg_test, product_msg_test.index)
 dis_PM = convert_timedelta(Product_message)

 # 15
 maintnc_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 15), 'Timestamp'])
 Maintenance = total_time(maintnc_test, maintnc_test.index)
 dis_maint = convert_timedelta(Maintenance)

 # 18
 setup_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 18), 'Timestamp'])
 Setup = total_time(setup_test, setup_test.index)
 dis_Setup = convert_timedelta(Setup)

 # 21
 operating_test = DataFrame(machine_status.loc[(machine_status['MachineStatus'] == 21), 'Timestamp'])
 Operating_instructions = total_time(operating_test, operating_test.index)
 dis_OI = convert_timedelta(Operating_instructions)

 # separating Downtime
 Downtime = Fault_Machine + Product_message + Operating_instructions
 # Separating Planned Downtime
 Planned_Downtime = No_Order + Standstill + Maintenance + No_Comm  # (Error Nr. 0,3,6 and 15)
 print(Runtime, Downtime, Planned_Downtime)


 # -------------------------------------------------------------------------------
 # ------------------------------------------------------------------------------

 # Over Downtime of the machine in HH:MM:SS
 OverallDowntime = Planned_Downtime + Downtime # Converting HH:SS:MM without decimal
 OverallDowntime_S = convert_timedelta(OverallDowntime)




 try:
     # Percentage of each Downtime in overall Downtime of the machine
     downtime_causes0 = (No_Order/OverallDowntime)*100
     downtime_causes3 = (Standstill/OverallDowntime)*100
     downtime_causes6 = (No_Comm/OverallDowntime)*100
     downtime_causes9 = (Fault_Machine/OverallDowntime)*100
     downtime_causes12 = (Product_message/OverallDowntime)*100
     downtime_causes15 = (Maintenance/OverallDowntime)*100
     downtime_causes18 = (Setup/OverallDowntime)*100
     downtime_causes21 = (Operating_instructions/OverallDowntime)*100
 except ZeroDivisionError:
     downtime_causes0 = 0
     downtime_causes3 = 0
     downtime_causes6 = 0
     downtime_causes9 = 0
     downtime_causes12 = 0
     downtime_causes15 = 0
     downtime_causes18 = 0
     downtime_causes21 = 0

 # Utilization
 utilization = (db_utili['Utilization'].sum()/(db_utili['Utilization'].count()))


 # -------------------------------------------------------------------------------
 # ------------------------------------------------------------------------------

 # labels and values for pie charts
 labels = ['No Order','Standstill w/o fault','No communication', 'Machine Fault', 'Product Message','maintenance','Setup', 'Operating Instructions']
 values = [downtime_causes0, downtime_causes3, downtime_causes6, downtime_causes9, downtime_causes12,downtime_causes15,downtime_causes18,downtime_causes21]
 Hunderpercent = 100


 # Availaibitly Spool- Runtime, Downtime and Planned Downtime
 spoolavailabilty = go.Figure(data = [go.Pie(
                    values = [(Runtime/ (mostrecent_dt - mostlate_dt)) * 100 ,(Downtime/(mostrecent_dt - mostlate_dt)) * 100 ,(Planned_Downtime/(mostrecent_dt - mostlate_dt))*100],
                    labels = ['Production','Downtime', 'Standstill'],
                    name = 'Utilization', marker= {'colors': ['rgb(102,153,204)','rgb(255,153,0)', 'rgb(235,235,235)']})],
                    layout=(go.Layout( title= {'text':"Utilization", 'xanchor': 'left','yanchor': 'top'}, titlefont= {'color': '#444'},
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')))
 spoolavailabilty.update_layout(title_x = 0.5,title_font_family = 'Arial', legend= {'x':0, 'y':1})
 spoolavailabilty.write_image('Spool_Dash/Availabilitypie.png')


 # Interruption Causes Pie chart
 interpcauses = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.4,
                                    marker={'colors': ['rgb(51, 102, 204)', 'rgb(102, 204, 255)', 'rgb(153, 153, 153)',
                      'rgb(255,153,0)', 'rgb(255,255,51)','rgb(102,153,204)','rgb(102,102,102)','rgb(153,204,102)']})],
                       layout=go.Layout(title="Interruption Causes",
                                        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'))
 interpcauses.update_layout(title_x=0.5,title_font_family = 'Arial')
 interpcauses.write_image('Spool_Dash/interpCausespie.png')



 # Speed, Temperature and Power Input Chart
 SpTemp = make_subplots(specs=[[{'secondary_y': True}]])
 SpTemp.add_trace(go.Scatter(x=machine_data['Timestamp'], y=machine_data['SpeedAct'], name='Speed Actual',
                             mode='lines', connectgaps=True, line_shape = 'hv'), secondary_y=True)

 SpTemp.add_trace(go.Scatter(x=machine_data['Timestamp'], y=machine_data['SlipwayTemperatureAct'],
                             name='Slipway Temperature', mode='lines + markers', connectgaps=True,  line_shape = 'hv'), secondary_y=False)

 SpTemp.add_trace(
     go.Scatter(x=machine_data['Timestamp'], y=machine_data['PowerInput'], name='Energy (kWh)', mode='lines',
                opacity=0.7, connectgaps=True, yaxis='y3',  line_shape = 'hv'))

 SpTemp.update_layout(title_text='Speed, Temperature & Power Input', titlefont={'color': '#0B0B0A'},
                      legend={'font': {'color': '#0B0B0A'}}, xaxis={'color': '#0B0B0A', 'showgrid': True},
                      yaxis={'color': '#0B0B0A', 'showgrid': True}, yaxis2={'color': '#0B0B0A', 'showgrid': True},
                      yaxis3={'title': 'Energy(kWh)', 'color': '#0B0B0A', 'showgrid': True, 'overlaying': 'y',
                              'position': 0.99, 'side': 'right'},
                      autosize=False, width=1300, height=500, title_font_family = 'Arial')
 SpTemp.update_yaxes(title_text='Speed rpm', secondary_y=True)
 SpTemp.update_yaxes(title_text='Temperature °C', secondary_y=False)
 SpTemp.write_image('Spool_Dash/SpeedvsTemp.png')  # Image stored in


 # Average Speed Per Hour
 AvgSpeed = go.Figure(data= [go.Histogram(x = machinespeedLength['Timestamp'], y = machinespeedLength['SpeedAct'], histfunc= 'avg', name = 'Average Speed (rpm)/ Hour')],
                      layout= go.Layout(title ='Average Speed Line per Hour', yaxis = {'title': 'Speed rpm'}, showlegend= True, autosize=False, width=1000, height=500))
 AvgSpeed.add_shape(type="line",x0=machinespeedLength['Timestamp'].min(), y0=machinespeedLength['SpeedAct'].mean(), x1=machinespeedLength['Timestamp'].max(),
                    y1=machinespeedLength['SpeedAct'].mean(), line=dict(color="DarkOrange",width=2))
 AvgSpeed.update_layout(title_font_family = 'Arial')
 AvgSpeed.write_image('Spool_Dash/AvgSpeed.png')

 # Average Spool length
 AvgLength = go.Figure(data = [go.Scatter(x = machinespeedLength['Timestamp'], y = machinespeedLength['LengthcounterAct'], mode = 'lines',connectgaps= True, name = 'Length(m)')],
                       layout = go.Layout(title = 'Length Counter',  yaxis = {'title': 'Length (meters)'}, showlegend= True, autosize=False, width=1000, height=500))
 AvgLength.update_layout(title_font_family = 'Arial')
 AvgLength.write_image('Spool_Dash/AverageLenght.png')

# --------------------------------------------------------------------------------------------------------

 # ---------------------------------------------------------------------------
 # -----------------------------------------------------------------------
 # PDF creator Script
 pdfmetrics.registerFont(TTFont('Arial', 'Arial.ttf'))
 pdfmetrics.registerFont(TTFont('ArialBd', 'ArialBd.ttf'))

 logo = 'Spool_Dash/logo.png'
 reportTitle = 'BMV16 Single Spool Report'
 font = 'Arial'
 width, height = A4
 fontsize = 10

 # Title and logo
 pdf = canvas.Canvas('PDF-Dashboards/SingleSpool_test.pdf', pagesize=A4)
 pdf.setFont('Arial', 20)
 pdf.drawString(15, 800, reportTitle)
 pdf.drawImage(logo,  515,775, width= 0.7 * inch, height= 0.7 * inch)

 # header line
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.3)
 pdf.line(0, 775, 480, 775)
 pdf.line(480, 775, 510, 755)
 pdf.line(510, 755, 600, 755)

 # row two for machine report
 pdf.setFont(font, fontsize)
 pdf.drawString(15, 740, f'Machine ')
 pdf.drawString(100, 740, f"{MCname_result.iloc[0]['Alias']} - {Mnumber}")
 pdf.drawString(15, 710, f'Material')
 pdf.drawString(100, 710, f'{product_code}')
 pdf.drawString(350, 740, f"Spool Start {datetime.datetime.strftime(mostlate_dt, '%d-%m-%Y %H:%M')}")
 pdf.drawString(350, 710, f"Spool Stop {datetime.datetime.strftime(mostrecent_dt, '%d-%m-%Y %H:%M')} ")

 # header line
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.2)
 pdf.line(0, 700, 600, 700)

 # row 3 content
 pdf.setFont('ArialBd', fontsize)
 pdf.setFillColor(colors.black)
 pdf.drawString(15, 680, f'Length Produced {Actual_wireLength.max()} Meters  ')
 pdf.setFillColor(colors.black)
 pdf.setFont('ArialBd', fontsize)
 pdf.drawString(350, 680, f"Average Line Speed  {machinespeedLength['SpeedAct'].mean():.2f} rpm")
 pdf.drawString(350,650, f'Spool-Duration Time: {shiftime}')

 pdf.setFillColor(colors.grey)


 # getting images and its pixels for better image
 def get_image(x, y, path, width):
     img = utils.ImageReader(path)
     iw, ih = img.getSize()
     aspect = ih / float(iw)
     return pdf.drawImage(x=x, y=y, image=path, width=width, height=(width * aspect), mask='auto')

 # Pie Images
 get_image(-5, 420, 'Spool_Dash/Availabilitypie.png', width=11 * cm)
 get_image(250, 420, 'Spool_Dash/interpCausespie.png', width=11 * cm)
 get_image(5, 150, 'Spool_Dash/SpeedvsTemp.png', width= 21 * cm)


 # Table for Previous Spool
 pdf.setFont(font, fontsize)
 pdf.setFillColor(colors.black)
 pdf.drawString(35,110, 'Previous Spool ')
 PervHeader = ['Spool Start', 'Spool Stop', 'Average Speed (rpm)', 'Length Counter (mtrs) ']
 PervSpoolStart = datetime.datetime.strftime(spoolbeforestart, '%d-%m-%Y %H:%M:%S')
 PervSpoolStop = datetime.datetime.strftime(spoolbeforestop, '%d-%m-%Y %H:%M:%S')
 PervAvgSpeed = "{:.2f}".format(previousDf['SpeedAct'].mean())
 Pervlength = previousDf['LengthcounterAct'].max()
 pervdata = [PervSpoolStart, PervSpoolStop, PervAvgSpeed, Pervlength]
 pervTable = [PervHeader, pervdata]
 prevRowArray = [PervSpoolStart, PervSpoolStop, PervAvgSpeed, Pervlength]
 t1 = Table(data=pervTable, colWidths=1.7 * inch)
 t1.setStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
              ("ALIGN", (0, 0), (-1, -1), "CENTER"),
              ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
              ('BOX', (0, 0,), (-1, -1), 0.25, colors.black)])
 t1.wrapOn(pdf, width, height)
 t1.drawOn(pdf, 35, 60)
 pdf.drawString(35,40, f'Pervious Spool Duration: {previousspoolduration}')


 #footer line
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.3)
 pdf.line(0, 20, 480, 20)
 pdf.line(480, 20, 510, 40)
 pdf.line(510, 40, 600, 40)
 pdf.setFont('ArialBd', 6)
 pdf.setFillColor(colors.black)
 pdf.drawString(15,10,'Expertise, Customer Driven and Service - in Good Hands with NIEHOFF')

 pdf.showPage()

 # Second Page
 pdf.setFont('ArialBd', 20)
 pdf.drawString(120, 800, reportTitle)
 pdf.drawImage(logo, 500, 775, width=0.7 * inch, height=0.7 * inch)

 # header line
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.3)
 pdf.line(0, 775, 480, 775)
 pdf.line(480, 775, 510, 755)
 pdf.line(510, 755, 600, 755)

 # row two for machine report
 pdf.setFont(font, fontsize)
 pdf.drawString(15, 740, f'Machine')
 pdf.drawString(100, 740, f"{MCname_result.iloc[0]['Alias']} - {Mnumber}")
 pdf.drawString(15, 710, f'Material')
 pdf.drawString(100, 710, f'{product_code}')
 pdf.drawString(350, 740, f"Spool Start     {datetime.datetime.strftime(mostlate_dt, '%d-%m-%Y %H:%M')}")
 pdf.drawString(350, 710, f"Spool Stop  {datetime.datetime.strftime(mostrecent_dt, '%d-%m-%Y %H:%M')} ")



 # Graphs in Second Page
 get_image(80, 460, 'Spool_Dash/AverageLenght.png', width=17 * cm)
 get_image(80,230, 'Spool_Dash/AvgSpeed.png', width= 17*cm )




 # Prodcution Interuptiuon Causes and duration
 pdf.setFont('ArialBd', fontsize)
 pdf.drawString(20, 220, 'Interruption Causes')
 pdf.setFont(font, fontsize)
 pdf.drawString(20, 200, f'No Order')
 pdf.drawString(20, 180, f'Standstill w/0 fault')
 pdf.drawString(20, 160, f'No Communication')
 pdf.drawString(20, 140, f'Maintenance')
 pdf.drawString(20, 120, f'Machine Fault')
 pdf.drawString(20, 100, f'Product Message')
 pdf.drawString(20, 80, f'Setup')
 pdf.drawString(20, 60, f'Operating Instruction')
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.1)
 pdf.line(20, 50, 200, 50)
 pdf.drawString(20,40,'Overall Interruption Time')

 pdf.drawString(150, 200, f'{dis_No_Order}')
 pdf.drawString(150, 180,f'{dis_standstill}')
 pdf.drawString(150, 160, f'{dis_no_comm}')
 pdf.drawString(150, 140, f'{dis_maint}')
 pdf.drawString(150, 120, f'{dis_FM}')
 pdf.drawString(150, 100, f'{dis_PM}')
 pdf.drawString(150, 80,  f'{dis_Setup}')
 pdf.drawString(150, 60,  f'{dis_OI}')
 pdf.drawString(150,  40, f'{OverallDowntime_S}')

 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.2)
 pdf.rect(15, 135, 220, 80)
 pdf.saveState()
 pdf.setFont(font, 8)
 pdf.rotate(90)
 pdf.drawString(140, -245, 'Planned Downtime')
 pdf.restoreState()

 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.2)
 pdf.rect(15, 50, 220, 80)
 pdf.saveState()
 pdf.setFont(font, 8)
 pdf.rotate(90)
 pdf.drawString(70, -245, 'Downtime')
 pdf.restoreState()

 # table for Lubricatinf OFF time.
 column1Heading = 'TIMESTAMP'
 column2Heading = 'LUBRICATING-OFF'
 row_array = [column1Heading, column2Heading]
 tableHeading = [row_array]
 t = Table(data=tableHeading + np.array(Lubr).tolist(), colWidths=1.7 * inch)
 t.setStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
             ("ALIGN", (0, 0), (-1, -1), "CENTER"),
             ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('BOX', (0, 0,), (-1, -1), 0.25, colors.black)])
 t.wrapOn(pdf, width, height)
 t.drawOn(pdf, 300, 50)
 pdf.drawString(300,180, 'Lubricating Off Time and Value')



 #footer line
 pdf.setStrokeColor(colors.grey)
 pdf.setLineWidth(0.3)
 pdf.line(0, 20, 480, 20)
 pdf.line(480, 20, 510, 40)
 pdf.line(510, 40, 600, 40)
 pdf.setFont('ArialBd', 6)
 pdf.setFillColor(colors.black)
 pdf.drawString(15,10,'Expertise, Customer Driven and Service - in Good Hands with NIEHOFF')

 pdf.save()

 print('PDF Generated')

 # -------------------------------------------------------------------------------
 # ------------------------------------------------------------------------------
 # excel Writer in the project folder

 writer = pd.ExcelWriter(f"Spool_Files/Spool{Mnumber}-{datetime.datetime.now().strftime('%Y%m%d %H%M%S')}.xlsx",
                         engine='xlsxwriter')
 lengthcsv.to_excel(writer, sheet_name='Length Counter Values')
 speedcsv.to_excel(writer, sheet_name='Speed')
 db_utili.to_excel(writer, sheet_name='Utilization')
 machine_status.to_excel(writer, sheet_name='Machine Status')

 writer.save()
 # ------------------------------------------------------------------------------
 # duplicating files
 orgexcel = f"SingleSpool_Files/Spool{Mnumber}-{datetime.datetime.now().strftime('%Y%m%d %H%M')}.xlsx"
 targetexcel = f"email_dash/reportcsv_test.xlsx"
 shutil.copyfile(orgexcel, targetexcel)

 original = f'PDF-Dashboards/SpoolReport_test.pdf'
 target = f'email_dash/Report_test.pdf'
 shutil.copyfile(original, target)
# -------------------------------------------------------------------------------
# ------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------



def braiding(name):
   print(f'{name} - {datetime.datetime.now()}')
   db = pymysql.connect(db=customerDB, user=user, passwd=password, host="86.109.253.36", port=55500)
   cursor3 = db.cursor()
   cursor3.execute(f"""
   SELECT
   Value,`Timestamp`,Alias
   FROM {customerDB}.tProcessData
   INNER JOIN {customerDB}.tDatapoint ON {customerDB}.tProcessData.idDatapoint={customerDB}.tDatapoint.id
   INNER JOIN {customerDB}.tMachine ON {customerDB}.tDatapoint.idMachine={customerDB}.tMachine.id
   AND Alias IN ('LengthcounterAct') AND Value IN (0)
   AND MCMachine IN ('{name}')
   """)
   lCounterDF = DataFrame(data = cursor3.fetchall(), columns= ['Value','Timestamp','Alias'])
   db.commit()
   db.close()

   LCApv = lCounterDF.pivot(values='Value', index='Timestamp', columns='Alias').reset_index().rename_axis('index', axis=1)

   start = LCApv.iloc[-2]['Timestamp']  # Takes second last reset timestamp
   stop = LCApv.iloc[-1]['Timestamp']  # Takes latest reset timestamp
   prevStart = datetime.datetime.strftime(LCApv.iloc[-5]['Timestamp'], '%Y%m%d%H%M%S')
   prevStop = datetime.datetime.strftime(LCApv.iloc[-4]['Timestamp'], '%Y%m%d%H%M%S')

   #Start_from = datetime.datetime.strftime(start, '%Y%m%d%H%M%S')
   Stop_until = datetime.datetime.strftime(stop, '%Y%m%d%H%M')

   sendtime = datetime.datetime.now().strftime('%Y%m%d%H%M%S')  # Present Time

   example = datetime.datetime(2020,11,4,12,40,31)  # 04-11-2020 12:40:31
   example_conv = datetime.datetime.strftime(example, '%Y%m%d%H%M')
   print(f'running - {name} ')
   machinenumber = name

   if Stop_until == example_conv:      # If present time is equal to latest timestamp in LenghtcounterAct
    TimeFrom = 20201023212235
    TimeTill = 20201104124031
    dellbottom(TimeTill, TimeFrom, prevStart, prevStop,machinenumber)
    print(f'Prefect PDF SENT {datetime.datetime.now()} ')







# -------------------------------------------------------------
# -------------------------------------------------------------
db = pymysql.connect(db=customerDB, user=user, passwd=password, host="86.109.253.36", port=55500)
machines = db.cursor()
machines.execute(f"""
   SELECT 
    MCHeadstation
    FROM {customerDB}.tHeadstation 
    WHERE {customerDB}.tHeadstation.Line = 'Flechtanlage'
""")

bmv = DataFrame(data = machines.fetchall(), columns= ['MCHeadstation'])
db.commit()
db.close()
braiding_machines = bmv['MCHeadstation'].to_numpy()


if __name__ == '__main__':
  while True:
     p= []
     for j in range(len(braiding_machines)):
      for i in braiding_machines:
        p.append(mp.Process(target=braiding, args= [i]))
        p[j].start()
        p[j].join()
        j = j + 1
      break







    


