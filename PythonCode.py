# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 17:08:53 2018

@author: kaszo
"""
import json
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize

#nacitanie dat anonymized o velkosti 2mil
string=""
riadky=1
raw_data = []
stav=59
stav1=58
stav2=56
stav3=58
stav4=55

for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97%2Fcustomers-anonymized%2Fproperties.json', 'r', encoding="utf8"):
    string+=str(line)
    riadky+=1
    if len(raw_data) == 364463 and stav>0:
        string=""
        stav-=1
    if len(raw_data) == 447378 and stav1>0:
        string=""
        stav1-=1
    if len(raw_data) == 452798 and stav2>0:
        string=""
        stav2-=1
    if len(raw_data) == 487531 and stav3>0:
        string=""
        stav3-=1
    if len(raw_data) == 1270419 and stav4>0:
        string=""
        stav4-=1   
    if string.count("{") == string.count("}") and string.count("{")!=0 :
        string=string.replace("\n","")
        raw_data.append(json.loads(string))
        if len(raw_data) == 2000000:
            break
        string=""


#vytvorenie DF
dfAnonym= pd.DataFrame.from_dict(json_normalize(raw_data), orient='columns') 
        


#nacitanie dat o kampaniach
raw_data = []
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97%2Fcampaign%2F2017-06-24T22-11-59.json', 'r'):
    raw_data.append(json.loads(line))    
    
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97_campaign_2017-07-09T22-11-59.json', 'r'):
    raw_data.append(json.loads(line))       
    
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97%2Fcampaign%2F2017-08-21T05-15-57.json', 'r'):
    raw_data.append(json.loads(line))    
    
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97_campaign_2017-11-20T14-40-16.json', 'r'):
    raw_data.append(json.loads(line))
    
for line in open('8a687dd4-2e2c-11e6-ad00-141877340e97_campaign_2017-12-03T02-40-04.json', 'r'):
    raw_data.append(json.loads(line))    
    
#vytvorenie DF
dfCampaign = pd.DataFrame.from_dict(json_normalize(raw_data), orient='columns')

#dfCampaign.to_pickle('dfColumn+.pkl')

"""
#nahranie ulozenych DF
dfAnonym = pd.read_pickle('dfAnonym300.pkl')
dfAnonym.replace('', '-', inplace=True)
#dfCampaign = pd.read_pickle('dfCampaign.pkl')
dfCampaign = pd.read_pickle('dfColumn+.pkl')
"""

#vyber dat, ktore sa tykaju mailovej komunikacie
dfCampaign=dfCampaign[dfCampaign['data.properties.action_type'] == 'email']




#vytvorenie novych atributov z timestamp
import time
dfCampaign['date'] = dfCampaign['timestamp'].apply(lambda x: time.ctime(x))
dfCampaign['date']=dfCampaign['date'].astype('str')

for i in dfCampaign.index:
    dfCampaign.at[i,"date"]=dfCampaign.at[i,"date"].replace('  ',' ')

dfCampaign['NDay'], dfCampaign['Month'], dfCampaign['Day'], dfCampaign['Time'], dfCampaign['Year'] = dfCampaign['date'].str.split(' ', 4).str

for i in dfCampaign.index:
    dfCampaign.at[i,"Time"]=dfCampaign.at[i,"Time"][:2]
    



#vymazanie vybranych atributov
dfCampaign=dfCampaign.drop('data.timestamp', axis = 1)
dfCampaign=dfCampaign.drop('timestamp', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.campaign_name', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.action_name', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.attempts', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.code', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.language', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.recipient', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.status_code', axis = 1)
dfCampaign=dfCampaign.drop('data.properties.url', axis = 1)
dfCampaign=dfCampaign.drop('date', axis = 1)

#status binary
dfCampaign['data.properties.status']=dfCampaign['data.properties.status'].astype('str')       
for i in dfCampaign.index:
    x=dfCampaign.at[i,"data.properties.status"]
    if "opened" in x:
       dfCampaign.at[i,"data.properties.status"]="Open"         
    else:
       dfCampaign.at[i,"data.properties.status"]="NonOpen"



#uprava hodnot atributu
dfCampaign['data.properties.message']=dfCampaign['data.properties.message'].astype('str')

for i in dfCampaign.index:
    x=dfCampaign.at[i, 'data.properties.message']
    if "Failed sending e-mail" in x:
        dfCampaign.at[i, 'data.properties.message']='Failed sending e-mail'
    if "Invalid recipient" in x:
        dfCampaign.at[i, 'data.properties.message']='Invalid recipient'   
    if "nan" in x:
        dfCampaign.at[i, 'data.properties.message']='OK'    





#doplnenie chybajucich hodnot transformacia na bool
dfAnonym['properties.17-12-12_SMS_Christmas']=dfAnonym['properties.17-12-12_SMS_Christmas'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.17-12-12_SMS_Christmas"]
    if "Aktiv" in x:
       dfAnonym.at[i,"properties.17-12-12_SMS_Christmas"]=True         
    if "Neaktiv"  in x:
       dfAnonym.at[i,"properties.17-12-12_SMS_Christmas"]=False  
    if "nan"  in x:
       dfAnonym.at[i,"properties.17-12-12_SMS_Christmas"]=False
    
dfAnonym['properties.18-03-21_SMS']=dfAnonym['properties.18-03-21_SMS'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.18-03-21_SMS"]
    if "True" in x:
       dfAnonym.at[i,"properties.18-03-21_SMS"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.18-03-21_SMS"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.18-03-21_SMS"]=False 

dfAnonym['properties.18-05-07_SMS_Andel']=dfAnonym['properties.18-05-07_SMS_Andel'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.18-05-07_SMS_Andel"]
    if "True" in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Andel"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Andel"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Andel"]=False   

dfAnonym['properties.18-05-07_SMS_Narodka']=dfAnonym['properties.18-05-07_SMS_Narodka'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.18-05-07_SMS_Narodka"]
    if "True" in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Narodka"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Narodka"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.18-05-07_SMS_Narodka"]=False     
       
dfAnonym['properties.Black_Friday_vyzva']=dfAnonym['properties.Black_Friday_vyzva'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Black_Friday_vyzva"]
    if "True" in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva"]=False   

dfAnonym['properties.Black_Friday_vyzva_v2']=dfAnonym['properties.Black_Friday_vyzva_v2'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Black_Friday_vyzva_v2"]
    if "True" in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva_v2"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva_v2"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.Black_Friday_vyzva_v2"]=False      

dfAnonym['properties.Brazilie_final']=dfAnonym['properties.Brazilie_final'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Brazilie_final"]
    if "True" in x:
       dfAnonym.at[i,"properties.Brazilie_final"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.Brazilie_final"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.Brazilie_final"]=False      
      
dfAnonym['properties.Bounce']=dfAnonym['properties.Bounce'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Bounce"]
    if "OK" in x:
       dfAnonym.at[i,"properties.Bounce"]=True         
    if "False"  in x:
       dfAnonym.at[i,"properties.Bounce"]=False
    if "nan"  in x:
       dfAnonym.at[i,"properties.Bounce"]=False  
 
dfAnonym['properties.Dluhopisy_Osobni']=dfAnonym['properties.Dluhopisy_Osobni'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Dluhopisy_Osobni"]
    if "True" in x:
       dfAnonym.at[i,"properties.Dluhopisy_Osobni"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Dluhopisy_Osobni"]=False

dfAnonym['properties.Dluhopisy_Osobni_2']=dfAnonym['properties.Dluhopisy_Osobni_2'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Dluhopisy_Osobni_2"]
    if "True" in x:
       dfAnonym.at[i,"properties.Dluhopisy_Osobni_2"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Dluhopisy_Osobni_2"]=False 
       
dfAnonym['properties.Home address']=dfAnonym['properties.Home address'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Home address"]
    if "True" in x:
       dfAnonym.at[i,"properties.Home address"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Home address"]=False        
       
dfAnonym['properties.Nakupili ZOOT Original']=dfAnonym['properties.Nakupili ZOOT Original'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Nakupili ZOOT Original"]
    if "true" in x:
       dfAnonym.at[i,"properties.Nakupili ZOOT Original"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Nakupili ZOOT Original"]=False       
       
dfAnonym['properties.Nezaplatili/Nevyzdvihli objednavku do zari']=dfAnonym['properties.Nezaplatili/Nevyzdvihli objednavku do zari'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Nezaplatili/Nevyzdvihli objednavku do zari"]
    if "true" in x:
       dfAnonym.at[i,"properties.Nezaplatili/Nevyzdvihli objednavku do zari"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Nezaplatili/Nevyzdvihli objednavku do zari"]=False       
       
dfAnonym["properties.RMK_reco_split"]=dfAnonym["properties.RMK_reco_split"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.RMK_reco_split"]
    if "nan" in x:
        dfAnonym.at[i,"properties.RMK_reco_split"]='-'        

dfAnonym['properties.Pres_1000']=dfAnonym['properties.Pres_1000'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Pres_1000"]
    if "True" in x:
       dfAnonym.at[i,"properties.Pres_1000"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Pres_1000"]=False  
       
dfAnonym['properties.RO-SK customer ZOOT ORIGINAL']=dfAnonym['properties.RO-SK customer ZOOT ORIGINAL'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.RO-SK customer ZOOT ORIGINAL"]
    if "true" in x:
       dfAnonym.at[i,"properties.RO-SK customer ZOOT ORIGINAL"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.RO-SK customer ZOOT ORIGINAL"]=False       
       
dfAnonym['properties.Reaktivace_Srpen_Reminder']=dfAnonym['properties.Reaktivace_Srpen_Reminder'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Reaktivace_Srpen_Reminder"]
    if "True" in x:
       dfAnonym.at[i,"properties.Reaktivace_Srpen_Reminder"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Reaktivace_Srpen_Reminder"]=False        
       
dfAnonym['properties.Registrovali sa ale nespravili objednavku do zari']=dfAnonym['properties.Registrovali sa ale nespravili objednavku do zari'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Registrovali sa ale nespravili objednavku do zari"]
    if "true" in x:
       dfAnonym.at[i,"properties.Registrovali sa ale nespravili objednavku do zari"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Registrovali sa ale nespravili objednavku do zari"]=False
       
dfAnonym["properties.SMS_BF2_Unique"]=dfAnonym["properties.SMS_BF2_Unique"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_BF2_Unique"]
    if "nan" in x:
        dfAnonym.at[i,"properties.SMS_BF2_Unique"]='-'       

dfAnonym['properties.SMS_BF_1']=dfAnonym['properties.SMS_BF_1'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_BF_1"]
    if "True" in x:
       dfAnonym.at[i,"properties.SMS_BF_1"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.SMS_BF_1"]=False 
       
dfAnonym['properties.SMS_Brazilie']=dfAnonym['properties.SMS_Brazilie'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_Brazilie"]
    if "nan" in x:
       dfAnonym.at[i,"properties.SMS_Brazilie"]='0'         
  
dfAnonym['properties.SMS_Vanoce_2']=dfAnonym['properties.SMS_Vanoce_2'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_Vanoce_2"]
    if "True" in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_2"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_2"]=False

dfAnonym['properties.SMS_Vanoce_3']=dfAnonym['properties.SMS_Vanoce_3'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_Vanoce_3"]
    if "True" in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_3"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_3"]=False

dfAnonym['properties.SMS_Vanoce_3_Oprava']=dfAnonym['properties.SMS_Vanoce_3_Oprava'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.SMS_Vanoce_3_Oprava"]
    if "True" in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_3_Oprava"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.SMS_Vanoce_3_Oprava"]=False

dfAnonym['properties.VIP_Praha']=dfAnonym['properties.VIP_Praha'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.VIP_Praha"]
    if "true" in x:
       dfAnonym.at[i,"properties.VIP_Praha"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.VIP_Praha"]=False

dfAnonym['properties.VIP_mimoPrahu']=dfAnonym['properties.VIP_mimoPrahu'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.VIP_mimoPrahu"]
    if "true" in x:
       dfAnonym.at[i,"properties.VIP_mimoPrahu"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.VIP_mimoPrahu"]=False
       
dfAnonym['properties.Varka_patek']=dfAnonym['properties.Varka_patek'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Varka_patek"]
    if "1" in x:
       dfAnonym.at[i,"properties.Varka_patek"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Varka_patek"]=False       
       
dfAnonym['properties.Women buing mens products in 15%']=dfAnonym['properties.Women buing mens products in 15%'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.Women buing mens products in 15%"]
    if "true" in x:
       dfAnonym.at[i,"properties.Women buing mens products in 15%"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.Women buing mens products in 15%"]=False  

dfAnonym['properties._cookies_count']=dfAnonym['properties._cookies_count'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties._cookies_count"]
    if "nan"  in x:
       dfAnonym.at[i,"properties._cookies_count"]='0'     
    
      
dfAnonym['properties.blacklist']=dfAnonym['properties.blacklist'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.blacklist"]
    if "true" in x:
       dfAnonym.at[i,"properties.blacklist"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.blacklist"]=False     
    if "false"  in x:
       dfAnonym.at[i,"properties.blacklist"]=False   
       
dfAnonym['properties.bounce']=dfAnonym['properties.bounce'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.bounce"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.bounce"]='-'   
       
dfAnonym["properties.city"]=dfAnonym["properties.city"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.city"]
    if "nan" in x:
       dfAnonym.at[i,"properties.city"]='-'       
    if "Praha" in x:
       dfAnonym.at[i,"properties.city"]='Praha'     
      
dfAnonym["properties.customer_type"]=dfAnonym["properties.customer_type"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.customer_type"]
    if "nan" in x:
        dfAnonym.at[i,"properties.customer_type"]='normal'

dfAnonym["properties.december2016_purchases"]=dfAnonym["properties.december2016_purchases"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.december2016_purchases"]
    if "nan" in x:
        dfAnonym.at[i,"properties.december2016_purchases"]='0'
        
dfAnonym['properties.dluhopisy']=dfAnonym['properties.dluhopisy'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.dluhopisy"]
    if "true" in x:
       dfAnonym.at[i,"properties.dluhopisy"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.dluhopisy"]=False       

dfAnonym['properties.domu_nevyzvednuto']=dfAnonym['properties.domu_nevyzvednuto'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.domu_nevyzvednuto"]
    if "true" in x:
       dfAnonym.at[i,"properties.domu_nevyzvednuto"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.domu_nevyzvednuto"]=False 

dfAnonym['properties.domu_vraceno']=dfAnonym['properties.domu_vraceno'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.domu_vraceno"]
    if "true" in x:
       dfAnonym.at[i,"properties.domu_vraceno"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.domu_vraceno"]=False

dfAnonym['properties.email_domain']=dfAnonym['properties.email_domain'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.email_domain"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.email_domain"]='-'

dfAnonym["properties.first_source"]=dfAnonym["properties.first_source"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.first_source"]
    if "nan" in x:
        dfAnonym.at[i,"properties.first_source"]='-' 
        
        
dfAnonym["properties.gender"]=dfAnonym["properties.gender"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.gender"]
    if "nan" in x:
        dfAnonym.at[i,"properties.gender"]='-'   
    if "1" in x:
        dfAnonym.at[i,"properties.gender"]='-'    
    if "2" in x:
        dfAnonym.at[i,"properties.gender"]='-'         

dfAnonym["properties.import"]=dfAnonym["properties.import"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.import"]
    if "nan" in x:
        dfAnonym.at[i,"properties.import"]='-' 

dfAnonym["properties.imported"]=dfAnonym["properties.imported"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.imported"]
    if "1.0" in x:
       dfAnonym.at[i,"properties.imported"]=True  
    if "nan" in x:
        dfAnonym.at[i,"properties.imported"]=False    
        
dfAnonym["properties.in_session"]=dfAnonym["properties.in_session"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.in_session"]
    if "20171023_1913" in x:
       dfAnonym.at[i,"properties.in_session"]=True  
    if "nan" in x:
        dfAnonym.at[i,"properties.in_session"]=False        

dfAnonym['properties.koupili hero']=dfAnonym['properties.koupili hero'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.koupili hero"]
    if "true" in x:
       dfAnonym.at[i,"properties.koupili hero"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.koupili hero"]=False

dfAnonym["properties.last_order_at Christmas"]=dfAnonym["properties.last_order_at Christmas"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.last_order_at Christmas"]
    if "nan" in x:
        dfAnonym.at[i,"properties.last_order_at Christmas"]=False

dfAnonym['properties.last_order_at_feb-apr']=dfAnonym['properties.last_order_at_feb-apr'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.last_order_at_feb-apr"]
    if "true" in x:
       dfAnonym.at[i,"properties.last_order_at_feb-apr"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.last_order_at_feb-apr"]=False

dfAnonym['properties.model_no order']=dfAnonym['properties.model_no order'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.model_no order"]
    if "true" in x:
       dfAnonym.at[i,"properties.model_no order"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.model_no order"]=False

dfAnonym['properties.model_order']=dfAnonym['properties.model_order'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.model_order"]
    if "true" in x:
       dfAnonym.at[i,"properties.model_order"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.model_order"]=False
       
dfAnonym['properties.model_purchase']=dfAnonym['properties.model_purchase'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.model_purchase"]
    if "true" in x:
       dfAnonym.at[i,"properties.model_purchase"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.model_purchase"]=False       
 
dfAnonym['properties.name_date']=dfAnonym['properties.name_date'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.name_date"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.name_date"]='-'
      
dfAnonym['properties.not_finished_order_before_6m']=dfAnonym['properties.not_finished_order_before_6m'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.not_finished_order_before_6m"]
    if "true" in x:
       dfAnonym.at[i,"properties.not_finished_order_before_6m"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.not_finished_order_before_6m"]=False

dfAnonym['properties.objednali hero']=dfAnonym['properties.objednali hero'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.objednali hero"]
    if "true" in x:
       dfAnonym.at[i,"properties.objednali hero"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.objednali hero"]=False

dfAnonym["properties.optin_type"]=dfAnonym["properties.optin_type"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.optin_type"]
    if "nan" in x:
       dfAnonym.at[i,"properties.optin_type"]="None"  

   
dfAnonym["properties.order_brands"]=dfAnonym["properties.order_brands"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.order_brands"]
    if "nan" in x:
       dfAnonym.at[i,"properties.order_brands"]="None" 

dfAnonym['properties.otevreli hero']=dfAnonym['properties.otevreli hero'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.otevreli hero"]
    if "true" in x:
       dfAnonym.at[i,"properties.otevreli hero"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.otevreli hero"]=False

dfAnonym['properties.over1000_4mesice']=dfAnonym['properties.over1000_4mesice'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.over1000_4mesice"]
    if "True" in x:
       dfAnonym.at[i,"properties.over1000_4mesice"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.over1000_4mesice"]=False

#pozor
dfAnonym["properties.period"]=dfAnonym["properties.period"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.period"]
    if "nan" in x:
       dfAnonym.at[i,"properties.period"]="-"

dfAnonym['properties.photo_no order']=dfAnonym['properties.photo_no order'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.photo_no order"]
    if "true" in x:
       dfAnonym.at[i,"properties.photo_no order"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.photo_no order"]=False 

dfAnonym['properties.photo_order']=dfAnonym['properties.photo_order'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.photo_order"]
    if "true" in x:
       dfAnonym.at[i,"properties.photo_order"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.photo_order"]=False
       
dfAnonym['properties.photo_purchase']=dfAnonym['properties.photo_purchase'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.photo_purchase"]
    if "true" in x:
       dfAnonym.at[i,"properties.photo_purchase"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.photo_purchase"]=False       


dfAnonym["properties.plussizer"]=dfAnonym["properties.plussizer"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.plussizer"]
    if "nan" in x:
       dfAnonym.at[i,"properties.plussizer"]="-"

dfAnonym['properties.postal_code']=dfAnonym['properties.postal_code'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.postal_code"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.postal_code"]='-'
       
dfAnonym['properties.premium_customers(3andmore_items_2015-2016)']=dfAnonym['properties.premium_customers(3andmore_items_2015-2016)'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.premium_customers(3andmore_items_2015-2016)"]
    if "true" in x:
       dfAnonym.at[i,"properties.premium_customers(3andmore_items_2015-2016)"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.premium_customers(3andmore_items_2015-2016)"]=False        

dfAnonym["properties.region"]=dfAnonym["properties.region"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.region"]
    if "nan" in x:
       dfAnonym.at[i,"properties.region"]="-"

dfAnonym["properties.reminder"]=dfAnonym["properties.reminder"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.reminder"]
    if "1" in x:
       dfAnonym.at[i,"properties.reminder"]=True  
    if "nan" in x:
        dfAnonym.at[i,"properties.reminder"]=False
    if "0" in x:
        dfAnonym.at[i,"properties.reminder"]=False
    if "-" in x:
        dfAnonym.at[i,"properties.reminder"]=False 
        
dfAnonym['properties.resubscribe10nov2017']=dfAnonym['properties.resubscribe10nov2017'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.resubscribe10nov2017"]
    if "true" in x:
       dfAnonym.at[i,"properties.resubscribe10nov2017"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.resubscribe10nov2017"]=False 

dfAnonym['properties.revenue_12m_actual']=dfAnonym['properties.revenue_12m_actual'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.revenue_12m_actual"]
    if "-" in x:
       dfAnonym.at[i,"properties.revenue_12m_actual"]='0'         
    if "nan" in x:
       dfAnonym.at[i,"properties.revenue_12m_actual"]='0'


dfAnonym['properties.revenue_12m_day_before']=dfAnonym['properties.revenue_12m_day_before'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.revenue_12m_day_before"]
    if "-" in x:
       dfAnonym.at[i,"properties.revenue_12m_day_before"]='0'         
    if "nan" in x:
       dfAnonym.at[i,"properties.revenue_12m_day_before"]='0'

dfAnonym["properties.salecode_hunter"]=dfAnonym["properties.salecode_hunter"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.salecode_hunter"]
    if "nan" in x:
        dfAnonym.at[i,"properties.salecode_hunter"]='-'

dfAnonym["properties.salehunter"]=dfAnonym["properties.salehunter"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.salehunter"]
    if "nan" in x:
        dfAnonym.at[i,"properties.salehunter"]='-'    
    
dfAnonym["properties.second_order_month"]=dfAnonym["properties.second_order_month"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.second_order_month"]
    if "nan" in x:
        dfAnonym.at[i,"properties.second_order_month"]='-' 

dfAnonym["properties.segment"]=dfAnonym["properties.segment"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.segment"]
    if "nan" in x:
        dfAnonym.at[i,"properties.segment"]='-'        

dfAnonym["properties.source"]=dfAnonym["properties.source"].astype('str')  
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.source"]
    if "nan" in x:
        dfAnonym.at[i,"properties.source"]='-'

dfAnonym['properties.synkac']=dfAnonym['properties.synkac'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.synkac"]
    if "true" in x:
       dfAnonym.at[i,"properties.synkac"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.synkac"]=False
       
dfAnonym['properties.synkac_2']=dfAnonym['properties.synkac_2'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.synkac_2"]
    if "true" in x:
       dfAnonym.at[i,"properties.synkac_2"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.synkac_2"]=False       
       
dfAnonym['properties.synkac_3']=dfAnonym['properties.synkac_3'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.synkac_3"]
    if "true" in x:
       dfAnonym.at[i,"properties.synkac_3"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.synkac_3"]=False             

dfAnonym['properties.tester']=dfAnonym['properties.tester'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.tester"]
    if "True"  in x:
       dfAnonym.at[i,"properties.tester"]=True  
    if "nan"  in x:
       dfAnonym.at[i,"properties.tester"]=False      
    if "False"  in x:
       dfAnonym.at[i,"properties.tester"]=False  

dfAnonym['properties.top 100']=dfAnonym['properties.top 100'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.top 100"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.top 100"]=False      
    if "-"  in x:
       dfAnonym.at[i,"properties.top 100"]=False  
    if "1"  in x:
       dfAnonym.at[i,"properties.top 100"]=True  

dfAnonym['properties.type_customer']=dfAnonym['properties.type_customer'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.type_customer"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.type_customer"]='normal'      
    
       
dfAnonym['properties.unsubscribed']=dfAnonym['properties.unsubscribed'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.unsubscribed"]
    if "true" in x:
       dfAnonym.at[i,"properties.unsubscribed"]=True    
    if "True" in x:
       dfAnonym.at[i,"properties.unsubscribed"]=True    
    if "False"  in x:
       dfAnonym.at[i,"properties.unsubscribed"]=False   
    if "false"  in x:
       dfAnonym.at[i,"properties.unsubscribed"]=False  
    if "nan"  in x:
       dfAnonym.at[i,"properties.unsubscribed"]=False 

dfAnonym['properties.unsubscribed_from_daily_nl']=dfAnonym['properties.unsubscribed_from_daily_nl'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]
    if "true" in x:
       dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]=True    
    if "True" in x:
       dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]=True    
    if "False"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]=False   
    if "false"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]=False  
    if "nan"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_daily_nl"]=False
       
dfAnonym['properties.unsubscribed_from_weekly_nl']=dfAnonym['properties.unsubscribed_from_weekly_nl'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]
    if "true" in x:
       dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]=True    
    if "True" in x:
       dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]=True    
    if "False"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]=False   
    if "false"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]=False  
    if "nan"  in x:
       dfAnonym.at[i,"properties.unsubscribed_from_weekly_nl"]=False       

dfAnonym['properties.vydejna_nevyzvednuto']=dfAnonym['properties.vydejna_nevyzvednuto'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.vydejna_nevyzvednuto"]
    if "true" in x:
       dfAnonym.at[i,"properties.vydejna_nevyzvednuto"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.vydejna_nevyzvednuto"]=False
       
dfAnonym['properties.vydejna_vraceno']=dfAnonym['properties.vydejna_vraceno'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.vydejna_vraceno"]
    if "true" in x:
       dfAnonym.at[i,"properties.vydejna_vraceno"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.vydejna_vraceno"]=False

dfAnonym['properties.zpozdene']=dfAnonym['properties.zpozdene'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.zpozdene"]
    if "true" in x:
       dfAnonym.at[i,"properties.zpozdene"]=True         
    if "nan"  in x:
       dfAnonym.at[i,"properties.zpozdene"]=False
       
dfAnonym['properties.last_order_quantity']=dfAnonym['properties.last_order_quantity'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.last_order_quantity"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.last_order_quantity"]='0'
 


"""
#pokus o upravu ale vela chybajucich hodnot 
dfAnonym=dfAnonym[dfAnonym['properties.customer_profit_12m'] != 'nan']
      
       

dfAnonym['properties.customer_profit'].describe()
dfAnonym['properties.customer_profit'].mean()

dfAnonym["properties.customer_profit"].value_counts()#suma
dfAnonym['properties.customer_profit']=dfAnonym['properties.customer_profit'].astype('str')       
for i in dfAnonym.index:
    x=dfAnonym.at[i,"properties.customer_profit"]
    if "nan"  in x:
       dfAnonym.at[i,"properties.customer_profit"]='0'
    if "-"  in x:
       dfAnonym.at[i,"properties.customer_profit"]='0' 
dfAnonym['properties.customer_profit']=pd.to_numeric(dfAnonym['properties.customer_profit'])       
       
       
dfAnonym['properties.customer_profit'].fillna("0", inplace=True) 
      


dfAnonym['binned'] = pd.cut(dfAnonym['properties.customer_profit'], 100)
dfAnonym['binned'].head()

for i in dfAnonym.index:
    dfAnonym.at[i,"properties.customer_profit"]=dfAnonym.at[i,'binned'].mid
dfAnonym["properties.customer_profit"].hist()

import datetime
dfAnonym["properties.lastpurchaseat"].value_counts()#datum
dfAnonym["properties.lastpurchaseat"].head()


dfAnonym=dfAnonym[dfAnonym['properties.lastpurchaseat'] != '-']
dfAnonym["properties.lastpurchaseat"]=pd.to_datetime(dfAnonym["properties.lastpurchaseat"], format="%Y-%m-%d")
DATE=dfAnonym.at[1,"properties.lastpurchaseat"]
dfAnonym.at[1,"properties.lastpurchaseat"]=str(DATE.year)+"/"+str(DATE.month)


for i in dfAnonym.index:
   DATE=dfAnonym.at[i,"properties.lastpurchaseat"]
   dfAnonym.at[i,"properties.lastpurchaseatDATE"]=str(DATE.year)+"/"+str(DATE.month)


dfAnonym['properties.lastpurchaseat']=dfAnonym['properties.lastpurchaseat'].astype('str')       

for i in dfAnonym.index:
    dfAnonym.at[i,"properties.lastpurchaseat"]=dfAnonym.at[i,"properties.lastpurchaseatDATE"]

"""

#mazanie vybranych att
dfAnonym=dfAnonym.drop('properties.CB2000', axis = 1)
dfAnonym=dfAnonym.drop('properties.Varka', axis = 1)
dfAnonym=dfAnonym.drop('properties._timestamp', axis = 1)
dfAnonym=dfAnonym.drop('properties.ab_split', axis = 1)
dfAnonym=dfAnonym.drop('properties.affinity_brand', axis = 1)
dfAnonym=dfAnonym.drop('properties.affinity_category', axis = 1)
dfAnonym=dfAnonym.drop('properties.affinity_premium', axis = 1)
dfAnonym=dfAnonym.drop('properties.all_brands', axis = 1)
dfAnonym=dfAnonym.drop('properties.browser_push_notification.endpoint', axis = 1)
dfAnonym=dfAnonym.drop('properties.browser_push_notification.keys.auth', axis = 1)
dfAnonym=dfAnonym.drop('properties.browser_push_notification.subscriptionId', axis = 1)
dfAnonym=dfAnonym.drop('properties.churn_date', axis = 1)
dfAnonym=dfAnonym.drop('properties.code', axis = 1)
dfAnonym=dfAnonym.drop('properties.code_last_order_at_Xmas', axis = 1)
dfAnonym=dfAnonym.drop('properties.country', axis = 1)
dfAnonym=dfAnonym.drop('properties.date_modified', axis = 1)
dfAnonym=dfAnonym.drop('properties.first_name_disp', axis = 1)
dfAnonym=dfAnonym.drop('properties.ga_clid', axis = 1)
dfAnonym=dfAnonym.drop('properties.lastpurchaseat', axis = 1)
dfAnonym=dfAnonym.drop('properties.name_date_code', axis = 1)
dfAnonym=dfAnonym.drop('properties.name_date_code_end', axis = 1)
dfAnonym=dfAnonym.drop('properties.newsletters', axis = 1)
dfAnonym=dfAnonym.drop('properties.nextorderat', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_14D', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_1D', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_20m', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_30m', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_5m', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_60m', axis = 1)
dfAnonym=dfAnonym.drop('properties.order_1M_7D', axis = 1)
dfAnonym=dfAnonym.drop('properties.pickup_places', axis = 1)
dfAnonym=dfAnonym.drop('properties.pickup_purchases', axis = 1)
dfAnonym=dfAnonym.drop('properties.registered_at', axis = 1)
dfAnonym=dfAnonym.drop('properties.sale_code_dec2016', axis = 1)
dfAnonym=dfAnonym.drop('properties.stylistka_deals', axis = 1)
dfAnonym=dfAnonym.drop('properties.subscriber_created_at', axis = 1)
dfAnonym=dfAnonym.drop('properties.temp_utm_campaign', axis = 1)
dfAnonym=dfAnonym.drop('properties.temp_utm_content', axis = 1)
dfAnonym=dfAnonym.drop('properties.temp_utm_medium', axis = 1)
dfAnonym=dfAnonym.drop('properties.temp_utm_source', axis = 1)
dfAnonym=dfAnonym.drop('properties.temp_utm_term', axis = 1)
dfAnonym=dfAnonym.drop('properties.testing', axis = 1)
dfAnonym=dfAnonym.drop('properties.top_4_brands', axis = 1)
dfAnonym=dfAnonym.drop('properties.top_4_brands_json', axis = 1)
dfAnonym=dfAnonym.drop('properties.top_4_categories_json', axis = 1)
dfAnonym=dfAnonym.drop('properties.unsubscribe_link', axis = 1)
dfAnonym=dfAnonym.drop('properties.unsubscribe_link_profile', axis = 1)
dfAnonym=dfAnonym.drop('properties.voucher_first_order', axis = 1)
dfAnonym=dfAnonym.drop('properties.unsubscribed_from', axis = 1)

dfAnonym=dfAnonym.drop('_id', axis = 1)


#transformacia na bool

dfAnonym['properties.17-12-12_SMS_Christmas']=dfAnonym['properties.17-12-12_SMS_Christmas'].astype('int')      
 
dfAnonym['properties.18-03-21_SMS']=dfAnonym['properties.18-03-21_SMS'].astype('int')    

dfAnonym['properties.18-05-07_SMS_Andel']=dfAnonym['properties.18-05-07_SMS_Andel'].astype('int')      

dfAnonym['properties.18-05-07_SMS_Narodka']=dfAnonym['properties.18-05-07_SMS_Narodka'].astype('int')

dfAnonym['properties.Black_Friday_vyzva']=dfAnonym['properties.Black_Friday_vyzva'].astype('int')

dfAnonym['properties.Black_Friday_vyzva_v2']=dfAnonym['properties.Black_Friday_vyzva_v2'].astype('int')     

dfAnonym['properties.Bounce']=dfAnonym['properties.Bounce'].astype('int')       

dfAnonym['properties.Brazilie_final']=dfAnonym['properties.Brazilie_final'].astype('int')       

dfAnonym['properties.Dluhopisy_Osobni']=dfAnonym['properties.Dluhopisy_Osobni'].astype('int')       

dfAnonym['properties.Dluhopisy_Osobni_2']=dfAnonym['properties.Dluhopisy_Osobni_2'].astype('int')       
       
dfAnonym['properties.Home address']=dfAnonym['properties.Home address'].astype('int')      
       
dfAnonym['properties.Nakupili ZOOT Original']=dfAnonym['properties.Nakupili ZOOT Original'].astype('int')       
       
dfAnonym['properties.Nezaplatili/Nevyzdvihli objednavku do zari']=dfAnonym['properties.Nezaplatili/Nevyzdvihli objednavku do zari'].astype('int') 
       
dfAnonym['properties.Pres_1000']=dfAnonym['properties.Pres_1000'].astype('int')  

dfAnonym['properties.RO-SK customer ZOOT ORIGINAL']=dfAnonym['properties.RO-SK customer ZOOT ORIGINAL'].astype('int')   
    
dfAnonym['properties.Reaktivace_Srpen_Reminder']=dfAnonym['properties.Reaktivace_Srpen_Reminder'].astype('int')    
       
dfAnonym['properties.Registrovali sa ale nespravili objednavku do zari']=dfAnonym['properties.Registrovali sa ale nespravili objednavku do zari'].astype('int')     

dfAnonym['properties.SMS_BF_1']=dfAnonym['properties.SMS_BF_1'].astype('int')     

dfAnonym['properties.SMS_Vanoce_2']=dfAnonym['properties.SMS_Vanoce_2'].astype('int')    

dfAnonym['properties.SMS_Vanoce_3']=dfAnonym['properties.SMS_Vanoce_3'].astype('int')      

dfAnonym['properties.SMS_Vanoce_3_Oprava']=dfAnonym['properties.SMS_Vanoce_3_Oprava'].astype('int')     

dfAnonym['properties.VIP_Praha']=dfAnonym['properties.VIP_Praha'].astype('int')     

dfAnonym['properties.VIP_mimoPrahu']=dfAnonym['properties.VIP_mimoPrahu'].astype('int')   

dfAnonym['properties.Varka_patek']=dfAnonym['properties.Varka_patek'].astype('int')     
       
dfAnonym['properties.Women buing mens products in 15%']=dfAnonym['properties.Women buing mens products in 15%'].astype('int')    
      
dfAnonym['properties.blacklist']=dfAnonym['properties.blacklist'].astype('int')      
      
dfAnonym['properties.dluhopisy']=dfAnonym['properties.dluhopisy'].astype('int')   

dfAnonym['properties.domu_nevyzvednuto']=dfAnonym['properties.domu_nevyzvednuto'].astype('int')   

dfAnonym['properties.domu_vraceno']=dfAnonym['properties.domu_vraceno'].astype('int')   

dfAnonym['properties.koupili hero']=dfAnonym['properties.koupili hero'].astype('int')     

dfAnonym['properties.last_order_at_feb-apr']=dfAnonym['properties.last_order_at_feb-apr'].astype('int')      

dfAnonym['properties.model_no order']=dfAnonym['properties.model_no order'].astype('int')       

dfAnonym['properties.model_order']=dfAnonym['properties.model_order'].astype('int')       
       
dfAnonym['properties.model_purchase']=dfAnonym['properties.model_purchase'].astype('int')      
       
dfAnonym['properties.not_finished_order_before_6m']=dfAnonym['properties.not_finished_order_before_6m'].astype('int')      

dfAnonym['properties.objednali hero']=dfAnonym['properties.objednali hero'].astype('int') 

dfAnonym['properties.otevreli hero']=dfAnonym['properties.otevreli hero'].astype('int')

dfAnonym['properties.over1000_4mesice']=dfAnonym['properties.over1000_4mesice'].astype('int')      

dfAnonym['properties.photo_no order']=dfAnonym['properties.photo_no order'].astype('int')      
 
dfAnonym['properties.photo_order']=dfAnonym['properties.photo_order'].astype('int')   

dfAnonym['properties.photo_purchase']=dfAnonym['properties.photo_purchase'].astype('int')    
       
dfAnonym['properties.premium_customers(3andmore_items_2015-2016)']=dfAnonym['properties.premium_customers(3andmore_items_2015-2016)'].astype('int')   
       
dfAnonym['properties.resubscribe10nov2017']=dfAnonym['properties.resubscribe10nov2017'].astype('int')

dfAnonym['properties.synkac']=dfAnonym['properties.synkac'].astype('int')

dfAnonym['properties.synkac_2']=dfAnonym['properties.synkac_2'].astype('int')      

dfAnonym['properties.synkac_3']=dfAnonym['properties.synkac_3'].astype('int') 
       
dfAnonym['properties.unsubscribed']=dfAnonym['properties.unsubscribed'].astype('int')  

dfAnonym['properties.unsubscribed_from_daily_nl']=dfAnonym['properties.unsubscribed_from_daily_nl'].astype('int')      
      
dfAnonym['properties.unsubscribed_from_weekly_nl']=dfAnonym['properties.unsubscribed_from_weekly_nl'].astype('int')       
       
dfAnonym['properties.vydejna_vraceno']=dfAnonym['properties.vydejna_vraceno'].astype('int')      

dfAnonym['properties.imported']=dfAnonym['properties.imported'].astype('int')   

dfAnonym['properties.in_session']=dfAnonym['properties.in_session'].astype('int')   

dfAnonym['properties.reminder']=dfAnonym['properties.reminder'].astype('int')   

dfAnonym['properties.top 100']=dfAnonym['properties.top 100'].astype('int')   

dfAnonym['properties.vydejna_nevyzvednuto']=dfAnonym['properties.vydejna_nevyzvednuto'].astype('int')   

dfAnonym['properties.tester']=dfAnonym['properties.tester'].astype('int')   

dfAnonym["properties.zpozdene"]=dfAnonym["properties.zpozdene"].astype('int')
  


#vypocet korelacie 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


numericCollumns=dfColumn.select_dtypes(include=['int'])


numericCollumns.corr()

import matplotlib.pyplot as plt

plt.matshow(numericCollumns.corr())




import seaborn as sns
corr = numericCollumns.corr()
sns.heatmap(corr, 
            xticklabels=corr.columns.values,
            yticklabels=corr.columns.values)


import pandas as pd
import numpy as np

rs = np.random.RandomState(0)
corr = numericCollumns.corr()
corr.style.background_gradient()
numericCollumns.corr().unstack().sort_values().drop_duplicates()


#dfColumn.to_pickle('dfColumn.pkl') 
dfColumn.to_pickle('dfColumn800.pkl')
dfColumn = pd.read_pickle('dfColumn.pkl')


#spojenie DF
dfCampaign['id']=dfCampaign['data.customer_id']

dfJoin=pd.merge(dfAnonym,dfCampaign, on='id')
dfColumn

dfJoin['data.properties.status']=dfJoin['data.properties.status'].astype('str')
dfColumn["data.properties.status"].value_counts()

dfJoin=dfJoin[dfJoin['data.properties.status'] != 'nan']


dfColumn=dfJoin.dropna(axis='columns')
dfRow=dfJoin.dropna(axis='rows')

a=list(dfColumn)
del a[-8]


#transformacia

from sklearn.preprocessing import LabelEncoder
lb_make = LabelEncoder()
dfColumn['NDay'] = lb_make.fit_transform(dfColumn['NDay'])
dfColumn['Month'] = lb_make.fit_transform(dfColumn['Month'])
dfColumn['data.properties.status'] = lb_make.fit_transform(dfColumn['data.properties.status'])

#test
dfColumn['Time'] = lb_make.fit_transform(dfColumn['Time'])



print (dfColumn.loc[121,:])
dfColumn['Time'].value_counts()

dfColumn['data.properties.status']=dfJoin['data.properties.status']





#modelovanie


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB, BernoulliNB, MultinomialNB

# Importing dataset

#dfColumn = dfColumn.drop('data.properties.status', axis=1)  
dfColumn = dfColumn.drop('type', axis=1)
dfColumn = dfColumn.drop('data.type', axis=1)
#dfColumn = dfColumn.drop('Time', axis=1)
dfColumn = dfColumn.drop('id', axis=1)

#testovat
dfColumn = dfColumn.drop('properties.city', axis=1)
dfColumn = dfColumn.drop('properties.email_domain', axis=1)
dfColumn = dfColumn.drop('properties.name_date', axis=1)
dfColumn = dfColumn.drop('properties.region', axis=1)
dfColumn = dfColumn.drop('properties.segment', axis=1)


dfColumn = dfColumn.drop('data.customer_id', axis=1)
dfColumn = dfColumn.drop('data.project_id', axis=1)
dfColumn = dfColumn.drop('data.properties.campaign_id', axis=1)
dfColumn = dfColumn.drop('properties.postal_code', axis=1)
dfColumn = dfColumn.drop('data.properties.action_type', axis=1)
dfColumn = dfColumn.drop('properties.name_date', axis=1)

'''#Test
dfColumn = dfColumn.drop('properties.type_customer', axis=1)
dfColumn = dfColumn.drop('properties.zpozdene', axis=1)
dfColumn = dfColumn.drop('data.project_id', axis=1)
dfColumn = dfColumn.drop('data.customer_id', axis=1)
dfColumn = dfColumn.drop('data.properties.action_type', axis=1)
dfColumn = dfColumn.drop('data.properties.campaign_id', axis=1)
dfColumn = dfColumn.drop('data.properties.message', axis=1)
dfColumn = dfColumn.drop('properties.postal_code', axis=1)
dfColumn = dfColumn.drop('properties.december2016_purchases', axis=1)
dfColumn = dfColumn.drop('properties.name_date', axis=1)
'''


# Convert categorical variable to numeric
dfColumn["properties.RMK_reco_split"]=np.where(dfColumn["properties.RMK_reco_split"]=="-",0, 
        np.where(dfColumn["properties.RMK_reco_split"]=="RMK-reco-CG",1,2))
dfColumn["properties.SMS_BF2_Unique"]=np.where(dfColumn["properties.SMS_BF2_Unique"]=="-",0, 
        np.where(dfColumn["properties.SMS_BF2_Unique"]=="Active",1,2))
dfColumn["properties.bounce"]=np.where(dfColumn["properties.bounce"]=="-",0, 
        np.where(dfColumn["properties.bounce"]=="block",1,2))
dfColumn["properties.customer_type"]=np.where(dfColumn["properties.customer_type"]=="normal",0,1)
dfColumn["properties.first_source"]=np.where(dfColumn["properties.first_source"]=="-",0, 
        np.where(dfColumn["properties.first_source"]=="settings",1,       
                 np.where(dfColumn["properties.first_source"]=="order",2,   
                          np.where(dfColumn["properties.first_source"]=="registration",3,   
                                   np.where(dfColumn["properties.first_source"]=="homepage basic box",4,   
                                            np.where(dfColumn["properties.first_source"]=="newsletter box",5,6))))))   
dfColumn["properties.gender"]=np.where(dfColumn["properties.gender"]=="-",0, 
        np.where(dfColumn["properties.gender"]=="male",1,2))
dfColumn["properties.import"]=np.where(dfColumn["properties.import"]=="-",0, 
        np.where(dfColumn["properties.import"]=="sk_mailing",1,
                 np.where(dfColumn["properties.import"]=="bezZeme",2,3)))
dfColumn["properties.last_order_at Christmas"]=np.where(dfColumn["properties.last_order_at Christmas"]=="False",0, 
        np.where(dfColumn["properties.last_order_at Christmas"]=="true",1,2))
dfColumn["properties.optin_type"]=np.where(dfColumn["properties.optin_type"]=="None",0, 
        np.where(dfColumn["properties.optin_type"]=="Daily",1,       
                 np.where(dfColumn["properties.optin_type"]=="Weekly",2,   
                          np.where(dfColumn["properties.optin_type"]=="Daily|WeekEnd|Special",3,   
                                   np.where(dfColumn["properties.optin_type"]=="Weekly|WeekEnd|Special",4,   
                                            np.where(dfColumn["properties.optin_type"]=="Weekly|WeekEnd",5,6))))))   
dfColumn["properties.order_brands"]=np.where(dfColumn["properties.order_brands"]=="None",0,1)
dfColumn["properties.period"]=np.where(dfColumn["properties.period"]=="-",0, 
        np.where(dfColumn["properties.period"]=="2quarter",1,       
                 np.where(dfColumn["properties.period"]=="1quarter",2,   
                          np.where(dfColumn["properties.period"]=="2month",3,   
                                   np.where(dfColumn["properties.period"]=="1year",4,   
                                            np.where(dfColumn["properties.period"]=="1month",5, 
                                                np.where(dfColumn["properties.period"]=="2week",6,7))))))) 
dfColumn["properties.plussizer"]=np.where(dfColumn["properties.plussizer"]=="-",0, 
        np.where(dfColumn["properties.plussizer"]=="No",1,       
                 np.where(dfColumn["properties.plussizer"]=="Small",2,   
                          np.where(dfColumn["properties.plussizer"]=="Middle",3,   
                                   np.where(dfColumn["properties.plussizer"]=="Full",4,5)))))
dfColumn["properties.salecode_hunter"]=np.where(dfColumn["properties.salecode_hunter"]=="-",0, 
        np.where(dfColumn["properties.salecode_hunter"]=="No",1,       
                 np.where(dfColumn["properties.salecode_hunter"]=="Middle",2,   
                          np.where(dfColumn["properties.salecode_hunter"]=="Small",3,   
                                   np.where(dfColumn["properties.salecode_hunter"]=="Big",4,5)))))
dfColumn["properties.salehunter"]=np.where(dfColumn["properties.salehunter"]=="-",0, 
        np.where(dfColumn["properties.salehunter"]=="No",1,       
                 np.where(dfColumn["properties.salehunter"]=="Middle",2,   
                          np.where(dfColumn["properties.salehunter"]=="Small",3,   
                                   np.where(dfColumn["properties.salehunter"]=="Big",4,5)))))
dfColumn["properties.second_order_month"]=np.where(dfColumn["properties.second_order_month"]=="-",0, 
        np.where(dfColumn["properties.second_order_month"]=="<=1",1,       
                 np.where(dfColumn["properties.second_order_month"]=="over 12",2,   
                          np.where(dfColumn["properties.second_order_month"]=="1-3",3,   
                                   np.where(dfColumn["properties.second_order_month"]=="3-6",4,   
                                            np.where(dfColumn["properties.second_order_month"]=="6-9",5,6))))))
dfColumn["properties.source"]=np.where(dfColumn["properties.source"]=="-",0, 
        np.where(dfColumn["properties.source"]=="order",1,       
                 np.where(dfColumn["properties.source"]=="waitlist",2,   
                          np.where(dfColumn["properties.source"]=="registration",3,   
                                   np.where(dfColumn["properties.source"]=="homepage basic box",4,   
                                            np.where(dfColumn["properties.source"]=="settings",5, 
                                                np.where(dfColumn["properties.source"]=="newsletter box",6,
                                                     np.where(dfColumn["properties.source"]=="homepage newsletter",7, 
                                                              np.where(dfColumn["properties.source"]=="magazine",8, 
                                                                       np.where(dfColumn["properties.source"]=="homepage",9,10)))))))))) 
dfColumn["properties.type_customer"]=np.where(dfColumn["properties.type_customer"]=="normal",0, 
        np.where(dfColumn["properties.type_customer"]=="hodnenakupuju",1,       
                 np.where(dfColumn["properties.type_customer"]=="prvonakupci",2,   
                          np.where(dfColumn["properties.type_customer"]=="vip",3,4  )))) 

dfColumn["data.properties.status"].value_counts()
dfColumn["data.properties.message"]=np.where(dfColumn["data.properties.message"]=="OK",0, 
        np.where(dfColumn["data.properties.message"]=="This customer is unsubscribed from all communication.",1,       
                 np.where(dfColumn["data.properties.message"]=="Failed sending e-mail",2,   
                          np.where(dfColumn["data.properties.message"]=="This customer is unsubscribed from S novou kolekcí.",3,   
                                   np.where(dfColumn["data.properties.message"]=="Invalid recipient",4,   
                                            np.where(dfColumn["data.properties.message"]=="The policy only allows 1 message per 1 day",5,6))))))
                                        

dfColumn["data.properties.status"]=np.where(dfColumn["data.properties.status"]=="enqueued",0, 
        np.where(dfColumn["data.properties.status"]=="delivered",1,       
                 np.where(dfColumn["data.properties.status"]=="enqueue_failed",2,   
                          np.where(dfColumn["data.properties.status"]=="opened",3,   
                                   np.where(dfColumn["data.properties.status"]=="clicked",4,   
                                            np.where(dfColumn["data.properties.status"]=="dropped",5,
                                               np.where(dfColumn["data.properties.status"]=="sent",6,7)))))))


dfColumn["data.properties.status"]=np.where(dfColumn["data.properties.status"]=="NonOpen",0,1)

dfColumn=dfColumn[dfColumn['properties.december2016_purchases'] != '-']


# Split dataset in training and test datasets
X_train, X_test = train_test_split(dfColumn, test_size=0.8, random_state=int(time.time()))

#predict day
gnb = GaussianNB()

used_features =['properties.17-12-12_SMS_Christmas',
 'properties.18-03-21_SMS',
 'properties.18-05-07_SMS_Andel',
 'properties.18-05-07_SMS_Narodka',
 'properties.Black_Friday_vyzva',
 'properties.Black_Friday_vyzva_v2',
 'properties.Bounce',
 'properties.Brazilie_final',
 'properties.Dluhopisy_Osobni',
 'properties.Dluhopisy_Osobni_2',
 'properties.Home address',
 'properties.Nakupili ZOOT Original',
 'properties.Nezaplatili/Nevyzdvihli objednavku do zari',
 'properties.Pres_1000',
 'properties.RMK_reco_split',
 'properties.RO-SK customer ZOOT ORIGINAL',
 'properties.Reaktivace_Srpen_Reminder',
 'properties.Registrovali sa ale nespravili objednavku do zari',
 'properties.SMS_BF2_Unique',
 'properties.SMS_BF_1',
 'properties.SMS_Brazilie',
 'properties.SMS_Vanoce_2',
 'properties.SMS_Vanoce_3',
 'properties.SMS_Vanoce_3_Oprava',
 'properties.VIP_Praha',
 'properties.VIP_mimoPrahu',
 'properties.Varka_patek',
 'properties.Women buing mens products in 15%',
 'properties._cookies_count',
 'properties.blacklist',
 'properties.bounce',
 'properties.customer_type',
 'properties.december2016_purchases',
 'properties.dluhopisy',
 'properties.domu_nevyzvednuto',
 'properties.domu_vraceno',
 'properties.first_source',
 'properties.gender',
 'properties.import',
 'properties.imported',
 'properties.in_session',
 'properties.koupili hero',
 'properties.last_order_at Christmas',
 'properties.last_order_at_feb-apr',
 'properties.last_order_quantity',
 'properties.model_no order',
 'properties.model_order',
 'properties.model_purchase',
 'properties.not_finished_order_before_6m',
 'properties.objednali hero',
 'properties.optin_type',
 'properties.order_brands',
 'properties.otevreli hero',
 'properties.over1000_4mesice',
 'properties.period',
 'properties.photo_no order',
 'properties.photo_order',
 'properties.photo_purchase',
 'properties.plussizer',
 'properties.premium_customers(3andmore_items_2015-2016)',
 'properties.reminder',
 'properties.resubscribe10nov2017',
 'properties.revenue_12m_actual',
 'properties.revenue_12m_day_before',
 'properties.salecode_hunter',
 'properties.salehunter',
 'properties.second_order_month',
 'properties.source',
 'properties.synkac',
 'properties.synkac_2',
 'properties.synkac_3',
 'properties.tester',
 'properties.top 100',
 'properties.type_customer',
 'properties.unsubscribed',
 'properties.unsubscribed_from_daily_nl',
 'properties.unsubscribed_from_weekly_nl',
 'properties.vydejna_nevyzvednuto',
 'properties.vydejna_vraceno',
 'properties.zpozdene',
 'data.properties.action_id',
 'data.properties.message',
 'NDay',
 'Month',
 'Day',
 'Year',
 'Time']

gnb.fit(
    X_train[used_features].values,
    #X_train['data.properties.status']
    X_train['Time']
)

y_pred = gnb.predict(X_test[used_features])

print("Number of mislabeled points out of a total {} points : {}, performance {:05.2f}%"
      .format(
          X_test.shape[0],
          (X_test["Time"] != y_pred).sum(),
          100*(1-(X_test["Time"] != y_pred).sum()/X_test.shape[0])
))


#predict time
gnb = GaussianNB()

used_features =['properties.17-12-12_SMS_Christmas',
 'properties.18-03-21_SMS',
 'properties.18-05-07_SMS_Andel',
 'properties.18-05-07_SMS_Narodka',
 'properties.Black_Friday_vyzva',
 'properties.Black_Friday_vyzva_v2',
 'properties.Bounce',
 'properties.Brazilie_final',
 'properties.Dluhopisy_Osobni',
 'properties.Dluhopisy_Osobni_2',
 'properties.Home address',
 'properties.Nakupili ZOOT Original',
 'properties.Nezaplatili/Nevyzdvihli objednavku do zari',
 'properties.Pres_1000',
 'properties.RMK_reco_split',
 'properties.RO-SK customer ZOOT ORIGINAL',
 'properties.Reaktivace_Srpen_Reminder',
 'properties.Registrovali sa ale nespravili objednavku do zari',
 'properties.SMS_BF2_Unique',
 'properties.SMS_BF_1',
 'properties.SMS_Brazilie',
 'properties.SMS_Vanoce_2',
 'properties.SMS_Vanoce_3',
 'properties.SMS_Vanoce_3_Oprava',
 'properties.VIP_Praha',
 'properties.VIP_mimoPrahu',
 'properties.Varka_patek',
 'properties.Women buing mens products in 15%',
 'properties._cookies_count',
 'properties.blacklist',
 'properties.bounce',
 'properties.customer_type',
 'properties.december2016_purchases',
 'properties.dluhopisy',
 'properties.domu_nevyzvednuto',
 'properties.domu_vraceno',
 'properties.first_source',
 'properties.gender',
 'properties.import',
 'properties.imported',
 'properties.in_session',
 'properties.koupili hero',
 'properties.last_order_at Christmas',
 'properties.last_order_at_feb-apr',
 'properties.last_order_quantity',
 'properties.model_no order',
 'properties.model_order',
 'properties.model_purchase',
 'properties.not_finished_order_before_6m',
 'properties.objednali hero',
 'properties.optin_type',
 'properties.order_brands',
 'properties.otevreli hero',
 'properties.over1000_4mesice',
 'properties.period',
 'properties.photo_no order',
 'properties.photo_order',
 'properties.photo_purchase',
 'properties.plussizer',
 'properties.premium_customers(3andmore_items_2015-2016)',
 'properties.reminder',
 'properties.resubscribe10nov2017',
 'properties.revenue_12m_actual',
 'properties.revenue_12m_day_before',
 'properties.salecode_hunter',
 'properties.salehunter',
 'properties.second_order_month',
 'properties.source',
 'properties.synkac',
 'properties.synkac_2',
 'properties.synkac_3',
 'properties.tester',
 'properties.top 100',
 'properties.type_customer',
 'properties.unsubscribed',
 'properties.unsubscribed_from_daily_nl',
 'properties.unsubscribed_from_weekly_nl',
 'properties.vydejna_nevyzvednuto',
 'properties.vydejna_vraceno',
 'properties.zpozdene',
 'data.properties.action_id',
 'data.properties.message',
 'NDay',
 'Month',
 'Day',
 'Year',
 'Time']

gnb.fit(
    X_train[used_features].values,
    #X_train['data.properties.status']
    X_train['Time']
)

y_pred = gnb.predict(X_test[used_features])

print("Number of mislabeled points out of a total {} points : {}, performance {:05.2f}%"
      .format(
          X_test.shape[0],
          (X_test["Time"] != y_pred).sum(),
          100*(1-(X_test["Time"] != y_pred).sum()/X_test.shape[0])
))

mean_survival=np.mean(X_train["data.properties.status"])
mean_not_survival=1-mean_survival
print("Survival prob = {:03.2f}%, Not survival prob = {:03.2f}%"
.format(100*mean_survival,100*mean_not_survival))


         
list(dfColumn)


gnb = GaussianNB()


used_features=['properties.17-12-12_SMS_Christmas',
 'properties.18-03-21_SMS',
 'properties.18-05-07_SMS_Andel',
 'properties.18-05-07_SMS_Narodka',
 'properties.Black_Friday_vyzva',
 'properties.Black_Friday_vyzva_v2',
 'properties.Bounce',
 'properties.Brazilie_final',
 'properties.Dluhopisy_Osobni',
 'properties.Dluhopisy_Osobni_2',
 'properties.Home address',
 'properties.Nakupili ZOOT Original',
 'properties.Nezaplatili/Nevyzdvihli objednavku do zari',
 'properties.Pres_1000',
 'properties.RMK_reco_split',
 'properties.RO-SK customer ZOOT ORIGINAL',
 'properties.Reaktivace_Srpen_Reminder',
 'properties.Registrovali sa ale nespravili objednavku do zari',
 'properties.SMS_BF2_Unique',
 'properties.SMS_BF_1',
 'properties.SMS_Brazilie',
 'properties.SMS_Vanoce_2',
 'properties.SMS_Vanoce_3',
 'properties.SMS_Vanoce_3_Oprava',
 'properties.VIP_Praha',
 'properties.VIP_mimoPrahu',
 'properties.Varka_patek',
 'properties.Women buing mens products in 15%',
 'properties._cookies_count',
 'properties.blacklist',
 'properties.bounce',
 #'properties.city',
 'properties.customer_type',
 'properties.dluhopisy',
 'properties.domu_nevyzvednuto',
 'properties.domu_vraceno',
 #'properties.email_domain',
 'properties.first_source',
 'properties.gender',
 'properties.import',
 'properties.imported',
 'properties.in_session',
 'properties.koupili hero',
 'properties.last_order_at Christmas',
 'properties.last_order_at_feb-apr',
 'properties.last_order_quantity',
 'properties.model_no order',
 'properties.model_order',
 'properties.model_purchase',
 'properties.not_finished_order_before_6m',
 'properties.objednali hero',
 'properties.optin_type',
 'properties.order_brands',
 'properties.otevreli hero',
 'properties.over1000_4mesice',
 'properties.period',
 'properties.photo_no order',
 'properties.photo_order',
 'properties.photo_purchase',
 'properties.plussizer',
 #'properties.postal_code',
 'properties.premium_customers(3andmore_items_2015-2016)',
 #'properties.region',
 'properties.reminder',
 'properties.resubscribe10nov2017',
 'properties.revenue_12m_actual',
 'properties.revenue_12m_day_before',
 'properties.salecode_hunter',
 'properties.salehunter',
 'properties.second_order_month',
 #'properties.segment',
 'properties.source',
 'properties.synkac',
 'properties.synkac_2',
 'properties.synkac_3',
 'properties.tester',
 'properties.top 100',
 'properties.type_customer',
 'properties.unsubscribed',
 'properties.unsubscribed_from_daily_nl',
 'properties.unsubscribed_from_weekly_nl',
 'properties.vydejna_nevyzvednuto',
 'properties.vydejna_vraceno',
 'properties.zpozdene',
 'NDay',
 'Month',
 'Day',
 'Year']

gnb.fit(
    X_train[used_features].values,
    X_train["data.properties.status"]
)

y_pred = gnb.predict(X_test[used_features])

print("Number of mislabeled points out of a total {} points : {}, performance {:05.2f}%"
      .format(
          X_test.shape[0],
          (X_test["data.properties.status"] != y_pred).sum(),
          100*(1-(X_test["data.properties.status"] != y_pred).sum()/X_test.shape[0])
))



#neural network


z= dfColumn.drop('data.properties.status',axis=1)
z= z.drop('NDay',axis=1)
#c = dfColumn['NDay']
c = dfColumn['data.properties.status']


from sklearn.model_selection import train_test_split


X_train, X_test, y_train, y_test = train_test_split(z, c)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()

# Fit only to the training data
scaler.fit(X_train)




# Now apply the transformations to the data:
X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test)




from sklearn.neural_network import MLPClassifier


mlp = MLPClassifier(hidden_layer_sizes=(13,13,13),max_iter=500)

mlp.fit(X_train,y_train)




predictions = mlp.predict(X_test)

from sklearn.metrics import classification_report,confusion_matrix


print(confusion_matrix(y_test,predictions))



print(classification_report(y_test,predictions))


labelsanonym=list(dfAnonym.columns.values)
dfAnonym["properties.17-12-12_SMS_Christmas"].value_counts()
dfAnonym["properties.18-03-21_SMS"].value_counts()
dfAnonym["properties.18-05-07_SMS_Andel"].value_counts()
dfAnonym["properties.18-05-07_SMS_Narodka"].value_counts()
dfAnonym["properties.Black_Friday_vyzva"].value_counts()
dfAnonym["properties.Black_Friday_vyzva_v2"].value_counts()
dfAnonym["properties.Bounce"].value_counts()
dfAnonym["properties.Brazilie_final"].value_counts()
dfAnonym["properties.CB2000"].value_counts()#remove
dfAnonym["properties.Dluhopisy_Osobni"].value_counts()
dfAnonym["properties.Dluhopisy_Osobni_2"].value_counts()
dfAnonym["properties.Home address"].value_counts()
dfAnonym["properties.Nakupili ZOOT Original"].value_counts()
dfAnonym["properties.Nezaplatili/Nevyzdvihli objednavku do zari"].value_counts()
dfAnonym["properties.Pres_1000"].value_counts()
dfAnonym["properties.RMK_reco_split"].value_counts()
dfAnonym["properties.RO-SK customer ZOOT ORIGINAL"].value_counts()
dfAnonym["properties.Reaktivace_Srpen_Reminder"].value_counts()
dfAnonym["properties.Registrovali sa ale nespravili objednavku do zari"].value_counts()
dfAnonym["properties.SMS_BF2_Unique"].value_counts()#
dfAnonym["properties.SMS_BF_1"].value_counts()
dfAnonym["properties.SMS_Brazilie"].value_counts()#!!!
dfAnonym["properties.SMS_Vanoce_2"].value_counts()
dfAnonym["properties.SMS_Vanoce_3"].value_counts()
dfAnonym["properties.SMS_Vanoce_3_Oprava"].value_counts()
dfAnonym["properties.VIP_Praha"].value_counts()
dfAnonym["properties.VIP_mimoPrahu"].value_counts()
dfAnonym["properties.Varka"].value_counts()#remove
dfAnonym["properties.Varka_patek"].value_counts()
dfAnonym["properties.Women buing mens products in 15%"].value_counts()
dfAnonym["properties._cookies_count"].value_counts()
dfAnonym["properties._timestamp"].value_counts() #datum
dfAnonym["properties.ab_split"].value_counts() #?
dfAnonym["properties.affinity_brand"].value_counts()#remove
dfAnonym["properties.affinity_category"].value_counts()#remove
dfAnonym["properties.affinity_premium"].value_counts()#remove
dfAnonym["properties.all_brands"].value_counts()#remove
dfAnonym["properties.blacklist"].value_counts()
dfAnonym["properties.bounce"].value_counts()#!!!
dfAnonym["properties.browser_push_notification.endpoint"].value_counts()#remove
dfAnonym["properties.browser_push_notification.keys.auth"].value_counts()#remove
dfAnonym["properties.browser_push_notification.subscriptionId"].value_counts()#remove
dfAnonym["properties.churn_date"].value_counts()#date
dfAnonym["properties.city"].value_counts()
dfAnonym["properties.code"].value_counts()#remove
dfAnonym["properties.code_last_order_at_Xmas"].value_counts()#remove
dfAnonym["properties.country"].value_counts()#remove
dfAnonym["properties.customer_profit"].value_counts()#suma
dfAnonym["properties.customer_profit_12m"].value_counts()#suma
dfAnonym["properties.customer_type"].value_counts()#####
dfAnonym["properties.date_modified"].value_counts()#datum
dfAnonym["properties.december2016_purchases"].value_counts()#####??
dfAnonym["properties.dluhopisy"].value_counts()
dfAnonym["properties.domu_nevyzvednuto"].value_counts()
dfAnonym["properties.domu_vraceno"].value_counts()####
dfAnonym["properties.email_domain"].value_counts()
dfAnonym["properties.first_name_disp"].value_counts()#remove
dfAnonym["properties.first_source"].value_counts()
dfAnonym["properties.ga_clid"].value_counts()#!!!remove
dfAnonym["properties.gender"].value_counts()###########
dfAnonym["properties.import"].value_counts()###
dfAnonym["properties.imported"].value_counts()################
dfAnonym["properties.in_session"].value_counts()###
dfAnonym["properties.koupili hero"].value_counts()
dfAnonym["properties.last_order_at Christmas"].value_counts()#!!!
dfAnonym["properties.last_order_at_feb-apr"].value_counts()
dfAnonym["properties.last_order_quantity"].value_counts()###remove
dfAnonym["properties.lastpurchaseat"].value_counts()#datum
dfAnonym["properties.model_no order"].value_counts()
dfAnonym["properties.model_order"].value_counts()
dfAnonym["properties.model_purchase"].value_counts()
dfAnonym["properties.name_date"].value_counts()####
dfAnonym["properties.name_date_code"].value_counts()#remove
dfAnonym["properties.name_date_code_end"].value_counts()#datum
dfAnonym["properties.newsletters"].value_counts()#datum
dfAnonym["properties.nextorderat"].value_counts()#datum
dfAnonym["properties.not_finished_order_before_6m"].value_counts()
dfAnonym["properties.objednali hero"].value_counts()
dfAnonym["properties.optin_type"].value_counts()####
dfAnonym["properties.order_1M_10m"].value_counts()##################
dfAnonym["properties.order_1M_14D"].value_counts()##################
dfAnonym["properties.order_1M_1D"].value_counts()###################
dfAnonym["properties.order_1M_20m"].value_counts()###################
dfAnonym["properties.order_1M_30m"].value_counts()###################
dfAnonym["properties.order_1M_5m"].value_counts()###################
dfAnonym["properties.order_1M_60m"].value_counts()###################
dfAnonym["properties.order_1M_7D"].value_counts()###################
dfAnonym["properties.order_brands"].value_counts()#!!!!
dfAnonym["properties.otevreli hero"].value_counts()
dfAnonym["properties.over1000_4mesice"].value_counts()
dfAnonym["properties.period"].value_counts()#####################
dfAnonym["properties.photo_no order"].value_counts()
dfAnonym["properties.photo_order"].value_counts()
dfAnonym["properties.photo_purchase"].value_counts()
dfAnonym["properties.pickup_places"].value_counts()###
dfAnonym["properties.pickup_purchases"].value_counts()#####remove
dfAnonym["properties.plussizer"].value_counts()#####
dfAnonym["properties.postal_code"].value_counts()######
dfAnonym["properties.premium_customers(3andmore_items_2015-2016)"].value_counts()
dfAnonym["properties.region"].value_counts()####
dfAnonym["properties.registered_at"].value_counts()#datum
dfAnonym["properties.reminder"].value_counts()####
dfAnonym["properties.resubscribe10nov2017"].value_counts()
dfAnonym["properties.revenue_12m_actual"].value_counts()#suma
dfAnonym["properties.revenue_12m_day_before"].value_counts()#suma
dfAnonym["properties.sale_code_dec2016"].value_counts()#!!!remove
dfAnonym["properties.salecode_hunter"].value_counts()######
dfAnonym["properties.salehunter"].value_counts()#####
dfAnonym["properties.second_order_month"].value_counts()#####
dfAnonym["properties.segment"].value_counts()####
dfAnonym["properties.source"].value_counts()####
dfAnonym["properties.stylistka_deals"].value_counts()
dfAnonym["properties.subscriber_created_at"].value_counts()#timestamp
dfAnonym["properties.synkac"].value_counts()
dfAnonym["properties.synkac_2"].value_counts()
dfAnonym["properties.synkac_3"].value_counts()
dfAnonym["properties.temp_utm_campaign"].value_counts()
dfAnonym["properties.temp_utm_content"].value_counts()
dfAnonym["properties.temp_utm_medium"].value_counts()
dfAnonym["properties.temp_utm_source"].value_counts()
dfAnonym["properties.temp_utm_term"].value_counts()
dfAnonym["properties.tester"].value_counts()#####
dfAnonym["properties.testing"].value_counts()
dfAnonym["properties.top 100"].value_counts()#####
dfAnonym["properties.top_4_brands"].value_counts()#####
dfAnonym["properties.top_4_brands_json"].value_counts()#####
dfAnonym["properties.top_4_categories_json"].value_counts()####
dfAnonym["properties.type_customer"].value_counts()#!!!
dfAnonym["properties.unsubscribe_link"].value_counts()
dfAnonym["properties.unsubscribe_link_profile"].value_counts()
dfAnonym["properties.unsubscribed"].value_counts()#
dfAnonym["properties.unsubscribed_from"].value_counts()#
dfAnonym["properties.unsubscribed_from_daily_nl"].value_counts()#
dfAnonym["properties.unsubscribed_from_weekly_nl"].value_counts()#
dfAnonym["properties.voucher_first_order"].value_counts()
dfAnonym["properties.vydejna_nevyzvednuto"].value_counts()#####
dfAnonym["properties.vydejna_vraceno"].value_counts()######
dfAnonym["properties.zpozdene"].value_counts()######
dfCampaign["data.properties.message"].value_counts()######
dfAnonym["properties.name_date"].value_counts()######


   
       
#multikorelacia
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import Imputer

from statsmodels.stats.outliers_influence import variance_inflation_factor

transformer = ReduceVIF()


from statsmodels.stats.outliers_influence import variance_inflation_factor

class ReduceVIF(BaseEstimator, TransformerMixin):
    def __init__(self, thresh=5.0, impute=True, impute_strategy='median'):
        # From looking at documentation, values between 5 and 10 are "okay".
        # Above 10 is too high and so should be removed.
        self.thresh = thresh
        
        # The statsmodel function will fail with NaN values, as such we have to impute them.
        # By default we impute using the median value.
        # This imputation could be taken out and added as part of an sklearn Pipeline.
        if impute:
            self.imputer = Imputer(strategy=impute_strategy)

    def fit(self, X, y=None):
        print('ReduceVIF fit')
        if hasattr(self, 'imputer'):
            self.imputer.fit(X)
        return self

    def transform(self, X, y=None):
        print('ReduceVIF transform')
        columns = X.columns.tolist()
        if hasattr(self, 'imputer'):
            X = pd.DataFrame(self.imputer.transform(X), columns=columns)
        return ReduceVIF.calculate_vif(X, self.thresh)

    @staticmethod
    def calculate_vif(X, thresh=5.0):
        # Taken from https://stats.stackexchange.com/a/253620/53565 and modified
        dropped=True
        while dropped:
            variables = X.columns
            dropped = False
            vif = [variance_inflation_factor(X[variables].values, X.columns.get_loc(var)) for var in X.columns]
            
            max_vif = max(vif)
            if max_vif > thresh:
                maxloc = vif.index(max_vif)
                print(f'Dropping {X.columns[maxloc]} with vif={max_vif}')
                X = X.drop([X.columns.tolist()[maxloc]], axis=1)
                dropped=True
        return X
    
data=dfColumn
y = data.pop('data.properties.status')
    
X = transformer.fit_transform(data, y)    
    
    
X.to_pickle('dfX.pkl') 
X['data.properties.status']=dfColumn['data.properties.status']
 
XX_train, XX_test = train_test_split(X, test_size=0.65, random_state=int(time.time()))

gnb = GaussianNB()

list(X)

used_features =['properties.17-12-12_SMS_Christmas',
 'properties.18-03-21_SMS',
 'properties.18-05-07_SMS_Andel',
 'properties.18-05-07_SMS_Narodka',
 'properties.Black_Friday_vyzva',
 'properties.Bounce',
 'properties.Brazilie_final',
 'properties.Dluhopisy_Osobni_2',
 'properties.Home address',
 'properties.Nakupili ZOOT Original',
 'properties.Nezaplatili/Nevyzdvihli objednavku do zari',
 'properties.Pres_1000',
 'properties.RMK_reco_split',
 'properties.RO-SK customer ZOOT ORIGINAL',
 'properties.Reaktivace_Srpen_Reminder',
 'properties.Registrovali sa ale nespravili objednavku do zari',
 'properties.SMS_BF2_Unique',
 'properties.SMS_BF_1',
 'properties.SMS_Brazilie',
 'properties.SMS_Vanoce_2',
 'properties.SMS_Vanoce_3',
 'properties.SMS_Vanoce_3_Oprava',
 'properties.VIP_Praha',
 'properties.VIP_mimoPrahu',
 'properties.Varka_patek',
 'properties.Women buing mens products in 15%',
 'properties._cookies_count',
 'properties.blacklist',
 'properties.bounce',
 'properties.customer_type',
 'properties.dluhopisy',
 'properties.domu_nevyzvednuto',
 'properties.domu_vraceno',
 'properties.import',
 'properties.imported',
 'properties.in_session',
 'properties.koupili hero',
 'properties.last_order_at_feb-apr',
 'properties.last_order_quantity',
 'properties.model_order',
 'properties.model_purchase',
 'properties.not_finished_order_before_6m',
 'properties.objednali hero',
 'properties.order_brands',
 'properties.otevreli hero',
 'properties.period',
 'properties.photo_no order',
 'properties.photo_order',
 'properties.photo_purchase',
 'properties.plussizer',
 'properties.premium_customers(3andmore_items_2015-2016)',
 'properties.reminder',
 'properties.resubscribe10nov2017',
 'properties.revenue_12m_day_before',
 'properties.salecode_hunter',
 'properties.salehunter',
 'properties.second_order_month',
 'properties.source',
 'properties.synkac_3',
 'properties.tester',
 'properties.top 100',
 'properties.unsubscribed',
 'properties.unsubscribed_from_daily_nl',
 'properties.vydejna_nevyzvednuto',
 'properties.vydejna_vraceno',
 'NDay',
 'Month']

gnb.fit(
    XX_train[used_features].values,
    XX_train['data.properties.status']
)

y_pred = gnb.predict(XX_test[used_features])

print("Number of mislabeled points out of a total {} points : {}, performance {:05.2f}%"
      .format(
          XX_test.shape[0],
          (XX_test["data.properties.status"] != y_pred).sum(),
          100*(1-(XX_test["data.properties.status"] != y_pred).sum()/XX_test.shape[0])
))


mean_survival=np.mean(XX_train["data.properties.status"])
mean_not_survival=1-mean_survival
print("Survival prob = {:03.2f}%, Not survival prob = {:03.2f}%"
.format(100*mean_survival,100*mean_not_survival))

    


z= X.drop('data.properties.status',axis=1)
c = X['data.properties.status']


from sklearn.model_selection import train_test_split


X_train, X_test, y_train, y_test = train_test_split(z, c)

from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()

# Fit only to the training data
scaler.fit(X_train)




# Now apply the transformations to the data:
X_train = scaler.transform(X_train)
X_test = scaler.transform(X_test)




from sklearn.neural_network import MLPClassifier


mlp = MLPClassifier(hidden_layer_sizes=(13,13,13),max_iter=500)

mlp.fit(X_train,y_train)




predictions = mlp.predict(X_test)

from sklearn.metrics import classification_report,confusion_matrix


print(confusion_matrix(y_test,predictions))



print(classification_report(y_test,predictions))