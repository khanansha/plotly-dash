# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 19:35:50 2021

@author: admin
"""


# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 18:37:53 2021

@author: Admin
"""

# =============================================================================
# ### Importing Lib
# =============================================================================
import time
time1=time.time()
import pandas as pd
import numpy as np
import glob
pd.pandas.set_option('display.max_columns', None)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# from unilever_user_input import *
from period_input import *
import os
from functools import reduce #python 3


# =============================================================================
# ### Taking Input
# =============================================================================

# country="UAE"
# category="TOTAL FABRIC SOLUTIONS"
# channel='TOTAL UAE'
# format_1="PRODUCT FORM"
# format_2="TYPE"
# format_3=""
# SDESC_1=""

# country="UAE"
# category="HAIR CARE"
# channel='TOTAL UAE'
# format_1="SUB CATEGORY"
# format_2="SEGMENT"
# format_3=""
# SDESC_1=""

# # for FAB SOL
# country="UAE"
# category="FAB SOL"
# channel='TOTAL UAE'
# format_1="PRODUCT FORM"
# format_2="TYPE"
# format_3="TEA FORMAT"
# SDESC_1=""

#for HHC
# country="UAE"
# category="HHC"
# channel='TOTAL UAE'
# format_1="SEGMENT"
# format_2="PACK TYPE"
# format_3="TEA FORMAT"
# SDESC_1=""
# 

# country="UAE"
# category="TOTAL SKIN CLEANSING"
# channel='TOTAL UAE'
# format_1="FORMAT"
# format_2=""
# format_3=""
# SDESC_1=""

# country="Egypt"
# category="TOOTHPASTE(TP)"
# channel='EGYPT'
# format_1="PLATFORM"
# format_2=""
# format_3=""
# SDESC_1=""

# country="KSA"
# category="HAIR CARE"
# channel='TOTAL SAUDI ARABIA'
# format_1="SUB CATEGORY"
# format_2="SEGMENT"
# format_3=""
# SDESC_1=""

# country="UAE"
# category="HAND & BODY"
# channel='TOTAL UAE'
# format_1="SEGMENT"
# format_2=""
# format_3=""
# SDESC_1=""

# country="Morocco"
# category="Toothpaste"
# channel='TOTAL MOROCCO'
# format_1="PURPOSE"
# format_2=""
# format_3=""
# SDESC_1=""

# country="KSA"
# category="Face care"
# channel='TOTAL SAUDI ARABIA'
# format_1="PLATFORM UA"
# format_2="SEGMENT"
# format_3=""
# SDESC_1=""

# country="Egypt"
# category="bouillon"
# channel='TOTAL EGYPT'
# format_1="PRODUCT FORM"
# format_2=""
# format_3=""
# SDESC_1=""

# country="Lebanon"
# category="DEODORANT"
# channel='TOTAL LEBANON'
# format_1="PRODUCT FORM"
# format_2=""
# format_3=""

# country="Egypt"
# category="SHAMPOOS(SH)"
# channel='TOTAL EGYPT'
# format_1="FORMAT"
# format_2=""
# format_3=""

# country="Algeria"
# category="FABRIC SOLUTIONS"
# channel='TOTAL ALGERIA'
# format_1="PRODUCT FORM"
# format_2="TYPE"
# format_3=""

# country="Egypt"
# category="TOTAL DETERGENTS"
# channel='TOTAL EGYPT'
# format_1="FORM"
# format_2="TYPE"
# format_3=""

# country="KSA"
# category="Skin cleansing"
# channel='TOTAL SAUDI ARABIA'
# format_1="FORMAT"
# format_2=""
# format_3=""

# country="Egypt"
# category="TOOTHPASTE(TP)"
# channel='EGYPT'
# format_1="PLATFORM"
# format_2=""
# format_3=""

# country="Lebanon"
# category="SHAMPOO"
# channel='TOTAL LEBANON'
# format_1="FORMAT"
# format_2=""
# format_3=""


country="KSA"
category="TOOTHPASTE&MOUTHWASHES"
channel='TOTAL SAUDI ARABIA'
format_1="PLATFORM"
format_2=""
format_3=""

#def main_fun(country,category,channel,format_1,format_2,format_3):
mylist = [f for f in glob.glob("input_files"+"/*.csv")]
sheets = [name for name in mylist if country+ "_"+category in name]
format_add= [col for col in sheets if ("_AF_" in col)]

for f in format_add:
    df= pd.read_csv(f,encoding='utf-8',thousands=r',').dropna(how='all')
    df['FORMAT']=df['CATEGORY']
    df_files=f.split('\\')[1]
    df_rem_csv=df_files.split('.')[0]
    df.to_csv('input_files/'+df_rem_csv+".csv", encoding='utf-8', index=False)
   
TDP_sheets=[col for col in sheets if ("TDP" in col)]
WD_sheets = [col for col in sheets if ("TDP" not in col)]
WD_sheets2 = [col for col in WD_sheets if ("_M_" not in col) and ("_B_" not in col)]
WD_sheets3 = [col for col in WD_sheets2 if ("_F1_" not in col) and ("_F2_" not in col)]

QC_df_datacheck=pd.DataFrame(columns = ["question","check_satus","comment"])
QC_df_calculation=pd.DataFrame(columns = ["question","check_satus","comment"])
if len(mylist)==len(sheets):
    df2 = {'question':"Check file naming in input folder",'check_satus':"Pass",'comment':' '}
else:
    df2 = {'question':"Check file naming in input folder",'check_satus':"Fail",'comment':'Please Fallow the naming convention'}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)

# =============================================================================
# ### create dataframe
# =============================================================================
df_file = pd.DataFrame([i.split('\\')[1].split("_") for i in WD_sheets])
df_file.columns = ["file_type","Category","Channel/Market","Category","Manufacturer","Brand","Sub Brand","SKU","Format 1","Format 2","Format 3","Time Period","No_Split"]
df_file['No_Split'] = df_file['No_Split'].map(lambda x: x.rstrip('.csv'))
df_file.head()
    
### create the dict to convert the col name to make consistant across
col_name_dict={"MANUFACTURER":"MANUFACTURER","TOTAL CATEGORY":"CATEGORY","MANUFACTURES":"MANUFACTURER","category":"CATEGORY","CATEGORYs":"CATEGORY","Catrgories":"CATEGORY","Channel/Market":"MARKET",
"channel/market":"MARKET","channel/markets":"MARKET","Channels":"MARKET","Channel":"MARKET",
"channels/markets":"MARKET","market":"MARKET","market":"MARKET","Manufacturer":"MANUFACTURER",
"Manufacturers":"MANUFACTURER","MANUFACTURERs":"MANUFACTURER","brand owner":"MANUFACTURER",
"Brand Owner":"MANUFACTURER","Brand owner":"MANUFACTURER","BRAND OWNER":"MANUFACTURER","BRANDS":"BRAND","Brand":"BRAND","Brands":"BRAND",
"Sub Brands":"SUB BRANDS","Sub Brand":"SUB BRANDS",format_1:"FORMAT_1",format_2:"FORMAT_2",format_3:"FORMAT_3",
"SDESC":"CHANNEL","SDESC.1":"SDESC1","SDESC2":"TIME PERIOD","SDESC.2":"TIME PERIOD",
"Sales Value 1000 AED":"VALUE","Sales Value 100000":"VALUE","Sales Volume 1000 Kgs":"VOLUME",
"Sales Volume 1000 KGS":"VOLUME","Sales Volume 1000 LTRS":"VOLUME","AVG ND":"ND","Avg WD":"WD",
"Avg ND":"ND","AVG WD":"WD","Avg. ND":"ND","Avg. WD":"WD","Sales Value 1000000 LE":"VALUE","Sales Volume1000 Kgs":"VOLUME",
"Sales Value 100000SR":"VALUE","Sales Volume 1000 Ltrs":"VOLUME","TOOTHPASTE":"CATEGORY",
"Sales Volume 1000KG":"VOLUME","Sales Value 100.000MAD":"VALUE",
"Sales Volume 1000 Kgs/Ltrs":"VOLUME","Sales Value 100000 SR":"VALUE","Sales Value (1MLL)":"VALUE","Sales Volume (1000 KG)":"VOLUME",
"Sales Volume 1000 KGS/LTRS":"VOLUME","MANUFACTURE":"MANUFACTURER",
'Sales Value 1000000LE':'VALUE', 'Sales Volume 1000LTRS':'VOLUME','Sales Volume(1000 LTRS)':'VOLUME','Sales Value(1000 JD)':'VALUE','Sales Value SR 100000':'VALUE',"Sales Volume 1000 KGS/LTRS":"VOLUME","Sales Value 1000000 LE":"VALUE"
,"Sales Volume 1000LT":"VOLUME","Sales Value 100000MAD":"VALUE","Sales Volume (1000 KGS)":"VOLUME","Sales Value (1000 JD)":"VALUE","Sales Volume (1000 LTR)":"VOLUME","Sales Value (1M LL)":"VALUE","Sales Volume ( 1000 KGS )":"VOLUME","Sales Value ( 1000 JD )":"VALUE","Stocks":"CATEGORY",
"TOOTHPASTES":"CATEGORY"}

# read support files
forexchange=pd.read_csv("support_files/forexchange.csv")
forexchange=forexchange[(forexchange["COUNTRY"]==country) & (forexchange["CATEGORY"]==category)]
forexchange.reset_index(drop=True,inplace=True)
seg_map_df=pd.read_excel("support_files/segment_map.xlsx")
seg_map_df=seg_map_df[(seg_map_df["COUNTRY"]==country) & (seg_map_df["CATEGORY"]==category)]
seg_lookup_dict = dict(zip(seg_map_df['SEG_F1_AND_F2_AND_F3'], seg_map_df['DEF_SEGMENT']))
seg_display_dict = dict(zip(seg_map_df['DEF_SEGMENT'], seg_map_df['SEGMENT_display']))

brands_UL_map_df=pd.read_csv("support_files/brands_UL_map.csv")
brands_UL_map_df=brands_UL_map_df[(brands_UL_map_df["COUNTRY"]==country) & (brands_UL_map_df["CATEGORY"]==category)]
brand_UL_lookup_dict = dict(zip(brands_UL_map_df['BRAND'], brands_UL_map_df['BRAND_RN']))
dimension_map=pd.read_csv("support_files/dimension_map.csv")
final_display_CC=pd.read_csv("support_files/final_disply_CC_map.csv")
final_display_CA_dict = dict(zip(final_display_CC['Category'], final_display_CC['Category_display']))
final_display_CH_dict = dict(zip(final_display_CC['Channel'], final_display_CC['Channel_display']))

final_display_B=pd.read_csv("support_files/final_disply_B_map.csv")
final_display_B=final_display_B[final_display_B["Category"]==category]
final_display_B_dict = dict(zip(final_display_B['Brand'].str.upper() , final_display_B['Brand_display']))

final_display_M=pd.read_csv("support_files/final_disply_M_map.csv")
final_display_M=final_display_M[final_display_M["Category"]==category]
final_display_M_dict = dict(zip(final_display_M['Manufacturer'].str.upper() , final_display_M['Manuf_disply']))

# =============================================================================
# ### Read wd filde to Combining all file and adding total
# =============================================================================
type1_df=pd.DataFrame()
type2_df=pd.DataFrame()
type3_df=pd.DataFrame()
col_list_all=[]
for sheet in WD_sheets:
    k1=pd.read_csv(sheet,encoding='utf-8',thousands=r',').dropna(how='all')  #encoding='latin-1'
    # k1.apply(lambda x: x.astype(str).str.upper())
    k1= k1.applymap(lambda s:s.upper() if type(s) == str else s)
    col_list=k1.columns.tolist()
    col_list_all.append(col_list)
    k1.rename(col_name_dict, axis=1, inplace=True)
    k1["TIME PERIOD"]=k1["TIME PERIOD"].replace({"L12W TY_COPY" : "L12W TY_QOPY","L12W TY_Copy" : "L12W TY_QOPY"})
    if sheet in WD_sheets3:        
        type1_df = type1_df.append(k1, ignore_index=True)
    elif sheet in WD_sheets2:
        if "FORMAT_2" in k1.columns:
            k1["FORMAT_1"]=k1["FORMAT_1"]+" "+k1["FORMAT_2"]
        else:
            pass
        type2_df = type2_df.append(k1, ignore_index=True)
    else:
        if "FORMAT_2" in k1.columns:
            k1["FORMAT_1"]=k1["FORMAT_1"]+" "+k1["FORMAT_2"]
        else:
            pass
        type3_df = type3_df.append(k1, ignore_index=True)
         
## data clean as per requirement     
type1_df = type1_df.loc[:, ~type1_df.columns.str.contains('^Unnamed')]
type2_df = type2_df.loc[:, ~type2_df.columns.str.contains('^Unnamed')]
type3_df = type3_df.loc[:, ~type3_df.columns.str.contains('^Unnamed')]

if "FORMAT_2" in type2_df.columns:
    type2_df["SEGMENT"]=type2_df["FORMAT_1"]
    type2_df.drop(["FORMAT_1","FORMAT_2","SDESC1"], axis=1,inplace=True)
    type2_df["SEGMENT"].replace(np.nan,"TOTAL", inplace=True)
    type2_df["SEGMENT"]=type2_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})
elif "FORMAT_1" in type2_df.columns:
    type2_df["SEGMENT"]=type2_df["FORMAT_1"]
    type2_df.drop(["FORMAT_1","SDESC1"], axis=1,inplace=True)
    type2_df["SEGMENT"].replace(np.nan,"TOTAL", inplace=True)        
    type2_df["SEGMENT"]=type2_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})
        
if "FORMAT_2" in type3_df.columns:
    type3_df["SEGMENT"]=type3_df["FORMAT_1"]
    type3_df.drop(["FORMAT_1","FORMAT_2","SDESC1"], axis=1,inplace=True)
    type3_df["SEGMENT"].replace(np.nan,"TOTAL", inplace=True) 
    type3_df["SEGMENT"]=type3_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})   
elif "FORMAT_1" in type3_df.columns:
    type3_df["SEGMENT"]=type3_df["FORMAT_1"]
    type3_df.drop(["FORMAT_1","SDESC1"], axis=1,inplace=True)
    type3_df["SEGMENT"].replace(np.nan,"TOTAL", inplace=True)        
    type3_df["SEGMENT"]=type3_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})   

num_col=["VOLUME","VALUE","ND","WD"]
for col in  num_col:
    type3_df[col] = pd.to_numeric(type3_df[col], errors='coerce')

num_col=["VOLUME","VALUE","ND","WD"]
for col in  num_col:
    type2_df[col] = pd.to_numeric(type2_df[col], errors='coerce')

num_col=["VOLUME","VALUE","ND","WD"]
for col in  num_col:
    type1_df[col] = pd.to_numeric(type1_df[col], errors='coerce')
# period column format
        
type1_df.drop(["SDESC1"], axis=1,inplace=True)
type1_df=type1_df.assign(**dict.fromkeys(['MANUFACTURER','BRAND','SEGMENT'], 'TOTAL'))
# type1_df[['MANUFACTURER','BRAND','SEGMENT']] = ["Total","Total","Total"]
# type2_df[['MANUFACTURER','BRAND']] = ["Total","Total"]
type2_df=type2_df.assign(**dict.fromkeys(['MANUFACTURER','BRAND'], 'TOTAL'))
type2_df.fillna({'MANUFACTURER':"TOTAL", 'BRAND':'TOTAL','SEGMENT':'TOTAL'}, inplace=True)
type3_df.fillna({'MANUFACTURER':"TOTAL", 'BRAND':'TOTAL','SEGMENT':'TOTAL'}, inplace=True)
    
# type3_df.update(type3_df[['MANUFACTURER','BRAND','SEGMENT']].fillna("TOTAL"))
# type2_df.update(type2_df[['MANUFACTURER','BRAND','SEGMENT']].fillna("TOTAL"))
type1_df['TIME PERIOD']=type1_df['TIME PERIOD'].replace({k.upper(): v for k, v in period_dict.items()})   
type2_df['TIME PERIOD']=type2_df['TIME PERIOD'].replace({k.upper(): v for k, v in period_dict.items()})   
type3_df['TIME PERIOD']=type3_df['TIME PERIOD'].replace({k.upper(): v for k, v in period_dict.items()})   
# type1_df['TIME PERIOD'].replace(period_dict, inplace=True)
# type2_df['TIME PERIOD'].replace(period_dict, inplace=True)
# type3_df['TIME PERIOD'].replace(period_dict, inplace=True)
# type3_df['BRAND']=type3_df['BRAND'].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()})   
# type3_df.loc[type3_df.MANUFACTURER == 'UNILEVER' , ['BRAND']].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()},inplace=True)   
type3_df1=type3_df[type3_df.MANUFACTURER == 'UNILEVER']
type3_df=type3_df[type3_df.MANUFACTURER != 'UNILEVER']
type3_df1['BRAND']=type3_df1['BRAND'].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()})
type3_df=type3_df.append(type3_df1)
## Take the unique for manuf
manuf_lst=type3_df.MANUFACTURER.unique().tolist()
#print(len(manuf_lst))
seg_type_lst=type3_df.SEGMENT.unique().tolist()
#print(len(seg_type_lst))
# print("seg_type_lst ",seg_type_lst)
  ## create unwanted list
manuf_rm_lst=['UNILEVER','OTHER','PRIVATE LABEL','NOT APPLICABLE','NOT AVAILABLE']
manuf_unwant_lst = [s for s in manuf_lst if any(xs in s for xs in manuf_rm_lst)]
        
## create list to pass in segwise function
segwise_grup_col2=['CHANNEL','CATEGORY','MANUFACTURER','TIME PERIOD']
segwise_grup_col1=['CHANNEL','CATEGORY','MANUFACTURER','TIME PERIOD','SUB SEGMENT']
segwise_grup_col=['CHANNEL','CATEGORY','MANUFACTURER','TIME PERIOD','SEGMENT']
    
#define funtion
def segwisetop5(df,seg_val,channel,seg_col,grup_col,seg_type):
    if seg_type==1:
        Latest_MAT=df[(df[seg_col]==seg_val)]
    elif seg_type==2:
        Latest_MAT=df[(df['SEGMENT']=='PURE BLACK TEA')&(df['SUB SEGMENT']=='LOOSE TEA')]
    elif seg_type==3:
        Latest_MAT=df[(df['SEGMENT']=='PURE BLACK TEA')&(df['SUB SEGMENT']=='LOOSE TEA')&(df['FORMAT3']==seg_val)]
    else:
        Latest_MAT=df.copy()
    Latest_MAT1=Latest_MAT[(Latest_MAT['TIME PERIOD']=='MAT TY')&(Latest_MAT['CHANNEL']==channel)&
                            (~Latest_MAT['MANUFACTURER'].isin(manuf_unwant_lst))]
    Latest_MAT1=Latest_MAT1.groupby(grup_col)['VALUE'].sum().reset_index()
    Top5=Latest_MAT1.sort_values(by="VALUE",ascending=False).head()
    List1=Top5.MANUFACTURER.unique().tolist()
    List1.append('UNILEVER')
    Latest_MAT.loc[~Latest_MAT["MANUFACTURER"].isin(List1), "MANUFACTURER"] = "OTHERS"
    # print(List1)
    return Latest_MAT
        
    
segtop5_df=pd.DataFrame(columns=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','TIME PERIOD'])
for i in seg_type_lst:
    #print(i)
    segtop5_df=segtop5_df.append(segwisetop5(type3_df,i,channel,"SEGMENT",segwise_grup_col,1))

#print(segtop5_df.shape)

def group_sum_max_df(df):
    df1=df.groupby(['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'])['VALUE','VOLUME'].sum()
    df2=df.groupby(['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'])['ND','WD'].max()
    df3=pd.concat([df1,df2],axis=1,sort=False).reset_index()
    return df3
    
allcombinations=pd.concat([segtop5_df],sort=False)
allcombinations1=group_sum_max_df(type1_df)
allcombinations2=group_sum_max_df(type2_df)
allcombinations3=group_sum_max_df(allcombinations)
NDWD_final=pd.concat([allcombinations1,allcombinations2,allcombinations3],sort=False)
print ("ND WD ", NDWD_final.shape)
    
# NDWD_final.to_csv("NDWD_final.csv")

# =============================================================================
# #read TDP data from CSV
# =============================================================================
type4_df=pd.DataFrame()
for sheet in TDP_sheets:
    k1=pd.read_csv(sheet,encoding='utf-8',thousands=r',').dropna(how='all')  #encoding='latin-1'
    k1= k1.applymap(lambda s:s.upper() if type(s) == str else s)        
    k1.rename(col_name_dict, axis=1, inplace=True)
    k1["TIME PERIOD"]=k1["TIME PERIOD"].replace({"L12W TY_COPY" : "L12W TY_QOPY","L12W TY_Copy" : "L12W TY_QOPY"})    
    type4_df = type4_df.append(k1, ignore_index=True)
        
type4_df = type4_df.loc[:, ~type4_df.columns.str.contains('^Unnamed')]
type4_df1=type4_df[type4_df.MANUFACTURER == 'UNILEVER']
type4_df=type4_df[type4_df.MANUFACTURER != 'UNILEVER']
type4_df1['BRAND']=type4_df1['BRAND'].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()})
type4_df=type4_df.append(type4_df1)
#type4_df["BRAND"]=type4_df["BRAND"].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()})   
#type4_df.loc[type4_df.MANUFACTURER == 'UNILEVER' , ['BRAND']].replace({k.upper(): v for k, v in brand_UL_lookup_dict.items()},inplace=True)    
# type4_df['BRAND'].replace(brand_UL_lookup_dict, inplace=True)
type4_df["TIME PERIOD"]=type4_df["TIME PERIOD"].replace({k.upper(): v for k, v in period_dict.items()}) 
   
num_col=["VOLUME","VALUE","TDP"]
for col in  num_col:
    type4_df[col] = pd.to_numeric(type4_df[col], errors='coerce')      

if "FORMAT_2" in type4_df.columns:
    type4_df["SUB SEGMENT"]=type4_df["FORMAT_1"]+" "+type4_df["FORMAT_2"]
    type4_df["SEGMENT"]=type4_df["FORMAT_1"]
    type4_df["SUB SEGMENT"].replace(np.nan,"TOTAL", inplace=True)
    type4_df["SUB SEGMENT"]=type4_df["SUB SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})       
    type4_df["SEGMENT"]=type4_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})       
else:
    type4_df["SEGMENT"]=type4_df["FORMAT_1"]
    type4_df["SEGMENT"]=type4_df["SEGMENT"].replace({k.upper(): v for k, v in seg_lookup_dict.items()})       

for col in ["FORMAT_1","FORMAT_2","SDESC1","ITEM"]:
    if col in type4_df.columns:
        type4_df.drop([col], axis=1,inplace=True)
    else:
        pass
    
# this list is going to use latter
format_1=type4_df["SEGMENT"].unique().tolist()
format_2=[]
if "SUB SEGMENT" in type4_df.columns:
    format_2=type4_df["SUB SEGMENT"].unique().tolist()
        
latest_Periods=type4_df["TIME PERIOD"].unique()       
latest_Periods = [col for col in latest_Periods if ("FY" in col) or ("TY" in col)]
latest_Periods_2= [col for col in latest_Periods if ("FY" in col)]
if len (latest_Periods_2)==2:
    latest_Periods_2.sort(reverse=True)
    latest_Periods=list(set(latest_Periods)-set([latest_Periods_2[1]]))
    #print(latest_Periods)
else:
    pass
   
## Take the unique for manuf
manuf_lst=type4_df['MANUFACTURER'].unique().tolist()
#print(len(manuf_lst))
seg_type_lst=type4_df['SEGMENT'].unique().tolist()
#print(len(seg_type_lst))
seg_type_lst1=[]
if "SUB SEGMENT" in type4_df.columns:
    seg_type_lst1=type4_df['SUB SEGMENT'].unique().tolist()
# seg_type_lst2=type4_df['FORMAT3'].unique().tolist()

segtop5_df=pd.DataFrame(columns=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','TIME PERIOD'])
segtop5_df1=pd.DataFrame(columns=['CHANNEL','CATEGORY','SUB SEGMENT','MANUFACTURER','TIME PERIOD'])
# segtop5_df2=pd.DataFrame(columns=['CHANNEL','CATEGORY','FORMAT3','MANUFACTURER','TIME PERIOD'])

for i in seg_type_lst:
    # print(i)
    segtop5_df=segtop5_df.append(segwisetop5(type4_df,i,channel,"SEGMENT",segwise_grup_col,1))
    
# print(segtop5_df)    
for i in seg_type_lst1:
    segtop5_df1=segtop5_df1.append(segwisetop5(type4_df,i,channel,"SUB SEGMENT",segwise_grup_col1,1))
# print(segtop5_df1)
if len (segtop5_df1)>0:
    segtop5_df1.drop('SEGMENT',axis=1,inplace=True)
    segtop5_df1.rename({'SUB SEGMENT':'SEGMENT'}, axis=1, inplace=True)
    
# for i in seg_type_lst2:
#     print(i)
#     segtop5_df2=segtop5_df2.append(segwisetop5(type4_df,i,channel,"SUB SEGMENT",segwise_grup_col1,1))
# print(type4_df)    
if "SUB SEGMENT" in type4_df.columns:
    type4_df1=segwisetop5(type4_df,'s',channel,"SUB SEGMENT",segwise_grup_col2,4)  
    type4_df1.MANUFACTURER.unique()  
else:
    type4_df1=segwisetop5(type4_df,'s',channel,"SEGMENT",segwise_grup_col2,4)  
    type4_df1.MANUFACTURER.unique()
    

def group_sum_max_df_1(df,group_lst):
    df1=df.groupby(group_lst)['VALUE','VOLUME','TDP'].sum()
    # df2=df.groupby(group_lst)['ND','WD'].max()
    df1=df1.reset_index()
    return df1

    
#grouping at total level
#grouping at total level
if "SUB SEGMENT" in type4_df.columns:
    group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','TIME PERIOD']
    S3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
    group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','TIME PERIOD']
    U3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
    group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD']
    W3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','TIME PERIOD']
M3=group_sum_max_df_1(type4_df1,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','MANUFACTURER','TIME PERIOD']
O3=group_sum_max_df_1(type4_df1,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','MANUFACTURER','BRAND','TIME PERIOD']
P3=group_sum_max_df_1(type4_df1,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','TIME PERIOD']
Q3=group_sum_max_df_1(segtop5_df,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','TIME PERIOD']
R3=group_sum_max_df_1(segtop5_df,group_sum_lst)
group_sum_lst=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD']
N3=group_sum_max_df_1(segtop5_df,group_sum_lst)
# group_sum_lst=['CHANNEL','CATEGORY','SUB SEGMENT','TIME PERIOD']
# S3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
# group_sum_lst=['CHANNEL','CATEGORY','SUB SEGMENT','MANUFACTURER','TIME PERIOD']
# U3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
# group_sum_lst=['CHANNEL','CATEGORY','SUB SEGMENT','MANUFACTURER','BRAND','TIME PERIOD']
# W3=group_sum_max_df_1(segtop5_df1,group_sum_lst)
   
if "SUB SEGMENT" in type4_df.columns:
    TDP_final=pd.concat([M3,O3,P3,Q3,R3,N3,S3,U3,W3],sort=False)
else:
    TDP_final=pd.concat([M3,O3,P3,Q3,R3,N3],sort=False)
TDP_final.reset_index(inplace=True,drop=True)
print ("ND WD ", NDWD_final.shape)
print("tdp ", TDP_final.shape)
#TDP_final.to_excel("TDP_final.xlsx")
#NDWD_final.to_excel("NDWD_final.xlsx")


# QC_df=pd.DataFrame(columns = ["question","check_satus","comment"])
if len(TDP_final)==len(NDWD_final):
    df2 = {'question':"Check number of rows for NDWD and TDP processed data is same or not",'check_satus':"Pass",'comment':'TDP: '+str(len(TDP_final))+' and NDWD: '+str(len(NDWD_final))}
else:
    df2 = {'question':"Check number of rows for NDWD and TDP processed data is same or not",'check_satus':"Fail",'comment':'TDP: '+str(len(TDP_final))+' and NDWD: '+str(len(NDWD_final))}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)
        
    
TDP_final[['MANUFACTURER','BRAND','SEGMENT']] = TDP_final[['MANUFACTURER','BRAND','SEGMENT']].fillna(value="TOTAL")
#print("tdp ", TDP_final.shape)
#TDP_final.to_excel("TDP.xlsx")
TDP_final.drop(["VALUE","VOLUME"], axis=1,inplace=True)

#merge TDP and NDWD
NDWD_TDP_DF=NDWD_final.merge(TDP_final,on=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'],how='left')
NDWD_TDP_DF=NDWD_TDP_DF.drop_duplicates()

#create the dataframe with unique dimention:
cu = []
i = []
for cn in NDWD_TDP_DF.columns[:-5]:
    cu.append(NDWD_TDP_DF[cn].unique())
    i.append(cn)
unique_dim_df=pd.DataFrame(cu, index=i).T
unique_dim_df.replace(to_replace=[None], value=np.nan, inplace=True)

    
#check data check
supp_brand_list=final_display_B[final_display_B["Category"]==category].Brand.unique().tolist()
unique_brand_list=unique_dim_df.BRAND.unique().tolist()
if np.nan in unique_brand_list:
    unique_brand_list.remove(np.nan)         
if len(list(set(unique_brand_list)-set([x.upper() for x in supp_brand_list])))>0:
    df2 = {'question':"check number of brands in datapull and support files",'check_satus':"Fail",'comment':'check number of brands exceeds in datapull than support file'}
else:
    df2 = {'question':"check number of brands in datapull and support files",'check_satus':"pass",'comment':''}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)           

supp_manuf_list=final_display_M[final_display_M["Category"]==category].Manufacturer.unique().tolist()
unique_manuf_list=unique_dim_df.MANUFACTURER.unique().tolist()
if np.nan in unique_manuf_list:
    unique_manuf_list.remove(np.nan) 
if len(list(set(unique_manuf_list)-set([x.upper() for x in supp_brand_list])))>0:
    df2 = {'question':"check number of manufacturer in datapull and support files",'check_satus':"Fail",'comment':'check number of manufacturer exceeds in datapull than support file'}
else:
    df2 = {'question':"check number of manufacturer in datapull and support files",'check_satus':"pass",'comment':''}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)
   
supp_cat_list=final_display_CC.Category.unique().tolist()
unique_cat_list=unique_dim_df.CATEGORY.unique().tolist()
if np.nan in unique_cat_list:
    unique_cat_list.remove(np.nan) 

if len(list(set(unique_cat_list)-set([str(x).upper() for x in supp_cat_list])))>0:
    df2 = {'question':"check number of category in datapull and support files",'check_satus':"Fail",'comment':'check number of category exceeds in datapull than support file'}
else:
    df2 = {'question':"check number of category in datapull and support files",'check_satus':"pass",'comment':''}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)
   
supp_can_list=final_display_CC.Channel.unique().tolist()
unique_can_list=unique_dim_df.CHANNEL.unique().tolist()
if np.nan in unique_can_list:
    unique_can_list.remove(np.nan)

if len(list(set(unique_can_list)-set([str(x).upper() for x in supp_can_list])-set([None,"TOTAL"])))>0:
    df2 = {'question':"check number of Channel in datapull and support files",'check_satus':"Fail",'comment':'check number of category exceeds in datapull than support file'}
else:
    df2 = {'question':"check number of Channel in datapull and support files",'check_satus':"pass",'comment':''}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)

supp_period_list=period_display.keys()
unique_period_list=unique_dim_df['TIME PERIOD'].unique().tolist()
if np.nan in unique_period_list:
    unique_period_list.remove(np.nan)    
if len(list(set(unique_period_list)-set([str(x).upper() for x in supp_period_list])))>0:
    df2 = {'question':"check number of period in datapull and support files",'check_satus':"Fail",'comment':'check number of category exceeds in datapull than support file'}
else:
    df2 = {'question':"check number of period in datapull and support files",'check_satus':"pass",'comment':''}
QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)

if forexchange["Value in local Currency"][0] in  col_list_all and forexchange["Volume in Local Currency"][0] in  col_list_all:
    df2 = {'question':"check val vol column in dataframe",'check_satus':"Fail",'comment':''}
else:
    df2 = {'question':"check val vol column in dataframe",'check_satus':"pass",'comment':'Val or vol column name differe in raw'}

QC_df_datacheck=QC_df_datacheck.append(df2,ignore_index = True)
channels=NDWD_TDP_DF['CHANNEL'].unique().tolist()
Periods=NDWD_TDP_DF['TIME PERIOD'].unique().tolist()
formats=NDWD_TDP_DF['SEGMENT'].unique().tolist()
formats.remove('TOTAL')

time2=time.time()
#Function to calculate share
def share(df,channels,Periods):
    df_2=pd.DataFrame()
    for i in channels:
        for j in Periods:
            df_1=df[(df['CHANNEL']==i) & (df['TIME PERIOD']==j)]          
            df_1=df_1.groupby(['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'])['VALUE','VOLUME','TDP'].sum()
            df_1=df_1.apply(lambda x: x/x.sum()).reset_index()
            df_2=df_2.append(df_1,ignore_index=True,sort=True)
    return df_2

def share1(df,channels,formats,Periods):
    df_2=pd.DataFrame()
    for i in channels:
        for k in formats:
            for j in Periods:
                df_1=df[(df['CHANNEL']==i) & (df['TIME PERIOD']==j) & (df['SEGMENT']==k)]          
                df_1=df_1.groupby(['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'])['VALUE','VOLUME','TDP'].sum()
                df_1=df_1.apply(lambda x: x/x.sum()).reset_index()
                df_2=df_2.append(df_1,ignore_index=True,sort=True)
    return df_2
    
#create empty dataframe
df1=pd.DataFrame(columns=['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD',
                          'VALUE','VOLUME','TDP'])
df2=df1.copy()
df3=df1.copy()
df4=df1.copy()
df5=df1.copy()
df6=df1.copy()
time3=time.time()

share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT']=='TOTAL') & (NDWD_TDP_DF['MANUFACTURER'] !='TOTAL') & (NDWD_TDP_DF['BRAND']!='TOTAL')]
time21=time.time()

df1=share(share_df,channels,Periods)       
df1.fillna(0,inplace=True)
time4=time.time()

share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT']!='TOTAL') & (NDWD_TDP_DF['MANUFACTURER'] !='TOTAL') & (NDWD_TDP_DF['BRAND']!='TOTAL')]
df2=share1(share_df,channels,formats,Periods)       
df2.fillna(0,inplace=True)
time5=time.time()

    
share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT']=='TOTAL') & (NDWD_TDP_DF['MANUFACTURER'] !='TOTAL') & (NDWD_TDP_DF['BRAND']=='TOTAL')]
df3=share(share_df,channels,Periods)       
df3.fillna(0,inplace=True)
time6=time.time()


share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT']!='TOTAL') & (NDWD_TDP_DF['MANUFACTURER'] !='TOTAL') & (NDWD_TDP_DF['BRAND']=='TOTAL')]
df4=share1(share_df,channels,formats,Periods)       
df4.fillna(0,inplace=True)
time7=time.time()


share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT'].isin(format_1)) & (NDWD_TDP_DF['MANUFACTURER'] =='TOTAL')]
df5=share(share_df,channels,Periods)       
df5.fillna(0,inplace=True)
time8=time.time()


share_df=NDWD_TDP_DF[(NDWD_TDP_DF['SEGMENT'].isin(format_2)) & (NDWD_TDP_DF['MANUFACTURER'] =='TOTAL')]
df6=share(share_df,channels,Periods)       
df6.fillna(0,inplace=True)
time9=time.time()

all_shares=pd.concat([df1,df2,df3,df4,df5,df6],sort=False)
all_shares.rename(columns={"VOLUME":"VOLUME_SHARE","VALUE":"VALUE_SHARE","TDP":"TDP_SHARE"},inplace=True)

time10=time.time()

df_with_shares=NDWD_TDP_DF.merge( all_shares, on =['CHANNEL','CATEGORY','SEGMENT','MANUFACTURER','BRAND','TIME PERIOD'] ,how='left')
df_with_shares.drop_duplicates(inplace=True)
# df_with_shares.fillna(1,inplace=True)
df_with_shares.fillna(1,inplace=True)
df_with_shares[["VOLUME_SHARE","VALUE_SHARE", "TDP_SHARE"]] = df_with_shares[["VOLUME_SHARE","VALUE_SHARE", "TDP_SHARE"]].mul(100)


#read forexchange file to get value to multiply
try:
    df_with_shares.VALUE=df_with_shares.VALUE/forexchange["RATE"][0]
    df_with_shares.VALUE=df_with_shares.VALUE/forexchange["RATE_MUL"][0]
except:
    print("please check forexchange file there is some issue")
# df_with_shares.VOLUME=df_with_shares.VOLUME/forexchange["RATE_MUL"][0]

#calcilate AVG Price
df_with_shares['AVG_PRICE']=df_with_shares.VALUE / df_with_shares.VOLUME
df_with_shares["TIME PERIOD"].unique()

df_with_shares['VALUE1']=df_with_shares['VALUE'].shift(-1)
c=df_with_shares.apply(lambda row: (row.VALUE1-row.VALUE)/(row.VALUE)*100 if (row.VALUE!=0 and row.VALUE1!=0) or (pd.isna(row.VALUE) and pd.isna(row.VALUE1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(VALUE_GROWTH=c.shift(1))
df_with_shares.drop(['VALUE1'], axis=1, inplace=True)

#calcilate growth
# df_with_shares['VALUE_GROWTH']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.VALUE[i+1]!=0 and df_with_shares.VALUE[i]!=0) or (pd.isna(df_with_shares.VALUE[i+1]) and pd.isna(df_with_shares.VALUE[i])) :
#         df_with_shares.loc[i+1,'VALUE_GROWTH']=((df_with_shares.VALUE[i+1]-df_with_shares.VALUE[i])/df_with_shares.VALUE[i])*100
#     else:
#         pass

df_with_shares['VOLUME1']=df_with_shares['VOLUME'].shift(-1)
c=df_with_shares.apply(lambda row: (row.VOLUME1-row.VOLUME)/(row.VOLUME)*100 if (row.VOLUME!=0 and row.VOLUME1!=0) or (pd.isna(row.VOLUME) and pd.isna(row.VOLUME1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(VOLUME_GROWTH=c.shift(1))
df_with_shares.drop(['VOLUME1'], axis=1, inplace=True)

# df_with_shares['VOLUME_GROWTH']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.VOLUME[i+1]!=0 and df_with_shares.VOLUME[i]!=0) or (pd.isna(df_with_shares.VOLUME[i+1]) and pd.isna(df_with_shares.VOLUME[i])) :
#         df_with_shares.loc[i+1,'VOLUME_GROWTH']=((df_with_shares.VOLUME[i+1]-df_with_shares.VOLUME[i])/df_with_shares.VOLUME[i])*100
#     else:
#         pass
 

df_with_shares['WD1']=df_with_shares['WD'].shift(-1)
c=df_with_shares.apply(lambda row: (row.WD1-row.WD)*100 if (row.WD!=0 and row.WD1!=0) or (pd.isna(row.WD) and pd.isna(row.WD1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(WD_bps=c.shift(1))
df_with_shares.drop(['WD1'], axis=1, inplace=True)
   
# df_with_shares['WD_bps']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.WD[i+1]!=0 and df_with_shares.WD[i]!=0) or (pd.isna(df_with_shares.WD[i+1]) and pd.isna(df_with_shares.WD[i])) :
#         df_with_shares.loc[i+1,'WD_bps']=((df_with_shares.WD[i+1]-df_with_shares.WD[i]))*100
#     else:
#         pass 
 

df_with_shares['VOLUME_SHARE1']=df_with_shares['VOLUME_SHARE'].shift(-1)
c=df_with_shares.apply(lambda row: (row.VOLUME_SHARE1-row.VOLUME_SHARE)*100 if (row.VOLUME_SHARE!=0 and row.VOLUME_SHARE1!=0) or (pd.isna(row.VOLUME_SHARE) and pd.isna(row.VOLUME_SHARE1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(VOLUME_bps=c.shift(1))
df_with_shares.drop(['VOLUME_SHARE1'], axis=1, inplace=True)
   
# df_with_shares['VOLUME_bps']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.VOLUME_SHARE[i+1]!=0 and df_with_shares.VOLUME_SHARE[i]!=0) or (pd.isna(df_with_shares.VOLUME_SHARE[i+1]) and pd.isna(df_with_shares.VOLUME_SHARE[i])) :
#         df_with_shares.loc[i+1,'VOLUME_bps']=((df_with_shares.VOLUME_SHARE[i+1]-df_with_shares.VOLUME_SHARE[i]))*100
#     else:
#         pass 

df_with_shares['VALUE_SHARE1']=df_with_shares['VALUE_SHARE'].shift(-1)
c=df_with_shares.apply(lambda row: (row.VALUE_SHARE1-row.VALUE_SHARE)*100 if (row.VALUE_SHARE!=0 and row.VALUE_SHARE1!=0) or (pd.isna(row.VALUE_SHARE) and pd.isna(row.VALUE_SHARE1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(VALUE_bps=c.shift(1))
df_with_shares.drop(['VALUE_SHARE1'], axis=1, inplace=True)

# df_with_shares['VALUE_bps']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.VALUE_SHARE[i+1]!=0 and df_with_shares.VALUE_SHARE[i]!=0) or (pd.isna(df_with_shares.VALUE_SHARE[i+1]) and pd.isna(df_with_shares.VALUE_SHARE[i])) :
#         df_with_shares.loc[i+1,'VALUE_bps']=((df_with_shares.VALUE_SHARE[i+1]-df_with_shares.VALUE_SHARE[i]))*100
#     else:
#         pass


df_with_shares['TDP_SHARE1']=df_with_shares['TDP_SHARE'].shift(-1)
c=df_with_shares.apply(lambda row: (row.TDP_SHARE1-row.TDP_SHARE)*100 if (row.TDP_SHARE!=0 and row.TDP_SHARE1!=0) or (pd.isna(row.TDP_SHARE) and pd.isna(row.TDP_SHARE1)) else np.nan,axis=1)
df_with_shares=df_with_shares.assign(TDP_bps=c.shift(1))
df_with_shares.drop(['TDP_SHARE1'], axis=1, inplace=True)

# df_with_shares['TDP_bps']=np.nan
# for i in range(len(df_with_shares)-1):
#     if (df_with_shares.TDP_SHARE[i+1]!=0 and df_with_shares.TDP_SHARE[i]!=0) or (pd.isna(df_with_shares.TDP_SHARE[i+1]) and pd.isna(df_with_shares.TDP_SHARE[i])) :
#         df_with_shares.loc[i+1,'TDP_bps']=((df_with_shares.TDP_SHARE[i+1]-df_with_shares.TDP_SHARE[i]))*100
#     else:
#         pass
# df_with_shares['VALUE_GROWTH']=df_with_shares.VALUE.pct_change()*100
# df_with_shares['VOLUME_GROWTH']=df_with_shares.VOLUME.pct_change()*100
# df_with_shares['WD_bps']=df_with_shares.WD.diff()*100
# df_with_shares['VOLUME_bps']=df_with_shares.VOLUME_SHARE.diff()*100
# df_with_shares['VALUE_bps']=df_with_shares.VALUE_SHARE.diff()*100
# df_with_shares['TDP_bps']=df_with_shares.TDP_SHARE.diff()*100
    
    
#calculate API

API_df=pd.DataFrame(columns=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND',
'TIME PERIOD', 'VALUE', 'VOLUME', 'ND', 'WD', 'TDP', 'TDP_SHARE','VALUE_SHARE', 'VOLUME_SHARE',
'AVG_PRICE', 'VALUE_GROWTH','VOLUME_GROWTH', 'WD_bps', 'VOLUME_bps', 'VALUE_bps', 'TDP_bps'])

API_df2=API_df.copy()
API_df4=API_df.copy()

API_filter_df=df_with_shares[(df_with_shares['SEGMENT']=='TOTAL')]
def API(df,x,y):
    df=df[(df['CHANNEL']==x) & (df['TIME PERIOD']==y)]
    df.reset_index(inplace=True)
    df.drop(columns='index',inplace=True)
    if len(df)>0:
        df['API']=df.AVG_PRICE/df.AVG_PRICE.loc[0]
    else:
        pass
    return df
    #df['API']=df.AVG_PRICE/df.AVG_PRICE.loc[0]
    #return df

for i in channels:
    for j in Periods:
        API_df=API_df.append(API(API_filter_df,i,j),ignore_index=True,sort=True)

API_filter_df=df_with_shares[(df_with_shares['SEGMENT']!='TOTAL')]
def API2(df,x,y,z):
    df=df[(df['CHANNEL']==x) & (df['TIME PERIOD']==y) & (df['SEGMENT']==z)]
    df.reset_index(inplace=True)
    df.drop(columns='index',inplace=True)
    if len(df)>0:
        df['API']=df.AVG_PRICE/df.AVG_PRICE.loc[0]
    else:
        pass 
    #df['API']=df.AVG_PRICE/df.AVG_PRICE.loc[0]
    return df        

for i in channels:
    for j in Periods:
        for k in formats:
            API_df2=API_df2.append(API2(API_filter_df,i,j,k),ignore_index=True,sort=True)
            
API_df3=API_df2[(API_df2['MANUFACTURER']!='TOTAL')]
API_filter_df=df_with_shares[(df_with_shares['MANUFACTURER']=='TOTAL')]
for i in channels:
    for j in Periods:
        API_df4=API_df4.append(API(API_filter_df,i,j),ignore_index=True,sort=True)

API_df5=API_df4[(API_df4['SEGMENT']!='TOTAL')]
df_with_API=pd.concat([API_df,API_df3,API_df5],sort=False)
df_with_API.API=df_with_API.API*100

df_with_API=df_with_shares.merge( df_with_API, on =['BRAND', 'CATEGORY', 'CHANNEL', 'MANUFACTURER','SEGMENT', 'TIME PERIOD',
'ND', 'TDP', 'TDP_SHARE', 'TDP_bps', 'VALUE','VALUE_GROWTH', 'VALUE_SHARE', 'VALUE_bps',
'VOLUME', 'VOLUME_GROWTH','VOLUME_SHARE', 'VOLUME_bps', 'WD', 'WD_bps', 'AVG_PRICE'] ,how='left')

# df_with_API = df_with_API.replace(r'^\s*$', np.nan, regex=True)
# df_with_API['API_change']=df_with_API.API.diff()

df_with_API['API1']=df_with_API['API'].shift(-1)
c=df_with_API.apply(lambda row: (row.API1-row.API) if (row.API!=0 and row.API1!=0) or (pd.isna(row.API) and pd.isna(row.API1)) else np.nan,axis=1)
df_with_API=df_with_API.assign(API_change=c.shift(1))
df_with_API.drop(['API1'], axis=1, inplace=True)


df_with_API['AVG_PRICE1']=df_with_API['AVG_PRICE'].shift(-1)
c=df_with_API.apply(lambda row: (row.AVG_PRICE1-row.AVG_PRICE)/(row.AVG_PRICE)*100 if (row.AVG_PRICE!=0 and row.AVG_PRICE1!=0) or (pd.isna(row.AVG_PRICE) and pd.isna(row.AVG_PRICE1)) else np.nan,axis=1)
df_with_API=df_with_API.assign(AVG_PRICE_growth=c.shift(1))
df_with_API.drop(['AVG_PRICE1'], axis=1, inplace=True)

# df_with_API['AVG_PRICE_growth']=np.nan
# for i in range(len(df_with_API)-1):
#     if (df_with_API.AVG_PRICE[i+1]!=0 and df_with_API.AVG_PRICE[i]!=0) or (pd.isna(df_with_API.AVG_PRICE[i+1]) and pd.isna(df_with_API.AVG_PRICE[i])) :
#         df_with_API.loc[i+1,'AVG_PRICE_growth']=((df_with_API.AVG_PRICE[i+1]-df_with_API.AVG_PRICE[i])/df_with_API.AVG_PRICE[i])*100
#     else:
#         pass


df_with_API['ND1']=df_with_API['ND'].shift(-1)
c=df_with_API.apply(lambda row: (row.ND1-row.ND)*100 if (row.ND!=0 and row.ND1!=0) or (pd.isna(row.ND) and pd.isna(row.ND1)) else np.nan,axis=1)
df_with_API=df_with_API.assign(ND_bps=c.shift(1))
df_with_API.drop(['ND1'], axis=1, inplace=True)

# df_with_API['ND_bps']=np.nan
# for i in range(len(df_with_API)-1):
#     if (df_with_API.ND[i+1]!=0 and df_with_API.ND[i]!=0) or (pd.isna(df_with_API.ND[i+1]) and pd.isna(df_with_API.ND[i])) :
#         df_with_API.loc[i+1,'ND_bps']=((df_with_API.ND[i+1]-df_with_API.ND[i]))*100
#     else:
#         pass

# df_with_API['AVG_PRICE_growth']=df_with_API.AVG_PRICE.pct_change()*100
# df_with_API['ND_bps']=df_with_API.ND.diff()*100
df_with_API["AVG_PRICE"]=df_with_API["AVG_PRICE"]*1000 #multiplied to match with the expected output

#SISH Calculation
def SISH(df,x,y):
    df=df[(df['CHANNEL']==x) & (df['TIME PERIOD']==y)]
    df.reset_index(drop=True,inplace=True)
    df['Market_Value_Share']=df.VALUE/df.VALUE.loc[0]
    df['Market_Volume_Share']=df.VOLUME/df.VOLUME.loc[0]
    return df

df_with_SISH=pd.DataFrame(columns=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND',
        'TIME PERIOD', 'VALUE', 'VOLUME', 'ND', 'WD', 'TDP', 'TDP_SHARE',
        'VALUE_SHARE', 'VOLUME_SHARE', 'AVG_PRICE', 'VALUE_GROWTH',
        'VOLUME_GROWTH', 'WD_bps', 'VOLUME_bps', 'VALUE_bps', 'TDP_bps', 'API',
        'API_change', 'AVG_PRICE_growth', 'ND_bps'])

for i in channels:
    for j in Periods:
        df_with_SISH=df_with_SISH.append(SISH(df_with_API,i,j),ignore_index=True,sort=True)

df_with_SISH=df_with_API.merge(df_with_SISH,on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND',
        'TIME PERIOD', 'VALUE', 'VOLUME', 'ND', 'WD', 'TDP', 'TDP_SHARE',
        'VALUE_SHARE', 'VOLUME_SHARE', 'AVG_PRICE', 'VALUE_GROWTH',
        'VOLUME_GROWTH', 'WD_bps', 'VOLUME_bps', 'VALUE_bps', 'TDP_bps', 'API',
        'API_change', 'AVG_PRICE_growth', 'ND_bps'],how='left')

df_with_SISH[["Market_Value_Share","Market_Volume_Share"]] = df_with_SISH[["Market_Value_Share","Market_Volume_Share"]].mul(100)
    
df_with_SISH['SISH_Value']=(df_with_SISH['Market_Value_Share']/df_with_SISH.WD)*100
df_with_SISH['SISH_Volume']=(df_with_SISH['Market_Volume_Share']/df_with_SISH.WD)*100


# ddf=pd.read_excel("Output_Egypt_bouillon_before_formatting.xlsx")
# df=ddf[['SISH_Value',"SISH_Value_bps"]]



df_with_SISH['SISH_Value'].fillna(0,inplace=True)
df_with_SISH['SISH_Volume'].fillna(0,inplace=True)


df_with_SISH['SISH_Value1']=df_with_SISH['SISH_Value'].shift(-1)
c=df_with_SISH.apply(lambda row: (row.SISH_Value1-row.SISH_Value)*100 if (row.SISH_Value!=0 and row.SISH_Value1!=0) or (pd.isna(row.SISH_Value) and pd.isna(row.SISH_Value1)) else np.nan,axis=1)
df_with_SISH=df_with_SISH.assign(SISH_Value_bps=c.shift(1))
df_with_SISH.drop(['SISH_Value1'], axis=1, inplace=True)

# df_with_SISH['SISH_Value_bps']=np.nan
# for i in range(len(df_with_SISH)-1):
#     if (df_with_SISH['SISH_Value'][i+1]!=0 and df_with_SISH['SISH_Value'][i]!=0) or (pd.isna(df_with_SISH['SISH_Value'][i+1]) and pd.isna(df_with_SISH['SISH_Value'][i])) :
#         df_with_SISH.loc[i+1,'SISH_Value_bps']=((df_with_SISH['SISH_Value'][i+1]-df_with_SISH['SISH_Value'][i]))*100
#     else:
#         pass
# df_with_SISH.to_csv("df_with_SISH_befor.csv")

df_with_SISH['SISH_Volume1']=df_with_SISH['SISH_Volume'].shift(-1)
c=df_with_SISH.apply(lambda row: (row.SISH_Volume1-row.SISH_Volume)*100 if (row.SISH_Volume!=0 and row.SISH_Volume1!=0) or (pd.isna(row.SISH_Volume) and pd.isna(row.SISH_Volume1)) else np.nan,axis=1)
df_with_SISH=df_with_SISH.assign(SISH_Volume_bps=c.shift(1))
df_with_SISH.drop(['SISH_Volume1'], axis=1, inplace=True)

# df_with_SISH['SISH_Volume_bps']=np.nan
# for i in range(len(df_with_SISH)-1):
#     if (df_with_SISH['SISH_Volume'][i+1]!=0 and df_with_SISH['SISH_Volume'][i]!=0) or (pd.isna(df_with_SISH['SISH_Volume'][i+1]) and pd.isna(df_with_SISH['SISH_Volume'][i])) :
#         df_with_SISH.loc[i+1,'SISH_Volume_bps']=((df_with_SISH['SISH_Volume'][i+1]-df_with_SISH['SISH_Volume'][i]))*100
#     else:
#         pass
    
# df_with_SISH['SISH_Value_bps']=df_with_SISH['SISH_Value'].diff()*100
# df_with_SISH['SISH_Volume_bps']=df_with_SISH['SISH_Volume'].diff()*100

df_with_SISH['zeros'] = df_with_SISH[["VALUE","VOLUME","TDP","ND","WD"]].sum(axis=1)

# df_with_SISH['zeros']=df_with_SISH["VALUE"] + df_with_SISH.VOLUME + df_with_SISH.TDP + df_with_SISH.ND + df_with_SISH.WD
df_with_SISH=df_with_SISH[(df_with_SISH['zeros']!=0)]
df_with_SISH.reset_index(drop=True, inplace=True)
    
for i in  range(len(df_with_SISH)):
    if df_with_SISH['WD_bps'][i]==df_with_SISH['WD'][i]*100:
        df_with_SISH['WD_bps'][i]="NaN"
    else:
        pass

    if df_with_SISH['ND_bps'][i]==df_with_SISH['ND'][i]*100:
        df_with_SISH['ND_bps'][i]="NaN"
    else:
        pass

    if df_with_SISH['VOLUME_bps'][i]==df_with_SISH['VOLUME_SHARE'][i]*100:
        df_with_SISH['VOLUME_bps'][i]="NaN"
    else:
        pass

    if df_with_SISH['VALUE_bps'][i]==df_with_SISH['VALUE_SHARE'][i]*100:
        df_with_SISH['VALUE_bps'][i]="NaN"
    else:
        pass
        
    if df_with_SISH['TDP_bps'][i]==df_with_SISH['TDP_SHARE'][i]*100:
        df_with_SISH['TDP_bps'][i]="NaN" 
    else:
        pass
    if df_with_SISH['SISH_Value_bps'][i]==df_with_SISH['SISH_Value'][i]*100:
        df_with_SISH['SISH_Value_bps'][i]="NaN"
    else:
        pass
    if df_with_SISH['SISH_Volume_bps'][i]==df_with_SISH['SISH_Volume'][i]*100:
        df_with_SISH['SISH_Volume_bps'][i]="NaN"
    else:
        pass

df_with_SISH.shape

df_with_SISH_1=df_with_SISH[~df_with_SISH['TIME PERIOD'].isin(latest_Periods)]
df_with_SISH_1[['SISH_Volume_bps', 'SISH_Value_bps', 'API_change', 'TDP_bps', 'VALUE_bps','VOLUME_bps','ND_bps', 'WD_bps', 'VALUE_GROWTH', 'VOLUME_GROWTH', 'AVG_PRICE_growth']]=np.NAN           
df_with_SISH_2=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods)]
df_with_SISH=df_with_SISH_1.append(df_with_SISH_2)

df_with_SISH_3=df_with_SISH[(~df_with_SISH['TIME PERIOD'].isin(latest_Periods)) & (df_with_SISH['VALUE']==0)]
df_with_SISH_3[['VALUE_bps','VALUE_GROWTH','SISH_Value_bps','SISH_Value', 'API','API_change','AVG_PRICE_growth']]=np.NAN           
df_with_SISH_4=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods) | (df_with_SISH['VALUE']!=0) ]
df_with_SISH=df_with_SISH_3.append(df_with_SISH_4)

df_with_SISH_5=df_with_SISH[(~df_with_SISH['TIME PERIOD'].isin(latest_Periods)) & (df_with_SISH['VOLUME']==0)]
df_with_SISH_5[['VOLUME_bps','VOLUME_GROWTH','SISH_Volume','SISH_Volume_bps','API','API_change','AVG_PRICE_growth']]=np.NAN           
df_with_SISH_6=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods) | (df_with_SISH['VOLUME']!=0) ]
df_with_SISH=df_with_SISH_5.append(df_with_SISH_6)

df_with_SISH_7=df_with_SISH[(~df_with_SISH['TIME PERIOD'].isin(latest_Periods)) & (df_with_SISH['TDP']==0)]
df_with_SISH_7[['TDP_bps']]=np.NAN           
df_with_SISH_8=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods) | (df_with_SISH['TDP']!=0)]
df_with_SISH=df_with_SISH_7.append(df_with_SISH_8)

df_with_SISH_9=df_with_SISH[(~df_with_SISH['TIME PERIOD'].isin(latest_Periods)) & (df_with_SISH['ND_bps']==0)]
df_with_SISH_9[['ND_bps']]=np.NAN           
df_with_SISH_10=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods) | (df_with_SISH['ND_bps']!=0)]
df_with_SISH=df_with_SISH_9.append(df_with_SISH_10)

df_with_SISH_11=df_with_SISH[(~df_with_SISH['TIME PERIOD'].isin(latest_Periods)) & (df_with_SISH['WD_bps']==0)]
df_with_SISH_11[['WD_bps','SISH_Value_bps','SISH_Volume_bps']]=np.NAN           
df_with_SISH_12=df_with_SISH[df_with_SISH['TIME PERIOD'].isin(latest_Periods) | (df_with_SISH['WD_bps']!=0)]
df_with_SISH=df_with_SISH_11.append(df_with_SISH_12)

# df_with_SISH.to_excel('uae_hair_care_Sep_2020.xlsx')
# df_with_SISH.to_csv('uae_hair_care_Sep_2020.csv')          
              
# =============================================================================
# # Formate checking 
# =============================================================================

# 1. check 'VALUE', 'VOLUME', 'ND', 'WD', 'TDP', with raw data
absolut_df=df_with_SISH.sample(n = 1) 
absolut_df.reset_index(drop=True,inplace=True)
dict_Q = absolut_df.to_dict("list")

dict_A=NDWD_TDP_DF[(NDWD_TDP_DF["CHANNEL"]==dict_Q["CHANNEL"][0]) & (NDWD_TDP_DF["SEGMENT"]==dict_Q["SEGMENT"][0]) & (NDWD_TDP_DF["MANUFACTURER"]==dict_Q["MANUFACTURER"][0])&
                    (NDWD_TDP_DF["BRAND"]==dict_Q["BRAND"][0]) & (NDWD_TDP_DF["TIME PERIOD"]==dict_Q["TIME PERIOD"][0])]

dict_A.reset_index(drop=True,inplace=True)

    
if dict_Q["VALUE"][0]==(dict_A["VALUE"][0]/forexchange["RATE"][0])/forexchange["RATE_MUL"][0]:
    df2 = {'question':"check random value with raw and output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check random value with raw and output",'check_satus':"Fail",'comment':"Value not matching"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)
    
if dict_Q["VOLUME"][0]==(dict_A["VOLUME"][0]):
    df2 = {'question':"check random volumn with raw and output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check random volumn with raw and output",'check_satus':"Fail",'comment':"volumn not matching"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)

if dict_Q["TDP"][0]==(dict_A["TDP"][0]):
    df2 = {'question':"check random TDP with raw and output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check random TDP with raw and output",'check_satus':"Fail",'comment':"TDP not matching"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)

if dict_Q["ND"][0]==(dict_A["ND"][0]):
    df2 = {'question':"check random ND with raw and output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check random ND with raw and output",'check_satus':"Fail",'comment':"ND not matching"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)

if dict_Q["WD"][0]==(dict_A["WD"][0]):
    df2 = {'question':"check random WD with raw and output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check random WD with raw and output",'check_satus':"Fail",'comment':"WD not matching"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)

#2.Duplicate values to be check, if it there then there is some mistake in output
a=len(df_with_SISH)
df_with_SISH.drop_duplicates(inplace=True)
b=len(df_with_SISH)
if a==b:
    df2 = {'question':"check duplicate values in output",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"check duplicate values in output",'check_satus':"Fail",'comment':"Output is having duplicate value"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True)    

#4.Check Others private label/ Other Manufacturer should not come in manufacturer or brand
if "UNILEVER" in manuf_unwant_lst: manuf_unwant_lst.remove("UNILEVER")
dict_Q=df_with_SISH[df_with_SISH["BRAND"].isin(manuf_unwant_lst) | df_with_SISH["MANUFACTURER"].isin(manuf_unwant_lst) ]
if len(dict_Q)==0:
    df2 = {'question':"Check Others private label/ Other Manufacturer not in manufacturer or brand",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"Check Others private label/ Other Manufacturer not in manufacturer or brand",'check_satus':"Fail",'comment':"Output have some unwanted data"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True) 

#k=df_with_SISH[(df_with_SISH["MANUFACTURER"]=="TOTAL") & (df_with_SISH["BRAND"]=="TOTAL") & (df_with_SISH["CHANNEL"]=="PERFUMERIES") & (df_with_SISH["TIME PERIOD"]=="JUL 2020") & (df_with_SISH["SEGMENT"]=="Post Wash")]
#6.UnSelect 'total' in brand column and SELECT 'others' in manuf and then delete rows
df_with_SISH22=df_with_SISH[~((df_with_SISH["MANUFACTURER"]=="OTHERS") & (df_with_SISH["BRAND"]!="TOTAL"))]
df_with_SISH22.reset_index(drop=True,inplace=True)
k=df_with_SISH22[(df_with_SISH22["MANUFACTURER"]=="TOTAL") & (df_with_SISH22["BRAND"]=="TOTAL") & (df_with_SISH22["CHANNEL"]=="PERFUMERIES") & (df_with_SISH22["TIME PERIOD"]=="JUL 2020") & (df_with_SISH22["SEGMENT"]=="Post Wash")]
df_with_SISH22.loc[df_with_SISH22.SISH_Value >= 100.000000001, ['SISH_Value','SISH_Value_bps']] =  np.nan,  np.nan
df_with_SISH22.loc[df_with_SISH22.SISH_Volume >= 100.000000001, ['SISH_Volume','SISH_Volume_bps']] =  np.nan,  np.nan
a=len(df_with_SISH22)

#k=df_with_SISH22[(df_with_SISH22["MANUFACTURER"]=="TOTAL") & (df_with_SISH22["BRAND"]=="TOTAL") & (df_with_SISH22["CHANNEL"]=="PERFUMERIES") & (df_with_SISH22["TIME PERIOD"]=="JUL 2020") & (df_with_SISH22["SEGMENT"]=="Post Wash")]

#If Val/vol SISH is more than 100 than delete value/vol of SISH & Its Bps.
# df_with_SISH23=df_with_SISH22[df_with_SISH22["SISH_Value"]>100]
# df_with_SISH23[['SISH_Value','SISH_Value_bps']] = np.nan
# df_with_SISH24=df_with_SISH22[df_with_SISH22["SISH_Value"]<=100]
# if len(df_with_SISH23)>0:
#     df_with_SISH22=df_with_SISH23.append(df_with_SISH24,ignore_index = True)

# df_with_SISH23=df_with_SISH22[df_with_SISH22["SISH_Volume"]>100]
# df_with_SISH23[['SISH_Volume','SISH_Volume_bps']] = np.nan
# df_with_SISH24=df_with_SISH22[df_with_SISH22["SISH_Volume"]<=100]
# if len(df_with_SISH23)>0:
#     df_with_SISH22=df_with_SISH23.append(df_with_SISH24, ignore_index = True)
    
#7.Delete TDP, ND,WD and SISH (value,bps,share) for other manufacturer and Unilever Other brand.
df_with_SISH23=df_with_SISH22[(df_with_SISH22["MANUFACTURER"].isin(["OTHERS"]))]
df_with_SISH23[['TDP','TDP_SHARE','TDP_bps','ND','ND_bps','WD','WD_bps','SISH_Value','SISH_Value_bps','SISH_Volume','SISH_Volume_bps']] = np.nan
df_with_SISH24=df_with_SISH22[~(df_with_SISH22["MANUFACTURER"].isin(["OTHERS"]))]

if len(df_with_SISH23)>0:
    df_with_SISH22=df_with_SISH23.append(df_with_SISH24)

df_with_SISH23=df_with_SISH22[(df_with_SISH22["MANUFACTURER"].isin(["UNILEVER"])) & (df_with_SISH22["BRAND"]=="OTHERS")]
df_with_SISH23[['TDP','TDP_SHARE','TDP_bps','ND','ND_bps','WD','WD_bps','SISH_Value','SISH_Value_bps','SISH_Volume','SISH_Volume_bps']] = np.nan
df_with_SISH24=df_with_SISH22[~(df_with_SISH22["MANUFACTURER"].isin(["UNILEVER"]) & (df_with_SISH22["BRAND"]=="OTHERS"))]

if len(df_with_SISH23)>0:
    df_with_SISH22=df_with_SISH23.append(df_with_SISH24)
b=len(df_with_SISH22)

if a==b:
    df2 = {'question':"checking length of df befor and after deleting cell",'check_satus':"Pass",'comment':""}
else:
    df2 = {'question':"checking length of df befor and after deleting cell",'check_satus':"Fail",'comment':"Output is not matching, need to check"}
QC_df_calculation=QC_df_calculation.append(df2,ignore_index = True) 
    
#8.IF TDP is absolute 1 then delete those cells
df_with_SISH22.loc[df_with_SISH22['TDP']==1, 'TDP'] = np.nan

#5. Delete the "inf" and  '-inf' error that comes under data mostly it is there in Value, Volume and Avg Price growth columns.
df_with_SISH22.replace([np.inf, -np.inf], np.nan, inplace=True) 

#6. Value data to be multiplied by 1000. we have not done any multi and division in volumn kpi
df_with_SISH22["VALUE"]=df_with_SISH22["VALUE"]*1000
# df_with_SISH22["VOLUME"]=df_with_SISH22["VOLUME"]*1000

#9. We have to select Mat TY and in manufacture unselect Unilever and total and others 
#then we have to unselect total in brand and select value share less than 1% 
#then we have to delete those combinations from all time periods

sc_list=["BARS","BAR SOAP","BARS SOAP","TOILET SOAPS - BAR","TOILET SOAPS BAR","BAR SOAPS","SHOWER GEL","SHOWER","SHOWER GEL AND FOAM BATHS","SHOWER GEL",
"SHOWER GEL AND FOAM BATH","SHOWER GEL AND BATHS","SHOWER GEL AND BATH",
"Bar Soap","Shower Gel","Bars","Bars Soap","Toilet Soaps - Bar","Bar Soaps","Shower Gel","Shower","Shower Gel and Foam Baths","Shower Gel and foam Baths"
]

if df_with_SISH22["CATEGORY"].str.contains('SKIN|TOILET',regex=True).any():
    df_with_SISH23=df_with_SISH22[(df_with_SISH22["TIME PERIOD"]=="MAT TY") & ~(df_with_SISH22["MANUFACTURER"].isin(["OTHERS","UNILEVER","TOTAL","COLGATE PALMOLIVE"])) 
                              & ~(df_with_SISH22["BRAND"].isin(["TOTAL"])) & (df_with_SISH22["VALUE_SHARE"]<1)]
    df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND']]
    df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left', indicator=True)
    df_with_SISH22=df_with_SISH22[(df_with_SISH22["_merge"]=="left_only")]
    # df_with_SISH22.to_csv("df_with_SISH22_chk.csv")
    #unselecting bar shower gel
    # df_with_SISH22['SEGMENT'] = df_with_SISH22['SEGMENT'].apply(lambda name : name.upper())
    df_with_SISH22.reset_index(drop=True,inplace=True)
    df_with_SISH23=df_with_SISH22[(df_with_SISH22["TIME PERIOD"]=="MAT TY") & ~(df_with_SISH22["SEGMENT"].isin(sc_list)) 
                                  & (df_with_SISH22["MANUFACTURER"].isin(["COLGATE PALMOLIVE"])) 
                              & ~(df_with_SISH22["BRAND"].isin(["TOTAL"])) & (df_with_SISH22["VALUE_SHARE"]<1)]
    df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND']]
    df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left', indicator="exist")
    df_with_SISH22=df_with_SISH22[(df_with_SISH22["exist"]=="left_only")]
    # df_with_SISH22.to_csv("df_with_SISH22_chk1.csv")
    #selecting bar shower gel
    df_with_SISH23=df_with_SISH22[(df_with_SISH22["TIME PERIOD"]=="MAT TY") & (df_with_SISH22["SEGMENT"].isin(sc_list)) 
                                  & (df_with_SISH22["MANUFACTURER"].isin(["COLGATE PALMOLIVE"])) 
                              & ~(df_with_SISH22["BRAND"].isin(["TOTAL","PALMOLIVE"])) & (df_with_SISH22["VALUE_SHARE"]<1)]
    df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND']]
    df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left', indicator="exist1")
    df_with_SISH22=df_with_SISH22[(df_with_SISH22["exist1"]=="left_only")]  
    df_with_SISH22=df_with_SISH22.drop(["exist","exist1"], axis=1)
    # df_with_SISH22.to_csv("df_with_SISH22_chk3.csv")   
else:
    df_with_SISH23=df_with_SISH22[(df_with_SISH22["TIME PERIOD"]=="MAT TY") & ~(df_with_SISH22["MANUFACTURER"].isin(["OTHERS","UNILEVER","TOTAL"])) 
                              & ~(df_with_SISH22["BRAND"].isin(["TOTAL"])) & (df_with_SISH22["VALUE_SHARE"]<1)]

    df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND']]
    df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left', indicator="_merge")

    df_with_SISH22=df_with_SISH22[(df_with_SISH22["_merge"]=="left_only")]

#10 brand shorting for 37 cell except tea we need to improvides this in tea category
# df_with_SISH23=df_with_SISH22[(df_with_SISH22["Time Period"]=="MAT TY")]
# df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND','VALUE']]

# df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left', indicator=True)

   

df_with_SISH23=df_with_SISH22[(df_with_SISH22["TIME PERIOD"]=="MAT TY")]
df_with_SISH23=df_with_SISH23[['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND','VALUE']]

df_with_SISH22=df_with_SISH22.merge(df_with_SISH23, on=['CHANNEL', 'CATEGORY', 'SEGMENT', 'MANUFACTURER', 'BRAND'], how='left')

df_with_SISH22.rename(columns={'VALUE_y': "Brand Order",'VALUE_x': "VALUE"}, inplace=True)


#manufacturer, brand,period,Category Channel and  segment lable change

df_with_SISH22['MANUFACTURER']=df_with_SISH22['MANUFACTURER'].replace({k.upper(): v for k, v in final_display_M_dict.items() if type(k)==str})           
df_with_SISH22['BRAND']=df_with_SISH22['BRAND'].replace({k.upper(): v for k, v in final_display_B_dict.items() if type(k)==str})           
df_with_SISH22['TIME PERIOD']=df_with_SISH22['TIME PERIOD'].replace({k.upper(): v for k, v in period_display.items() if type(k)==str})
df_with_SISH22['CATEGORY']=df_with_SISH22['CATEGORY'].replace({k.upper(): v for k, v in final_display_CA_dict.items() if type(k)==str})           
df_with_SISH22['CHANNEL']=df_with_SISH22['CHANNEL'].replace({k.upper(): v for k, v in final_display_CH_dict.items() if type(k)==str})           
df_with_SISH22['SEGMENT']=df_with_SISH22['SEGMENT'].replace(seg_display_dict)  # no need to make upper case in datapull and display dict file        

#change column name and put in sequences

dimension_map_dict = dict(zip(dimension_map['Current_dim'], dimension_map['Output_dim']))
dimension_sort_dict = dict(zip(dimension_map['Output_dim'], dimension_map['Column_sort']))
dimension_sort2_dict = dict(zip(dimension_map['Column_sort'], dimension_map['Output_dim']))

df_with_SISH22['Market']=country
df_with_SISH22=df_with_SISH22.drop(['Market_Value_Share', 'Market_Volume_Share','zeros','_merge'], axis=1)
df_with_SISH22 = df_with_SISH22.rename(columns = dimension_map_dict)
df_with_SISH22 = df_with_SISH22.rename(columns=dimension_sort_dict).sort_index(axis=1)
df_with_SISH22 = df_with_SISH22.rename(columns=dimension_sort2_dict)
    
#we are deleting the row which contains the "shampoo delete" in segment column
df_with_SISH22=df_with_SISH22[~df_with_SISH22['Segment'].str.contains('delete')]
# print(df_with_SISH22)
# 11 SISH volume/Sish volume bps should not come for others Manufacturer/others
#df_with_SISH22=df_with_SISH22[(df_with_SISH22["MANUFACTURER"]=="OTHERS")& ~(df_with_SISH22["SISH_Volume"])& ~(df_with_SISH22["SISH_Volume_bps"])]

#print(df_with_SISH22)
# Growth/bps should not come for P12w ty

df_with_SISH24=df_with_SISH22[(df_with_SISH22['Time Period']=="P12W TY")]
df_with_SISH24[['Value Growth','Volume Growth','Value bps','Volume bps','TDP bps','Avg Price growth','API bps','ND bps','WD bps','SISH Value bps','SISH Volume bps']]= np.nan
df_with_SISH25=df_with_SISH22[(df_with_SISH22['Time Period']!="P12W TY")]
df_with_SISH22=df_with_SISH25.append(df_with_SISH24,ignore_index = True)


# If data are present in current year and not present in last year then will not show growth/bps for resepective KPI.
    
# latest_Periods=df_with_SISH22["Time Period"].unique() 
# latest_Periods = [col for col in latest_Periods if ("FY" in col) or ("TY" in col)]
# #print(latest_Periods)
# latest_Periods_2= [col for col in latest_Periods if ("FY" in col)]
# #print(latest_Periods_2)
# if len (latest_Periods_2)==2:
#     latest_Periods_2.sort(reverse=True)
#     latest_Periods=list(set(latest_Periods)-set([latest_Periods_2[1]]))
#     #print(latest_Periods)
# else:
#     pass
      
# df_with_SISH26=df_with_SISH22[df_with_SISH22["Time Period"].isin(latest_Periods)]
# #df_with_SISH26=df_with_SISH22[df_with_SISH22["Time Period"].isin(["P12W TY","L12W TY","L12W TY Vs P12W TY","MAT TY","YTD TY"])]
# df_with_SISH26[['Value Growth','Volume Growth','Value bps','Volume bps']]= np.nan
# df_with_SISH27=df_with_SISH22[~df_with_SISH22["Time Period"].isin(latest_Periods)]
# df_with_SISH22=df_with_SISH26.append(df_with_SISH27,ignore_index = True)
# #print(df_with_SISH26)
# # If data are present in last year and not present in current year then will not show growth/bps for resepective KPI.
# pre_Periods=df_with_SISH22["Time Period"].unique() 
# pre_Periods = [col for col in pre_Periods if ("FY" in col) or ("LY" in col)]
# #print(pre_Periods)
# pre_Periods_2= [col for col in pre_Periods if ("FY" in col)]
# #print(pre_Periods_2)
# if len (pre_Periods_2)==2:
#     pre_Periods_2.sort(reverse=True)
#     pre_Periods=list(set(pre_Periods)-set([pre_Periods_2[0]]))
#     #print(pre_Periods)
# else:
#     pass

# df_with_SISH28=df_with_SISH22[df_with_SISH22["Time Period"].isin(pre_Periods)]
# df_with_SISH28[['Value Growth','Volume Growth','Value bps','Volume bps']]= np.nan
# df_with_SISH29=df_with_SISH22[~df_with_SISH22["Time Period"].isin(pre_Periods)]
# df_with_SISH22=df_with_SISH28.append(df_with_SISH29,ignore_index = True)
#df_with_SISH22.to_excel("formated_output.xlsx")

# SISH volume/Sish volume bps should not come for others Manufacturer/others brand 
    
#df_with_SISH30=df_with_SISH22[(df_with_SISH22["Manufacturer"].isin(["OTHERS"]))]
#df_with_SISH31=df_with_SISH22[(df_with_SISH22["Brand"].isin(["Others"]))]
df_with_SISH30=df_with_SISH22[(df_with_SISH22["Manufacturer"]=="OTHERS") | (df_with_SISH22["Brand"]=="Others")]
df_with_SISH30[['SISH Volume','SISH Volume bps']]= np.nan
df_with_SISH31=df_with_SISH22[(df_with_SISH22["Manufacturer"]!="OTHERS") & (df_with_SISH22["Brand"]!="Others")]
df_with_SISH22=df_with_SISH30.append(df_with_SISH31,ignore_index = True)
#print(df_with_SISH27)

time13=time.time()

# channel delete
if df_with_SISH22["SmallC"].str.lower().unique().tolist()[0] in ['deodorants', 'hair care','shampoo','shampoos','deodorant'] and country.lower() in ["lebanon","lebanons","leba non","Leba-nons"]:
    df_with_SISH37=df_with_SISH22[~(df_with_SISH22['Channel'].isin(["CHIANS\\SUPER\\SELF","CHIANS\SUPER\SELF","CHIAN\\SUPER\\SELF","CHIAN\SUPER\SELF","CHAINS\\SUPER\\SELF","CHAINS\SUPER\SELF"]))]
    df_with_SISH22 = df_with_SISH37.reset_index(drop=True)
  
jj=len(df_with_SISH22)    
# deleting zero row  TDP ND WD VOL VAL
df_with_SISH35=df_with_SISH22[~((df_with_SISH22["TDP"].isin([0,"NAN"])) & (df_with_SISH22["ND"].isin([0,"NAN"])) & (df_with_SISH22["WD"].isin([0,"NAN"])) & (df_with_SISH22["Value Euro"].isin([0,"NAN"])) & (df_with_SISH22["Volume Kg"].isin([0,"NAN"])))]
df_with_SISH22 = df_with_SISH35.reset_index(drop=True)
kk=len(df_with_SISH22)
  
# deleting zero cell value TDP ND WD VOL VAL and related kpi
df_with_SISH22.loc[df_with_SISH22["Value Euro"].isin([0,'NAN']), ["Value Euro",'Avg Price Euro/Kg','API',
                  'Value Share','Value Growth','Value bps','Avg Price growth','SISH Value',
                  'API bps','SISH Value bps']] =  np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan
df_with_SISH22.loc[df_with_SISH22["Volume Kg"].isin([0,'NAN']),["Volume Kg",'Avg Price Euro/Kg','API',
                  'Volume Share','Volume Growth','Volume bps','Avg Price growth','SISH Volume',
                  'API bps','SISH Volume bps']] =  np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan
df_with_SISH22.loc[df_with_SISH22["TDP"].isin([0,'NAN']), ["TDP",'ND bps']] =  np.nan,np.nan
df_with_SISH22.loc[df_with_SISH22["ND"].isin([0,'NAN']), ["ND",'SISH Value','SISH Value bps']] = np.nan,  np.nan,  np.nan
df_with_SISH22.loc[df_with_SISH22["WD"].isin([0,'NAN']), ["WD",'WD bps','SISH Value','SISH Volume','SISH Value bps','SISH Volume bps']] = np.nan, np.nan,  np.nan,np.nan,  np.nan,np.nan

 

   
time12=time.time()

# export file in excel

path = 'output_'+country+'_'+category
if not os.path.isdir(path): os.makedirs(path)
QC_df_calculation.to_excel(os.path.join(path,r"QC_Log_Calculation_"+country+"_"+category+".xlsx"))
QC_df_datacheck.to_excel(os.path.join(path,r"QC_Log_data_check_"+country+"_"+category+".xlsx"))
df_with_SISH.to_excel(os.path.join(path,r"Output_"+country+'_'+category+'_before_formatting.xlsx'))
df_with_SISH22.to_excel(os.path.join(path,r"Output_"+country+'_'+category+'.xlsx'))          
unique_dim_df.to_excel(os.path.join(path,r"Unique_dim_"+country+'_'+category+'.xlsx'))

time11=time.time()
print("total time data merge",time2-time1)
print("share function",time3-time2)
print("share1 cal",time4-time3)
print("share2 cal",time5-time4)
print("share3 cal",time6-time5)
print("df filter for share cal",time21-time3)
print("share4 cal",time7-time6)
print("share5 cal",time8-time7)
print("share6 cal",time9-time8)
print("api cal",time10-time9)
print("formatting cal",time11-time10)
#print("delete time",time12-time13)
print("total time",time11-time1)
    # return 

# main_fun(country,category,channel,format_1,format_2,format_3)