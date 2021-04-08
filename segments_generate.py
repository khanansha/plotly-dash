# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 18:38:44 2021

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

# =============================================================================
# ### Taking Input
# =============================================================================

# country="UAE"
# category="HAIR CARE"
# channel='TOTAL UAE'
# format_1="SUB CATEGORY"
# format_2="SEGMENT"
# format_3=""
# SDESC_1=""

# for FAB SOL
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

# country=country.upper()
# category=category.upper()
# channel=channel.upper()
# format_1=format_1.upper()
# format_3=format_3.upper()
# format_2=format_2.upper()

# country="Egypt"
# category="TOOTHPASTE(TP)"
# channel='TOTAL EGYPT'
# format_1="PLATFORM"
# format_2="TYPE"
# format_3=""

def segment_generater_fun(country,category,channel,format_1,format_2,format_3):
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
    col_name_dict={"category":"CATEGORY","CATEGORYs":"CATEGORY","Catrgories":"CATEGORY","Channel/Market":"MARKET",
    "channel/market":"MARKET","channel/markets":"MARKET","Channels":"MARKET","Channel":"MARKET",
    "channels/markets":"MARKET","market":"MARKET","market":"MARKET","Manufacturer":"MANUFACTURER",
    "Manufacturers":"MANUFACTURER","MANUFACTURERs":"MANUFACTURER","brand owner":"MANUFACTURER",
    "Brand Owner":"MANUFACTURER","Brand owner":"MANUFACTURER","BRAND OWNER":"MANUFACTURER","BRANDS":"BRAND","Brand":"BRAND","Brands":"BRAND",
    "Sub Brands":"SUB BRANDS","Sub Brand":"SUB BRANDS",format_1:"FORMAT_1",format_2:"FORMAT_2",format_3:"FORMAT_3",
    "SDESC":"CHANNEL","SDESC.1":"SDESC1","SDESC2":"TIME PERIOD","SDESC.2":"TIME PERIOD",
    "Sales Value 1000 AED":"VALUE","Sales Value 100000":"VALUE","Sales Volume 1000 Kgs":"VOLUME",
    "Sales Volume 1000 KGS":"VOLUME","Sales Volume 1000 LTRS":"VOLUME","AVG ND":"ND","Avg WD":"WD",
    "Avg ND":"ND","AVG WD":"WD","Avg. ND":"ND","Avg. WD":"WD","Sales Value 1000000 LE":"VALUE","Sales Volume1000 Kgs":"VOLUME",
    "Sales Value 100000SR":"VALUE","Sales Volume 1000 Ltrs":"VOLUME","TOOTHPASTES":"CATEGORY",
    "Sales Volume 1000KG":"VOLUME","Sales Value 100.000MAD":"VALUE",
    "Sales Volume 1000 Kgs/Ltrs":"VOLUME","Sales Value 100000 SR":"VALUE","Sales Value (1MLL)":"VALUE","Sales Volume (1000 KG)":"VOLUME",
    "Sales Volume 1000 KGS/LTRS":"VOLUME","MANUFACTURE":"MANUFACTURER",
    'Sales Value 1000000LE':'VALUE', 'Sales Volume 1000LTRS':'VOLUME','Sales Volume(1000 LTRS)':'VOLUME','Sales Value(1000 JD)':'VALUE','Sales Value SR 100000':'VALUE','Sales Volume (1000 LTR)':'VOLUME','Sales Value (1M LL)':'VALUE','Sales Value 100000 DA':'VALUE',"MANUFACTURER":"MANUFACTURER"}
    
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
             

    # period column format
    print('\n Segments distinct need to be map in support files: \n')        
    print(type3_df.FORMAT_1.unique())
    
    type3_df1=type3_df[type3_df["MANUFACTURER"]=="UNILEVER"]
    print('\n Unilever Brands distinct need to be map in support files: \n')        
    print(type3_df1.BRAND.unique())
    return

# segment_generater_fun(country,category,channel,format_1,format_2,format_3)