# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 19:32:20 2020

@author: satish.gupta
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 14:56:23 2020

@author: O40723
"""
# =============================================================================
# Installing libraries
# =============================================================================
import unidecode
import time
t1=time.time()
import pickle
from dateutil.parser import parse
#import datetime as datetime
import pandas as pd
import numpy as np
import re
import random
from nltk.corpus import stopwords
import nltk
from dateutil.relativedelta import relativedelta
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import traceback
import sys
import os
import pymysql
import inflect
import pyodbc
import time
import numpy as np
from nltk import flatten
t1=time.time()
pd.set_option('display.max_columns',None)
engine = inflect.engine()
lmtzr = WordNetLemmatizer()
import json
import warnings
#import numpy as np
warnings.simplefilter(action='ignore', category=FutureWarning)

# =============================================================================
#create database connection
# =============================================================================
'''database_connection'''#connection details
host='52.205.96.129'#  localhost#comment
###host='localhost'#uncomment
user='satishcoke'
password='Halliburton9832!'
#db="GLB_DM_NIELSEN_GTC_v2"
db="GLB_DM_NIELSEN_GTC_0519"
tbl='dm.JIFFY_PREP_v2'
# conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+host+';DATABASE='+db+';UID='+user+';PWD='+password+'')
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+host+';DATABASE='+db+';UID='+user+';PWD='+password+'')
# path='/var/www/html/beta/jiffycokemssql_v2/py/'
#path='/var/www/html/beta/jiffycokemssql_v3_test/py/'
#path='C:/Users/rahul.kolarkar/OneDrive - Brandscapes Consultancy Pvt. Ltd/GTC_Jiffy/Final_Local/0525/'
path=r'C:\JIFFY\jiffy_v2_test_143/'
#path=r'C:\\Users\satish.gupta\OneDrive - Brandscapes Consultancy Pvt. Ltd\Data\Big Data\Jiffy\TCCC_GTC\scripts\052320\\'
# =============================================================================
# define functions
# =============================================================================
'''word to number'''# function define
def text2int (textnum, numwords={}):
    if not numwords:
        units = [ "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", 
                 "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen","sixteen", "seventeen", "eighteen", "nineteen",]
        tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
        scales = ["hundred", "thousand", "million", "billion", "trillion"]
        numwords["and"] = (1, 0)
        for idx, word in enumerate(units):  numwords[word] = (1, idx+1)
        for idx, word in enumerate(tens):       numwords[word] = (1, idx * 10)
        for idx, word in enumerate(scales): numwords[word] = (10 ** (idx * 3 or 2), 0)
    ordinal_words = {'first':1, 'second':2, 'third':3, 'fifth':5, 'eighth':8, 'ninth':9, 'twelfth':12}
    ordinal_endings = [('ieth', 'y')]
    current = result = 0
    curstring = ""
    onnumber = False
    for word in textnum.split():
        if word in ordinal_words:
            scale, increment = (1, ordinal_words[word])
            current = current * scale + increment
            if scale > 100:
                result += current
                current = 0
            onnumber = True
        else:
            for ending, replacement in ordinal_endings:
                if word.endswith(ending):
                    word = "%s%s" % (word[:-len(ending)], replacement)

            if word not in numwords:
                if onnumber:
                    curstring += repr(result + current) + " "
                curstring += word + " "
                result = current = 0
                onnumber = False
            else:
                scale, increment = numwords[word]

                current = current * scale + increment
                if scale > 100:
                    result += current
                    current = 0
                onnumber = True
    if onnumber:
        curstring += repr(result + current)
    return curstring


'''json creation''' #This function is used to create final json (draw_graph)
def draw_graph(df,title1,kind,xlabel,ylabel = 'Value',caption='',infer=''):
    global details_dict
    if len(df.index) < 1:
        details_dict["error"]=random.choice(error_list)
        details_dict = str(details_dict)
        for word, initial in unique_change.items():
            if len(re.findall(r'\W'+word+'\W',' '+details_dict+' '))>0:
                details_dict = details_dict.replace(word, initial)
            else:
                pass  
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
        print(str(details_dict))
    elif (df.columns[-1]) == 'number':
        details_dict["caption"]=caption
        details_dict = str(details_dict)
        for word, initial in unique_change.items():
            if len(re.findall(r'\W'+word+'\W',' '+details_dict+' '))>0:
                details_dict = details_dict.replace(word, initial)
            else:
                pass   
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")           
        print(str(details_dict))        
        
    elif (len(df.index) >=1):
        details_dict["file"]=df.to_json(orient='split')
        details_dict["title"]=title1
        details_dict["xlabel"]=xlabel
        details_dict["kind"]=kind
        details_dict["ylabel"]=ylabel
        details_dict["caption"]=caption  
        details_dict["infer"]=infer  
#        if kind=='waterfall':
#            details_dict["file"]=details_dict.get("file").replace("}"," , 'datavalue'=['5','4','3','2','1']}")
#        else:
#            pass
        details_dict = str(details_dict)
        for word, initial in unique_change.items():
            if len(re.findall(r'\W'+word+'\W',' '+details_dict+' '))>0:
#                print(word,":",initial)
                details_dict = details_dict.replace(word, initial)
            else:
                pass
            
        if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
            details_dict = details_dict.replace('Share', 'Share')
        elif (dim3=='Product_Category' or dim3=='Geography_Name') and (measure1==['share'] or measure3==['mix']):
            details_dict = details_dict.replace('Share', 'Mix')
        else:
            pass     
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")        
        print(str(details_dict))
    elif len(df.index) == 1:
        details_dict["caption"]=caption
        details_dict = str(details_dict)
                  
        for word, initial in unique_change.items():
            if len(re.findall(r'\W'+word+'\W',' '+details_dict+' '))>0:
                details_dict = details_dict.replace(word, initial)
            else:
                pass   
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")           
        print(str(details_dict))
    else:
        details_dict = str(details_dict)                   
        for word, initial in unique_change.items():
            if len(re.findall(r'\W'+word+'\W',' '+details_dict+' '))>0:
                details_dict = details_dict.replace(word, initial)
            else:
                pass
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")           
        print(str(details_dict))

'''prioritise_industry'''#This function is used to get Industry bar to first position with respect to all manufacturers             
def rc_ind(data):
    if input_Dim_lst1[0]=='Product_Company' and 'industry' in data.values.reshape(-1):
        condition = data[input_Dim_lst1[0]] =='industry'
        excluded = data[condition]
        included = data[~condition]
        data=pd.concat([excluded,included])
    elif input_Dim_lst1[0]=='Product_Company' and 'industry' in data.index:
        condition = data.index =='industry'
        excluded = data[condition]
        included = data[~condition]
        data=pd.concat([excluded,included])        
    elif input_Dim_lst1[0]!='Product_Company' and 'industry' in data.columns:
        condition = data.columns =='industry'
        excluded = data[data.columns[condition]]
        included = data[data.columns[~condition]]
        data=pd.concat([excluded,included],axis=1)
    else:
        pass
    return data        

'''filter_comma'''#This function is used to add ',' & 'and' between filter elements
def inference_comma(x):
    temp=""
    if len(x) != 0:
        if len(x) == 1:
            temp = ', '.join(x)
        if len(x) == 2:
            temp = ' and '.join(x)
        if len(x) >= 3:
            temp=x[0]
            for i in range(1, len(x)):
                if (i < len(x)-1):
                    temp = temp+', '+x[i]
                if (i == len(x)-1):
                    temp = temp + " and " + x[i]
    return temp

'''calculate_contribution'''
def contri_percent(data):
    if 23 < data <27:
        contrib=["(1/4)th"]
    elif 48 < data <52:
        contrib=["(1/2)"]
    elif 73 < data <78:
        contrib=["(3/4)th"]
    elif 31 < data <35:
        contrib=["(1/3)rd"]
    elif 64 < data <68:
        contrib=["(2/3)rd"]
    else:
        contrib=[]
    return contrib

# =============================================================================
'''unique_lists'''#These lists are used to get unique elements 
# =============================================================================
month_list = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
major_brand_list=pickle.load(open(path+"support_file/major_brand_list.p","rb"))
major_company_list=pickle.load(open(path+"support_file/major_company_list.p","rb"))
major_trademark_list=pickle.load(open(path+"support_file/major_trademark_list.p","rb"))
country_list= pickle.load(open(path+"support_file/country_list.p","rb"))

bu_list = ['mexicobusinessunit', 'latincenterbusinessunit', 'westerneuropebusinessunit',
'turkeyccabusinessunit', 'usabusinessunit', 'centralandeasterneuropebusinessunit',
'indiasouthwestasiabusinessunit', 'greaterchinaandkoreabusinessunit', 'southlatinbusinessunit',
'middleeastnorthafricabusinessunit', 'aseanbusinessunit','japanbusinessunit', 'westafricabusinessunit',
'southpacificbusinessunit', 'brazilbusinessunit','canadabusinessunit', 
'southandeastafricabusinessunit','all bu']

og_list = ['asiapacific', 'northamerica', 'emea', 'latinamerica', 'all og']
fo_list= pickle.load(open(path+"support_file/fo_list.p","rb"))
#top40_list = ['world']
top40_lst=pickle.load(open(path+"support_file/top40_lst.p","rb"))
cluster_type_list =['franchise', 'operating group', 'business unit', 'country', 'world']
manuf_list= pickle.load(open(path+"support_file/manuf_list.p","rb"))
category_list= pickle.load(open(path+"support_file/category_list.p","rb"))
brand_list=pickle.load(open(path+"support_file/brand_list.p","rb"))
brand_list.insert(0,"musclemilk")
if 'top' in brand_list:
    brand_list.remove('top')
trademark_list=pickle.load(open(path+"support_file/trademark_list.p","rb"))
t40_lst_agg=pickle.load(open(path+"support_file/t40_lst_agg.p","rb"))
consumption_list = ['futureconsumption','immediateconsumption','unassigned','others','all consumption'] 
refillable_list = ['refillable','nonrefillable','unassigned', 'others','all refillable']   
channel_list = ["traditional","conveniencepetrolgas", "onpremise","otheroffpremise","otherchannel",
"hypermarketssupermarketsdiscounters",'others','all channel']

container_list = ['bagnbox', 'bottle', 'brickpack', 'can', 'cup','drum','unassigned',
'gabletopcarton','jar', 'jug', 'notstated', 'other','pouch', 'tank', 'tube','others', 'all container']

material_list = ['aluminium', 'cardboard', 'flexiblelaminate', 'glass','notstated', 'other', 
'paper', 'plastic', 'stainlesssteel','tin', 'unassigned', 'waxedpaperboard', 'others','all material']

packtype_list = ['singlepack','multipack','unassigned','others','all packtype']
packsize_list = pickle.load(open(path+"support_file/packsize_list.p","rb"))

random_prompt = ['What is the YTD Volume Share for Water within All Manufacturers for USA in Jun-19?',
'What is Value Growth trend for all Business Units within SSD in Dec-19?','What is MTD Volume Category mix for TCCC?']
chart_list = ['barh','bar','line']

group2=['company','brand','beverage','trademark','flavour','manufacturer']
group3=['consumption','refillable','channel','container type', 'material','packsize','packtype']
brand_genlst = ['top','jan','swing','country','brand','we','vs','dairy','brazil','france','denmark','key','price','egypt','water','5','3','1','4','2','6','10','15','may'] #dairy comes in brand, categories and manuf
other_list=['unassigned','other','all other','all other manufacturers','all other manufacturers all brands',
 'other brand','all other brand','all other beverage product','all other brand all other beverage product',
 'other flavours','all other flavours','n/a','other size','pl masking','not available','not stated',
 'brand owner not identified','not determined','not available not available']
asean_bu = ['indonesia','philippines','thailand','vietnam','malaysia','singapore','myanmar'] # 'philippinesquarterly','malaysiaquarterly','myanmarquarterly'
central_eastern_europe_bu = ['austria','bosniaandherzegovina','bulgaria','switzerland','czechrepublic',
'estonia','greece','croatia','hungary','italy','lithuania','latvia','poland','romania','serbia',
'russianfederation','slovenia','slovakia','belarus','ukraine','bahrainquarterly','italyquarterly','polandquarterly',
'switzerlandquarterly']    #'russianfederationquarterly',
greater_china_korea_bu = ['china','hongkong','taiwan', 'koreaoffpremise','koreaonpremise']
india_southwest_asia_bu = ['bangladesh','india','nepal','srilanka']
latin_centerbu = ['ecuador','costarica','domenicanrepublic','guatemala','honduras','nicaragua',
                  'panama','elsalvador']
middle_east_north_africa_bu = ['morocco','algeria','tunisia',
'egypthybrid','pakistanhybrid','saudiarabiahybrid','unitedarabemirateshybrid',
'kuwaitquarterly','omanquarterly','saudiarabiaquarterly']
south_east_africa_bu = ['southafrica','ethiopia','kenya','tanzania','zambia','zimbabwe']
south_latin_bu = ['argentina','chile','peru','bolivia','paraguay','uruguay']
south_pacific_bu = ['australia','newzealand']
turkey_cca_bu= ['turkey','kazakhstan']
west_africa_bu=['nigeria','cameroon','angola','ghana','ivorycoast']
western_eu_bu = ['belgium','germany','denmark','spain','france','greatbritain',
                 'ireland','netherlands','norway','portugal','sweden','nireland']   
brazil_bu=['brazil']
canada_bu=['canada']
japan_bu=['japan','japanindustryestimates']
mexico_bu=['mexico']
usa_bu=['puertorico','unitedstates']

asean_bu_t40=['indonesia','philippines','thailand','vietnam']
central_eastern_europe_bu_t40=['italy','poland','romania','russianfederation']
greater_china_korea_bu_t40=['china']
india_southwest_asia_bu_t40= ['india']
latin_centerbu_t40 = ['ecuador']
middle_east_north_africa_bu_t40 = ['morocco','algeria','egypthybrid','pakistanhybrid','saudiarabiahybrid']
south_east_africa_bu_t40 = ['southafrica']
south_latin_bu_t40 = ['argentina','chile','peru','bolivia']
south_pacific_bu_t40 = ['australia']
turkey_cca_bu_t40= ['turkey','kazakhstan']
west_africa_bu_t40=['nigeria']
western_eu_bu_t40 = ['belgium','germany','spain','france','greatbritain','netherlands'] #rahul2403

asiapacific_og=['australia','china','indonesia','japanindustryestimates','philippines','thailand','vietnam',
                'hongkong','malaysia','newzealand','singapore','srilanka','taiwan','bangladesh','india',
                'koreaoffpremise','koreaonpremise','myanmar',
                'nepal','newzealand']#'philipinesquarterly','malaysiaquarterly','myanmarquarterly',
emea_og=['egypthybrid','france','germany','greatbritain','italy','morocco','nigeria','pakistanhybrid','poland'
         ,'romania','russianfederation','southafrica','spain','turkey','algeria',
             'belgium','kazakhstan','netherlands','saudiarabiahybrid',
'austria','belarus','bosniaandherzegovina','bulgaria','cameroon','croatia','czechrepublic',
'denmark','estonia','greece','hungary','ireland','latvia','nireland','norway',
'portugal','serbia','slovakia','slovenia','sweden','switzerland','tunisia','angola',
'bahrainquarterly','ethiopia','ghana','italyquarterly','ivorycoast','kenya','kuwaitquarterly',
'lithuania','omanquarterly','polandquarterly',
'qatarquarterly','romaniaquarterly','saudiarabiahybrid','saudiarabiaquarterly',
'tanzania','ukraine','unitedarabemirateshybrid',
'zambia','zimbabwe']
latin_america_og=['argentina','brazil','chile','ecuador','mexico','peru','bolivia',
'costarica','domenicanrepublic','elsalvador','guatemala','honduras','nicaragua','panama','paraguay','uruguay']
north_america_og=['canada','unitedstates','puertorico']

asiapacific_og_t40=['australia','china','indonesia','japanindustryestimates','philippines','thailand','vietnam']
emea_og_t40=['egypthybrid','france','germany','greatbritain','italy','morocco','nigeria','pakistanhybrid','poland','romania','russianfederation','southafrica','spain','turkey','algeria',
             'belgium','kazakhstan','netherlands','saudiarabiahybrid']
latin_america_og_t40=['argentina','brazil','chile','ecuador','mexico','peru','bolivia']
north_america_og_t40=['canada','unitedstates']

all_bu_lst=[asean_bu,central_eastern_europe_bu,greater_china_korea_bu,india_southwest_asia_bu,
            latin_centerbu,middle_east_north_africa_bu,south_east_africa_bu,south_latin_bu,
            south_pacific_bu,turkey_cca_bu,west_africa_bu,western_eu_bu,brazil_bu,canada_bu,japan_bu,mexico_bu,usa_bu] 

all_t40_bu_lst=[asean_bu_t40,central_eastern_europe_bu_t40,greater_china_korea_bu_t40,middle_east_north_africa_bu_t40,
              south_latin_bu_t40,turkey_cca_bu_t40,western_eu_bu_t40] 
all_og_lst=[asiapacific_og,emea_og,latin_america_og,north_america_og]
all_t40_og_lst=[asiapacific_og_t40,emea_og_t40,latin_america_og_t40,north_america_og_t40] 

# =============================================================================
'''word_dict'''#This dict contains various possibilities of a word that can appear in question
# =============================================================================

cluster_name_dict = pickle.load(open(path+"support_file/cluster_name_title.p","rb"))
cluster_name_dict1= pickle.load(open(path+"support_file/cluster_name_title1.p","rb"))
cluster_name_dict2= pickle.load(open(path+"support_file/cluster_name_title3.p","rb"))
fo_code_dict=pickle.load(open(path+"support_file/fo_code_dict.p","rb"))
cluster_code_dict=pickle.load(open(path+"support_file/cluster_code_dict.p","rb"))
t40_cluster_dict=pickle.load(open(path+"support_file/t40_cluster_dict.p","rb"))  #rahul2603
t40_name_cluster=pickle.load(open(path+"support_file/t40_name_cluster.p","rb"))  #rahul2603
cluster_type_dict = {"franchise's":"franchise",'business unit':'bu','operating group':'og','franchise operations':'fo'
                     ,'busines unit':'bu','business units':'bu','operation group':'og','franchise':'fo',
                     "franchise`s":"fo"}
 
category_name_dict=pickle.load(open(path+"support_file/category_name_dict.p","rb"))

brand_manuf_trad_dict= pickle.load(open(path+"support_file/brand_manuf_trad_dict.p","rb"))
brand_manuf_trad_dict1= pickle.load(open(path+"support_file/brand_manuf_trad_dict1.p","rb"))

#manuf_dict= pickle.load(open(path+"support_file/manuf_dict.p","rb"))
#brand_dict= pickle.load(open(path+"support_file/brand_dict.p","rb"))

trademark_dict= pickle.load(open(path+"support_file/trademark_dict.p","rb"))

bu_dict= {'aseanbusinessunit':asean_bu,'centralandeasterneuropebusinessunit':central_eastern_europe_bu,'greaterchinaandkoreabusinessunit':greater_china_korea_bu,
         'indiasouthwestasiabusinessunit':india_southwest_asia_bu,'latincenterbusinessunit':latin_centerbu,
         'middleeastnorthafricabusinessunit':middle_east_north_africa_bu,'southandeastafricabusinessunit':south_east_africa_bu,
         'southlatinbusinessunit':south_latin_bu,'southpacificbusinessunit':south_pacific_bu,'turkeyccabusinessunit':turkey_cca_bu,
         'westafricabusinessunit':west_africa_bu,'westerneuropebusinessunit':western_eu_bu,
         'usabusinessunit':usa_bu}     #rahul2403

bu_dict_t40= {'aseanbusinessunit':asean_bu_t40,'centralandeasterneuropebusinessunit':central_eastern_europe_bu_t40,'greaterchinaandkoreabusinessunit':greater_china_korea_bu_t40,
         'indiasouthwestasiabusinessunit':india_southwest_asia_bu_t40,'latincenterbusinessunit':latin_centerbu_t40,
         'middleeastnorthafricabusinessunit':middle_east_north_africa_bu_t40,'southandeastafricabusinessunit':south_east_africa_bu_t40,
         'southlatinbusinessunit':south_latin_bu_t40,'southpacificbusinessunit':south_pacific_bu_t40,'turkeyccabusinessunit':turkey_cca_bu_t40,
         'westafricabusinessunit':west_africa_bu_t40,'westerneuropebusinessunit':western_eu_bu_t40}

og_dict= {'asiapacific':asiapacific_og,'northamerica':north_america_og,'emea':emea_og,'latinamerica':latin_america_og}

og_dict_t40= {'asiapacific':asiapacific_og_t40,'northamerica':north_america_og_t40,'emea':emea_og_t40,'latinamerica':latin_america_og_t40}

refillable_dict = {'non refillable':'nonrefillable','refillabel':'refillable','refilled':'refillable'}

consumption_dict = {'future consumption':'futureconsumption','immediate consumption':'immediateconsumption',
'future consume':'futureconsumption','immediate consume':'immediateconsumption',
'future consumpt':'futureconsumption','immediate consumpt':'immediateconsumption',
'future':'futureconsumption','immediate':'immediateconsumption','fc':'futureconsumption','ic':'immediateconsumption', 'icfc':'ic fc'}

channel_name_dict = {'hypermarket':'hypermarketssupermarketsdiscounters','hypermarkets':'hypermarketssupermarketsdiscounters',
'supermarket':'hypermarketssupermarketsdiscounters','supermarkets':'hypermarketssupermarketsdiscounters',
'discounters':'hypermarketssupermarketsdiscounters','discounter':'hypermarketssupermarketsdiscounters',
'hypermarketsupermarket':'hypermarketssupermarketsdiscounters','hypermarketdiscounter':'hypermarketssupermarketsdiscounters',
'supermarketdiscounter':'hypermarketssupermarketsdiscounters','convenience':'conveniencepetrolgas',
'petrol':'conveniencepetrolgas','gas':'conveniencepetrolgas','conveniencepetrol':'conveniencepetrolgas',
'conveniencegas':'conveniencepetrolgas','petrolgas':'conveniencepetrolgas','off premise':'otheroffpremise','other off premise':'otheroffpremise','on premise':'onpremise'} 

container_mat_dict = {'containers':'container','materials':'material','container material':'material','waxpaper board': 'waxedpaperboard','wax paper': 'waxedpaperboard',
 'flex laminate': 'flexiblelaminate','flexible laminate': 'flexiblelaminate','card board': 'cardboard','stel': 'tin','paperboard': 'waxedpaperboard',
 'pepar': 'paper','papar': 'paper','alumenium': 'aluminium','stell': 'tin','alumnium': 'aluminium','aluminum': 'aluminium','waxed paperboard': 'waxedpaperboard',
 'peper': 'paper','paper': 'paper','palstic': 'plastic','plastic': 'plastic','glas': 'glass',"stainless steel":'stainlesssteel',
 'glass': 'glass','cardvoard': 'cardboard','cardboard': 'cardboard','steel': 'tin'}

container_type_dict = {'containers':'container','containor':'container','gagle top carton tetrapack': 'gabletopcarton',
 'gable top carton': 'gabletopcarton','bag and box': 'bagnbox',
 'gagle carton tetrapack': 'gabletopcarton','gagle top carton': 'gabletopcarton', 'bag nd box': 'bagnbox',
 'bag box': 'bagnbox','gable carton': 'gabletopcarton','brec pack': 'brickpack','bag n box': 'bagnbox',
 'gagle tetrapack': 'gabletopcarton','gable tetrapack': 'gabletopcarton','gagle carton': 'gabletopcarton',
 'puoch': 'pouch','puch': 'pouch','tetrapack': 'gabletopcarton','disoenser': 'dispenser',
 'dispnser': 'dispenser','dispinser': 'dispenser','despenser': 'dispenser','brecpac': 'brickpack',
 'brecpack': 'brickpack','brick pack': 'brickpack','bricks': 'brickpack','bottal': 'bottle',
 'bottlee': 'bottle','botle': 'bottle','tanc': 'tank'}

packtype_dict = {'single pack':'singlepack','multi pack':'multipack','singel pack':'singlepack',
'multiple pack':'multipack','multi packs':'multipack','single packs':'singlepack','pack type': 'packtype'}

size_dict = {'mililiters':'ml','mililiter':'ml','mls':'ml','liters':'ltr','liter':'ltr','litre':'ltr','litres':'ltr','l':'ltr','ls':'ltr'}
t40_dict = {'top 40':'t40','top40':'t40'}

kpi_name_dict = {'valeu': 'value','values': 'value','valu': 'value','shares': 'share','shar': 'share','sare': 'share',
'shre':'share','val': 'value','volume': 'volume','volumes': 'volume','vol': 'volume','volu': 'volume','change': 'change','sahre': 'share',
'growth': 'growth','grow': 'growth','growt': 'growth','sal':'sale', 'dollar':'value','revenue':'value','revnue':'value',
'uc':'volume','difference':'change','diff':'change','increase':'change','decrease':'change','incline':'change','decline':'change',
'declined':'change','declining':'change','declning':'change','increasing':'change','increased':'change','gaining':'driver','gain':'driver','gained':'driver','wining':'driver','losing':'dragger','loosing':'dragger',
'loose':'dragger','lose':'dragger','diffrence':'change',
'diference':'change','decrese':'change','grow':'growth','growths':'growth','growing':'growth','acceleration':'accelerate','aceleration':'accelerate','accelerating':'accelerate',
'swings':'swing','changing':'change','sare':'share','sahre':'share','prize':'price','price per unit ppu':'price','price per unit':'price',
'mixs':'mix','volumn':'volume','drag':'dragging','drive':'driving','selling':'sale','absolute':'sale'}

dim_name_dict = {'cat':'category','categories':'category','manuf':'manufacturer','manufacturers':'manufacturer','brandowner':'manufacturer',
'owner':'manufacturer','mfr':'manufacturer','company':'manufacturer','manufactur':'manufacturer',
'tredmark':'trademark','trade mark':'trademark','tredemark':'trademark','brands':'brand',
'bev':'beverage','beverages':'beverage','bevrage':'beverage','bevrages':'beverage',
'flavor':'flavour','flavors':'flavour','flavours':'flavour','consume':'consumption','consumtion':'consumption','refilable':'refillable','channels':'channel','chanel':'channel',
'container type':'containertype','container types':'containertype',
'materials':'material','pack size':'packsize','pack sizes':'packsize','packsizes':'packsize','pack type':'packtype',
'yrs':'year','yr':'year'}

additional_dict = {'us':'usa','across':'all','competition':'manufacturer','competitor':'manufacturer','competitors':'manufacturer',
'moth':'monthly','mathly':'monthly','montly':'monthly','monly':'monthly','mtd':'monthly','mat':'12mmt',
'bevrage':'beverage','contribution':'mix','contributon':'mix','current':'latest','ongoing':'latest',
'present':'latest','previous':'last','chng':'change','chg':'change',"industries":"industry",
'catagores':'category','catagory':'category','catagories':'category','all categories':'all category',
'across category':'all category','across category':'all category','companies':'company',
'company':'manufacturer','all company':'all manufacturer','all companies':'all manufacturer','driver':'driving',
'across category':'all category','across category':'all category','drivers':'driving','driver':'driving','dragers':'dragging','drives':'driving','drages':'dragging',
'company':'manufacturer','all company':'all manufacturer','all companies':'all manufacturer','am i':'tccc','i am':'tccc','do we':'tccc','we':'tccc',
'across category':'all category','across category':'all category','all ogs': 'all og','all bus': 'all bu','drive':'top driving','driving':'top driving','leading':'top driving','top losing':'bottom losing',
'drager':'bottom dragging','dragging':'bottom dragging','dragger':'bottom dragging','draggers':'bottom dragging','ic fc':'immediate future consumption type','fc ic':'immediate future consumption type',
'csp':'ssd','tcc':'tccc','tcccc':'tccc','fy':'dec','positive':'top driving','negative':'bottom dragging',
'competitions':'manufacturer','comp':'manufacturer'} #'fy':'dec' as user is aking for fy 2019 which means we need to show data for YTD dec 2019 and hence we have added fy for dec and ytd will come default
#'incremental':'absolute change','how many':'number','no':'number','count':'number',

unique_change= pickle.load(open(path+"support_file/unique_change.p","rb"))
unique_change['price mix']='Price mix'
num_dict = {'ten': '10','eight': '8','seven': '7','six': '6','five': '5', 'four': '4', 'three': '3', 'two': '2'}

month_name_title = {'march':'mar','april':'apr','may':'may','june':'jun','july':'jul','august':'aug',
'september':'sep','october':'oct','november':'nov','december':'dec','january':'jan','february':'feb',
'match':'mar','maech':'mar','marxh':'mar','marvh':'mar','aprol':'apr','juli':'jul','jily':'jul',
'augest':'aug','sept':'sep','novumber':'nov','janaury':'jan','januaru':'jan','febraury':'feb',
'mar':'mar','apr':'apr','may':'may','jun':'jun','jul':'jul','aug':'aug','sep':'sep','oct':'oct',
'nov':'nov','dec':'dec', 'jan':'jan','feb':'feb'}

month_dict = {'01':'january','02':'february','03':'march','04':'april','05':'may','06':'june','07':'july','08':'august','09':'september','10':'october','11':'november','12':'december'}
month_dict1 = {'01':'jan','02':'feb','03':'mar','04':'apr','05':'may','06':'jun','07':'jul','08':'aug','09':'sep','10':'oct','11':'nov','12':'dec'}
year_name_title = {'18':'2018','19':'2019','20':'2020','21':'2021'}

error_list = ['Sorry I cant find the answer right now. Would you like to ask something new?','Apologies! I cant find the answer right now. Would you like to ask something else.','Sorry I am not able to service your request. Would you like to ask something else ?']

# =============================================================================
'''automate_month year_dict'''#This automated dict contains various possibilities of month & year that can appear in question 
# =============================================================================
sql_query='SELECT  DISTINCT Period_Id FROM {}'.format(tbl) 
test_data = pd.read_sql_query(sql_query,conn)
test_data['Period_Id']=test_data['Period_Id'].apply(int).apply(str)
test_data.sort_values('Period_Id',inplace=True)
test_data.reset_index(drop=True,inplace=True)

year=[]
month=[]
for i in range(len(test_data)):
    year.append(test_data['Period_Id'][i][:4])
    month.append(test_data['Period_Id'][i][-2:])
month1=[month_dict1.get(w) if w in month_dict1.keys() else w for w in month]
month=[month_dict.get(w) if w in month_dict.keys() else w for w in month]
test_data['year']=year
test_data['month']=month
test_data['month1']=month1
test_data['month_year01']=test_data['month1']+' '+test_data['year']
test_data['month_year02']=test_data['month']+' '+test_data['year']
#month
date1, date2, date3, date4,date5,year_list= [], [], [], [], [], [] 
for i in range(0,len(test_data['Period_Id'])):
    date1.append(test_data['month'][i][:3].lower()+test_data['year'][i])
    date2.append(test_data['month'][i][:3].lower()+test_data['year'][i][-2:])
    date3.append(test_data['month'][i].lower()+test_data['year'][i])
    date4.append(test_data['month'][i].lower()+test_data['year'][i][-2:])
    date5.append(test_data['month'][i][:3].title()+'-'+test_data['year'][i][-2:])
date_dict1 = dict(zip(date1, test_data['month_year01']))
date_dict2 = dict(zip(date2, test_data['month_year01']))
date_dict3 = dict(zip(date3, test_data['month_year01']))
date_dict4 = dict(zip(date4, test_data['month_year01']))
date_dictid= dict(zip(date3, test_data['Period_Id'].str.lower()))
date_dictid.update(dict(zip(date1, test_data['Period_Id'].str.lower())))
date_dict5 = dict(zip(test_data['Period_Id'].str.lower(),date5))
unique_change.update(date_dict5)
reverse_dict_add={'ytd':'YTD','mtd':'MTD','value':'Value','volume':'Volume','share':'Share','month':'MTD',
                  '3mmt':'3MMT','price':'Price','for Industry within NARTD':'','ROmania':'Romania','Cluster':'Market','ukraine':'Ukraine','ddf':'DDF'}
unique_change.update(reverse_dict_add)
#year
year_list=test_data['year'].unique().tolist()
year_list.sort()
year_list1=[]
year_list1=year_list
monthstr=str(int(year_list[-1])+1)
year_list1.append(monthstr)
year_list=test_data['year'].unique().tolist()
year_list.sort()

# =============================================================================
'''kpi_checklist'''#These lists are used to get proper KPI  
# =============================================================================

period_list=['ytd','month','3mmt','12mmt']
metric_list=['value','volume','price']
measure1_list=['sale','share','growth','price']
measure2_list=['change','swing']#to check which type of comparison (vs Previous year)
measure3_list=['mix']#to find shares type
measure5_list=['ctg','driving','dragging']
fill5=['py_change']
measure4_list=['absolute','sale','size']

# =============================================================================
'''Question'''
# =============================================================================
################### posible question on growth ################################

details_dict=dict()
#ques='share trend of all category in korea'
#ques='tccc share in ksa'
#ques='tccc share drive for nartd usa'
#ques='what is share all country '
ques='share  of all category in italy' 
#ques='share  of all category in italy' 
#ques='share of tccc in  korea italy and switzerland'
#ques="Top 10 TCCC Brand Share within USA ssd"
#ques='top 5 country driver on the basis of value  increment ssd ytd dec 19'
#ques='what is value for top 10 companies for water in usa'
#ques='top 5 companies gaining value share energy drink in usa dec 19'
#ques='tccc share change across market'
#ques='top 5 brand gaining value share energy drink in usa dec 19'
#ques='share for t40 Countries'
#ques='all bus volume growth in top 40 for dec 2019'
#ques='Top 10 TCCC Brand Share Driver within SSD in USA for Jan 2020'
#ques='top 15 companies gaining value share within energy dirnk for ytd dec 2019'
#ques='Which competitors are winning shares in Great Britain?'
#ques='what i1s category growth in germany'
#ques='what is category growth in usa'
#ques='market share for TCCC within world'
#ques='asiapacific og share'
#ques='TCCC SSD Share within NARTD USA YTD December'
#ques='cee Business Unit country share'
#ques='Latin Center Business Unit country share'
#ques='South and East Africa Business Unit country share'
#ques='South Latin Business Unit country share'
#ques='all fo share'
#ques='latin america og country share'
#ques='Share change by country FY 2019'
#ques='growth across nireland'
#ques='all fo share'
#ques='cocacola company share for all category'
#ques='Ko share for all category'
#ques='Pepsi co share all category'
#ques='PCI share all category'
#ques='Pepsi company share category'
#ques='all country share'
#ques='Ukraine share'
#ques='top 5 fo share change'
#ques='diet coke share within usa'
#ques='What is the Value Share for Top 10 company in polandquarterly?'
#ques='growth for India & Southwest Asia Business Unit '
#ques='Top 40 - Turkey & CCA Business Unit growth'
#ques='growth for Top 40 - Saudi Arabia'
#ques='growth for - Saudi Arabia'
#ques='Top 40 - South Africa growth'
# =============================================================================
'''Question cleaning'''
# =============================================================================
try:
    # ques=sys.argv[1]
    if ques=='login prompt question':
        details_dict['ques']='Login Prompt Question'
        details_dict['file']=''
        details_dict['title']=''
        details_dict['xlabel']=''
        details_dict['ylabel']=''
        details_dict['caption']=''
        details_dict['kind']='prompt'
        details_dict['infer']=''
        details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you like to ask</b> : '+(random.choice(random_prompt)))))
        details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
        print(str(details_dict)) 
    else:
        ques=text2int(ques)
        ques=re.sub(r'\b0\b','and',ques) 
        details_dict['ques'] = ques
        ques = re.sub(r' +',' ',ques.lower().strip())
        ques=ques.replace('.','') 
        ques = re.sub(r'[_,-,/]','',ques)  
        ques = re.sub(r'\W+',' ',ques) 

        new_add_dict={'cocacola company':'tccc','coca cola company':'tccc','cctm':'coca cola trademark','within':' ',
                      'our share':'tccc share','ko':'tccc','pepsi co':'pepsico','pepsi company':'pepsico','pci':'pepsico',}          

        for word, initial in new_add_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass

    
        kd={'greater china and korea business unit' : 'greaterchinaandkoreabusinessunit',
            'korea republic of':'top40korearepublicof','south korea onpremise':'koreaonpremise koreaoffpremise','south korea offpremise':'koreaonpremise koreaoffpremise',
            'south korea on premise':'koreaonpremise koreaoffpremise','south korea off premise':'koreaonpremise koreaoffpremise',
            'korea onpremise':'koreaonpremise koreaoffpremise','korea offpremise':'koreaonpremise koreaoffpremise',
            'korea on premise':'koreaonpremise koreaoffpremise','korea off premise':'koreaonpremise koreaoffpremise',
            'south korea':'koreaonpremise koreaoffpremise','korea':'koreaonpremise koreaoffpremise','italy albania':'italyalbania','italy':'italy italyquarterly','italyquarterly':'italy italyquarterly','saudiarabiahybrid':'saudiarabiahybrid saudiarabiaquarterly',
            'saudi arabia':'saudiarabiahybrid saudiarabiaquarterly','ksa':'saudiarabiahybrid saudiarabiaquarterly','saudiarabiaquarterly':'saudiarabiahybrid saudiarabiaquarterly','oman':'omanquarterly',
            'romania':'romania romaniaquarterly','romaniaquarterly':'romania romaniaquarterly',"poland" : "poland polandquarterly","polandquarterly" : "poland polandquarterly",
            "switzerland" : "switzerland switzerlandquarterly","switzerlandquarterly" : "switzerland switzerlandquarterly","kuwait" : "kuwaitquarterly",
            "qatar":"qatarquarterly","great britain ireland":"greatbritainireland","gb ireland":"greatbritainireland","ireland":"ireland nireland","nireland":"ireland nireland"}
        
        if len(re.findall(r'\s'+'fo'+'\s',' '+ques+' '))==0 :
            for word, initial in kd.items():
                if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                    ques = ques.replace(word,initial)
                else:
                    pass
        else:
            pass        
        
        for word, initial in cluster_name_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass

        for word, initial in cluster_name_dict2.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
            
        for word, initial in cluster_type_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
        
        for word, initial in t40_dict.items():     
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
    
        for word, initial in kpi_name_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass 

        for word, initial in brand_manuf_trad_dict1.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass 
     
        for word, initial in category_name_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
    
        for word, initial in brand_manuf_trad_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass       
        
        for word, initial in dim_name_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
    
             
    #    for word, initial in brand_dict.items():
    #        if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
    #            ques = ques.replace(" "+word+" ", " "+initial+" ")
    #        else:
    #            pass
    #            
    #    for word, initial in manuf_dict.items():
    #        if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
    #            ques = ques.replace(" "+word+" ", " "+initial+" ")
    #        else:
    #            pass
        
    #    for word, initial in trademark_dict.items():
    #        if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
    #            ques = ques.replace(" "+word+" ", " "+initial+" ")
    #        else:
    #            pass 
        
        for word, initial in consumption_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass  
        
        for word, initial in container_type_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
        
        for word, initial in container_mat_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
        
        for word, initial in refillable_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
    
        for word, initial in channel_name_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
                
        for word, initial in packtype_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                ques = ques.replace(word,initial)
            else:
                pass
                
    #    for word, initial in additional_dict.items():
    #        if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
    #            ques = ques.replace(word,initial)
    #        else:
    #            pass
    
        for word, initial in additional_dict.items():
            if len(re.findall(r'\s'+word+'\s',' '+ques+' '))>0:
                if word in ['we']:
                    ques = ques.replace(" "+word+" ", " "+initial+" ")
                else:
                    ques = ques.replace(word,initial)
            else:
                pass 
    
                  
        ques_words=ques.split()
        ques_words = [date_dict1.get(w) if w in date_dict1.keys() else w for w in ques_words]
        ques_words = [date_dict2.get(w) if w in date_dict2.keys() else w for w in ques_words]
        ques_words = [date_dict3.get(w) if w in date_dict3.keys() else w for w in ques_words]
        ques_words = [date_dict4.get(w) if w in date_dict4.keys() else w for w in ques_words]
        ques_words = [month_name_title.get(w) if w in month_name_title.keys() else w for w in ques_words]
        ques_words = [year_name_title.get(w) if w in year_name_title.keys() else w for w in ques_words]
        ques_words = [size_dict.get(w) if w in size_dict.keys() else w for w in ques_words]
        ques = ' '.join(ques_words)	
        	
        '''remove_stopword'''#This engine is used for removing stop words e.g. (the,and,if,but,what,why etc.) 
        stop_words = set(stopwords.words('english'))
        stop_words.add('chart')
        stop_words.add('show')
        stop_words.discard('all')
        stop_words.discard('be')
        stop_words.discard('us')
        stop_words.discard('my')
        stop_words.discard('our')
        stop_words.discard('we')
        stop_words.discard('from')
        stop_words.discard('a')
        stop_words.discard('up')
        word_tokens = word_tokenize(ques)
        ques_words = [w for w in word_tokens if not w in stop_words]
        ques = ' '.join(ques_words)
        
        '''direct_replace'''#This engine is used for replacing  words  direct from question.
    
        ques=ques.replace('2 year', 'trend')
        ques=ques.replace('two year', 'trend')
        ques=ques.replace('2yrs', 'trend')
        ques=ques.replace('3 year', 'trend')
        ques=ques.replace('three year', 'trend')
        ques=ques.replace('3yrs', 'trend')
    
      
        t4=time.time()
        ##print("\n question cleaning time period: ")
        ##print(t4-t3, t4-t1)
        
        
        if ('category' in ques or 'category mix' in ques) and 'nartd' in ques:
            ques=ques.replace('nartd','')
        else:
            pass
        
        
#        if 'category mix' in ques and 'nartd' in ques:
#            ques=ques.replace('nartd','')
#        else:
#            pass
        if 'industry' in ques and ('manufacturer' in ques or 'company' in ques):
            ques=ques.replace('industry','')
        else:
            pass
        
        if 'price' in ques.split() and 'mix' in ques and('value' in ques or 'volume' in ques):
            ques=ques.replace('value','')
            ques=ques.replace('volume','')
        else:
            pass
        
        if ('dragging' in ques and 'driving' in ques):
            ques=ques.replace('driving','dragging')
        else:
            pass
        
        # =============================================================================
        '''kpi_identification'''#This engine is used for identifing KPI. 
        # =============================================================================
        period = [b for b in period_list if b in ques]
        if len(period)!=1:
            period=[period_list[0]]#default=ytd
        else:
            pass
        
        ques_dairy=ques.replace('value add value added dairy',"")
        metric = [b for b in metric_list if b in ques_dairy.split()]
        if len(metric)==0:
            metric=[metric_list[0]]#default=value
        else:
            pass
        
        measure1 = [b for b in measure1_list if b in ques.split()]
        if len(measure1)==0:
            if  ('mix' in ques):
                measure1=[measure1_list[1]]
            elif 'ctg' in ques:
                measure1=[measure1_list[2]]
            else:
                measure1=[measure1_list[0]]
        else:
            pass
        
        measure2 = [b for b in measure2_list if b in ques.split()]
        if measure1==['share']:
            measure3 = [b for b in measure3_list if b in ques.split()]
            if len(measure3)==0:
                measure3=['ind']#default=ind but not mix
            else:
                pass
        elif measure1==['price']:
            measure3 = [b for b in measure3_list if b in ques.split()]
        else:
            measure3=[]
        
        measure5=[b for b in measure5_list if b in ques.split()]
        
        
        # =============================================================================
        '''kpi_identification'''#This engine is used for identifing KPI.
        # =============================================================================
        measure1_x = [b for b in measure1_list if b in ques.split()]
        if (len(measure1_x)==0) and (('change'in ques.split()) or ('mix' in ques.split())):
            measure1_x=[measure1_list[1]]
        elif (len(measure1_x)==0) and (len(measure1)!=0):
            measure1_x=measure1
        elif (len(measure1_x)!=0):
            measure1_x = measure1_x
        else:
            details_dict["error"]="Please mention a business question with a relevant KPI-Dimensions combination"
            
        measure2_x = [b for b in measure2_list if b in ques.split()]
        
        if measure1_x==['share']:
            measure3_x = [b for b in measure3_list if b in ques.split()] #measure3 only available for shares
            if len(measure3_x)==0:
                measure3_x=['ind']#default=ind but not mix
            else:
                pass
        else:
            measure3_x=[]
        
        # =============================================================================
        '''kpi_df'''#This engine is used to create KPI identification DF 
        # =============================================================================
        periodlst=['month', '3mmt','12mmt','ytd', 'month', '3mmt','12mmt', 'ytd','month', '3mmt','12MMT', 'ytd' ]
        metriclist=['value', 'value','value', 'value', 'volume','volume', 'volume', 'volume','price','price','price','price']
        metriclist1=['Value_USD_Month', 'Value_USD_3MMT','Value_USD_12MMT','Value_USD_YTD', 'Volume_UCs_Month','Volume_UCs_3MMT','Volume_UCs_12MMT', 'Volume_UCs_YTD','Price_USD_Month','Price_USD_3MMT','Price_USD_12MMT','Price_USD_YTD']
        metriclist2=['Value_USD_Month_PY', 'Value_USD_3MMT_PY','Value_USD_12MMT_PY','Value_USD_YTD_PY', 'Volume_UCs_Month_PY','Volume_UCs_3MMT_PY','Volume_UCs_12MMT_PY', 'Volume_UCs_YTD_PY','Price_USD_Month_PY','Price_USD_3MMT_PY','Price_USD_12MMT_PY','Price_USD_YTD_PY']
        growth=['value_growth','value_growth','value_growth','value_growth','volume_growth','volume_growth','volume_growth','volume_growth','price_growth','price_growth','price_growth','price_growth']
        share=['value_share','value_share','value_share','value_share','volume_share','volume_share','volume_share','volume_share','price_share','price_share','price_share','price_share']
        share_change=['value_share_change','value_share_change','value_share_change','value_share_change','volume_share_change','volume_share_change','volume_share_change','volume_share_change','price_share_change','price_share_change','price_share_change','price_share_change']
        mix=['value_mix','value_mix','value_mix','value_mix','volume_mix','volume_mix','volume_mix','volume_mix','price_mix','price_mix','price_mix','price_mix']
        mix_change=['value_mix_change','value_mix_change','value_mix_change','value_mix_change','volume_mix_change','volume_mix_change','volume_mix_change','volume_mix_change','price_mix_change','price_mix_change','price_mix_change','price_mix_change']
        acceleration=['acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration','acceleration']
        ctg=['value_ctg','value_ctg','value_ctg','value_ctg','volume_ctg','volume_ctg','volume_ctg','volume_ctg','price_ctg','price_ctg','price_ctg','price_ctg']
        swing=['value_share_swing','value_share_swing','value_share_swing','value_share_swing','volume_share_swing','volume_share_swing','volume_share_swing','volume_share_swing','price_share_swing','price_share_swing','price_share_swing','price_share_swing']
        size=['value_size','value_size','value_size','value_size','volume_size','volume_size','volume_size','volume_size','price_size','price_size','price_size','price_size']
        absolute=['value_change','value_change','value_change','value_change','volume_change','volume_change','volume_change','volume_change','price_change','price_change','price_change','price_change']
        
        column_selections_df=pd.DataFrame({'tp':periodlst,'metriclist':metriclist,'metriclist1':metriclist1,'metriclist2':metriclist2,
                                           'growth':growth,'share':share,'share_change':share_change,'share_swing':swing,'mix':mix,'mix_change':mix_change,
                                           'accelerate':acceleration,'ctg':ctg,'size':size, 'absolute':absolute})
        
        #column_selections_df=pd.DataFrame({'tp':periodlst,'metriclist':metriclist,'metriclist1':metriclist1,'metriclist_py':metriclist2,'growth':growth})
        if 'value' in metric:
            metric_alt= ['volume']# for prompt
        else:
            metric_alt= ['value']# for prompt
         
        column_selections_df2 = column_selections_df[(column_selections_df.tp.isin(period)) &
                                          (column_selections_df.metriclist.isin(metric_alt)) ]# for prompt
        column_selections_df = column_selections_df[(column_selections_df.tp.isin(period))&
                                          (column_selections_df.metriclist.isin(metric))]
        
        
        
        # =============================================================================
        '''kpi_machine'''#This machine is used to building KPI and prompt KPI
        # =============================================================================
        if 'price' in measure1:# and 'mix' in measure3:
            kpi = ['Value_USD_'+period[0].upper(), 'Value_USD_'+period[0].upper()+'_PY','Volume_UCs_'+period[0].upper(), 'Volume_UCs_'+period[0].upper()+'_PY']
            kpi_alt = column_selections_df2[["metriclist1","metriclist2"]]
            kpi_alt = kpi_alt.iloc[0].tolist()
        else:
            kpi = column_selections_df[["metriclist1","metriclist2"]]
            kpi_alt = column_selections_df2[["metriclist1","metriclist2"]]
            kpi = kpi.iloc[0].tolist()
            kpi_alt = kpi_alt.iloc[0].tolist()
        
    
    #    if 'price' in measure1 and 'mix' in measure3:
    #        kpi = ['Value_USD_'+period[0].upper(), 'Value_USD_'+period[0].upper()+'_PY','Volume_UCs_'+period[0].upper(), 'Volume_UCs_'+period[0].upper()+'_PY']
    #        kpi_alt = column_selections_df2[["metriclist1","metriclist2"]]
    #        kpi_alt = kpi_alt.iloc[0].tolist()
    #    else:
    #        kpi = column_selections_df[["metriclist1","metriclist2"]]
    #        kpi_alt = column_selections_df2[["metriclist1","metriclist2"]]
    #        kpi = kpi.iloc[0].tolist()
    #        kpi_alt = kpi_alt.iloc[0].tolist()
        
#        if ('sale' in measure1 or (('size' in ques) and (('pack' not in ques) or ('packsize' not in ques)))) and (len(measure5)==0 and len(measure1)==0 and len(measure2)==0 and len(measure3)==0):
        if ('sale' in measure1 or (('size' in ques) and (('pack' not in ques) or ('packsize' not in ques)))) and (len(measure5)==0):
            kpi0=column_selections_df['metriclist'].tolist()
            kpi2=column_selections_df2['metriclist'].tolist()
            if ('change' in measure2):
                kpi0.append(column_selections_df["absolute"].tolist()[0])
                kpi2.append(column_selections_df2["absolute"].tolist()[0]) 
                
        elif ('growth' in measure1) and (len(measure5)==0):
            kpi0=column_selections_df['growth'].tolist()
            kpi2=column_selections_df2['growth'].tolist()
            if 'acceleration' in ques or 'accelerate' in ques:
                kpi0.append(column_selections_df['accelerate'].tolist()[0])
                kpi2.append(column_selections_df2['accelerate'].tolist()[0])
        
        elif (len(measure5)>0) and (measure1[0] not in ['share']) and measure3!=['mix']:#ashu0406
            kpi0=column_selections_df['ctg'].tolist()
            kpi2=column_selections_df2['ctg'].tolist()
            
        elif 'price' in measure1:
            if 'mix' in measure3:
                kpi0=column_selections_df['mix'].tolist()
                kpi2=column_selections_df2['mix'].tolist()
            else:
                kpi0=column_selections_df['metriclist'].tolist()
                kpi2=column_selections_df2['metriclist'].tolist()
                if ('change' in measure2):
                    kpi0.append(column_selections_df["absolute"].tolist()[0])
                    kpi2.append(column_selections_df2["absolute"].tolist()[0])
                    
        elif ('share' in measure1) :
            if 'mix' in measure3:
                kpi0=column_selections_df['mix'].tolist()
                kpi2=column_selections_df2['mix'].tolist()
                if ('change' in measure2):
                    kpi0.append(column_selections_df["mix_change"].tolist()[0])
                    kpi2.append(column_selections_df2["mix_change"].tolist()[0])
            else:
                kpi0=column_selections_df['share'].tolist()
                kpi2=column_selections_df2['share'].tolist()
            if ('change' in measure2 or len(measure5)>0) and ('mix' not in measure3):
                kpi0.append(column_selections_df["share_change"].tolist()[0])
                kpi2.append(column_selections_df2["share_change"].tolist()[0])
            elif ('swing' in measure2):
                kpi0.append(column_selections_df["share_swing"].tolist()[0])
                kpi2.append(column_selections_df2["share_swing"].tolist()[0])
        
        kpi0 = [w.replace('_', ' ') for w in kpi0]
        kpi2 = [w.replace('_', ' ') for w in kpi2]        
        kpi0 = [w.replace('ytd', 'YTD') for w in kpi0]
        kpi2 = [w.replace('ytd', 'YTD') for w in kpi2] 
        
        kpi_lst=[]
        for i in range(0,len(kpi0)):
            kpi_str=' '.join(kpi0[i].split())
            kpi_lst.append(kpi_str)
        kpi_lst=[x.title() for x in kpi_lst]#for main KPI 
        
        kpi_alt_lst=[]# for prompt
        for i in range(0,len(kpi2)):
            kpi_alt_str=' '.join(kpi2[i].split())
            kpi_alt_lst.append(kpi_alt_str)
        kpi_alt_lst=[x.title() for x in kpi_alt_lst]
        
        
        # =============================================================================
        '''dim_list_generator'''#This engine is used to generate all dimension from question
        # =============================================================================
        ques_sm=ques.split()
        a=re.compile(r'\s*\w*(top|bottom)+\s*\d*') # to extract top or bottom from question
        top_bottom=re.findall(a,ques)
        top_bottom = list(set(top_bottom))
        if len(top_bottom)>1:
            top_bottom=['bottom']
        else:
            pass 
        if len(top_bottom) > 0:
            c=re.compile(r'\w+\s*\d+')
            top=re.findall(c,ques)
            top1=[x for x in top if re.search('top|bottom', x)]
            e=re.compile(r'\s*\d+')
            try:
                top_num=re.findall(e,top1[0])
                top_num=int(top_num[0])
            except:
                    top_num=10
                               
    #    top40_cluster = list(set(top40_list).intersection(set(ques_sm))) 
        top40_cluster = list(set(top40_lst).intersection(set(ques_sm))) 
        bu_country=[]
        og_country=[]     
        if ('t40' in ques):
            if ('country' in ques) and 'bu' not in ques and ('og' not in ques and any(og in ques for og in og_list)==False):
                top40_cluster = top40_lst[0:-1]
            elif 'bu' in ques and 'country' in ques:
                bu_country=list(set(bu_list).intersection(set(ques_sm)))
                top40_cluster=[bu_dict_t40.get(w) if w in bu_dict_t40.keys() else w for w in bu_country]  
                top40_cluster=flatten(top40_cluster)
            elif ('og' in ques or any(og in ques for og in og_list)) and 'country' in ques:
                og_country=list(set(og_list).intersection(set(ques_sm)))
                top40_cluster=[og_dict_t40.get(w) if w in og_dict_t40.keys() else w for w in og_country] 
                top40_cluster=flatten(top40_cluster)
            else:
                pass
        else:
            top40_cluster=[]
        
        
        country_cluster = list(set(country_list).intersection(set(ques_sm)))
    #    if len(re.findall(r'\s'+'country'+'\s',' '+ques+' '))>0 and (('driving' in ques) or ('dragging' in ques) or (len(top_bottom)>0) or country_cluster==['world'] or len(country_cluster)==0) and len(top40_cluster)==0 :
        if len(re.findall(r'\s'+'country'+'\s',' '+ques+' '))>0  and (len(top40_cluster)==0  and len(country_cluster)==0):
    #    if ('all country' in ques) or ('driving' in ques) or ('dragging' in ques) or len(re.findall(r'\w*(top|bottom)\s*\d+\s*\w*(country)', ques)) and ('country' in ques and len(country_cluster)==0):
            country_cluster = country_list[0:-1]
            bu_country=[]
            og_country=[] 
            if 'bu' in ques:
                bu_country=list(set(bu_list).intersection(set(ques_sm)))
                country_cluster=[bu_dict.get(w) if w in bu_dict.keys() else w for w in bu_country]  #rahul2403
                country_cluster=flatten(country_cluster)
            elif 'og' in ques or any(og in ques for og in og_list):
                og_country=list(set(og_list).intersection(set(ques_sm)))
                country_cluster=[og_dict.get(w) if w in og_dict.keys() else w for w in og_country]  #rahul2403
                country_cluster=flatten(country_cluster)
            else:
                pass
        else:
            pass
        
        og_cluster =  list(set(og_list).intersection(set(ques_sm))) 
        if len(re.findall(r'\s'+'og'+'\s',' '+ques+' '))>0 and (len(og_cluster)==0):
            og_cluster = og_list[:-1]
        else:
            pass
     
        fo_cluster = list(set(fo_list).intersection(set(ques_sm)))
        if len(re.findall(r'\s'+'fo'+'\s',' '+ques+' '))>0 and (len(fo_cluster)==0):
            fo_cluster=fo_list[:-1]  
        else:
            pass
    
        len(re.findall(r'\s'+'bu'+'\s',' '+ques+' '))>0
        bu_cluster = list(set(bu_list).intersection(set(ques_sm)))
        if (len(re.findall(r'\s'+'bu'+'\s',' '+ques+' '))>0) and (len(bu_cluster)==0):
            bu_cluster = bu_list[:-1]   
        else:
            pass
        
        if len(top40_cluster)>0:        
            country_cluster[:]= []       
            og_cluster[:]= []
            bu_cluster[:] = []
            fo_cluster[:] = []
        elif len(country_cluster)>0 and ' fo' not in ques:
            og_cluster[:]= []
            bu_cluster[:] = []
            fo_cluster[:] = []
        elif len(og_cluster)>0:
            bu_cluster[:] = []
            country_cluster[:] = []
            fo_cluster[:] = []
        elif len(bu_cluster)>0:
            og_cluster[:]= []
            country_cluster[:] = []
            fo_cluster[:] = []
        elif len(fo_cluster)>0:
            og_cluster[:]= []
            bu_cluster[:] = []
            country_cluster[:] = []
        cluster = og_cluster+bu_cluster+fo_cluster+country_cluster+top40_cluster 
        if (len(cluster)==0):
            cluster=['world']
        else:
            pass
        
        if (len(country_cluster)>0 or len(top40_cluster)>0) and ('fo' not in ques or 'og' not in ques or 'bu' not in ques):
            cluster_type=['country']    
        elif len(fo_cluster)>0:
            cluster_type=['franchise']
        elif len(bu_cluster)>0:
            cluster_type=['bu']
        elif len(og_cluster)>0:
            cluster_type=['og']
        elif 'world' in cluster:
            cluster_type=['world']
       
        if 't40' in ques and ('country' not in ques):  
            t40_indi=['1']
        elif ('world' not in country_cluster) and (('country' in ques) or len(country_cluster)>0) :
            t40_indi=['2']
        else:
            t40_indi=['0']
    
        if t40_indi==['1']:
            cluster=[t40_name_cluster.get(w) if w in t40_name_cluster.keys() else w for w in cluster]
#            if len(bu_cluster)==17:
#                try:
#                    cluster.remove('indiasouthwestasiabusinessunit')    #rahul2705
#                except ValueError:
#                    pass                    
        else:
            pass     
        
        '''If countries and world/t40 required to come in df then we need to have t40_indi as 0&2 or 1&2'''    
        
    #    if 'world' in country_cluster and len(country_cluster)>1:
    #        t40_indi=['1','2']
        
        
        channel= list(set(channel_list).intersection(set(ques_sm)))
        if (len(re.findall(r'\s'+'channel'+'\s',' '+ques+' '))>0) and ( len(channel)==0):
            channel = channel_list[0:-1]
        else:
            pass
    
        category = list(set(category_list).intersection(set(ques_sm)))
        if ('category' in ques) and (len(category)==0):
            category=category_list[:-1]
        else:
            pass
        
        refillable = list(set(refillable_list).intersection(set(ques_sm)))
        if ('refillable' in ques) and ( len(refillable)==0):
            refillable=refillable_list[:-1]
        else:
            pass     
    
        consum_type = list(set(consumption_list).intersection(set(ques_sm))) 
        if ('consumption' in ques) and ( len(consum_type)==0):
            consum_type=consumption_list[:-1]
        else:
            pass
               
        container = list(set(container_list).intersection(set(ques_sm))) 
        if ('container' in ques) and (len(container)==0):
            container=container_list[0:-1]
        else:
            pass 
            
        material = list(set(material_list).intersection(set(ques_sm))) 
        if ('material' in ques) and ( len(material)==0):
             material=material_list[0:-1]
        else:
            pass
         
        packtype =list(set(packtype_list).intersection(set(ques_sm)))  
        if ('packtype' in ques) and ( len(packtype)==0):
             packtype=packtype_list[0:-1]
        else:
            pass 
               
        p = re.compile(r'\b\d+\s*\w*(ml|ltr)\b') #to check ml or ltr
        pck_size=(re.findall(p, ques))
        pack_size=[]
        if ('packsize' in ques) and ( len(pck_size)==0):
            pack_size=packsize_list[:-1]
        elif len(pck_size)>0:
            p = re.compile(r'\b\d+\s*\w*\b')
            ps=re.findall(p, ques)
            ps=[x for x in ps if re.search('ml|ltr', x)]
            for element in ps:
                if element[0:2]=="0 ":
                   res = list(map(lambda st: str.replace(st, element, element.replace('0 ', '')), ps))   
                   pack_size =[x.replace(' ', '') for x in res]
                else:
                   pack_size =[x.replace(' ', '') for x in ps]
        else:
            pass
        
        
        
        # =============================================================================
        '''checking flavour, beverages, brand'''#This engine use to identify dimension using probability.
        # =============================================================================
    
        # if 'flavour' in ques_sm:
        #     ques_sm=ques.partition(' flavour')[0].split()
        #     if (len(top_bottom)>0) or ("all flavour" in ques):
        #         flavour_list=uni_tab.Product_Flavours.unique().tolist()
        #         flavour_name = [str(element).lower() for element in flavour_list]
        #     else:
        #         flavour_list=uni_tab.Product_Flavours.tolist()
        #         flavour_list = [str(element).lower() for element in flavour_list]
        #         c=[]
        #         for k in flavour_list:
        #             c.append(k.split())
        #         flavour_name=[' '.join(i) for i in c if ques_sm[-1] in i]
        #         flavour_name1=[i for i in flavour_name if ' '.join(ques_sm[-2:]) in i]
        #         if len(flavour_name1)>0:
        #             flavour_name=flavour_name1
        #         elif len(flavour_name1)==0:
        #             flavour_name=[ques_sm[-1]]
        #         else:
        #             flavour_name
        # else:
        flavour_name=[]
                       
        # if 'Product_Bev_Product' in ques_sm:
        #     ques_sm=ques.partition(' beverage')[0].split()
        #     if (len(top_bottom)>0) or ("all beverage" in ques):
        #         bev_list=uni_tab.Product_Bev_Product.unique().tolist()
        #         bev_name = [str(element).lower() for element in bev_list]
        #     else:
        #         bev_list=uni_tab.Product_Bev_Product.tolist()
        #         bev_list = [str(element).lower() for element in bev_list]
        #         c=[]
        #         for k in bev_list:
        #             c.append(k.split())
        #         bev_name=[' '.join(i) for i in c if ques_sm[-1] in i]
        #         bev_name1=[i for i in bev_name if ' '.join(ques_sm[-2:]) in i]
        #         if len(bev_name1)>0:
        #             bev_name=bev_name1
        #         elif len(bev_name1)==0:
        #             bev_name=[ques_sm[-1]]
        #         else:
        #             bev_name
        # else:
        bev_name=[]
    
        brand_name=list(set(brand_list).intersection(set(ques_sm)))
        if len(list((set(brand_genlst)).intersection(set(brand_name))))>0:# and 'brand' not in ques: 
            brand_name = [i for i in brand_name if i not in list((set(brand_genlst)).intersection(set(brand_name)))] 
    
        if ('brand' in ques) and ( len(brand_name)==0):
                brand_name = brand_list[:-1]
        else:
            brand_name
            
        manufacturer=list(set(manuf_list).intersection(set(ques_sm)))
        if len(list((set(['dairy','united','may'])).intersection(set(manufacturer))))>0:# this mechanism will hellp to remove dairy from manufacturers 
            manufacturer = [i for i in manufacturer if i not in list((set(['dairy','united','may'])).intersection(set(manufacturer)))]  
            
            
        if ('manufacturer' in ques) and (len(manufacturer)==0):
            manufacturer = manuf_list[:-1]
        else:
            manufacturer
            
        if ('swing' in ques):
            if 'tccc' in manufacturer:
                manufacturer1=manufacturer.copy()
                manufacturer.append('pepsico')
                manufacturer2=['pepsico']
            else:
                manufacturer1=manufacturer.copy()
                manufacturer.append('tccc')
                manufacturer2=['tccc']
        
        trademark=list(set(trademark_list).intersection(set(ques_sm)))
        if ('trademark' in ques) and (len(trademark)==0):
            trademark = trademark_list[:-1]
        else:
            trademark
        
        if len(list(set(brand_name) & set(manufacturer) & set(trademark)))>0 and 'manufacturer' in ques:
            trademark= []
            brand_name= []
        elif len(list(set(brand_name) & set(manufacturer) & set(trademark)))>0 and 'brand' in ques:
            trademark= []
            manufacturer= []
        elif len(list(set(brand_name) & set(manufacturer) & set(trademark)))>0 and 'trademark' in ques:
            manufacturer= []
            brand_name= []
        elif len(list(set(brand_name) & set(manufacturer) & set(trademark)))>0:
            trademark= []
            brand_name= []
           
        elif len(list(set(brand_name) & set(manufacturer)))>0 and 'manufacturer' in ques:
            brand_name= []
            trademark= []
        elif len(list(set(brand_name) & set(manufacturer)))>0 and 'brand' in ques:
            manufacturer= ['industry']
            trademark= []
        elif len(list(set(brand_name) & set(manufacturer)))>0:
            brand_name= []
            trademark= []
        elif len(list(set(brand_name) & set(trademark)))>0 and 'brand' in ques:
            trademark= []
        elif len(list(set(brand_name) & set(trademark)))>0 and 'trademark' in ques:
            brand_name= []
        elif len(list(set(brand_name) & set(trademark)))>0:
            trademark= []
        elif len(list(set(manufacturer) & set(trademark)))>0 and 'manufacturer' in ques:           
            trademark= []
        elif len(list(set(manufacturer) & set(trademark)))>0 and 'trademark' in ques:           
            manufacturer= []
        elif len(list(set(manufacturer) & set(trademark)))>0:           
            trademark= []
        elif manufacturer== ['industry'] and len(brand_name)>0:
            trademark=[]
          
        # =============================================================================
        '''cluster_breakdown'''#This engine is used to denote proper cluster names 
        # =============================================================================
    
        cluster_name=[]
        if len(top40_cluster)>0:
            cluster_name='Country(Top 40)'
        elif 't40' in ques:
            cluster_name='Top 40'
        elif len(og_cluster)>0:
            cluster_name="Operating Group"
        elif len(bu_cluster)>0:
            cluster_name="Business Unit"
        elif len(fo_cluster)>0:
            cluster_name='Franchise Operation'
        elif len(country_cluster)>0:
            cluster_name='Country'
        else:
            cluster_name="world" 
            
        if 'world' in cluster_name:
            cluster_name2= 'Cluster'
        else:
            cluster_name2= cluster_name
           
        # =============================================================================
        '''month_kpi_list'''#This engine is for creating required month year with respect to KPI
        # =============================================================================
        
        year = [b for b in year_list1 if b in ques]                  
        if len(year)>0:
            dif=int(year[-1])-int(year[0])
            if dif>0:
                year=list(map(str,range(int(year[0]), int(year[-1])+1)))
            else:
                year
        
        if all(x in ques for x in ['latest','year']):
            year = [year_list[-1]]
        elif all(x in ques for x in ['last','year']):
            year = [year_list[-2]]
        
        month=[]
        new_tokens = word_tokenize(ques)
        for x in new_tokens:
            if x in month_list:
                month.append(x)
                
        ##########################################
        
        month_year=[]
        for i in range(len(test_data)):
            month_year.append(test_data['month'][i][:3]+' '+test_data['year'][i][-2:])
        test_data['period']=month_year
        test_data['period'] = pd.to_datetime(test_data['period'], format='%b %y')
        test_data['Period_Id']=sorted(test_data['Period_Id'])
        test_data['period'] = test_data['period'].dt.strftime('%b-%y')
        test_data['period'] = test_data['period'].str.lower()
        all_date=test_data['period'].tolist()
        month_kpi1=test_data['period'][-12:].tolist()#sat0405#1year data
        month_kpi2=test_data['period'][-24:].tolist()#sat0405#2year data
        
        dlst=  test_data['period'].tolist()
        jan_lst=[]
        for x in dlst:
            if x.startswith('jan'):
                jan_lst.append(x)
                    
        t= test_data[test_data['period']==jan_lst[-1]].index.tolist() 
        month_kpi3=test_data['period'][t[0]:].tolist()#Latest year data starting from Jan
            
        t= test_data[test_data['period']==jan_lst[-2]].index.tolist()
        month_kpi4=test_data['period'][t[0]:].tolist()#Month starting from jan from last year
        
        '''month_year_machine'''#This machine is used to get final month_year
        if 'from' in ques or 'onwards' in ques or 'onward' in ques:
            if len(year)==1 and len(month)==1:
                year.append(year_list[-1])
                month.append(month_kpi3[-1][:3])
            elif len(year)==1 and len(month)==0 and year[-1]!=year_list[-1]:
                year.append(year_list[-1])
            elif len(year)==0 and len(month)==1 and month[-1]!=month_kpi3[-1][:3]:
                year.append(year_list[-1])
                month.append(month_kpi3[-1][:3])
        else:
            pass
        
        if len(year)>=1 and len(month)>=1:
            if month[-1]+'-'+year[-1][2:] not in test_data['period'].tolist():
                year[-1]=year_list[-1]
                month[-1]=month_kpi3[-1][:3]
        else:
            pass
        
        if len(month)==1 and len(year)>1:
             month=[month[0],month[0]]
        
        month_lst=[]
        if len(year)==1 and len(month)==1:
            for x in month:
                month_temp=x+'-'+year[0][2:]
                month_lst.append(month_temp)
        elif len(year)>=1 and len(month)==0:
            test_data_sat=test_data[test_data['year'].isin(year)]
            month_lst=test_data_sat['period'].tolist()
        elif len(year)==0 and len(month)>=1:
            test_data_sat=test_data[test_data['month1'].isin(month)]
            test_data_sat=test_data_sat[test_data_sat['year'].isin(year_list[1:])]
            month_lst=test_data_sat['period'].tolist()
    #    elif len(year)==0 and len(month)>1:
    #        start=month[0]+'-'+year_list[-1][2:]
    #        end=month[1]+'-'+year_list[-1][2:]
    #        strtind=test_data[test_data['period']==start].index[0]
    #        endind=test_data[test_data['period']==end].index[0]
    #        month_lst=test_data[strtind:endind+1]['period'].tolist()        
        elif len(month)>1 and len(year)>1:
            start=month[0]+'-'+year[0][2:]
            end=month[1]+'-'+year[-1][2:]
            strtind=test_data[test_data['period']==start].index[0]
            endind=test_data[test_data['period']==end].index[0]
            month_lst=test_data[strtind:endind+1]['period'].tolist()
        elif len(month)>1 and len(year)==1:
            start=month[0]+'-'+year[0][2:]
            end=month[1]+'-'+year[0][2:]
            strtind=test_data[test_data['period']==start].index[0]
            endind=test_data[test_data['period']==end].index[0]
            month_lst=test_data[strtind:endind+1]['period'].tolist()
        elif ('all month' in ques) or ('all ytd' in ques) or ('all year' in ques) or ('accelerate' in ques) or ('trend' in ques.split()) or len(year)>0:  
    #       if('share' in measure1) and ('change' not in measure2) and (period == ['ytd']):
    #           month_lst = month_kpi4
    #       else:
    #           month_lst= month_kpi3
            month_lst = month_kpi4
    #    elif ('all month' in ques) or ('all ytd' in ques) or ('all year' in ques) or ('accelerate' in ques) or ('trend' in ques) or len(year)>0:  
    #        if period == ['ytd']:
    #            if ('growth' in measure1) and ('accelerate' in ques):
    #                month_lst = month_kpi1
    #            elif ('growth' in measure1) or ('change' in measure2):
    #                month_lst =month_kpi3
    #            elif ('share' in measure1)and ('change' not in measure2):
    #                month_lst = month_kpi4
    #            elif ('price' in measure1) and ('mix' in measure3):
    #                month_lst= month_kpi3
    #        else:
    #            if ('growth' in measure1) or ('change' in measure2):
    #                month_lst = month_kpi1
    #            elif ('share' in measure1)and ('change' not in measure2):
    #                month_lst = month_kpi1
    #            elif ('price' in measure1) and ('mix' in measure3):
    #                month_lst= month_kpi3
        else:
            month_lst=[month_kpi3[-1]]
        
        monthcp_lst=[]
        if len(year)>0 and len(month)==0:
            month_lst1=[] 
            for x in year:
                for i in range(0,len(month_kpi1)):
                    if month_kpi1[i].endswith(x[-2:]):
                        month_lst1.append(month_kpi1[i])
            monthcp_lst=month_lst1
        
        if len(year)>0 and len(month)==0:
            month_lst1=[] 
            for x in year:
                for i in range(0,len(month_lst)):
                    if month_lst[i].endswith(x[-2:]):
                        month_lst1.append(month_lst[i])
            month_lst=month_lst1
        
        '''month_kpi_error''' #This engine is for creating error if month year asked incorrectly with respect to KPI       
        if len(month_lst)==0:
            month_lst=monthcp_lst
            #Error basis Periodic KPI
        if period == ['ytd']:
            if ('growth' in measure1) or ('change' in measure2) or ('mix' in measure3) or (len(measure5)>0):
                if any(x for x in month_lst if x not in month_kpi4):
                    details_dict["error"]="Please ask YTD growth%--Share change--Price Mix--CTG between "+ month_kpi4[0].title() +" to " +month_kpi4[-1].title()        
            elif (('share' in measure1) or ('sale' in measure1) or ('price' in measure1)) and ('change' not in measure2 or 'mix' not in measure3):
                if any(x for x in month_lst if x not in month_kpi2):
                    details_dict["error"]="Please ask YTD share%--PPU between "+ month_kpi2[0].title() +" to " +month_kpi2[-1].title()  
        elif period == ['month'] or period == ['3mmt'] or period == ['12mmt']:
            if ('growth' in measure1) or ('change' in measure2) or ('mix' in measure3) or (len(measure5)>0):
                if any(x for x in month_lst if x not in month_kpi4) :
                    details_dict["error"]="Please ask Growth% & Share change between "+ month_kpi4[0].title() +" to " +month_kpi4[-1].title()
            elif (('share' in measure1) or ('sale' in measure1) or ('price' in measure1)) and ('change' not in measure2 or 'mix' not in measure3):
                if any(x for x in month_lst if x not in month_kpi2):
                    details_dict["error"]="Please ask Share% between "+ month_kpi2[0].title() +" to " +month_kpi2[-1].title()
    
        '''month_year_title'''# This engine is for creating month year for title
        month_tag =[]
        if (len(month)==0) and (len(year)==1):
            month_tag = year 
        
        else:
            month_tag = [month_lst[-1].title()]
            
        '''month_year_sql'''#This engine is used to create exact month year for SQL query
        month_sql=[]
        for i in range(0, len(month_lst)):
            month_str=['20'.join(month_lst[i].split('-'))]
            month_str=[date_dictid.get(w) if w in date_dictid.keys() else w for w in month_str]
            month_sql.append(month_str)
            month_sql=flatten(month_sql)
        
            
        # =============================================================================
        '''auto_company and category'''#This engine is used to get brands automatically if usedr did not ask about brand
        # =============================================================================
        if any(len(lst) > 0 for lst in [bev_name,brand_name,flavour_name]):
            '''auto_cat''' #This engine is used to get category automatically if usedr did not ask about category   
            if len(category)==0 and len(manufacturer)>0:
                category=['nartd']     
                '''auto_brand_cat''' #This engine is used to get company & category automatically if usedr did not ask about brand and category       
            elif (len(category)==0) and (len(manufacturer)==0):
                if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                    manufacturer=['tccc']
                else:
                    manufacturer=['industry']
                category=['nartd']
            elif len(category)>0 and len(manufacturer)==0:
                if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                    manufacturer=['tccc']
                else:
                    manufacturer=['industry']
            else:
                pass            
        else:
            pass
        '''auto_company'''#This engine is used to get brands automatically if usedr did not ask about brand
        if len(category)>0 and len(manufacturer)==0:
            if (('share' not in measure1) or ('mix' in measure3)):
                manufacturer=['industry']
            elif ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                manufacturer=['tccc']
            elif ('share' in measure1) :
                manufacturer=['industry']
            else:
                manufacturer=['tccc']
                
            '''auto_cat''' #This engine is used to get category automatically if usedr did not ask about category   
        elif len(category)==0 and len(manufacturer)>0:
            if measure3==['mix'] and measure1!=['price'] :
                category=['coresparkling']
            else:
                category=['nartd']
                
                '''auto_brand_cat''' #This engine is used to get brand & category automatically if usedr did not ask about brand and category       
        elif len(category)==0 and len(manufacturer)==0:
            category=['nartd']
            if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                manufacturer=['tccc']
            else:
                manufacturer=['industry']
    
        # =============================================================================
        '''dim_filter_engine'''  #This engine is used to get exact filter and dimension      
        # =============================================================================
        filter_check=[month_lst,category,manufacturer,channel,trademark,brand_name,bev_name,flavour_name,cluster,consum_type,refillable,material,packtype,pack_size,container]
        input_Dim_lst=[]
        input_fltr_lst=[]
        for i in filter_check:
            len_chk=len(i)
#            print(len_chk)
            if len_chk==1 or (len_chk==2 and (cluster==['koreaonpremise', 'koreaoffpremise'] or (len(cluster)==2 and any('quarterly' in string for string in cluster)==True))):# if user has mentioned exactly 1 label then I am considering that variable into as filter for data
                fltr1=filter_check[filter_check.index(i)]
                input_fltr_lst.append(fltr1)
            elif len_chk>1 and (i!=cluster or (cluster!=['koreaonpremise', 'koreaoffpremise'] and (len(cluster)!=2 or any('quarterly' in string for string in cluster)==False))):
                Dim1=filter_check[filter_check.index(i)]
                input_Dim_lst.append(Dim1)
#        if cluster==['koreaonpremise', 'koreaoffpremise'] and cluster in input_fltr_lst:
#            input_fltr_lst.remove(cluster)
#            input_fltr_lst.append(['korea'])
#        else:
#            pass
        filter_lst=input_Dim_lst+input_fltr_lst
        
        '''dim_name_engine''' #This engine is used to get exact name of the dimension  
        input_Dim_lst1=[]
        if (month_lst in input_Dim_lst):
            dim_temp="Period_Id"
            input_Dim_lst1.append(dim_temp) 
        if (category in input_Dim_lst):
            dim_temp="Product_Category"
            input_Dim_lst1.append(dim_temp)
        if (manufacturer in input_Dim_lst):
            dim_temp="Product_Company"
            input_Dim_lst1.append(dim_temp)
        if (cluster in input_Dim_lst):
            dim_temp="Geography_Name" 
            input_Dim_lst1.append(dim_temp)
        if (brand_name in input_Dim_lst):
            dim_temp="Product_Brand"
            input_Dim_lst1.append(dim_temp)
        if (bev_name in input_Dim_lst):
            dim_temp="Product_Bev_Product"
            input_Dim_lst1.append(dim_temp)
        if (flavour_name in input_Dim_lst):
            dim_temp="Product_Flavours"
            input_Dim_lst1.append(dim_temp)
        if (pack_size in input_Dim_lst):
            dim_temp="Product_Pack_Size"
            input_Dim_lst1.append(dim_temp)
        if (consum_type in input_Dim_lst):
            dim_temp="Product_Type_of_Consumption"
            input_Dim_lst1.append(dim_temp)
        if (refillable in input_Dim_lst):
            dim_temp="Product_Refillable"
            input_Dim_lst1.append(dim_temp)
        if (material in input_Dim_lst):
            dim_temp="Product_Container_Material"
            input_Dim_lst1.append(dim_temp)
        if (container in input_Dim_lst):
            dim_temp="Product_Container_Type"
            input_Dim_lst1.append(dim_temp)
        if (packtype in input_Dim_lst):
            dim_temp="Product_Single_Multi_Pack"
            input_Dim_lst1.append(dim_temp)
        if (channel in input_Dim_lst):
            dim_temp="Channel_Name"
            input_Dim_lst1.append(dim_temp)
        if (trademark in input_Dim_lst):
            dim_temp="Product_Trademark"
            input_Dim_lst1.append(dim_temp)
        
           
        '''dim_title'''#This engine is used in title to denote dimension
        input_Dim_lst2=[]#for x and Y lables only
        input_Dim_lst2 = ["Time period" if x=="Period_Id" else x for x in input_Dim_lst1]
        input_Dim_lst2 = [cluster_name if x=="Geography_Name" else x for x in input_Dim_lst2]
        
        '''filter_name_engine'''   #This engine is used to get exact name of the filter 
        input_fltr_lst1=[]
        if (month_lst in input_fltr_lst):
            dim_temp="Period_Id"
            input_fltr_lst1.append(dim_temp) 
        if (category in input_fltr_lst):
            dim_temp="Product_Category"
            input_fltr_lst1.append(dim_temp)
        if (manufacturer in input_fltr_lst):
            dim_temp="Product_Company"
            input_fltr_lst1.append(dim_temp)
        if (cluster in input_fltr_lst):
            dim_temp="Geography_Name"
            input_fltr_lst1.append(dim_temp)
        if (brand_name in input_fltr_lst):
            dim_temp="Product_Brand"
            input_fltr_lst1.append(dim_temp)
        if (bev_name in input_fltr_lst):
            dim_temp="Product_Bev_Product"
            input_fltr_lst1.append(dim_temp)
        if (flavour_name in input_fltr_lst):
            dim_temp="Product_Flavours"
            input_fltr_lst1.append(dim_temp)
        if (pack_size in input_fltr_lst):
            dim_temp="Product_Pack_Size"
            input_fltr_lst1.append(dim_temp)
        if (consum_type in input_fltr_lst):
            dim_temp="Product_Type_of_Consumption"
            input_fltr_lst1.append(dim_temp)
        if (refillable in input_fltr_lst):
            dim_temp="Product_Refillable"
            input_fltr_lst1.append(dim_temp)
        if (material in input_fltr_lst):
            dim_temp="Product_Container_Material"
            input_fltr_lst1.append(dim_temp)
        if (container in input_fltr_lst):
            dim_temp="Product_Container_Type"
            input_fltr_lst1.append(dim_temp)        
        if (packtype in input_fltr_lst):
            dim_temp="Product_Single_Multi_Pack"
            input_fltr_lst1.append(dim_temp)
        if (channel in input_fltr_lst):
            dim_temp="Channel_Name"
            input_fltr_lst1.append(dim_temp)
        if (trademark in input_fltr_lst):
            dim_temp="Product_Trademark"
            input_fltr_lst1.append(dim_temp)
            
        '''filter_caption'''#This engine is used in caption to denote filter
        input_fltr_lst2=[]#for caption only
        input_fltr_lst2 = ["Time period" if x=="Period_Id" else x for x in input_fltr_lst1]
        
        if cluster == ['world']:
            input_fltr_lst2 = ["cluster" if x=="Geography_Name" else x for x in input_fltr_lst2]
        else:    
            input_fltr_lst2 = [cluster_name if x=="Geography_Name" else x for x in input_fltr_lst2]
        
        k1d0_remove=[]
    #    if 'share' in measure1 or len(measure5)>0:
        ifl=flatten(input_fltr_lst)     
        idl=flatten(input_Dim_lst)
    #        test_x=ifl+idl
        test_x=ifl+input_Dim_lst1+idl+input_fltr_lst1   #rahul2703        
        dict_x={'category':'Product_Category','categories':'Product_Category','company':'Product_Company','manufacturer':'Product_Company',
                'singlepack':'Product_Single_Multi_Pack','multipack':'Product_Single_Multi_Pack','packtype':'Product_Single_Multi_Pack','material':'Product_Container_Material',
                'containertype':'Product_Container_Type','container':'Product_Container_Type','markets':'Geography_Name','market':'Geography_Name', 'consumption':'Product_Type_of_Consumption','bu':'Geography_Name','og':'Geography_Name',
                'brand':'Product_Brand','trademark':'Product_Trademark','channel':'Channel_Name','country':'Geography_Name','packsize':'Product_Pack_Size','size':'Product_Pack_Size', 'fo':'Geography_Name'}
        ques_words=ques.split()
        ques_words = [dict_x.get(w) if w in dict_x.keys() else w for w in ques_words]
        ques_words=[t40_name_cluster.get(w) if w in t40_name_cluster.keys() else w for w in ques_words]
        ques_1 = ' '.join(ques_words)
        
        dim_number_lst=[]  
        
        #below commmented bcoz it is considering 'top', 'in' and 'the' as abrand name as we have  these as brand name in list/dict and it is giving as positions
#        for x in test_x:
#            try:
#                dim_number=ques_1.split().index(x)
#            except:
#                dim_number=-1
#            dim_number_lst.append(dim_number)
        
        
        
        for x in test_x:
            dim_number=ques_1.find(x)
            dim_number_lst.append(dim_number)
        
            
        try:
            dim1_num=min([n for n in dim_number_lst  if n>=0])
            dim1_num_ix=dim_number_lst.index(dim1_num)
            dim1=test_x[dim1_num_ix] 
    #        dim11_num=max([n for n in dim_number_lst  if n>=0])
    #        dim11_num_ix=dim_number_lst.index(dim11_num)
    #        dim11=test_x[dim11_num_ix]
        except:
            dim1='nartd'
        
        

        
        if ((dim1 in country_list) or (dim1 in fo_list) or (dim1 in og_list) or (dim1 in bu_list) or (dim1=='Geography_Name') or dim1 in t40_lst_agg) and 'top40world' not in cluster and 't40' in ques  and(len(bu_cluster)!=(len(bu_list)-1) and len(og_cluster)!=(len(og_list)-1)):
            cluster.append('top40world')   
            k1d0_remove=['t40']
            
        if 'top40world' in cluster and len(country_cluster)>0:  #rahul3003
                t40_indi=['1','2']
        elif ((dim1 in country_list) or (dim1 in fo_list) or (dim1 in og_list) or (dim1 in bu_list) or (dim1=='Geography_Name')) and 'world' not in cluster  and(len(bu_cluster)!=(len(bu_list)-1) and len(og_cluster)!=(len(og_list)-1)):
            cluster.append('world')  
            k1d0_remove=['tw']
            if 'world' in cluster and len(country_cluster)>0:   #rahul3003
                t40_indi=['0','2']
        elif cluster==['top40world'] and ((dim1 in country_list) or (dim1 in fo_list) or (dim1 in og_list) or (dim1 in bu_list) or (dim1=='Geography_Name') or (dim1 in t40_lst_agg)):    #rahul3003
            cluster.append('world')
            t40_indi=['0','1']
            k1d0_remove=['tw']
    
            
    #        for x in test_x:
    #            #print(x)
    #            dim_number=ques_1.find(x)
    #            dim_number_lst.append(dim_number)
    #        try:
    #            dim1_num=min([n for n in dim_number_lst  if n>=0])
    #            dim1_num_ix=dim_number_lst.index(dim1_num)
    #            dim1=test_x[dim1_num_ix]
    #
    #        except:
    #            dim1='TCCC' 
    #    else:
    #        pass    
            
        # =============================================================================
        ''' get geography id'''
        # =============================================================================
        if 'franchise' in cluster_type and t40_indi==['0']:
            cluster_id=[fo_code_dict.get(w) if w in fo_code_dict.keys() else w for w in cluster]
        elif t40_indi==['1'] and len(country_cluster)==0:   #rahul2603
            cluster_id=[t40_cluster_dict.get(w) if w in t40_cluster_dict.keys() else w for w in cluster]
        else:
            cluster_id=[cluster_code_dict.get(w) if w in cluster_code_dict.keys() else w for w in cluster]
        
        ''' get time period for dim 2'''
        if (len(input_Dim_lst1)==2 and 'Period_Id' in input_Dim_lst1):
            month_sql= month_sql[-6:]
            
        # =============================================================================
        '''str1 and str2, str3''' #creation
        # =============================================================================
        str1=[]
        str2=[]
        str3=[]
    
        if len(cluster)>0:
            cluster1 = [cluster_name_dict1.get(w) if w in cluster_name_dict1.keys() else w for w in cluster]
            str1.append('LOWER(Geography_Name) AS Geography_Name')
            str2.append("Geography_ID in ('"+"','".join(cluster_id)+"')")
    #        str2.append("Top40_Indicator in ('"+"','".join(t40_indi)+"')")      #rahul2603
            str3.append("Geography_Name")
        else:
            pass
        
        if (len(category)>0) and (category!=['nartd']) :  #rahul2802and ('nartd' not in category)
            str1.append('LOWER(Product_Category) AS Product_Category')  
      
        else:
            pass
        
        if (len(manufacturer)>0) and (manufacturer!=['industry']):  #rahul2802
            str1.append('LOWER(Product_Company) AS Product_Company')
    
        else:
            pass
        
        if len(month_sql)>0:
            str1.append('LOWER(Period_Id) AS Period_Id')
            str2.append("Period_Id in ('"+"','".join(month_sql)+"')")
            str3.append("Period_Id")
            
        else:
            pass
        
        if len(channel)>0:
            str1.append('LOWER(Channel_Name) AS Channel_Name')
            str3.append("Channel_Name")
        
        else:
            pass
        
        if len(pack_size)>0:
            str1.append('LOWER(Product_Pack_Size) AS Product_Pack_Size')
            str3.append("Product_Pack_Size")
        else:
            pass
        
        if len(brand_name)>0:
            str1.append('LOWER(Product_Brand) AS Product_Brand')
            str3.append("Product_Brand")
        else:
            pass
        
        if len(bev_name)>0:
            str1.append('LOWER(Product_Bev_Product) AS Product_Bev_Product')
            str3.append("Product_Bev_Product")
        else:
            pass
        
        if len(flavour_name)>0:
            str1.append('LOWER(Product_Flavours) AS Product_Flavours')
            str3.append("Product_Flavours")
        else:
            pass
        
        if len(packtype)>0:
            str1.append('LOWER(Product_Single_Multi_Pack) AS Product_Single_Multi_Pack')
            str3.append("Product_Single_Multi_Pack")
        
        else:
            pass
        
        if len(material)>0:
            str1.append('LOWER(Product_Container_Material) AS Product_Container_Material')
            str3.append("Product_Container_Material")
        
        else:
            pass
        
        if len(trademark)>0:
            str1.append('LOWER(Product_Trademark) AS Product_Trademark')
            str3.append("Product_Trademark")
        
        if len(container)>0:
            str1.append('LOWER(Product_Container_Type) AS Product_Container_Type')
            str3.append("Product_Container_Type")
        
        else:
            pass
        
        if len(refillable)>0:
            str1.append('LOWER(Product_Refillable) AS Product_Refillable')
            str3.append("Product_Refillable")
        
        else:
            pass
        
        if len(consum_type)>0:
            str1.append('LOWER(Product_Type_of_Consumption) AS Product_Type_of_Consumption')
            str3.append("Product_Type_of_Consumption")
        
        else:
            pass
         
    
        if (category!=['nartd']) and (manufacturer==['industry']):
            str3.append("ROLLUP(Product_Category)")
        elif (manufacturer!=['industry']) and (category==['nartd']):
            str3.append("ROLLUP(Product_Company)")
        elif (manufacturer!=['industry']) and (category!=['nartd']):# or (manufacturer==['industry']) and (category==['nartd']):
            str3.append("ROLLUP(Product_Company)") 
            str3.append("ROLLUP(Product_Category)")
        else:
            pass
        
        
        # =============================================================================
        '''DF_machine'''#This machine is used to extrat final DF from SQL
        # =============================================================================
        
    
        
        obj1=','.join(str1)
        obj2=' AND '.join(str2) 
        obj3=', '.join(str3)
    #
    #    if 'price' in measure1 and 'mix' in measure3:
    #        col_sql=obj1+',sum('+kpi[0]+') as ' +kpi[0]+', sum('+kpi[1]+') as ' +kpi[1]+',sum('+kpi[2]+') as ' +kpi[2]+', sum('+kpi[3]+') as ' +kpi[3]
    #    else:
    #        col_sql=obj1+',sum('+kpi[0]+') as ' +kpi[0]+', sum('+kpi[1]+') as ' +kpi[1]
        
    
        if 'price' in measure1:# and 'mix' in measure3:
            col_sql=obj1+',sum('+kpi[0]+') as ' +kpi[0]+', sum('+kpi[1]+') as ' +kpi[1]+',sum('+kpi[2]+') as ' +kpi[2]+', sum('+kpi[3]+') as ' +kpi[3]
        else:
            col_sql=obj1+',sum('+kpi[0]+') as ' +kpi[0]+', sum('+kpi[1]+') as ' +kpi[1]
        
    
        
        #####################   sql query  ##############
        sql_query1="SELECT {} \
                    FROM {} WHERE {} GROUP BY {} \
                    ORDER BY 1,2,3".format(col_sql,tbl,obj2,obj3)
         
        print(sql_query1)
        df=pd.read_sql_query(sql_query1, conn)
        df=df.replace(other_list,'others')
        df_col=df.columns.tolist()        
    
        if 'Product_Category' not in df_col and category==['nartd']:
            df.insert(1, 'Product_Category', 'nartd')
            str3.append('Product_Category')
        elif 'Product_Category' not in df_col and 'nartd' in category:
            str3.append('Product_Category')
        if 'Product_Company' not in df_col and manufacturer==['industry']:
            df.insert(2, 'Product_Company', 'industry')
            str3.append('Product_Company')
        elif 'Product_Company' not in df_col and 'industry' in manufacturer:
            str3.append('Product_Company')
        df.update(df[kpi].fillna(0))
        
        if ('Product_Category' in str3) or ('ROLLUP(Product_Category)' in str3):  #rahul2802
            if any(df.Product_Category.isnull()):
                df["Product_Category"]=df.Product_Category.replace(np.nan,'nartd')
            else:
                pass
        if ('Product_Company' in str3) or ('ROLLUP(Product_Company)' in str3):  #rahul2802
            if any(df.Product_Company.isnull()):
                df["Product_Company"]=df.Product_Company.replace(np.nan,'industry')
            else:
                pass
        
#        if cluster==['koreaonpremise', 'koreaoffpremise']:
#            df.Geography_Name='korea'
#        elif len(cluster)==2 and (any('quarterly' in string for string in cluster)==True or any('hybrid' in string for string in cluster)==True):
#            df.Geography_Name=[x.replace('quarterly','') for x in df.Geography_Name]
#            df.Geography_Name=[x.replace('hybrid','') for x in df.Geography_Name]
#        else:
#            pass
                
        col_list=['Geography_Name','Product_Category','Product_Company','Product_Trademark','Product_Brand',
        'Product_Bev_Product','Product_Flavours','Product_Type_of_Consumption','Product_Refillable','Channel_Name',
        'Product_Container_Type','Product_Container_Material','Product_Single_Multi_Pack']
      
        for col in col_list:
            if col in df.columns:
                df[col] = df[col].str.replace('\W', '')

        kd1={'koreaonpremise':'korea','koreaoffpremise':'korea','italyquarterly':'italy',
            'saudiarabiahybrid':'saudiarabia','saudiarabiaquarterly':'saudiarabia','omanquarterly':'oman',
            'romaniaquarterly':'romania', "polandquarterly":"poland",
            "switzerlandquarterly":"switzerland","kuwaitquarterly":"kuwait",
            "qatarquarterly":"qatar","nireland":"ireland",'top40saudiarabiahybrid':'saudiarabia',}
        
        cluster[:]=[kd1.get(e,e) for e in cluster]
        
        df=df.replace({"Geography_Name" : kd1})
            
        df_col1=list(set(df.columns.tolist()) - set(kpi))    
        df=df.groupby(df_col1).sum()
        df.reset_index(inplace=True)
        if 'ROLLUP(Product_Company)' in str3:
            str3=[sub.replace('ROLLUP(Product_Company)', 'Product_Company') for sub in str3]
        else:
            pass
        if 'ROLLUP(Product_Category)' in str3:
            str3=[sub.replace('ROLLUP(Product_Category)', 'Product_Category') for sub in str3]
        else:
            pass
    
        ##print(category, manufacturer, cluster)
        
        # =============================================================================
        '''colume for share and mix calculation'''#column identify
        # =============================================================================
    
        c=dim1
        
        if (c in category_list) or (c=='category') or (c=='Product_Category'):
            dim3="Product_Category"
        elif (c in manuf_list and 'brand' not in ques) or (c=='manufacturer') or (c=='Product_Company'):
            dim3="Product_Company"
        elif ((c in brand_list and 'trademark' not in ques) or (c=='brand') or (c=='Product_Brand')) and (c not in brand_genlst) :
            dim3="Product_Brand"
        elif (c in trademark_list and 'brand' not in ques) or (c=='trademark') or (c=='Product_Trademark'):
            dim3="Product_Trademark"
        elif (c in channel_list) or (c=='channel') or (c=='Channel_Name'):
            dim3="Channel_Name"
        elif (c in pack_size) or (c in pack_size) or (c=='Product_Pack_Size'):
            dim3="Product_Pack_Size"
        elif (c in packtype_list) or (c=='packtype') or (c=='Product_Single_Multi_Pack'):
            dim3="Product_Single_Multi_Pack"
        elif (c in material_list) or (c=='material') or (c=='Product_Container_Material'):
            dim3="Product_Container_Material"
        elif (c in container_list) or (c=='container') or (c=='Product_Container_Type'):
            dim3="Product_Container_Type"
        elif (c in consumption_list) or (c=='consumption') or (c=='Product_Type_of_Consumption'):
            dim3="Product_Type_of_Consumption"
        elif (c in refillable_list) or (c=='refillable') or (c=='Product_Refillable'):
            dim3="Product_Refillable"
        elif (c in country_list) or (c in fo_list) or (c in og_list) or (c in bu_list) or (c in t40_lst_agg)  or (c=='Geography_Name'):
            dim3="Geography_Name"
        else:
            dim3="Product_Company"
        try:
            str3.remove(dim3)
        except ValueError:
            pass
        str3.append(dim3)
    
        #Finding second dimensions for share driver anlaysis
        if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
            try:
                dim2_numm=sorted(set([n for n in dim_number_lst  if n>=0]))[1]
                dim2_num_ix=dim_number_lst.index(dim2_numm)
                dim21=test_x[dim2_num_ix] 
            except:
                dim21='nartd'    
            
                    #for share drive identify base column
            if (dim21 in category_list) or (dim21=='category') or (dim21=='Product_Category'):
                dim31="Product_Category"
            elif (dim21 in manuf_list and 'brand' not in ques) or (dim21=='manufacturer') or (dim21=='Product_Company'):
                dim31="Product_Company"
            elif ((dim21 in brand_list and 'trademark' not in ques) or (dim21=='brand') or (dim21=='Product_Brand')) and (dim21 not in brand_genlst) :
                dim31="Product_Brand"
            elif (dim21 in trademark_list and 'brand' not in ques) or (dim21=='trademark') or (dim21=='Product_Trademark'):
                dim31="Product_Trademark"
            elif (dim21 in channel_list) or (dim21=='channel') or (dim21=='Channel_Name'):
                dim31="Channel_Name"
            elif (dim21 in pack_size) or (dim21 in pack_size) or (dim21=='Product_Pack_Size'):
                dim31="Product_Pack_Size"
            elif (dim21 in packtype_list) or (dim21=='packtype') or (dim21=='Product_Single_Multi_Pack'):
                dim31="Product_Single_Multi_Pack"
            elif (dim21 in material_list) or (dim21=='material') or (dim21=='Product_Container_Material'):
                dim31="Product_Container_Material"
            elif (dim21 in container_list) or (dim21=='container') or (dim21=='Product_Container_Type'):
                dim31="Product_Container_Type"
            elif (dim21 in consumption_list) or (dim21=='consumption') or (dim21=='Product_Type_of_Consumption'):
                dim31="Product_Type_of_Consumption"
            elif (dim21 in refillable_list) or (dim21=='refillable') or (dim21=='Product_Refillable'):
                dim31="Product_Refillable"
            elif (dim21 in country_list) or (dim21 in fo_list) or (dim21 in og_list) or (dim21 in bu_list) or (dim21 in t40_lst_agg)  or (dim21=='Geography_Name'):
                dim31="Geography_Name"
            else:
                dim31="Product_Company"
                
            

    #    try:
    #        str3.remove(dim3)
    #    except ValueError:
    #        pass
    #    str3.append(dim3)
        
#        col_list=['Geography_Name','Product_Category','Product_Company','Product_Trademark','Product_Brand',
#        'Product_Bev_Product','Product_Flavours','Product_Type_of_Consumption','Product_Refillable','Channel_Name',
#        'Product_Container_Type','Product_Container_Material','Product_Single_Multi_Pack']
#      
#        for col in col_list:
#            if col in df.columns:
#                df[col] = df[col].str.replace('\W', '')
        
        df = df[str3+kpi]
        coke_data= pd.DataFrame()
        coke_data= coke_data.append(df)
    #    coke_data = coke_data[str3+kpi]
        
        if (len(top_bottom)>0):
            numrange=750
            numrange1=600
        else:
            numrange=550
            numrange1=300 
            
        if len(measure5)==0 and len(input_fltr_lst1)<5: 
            if (len(brand_name)>20) and (('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0)))!=True):      # for creating other brands if i have more than 20 brands in my brand_name variable.
                major_brand_name=list(set(brand_name)&set(major_brand_list))
                other_brand_name=list(set(major_brand_name)^set(brand_name))
                replacement_map = {str(i1): str(i1) for i1 in (major_brand_name)}
                coke_data['Product_Brand'] = coke_data['Product_Brand'].map(replacement_map)
                coke_data.Product_Brand=coke_data.Product_Brand.fillna('others')
                coke_data_o=coke_data[coke_data['Product_Brand']=='others']
                coke_data=coke_data[coke_data['Product_Brand']!='others']
                cd1_cols=coke_data.columns.tolist()
                if len(kpi)==2:
                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data.loc[numrange:,'Product_Brand'] = 'others'
                    coke_data=pd.concat([coke_data,coke_data_o])
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum() 
                elif len(kpi)==4:
                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data.loc[numrange:,'Product_Brand'] = 'others'
                    coke_data=pd.concat([coke_data,coke_data_o])
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data=coke_data.groupby(cd1_cols[:-4]).sum()
                coke_data.reset_index(inplace=True)
    
            else:
                pass
            
            if (len(trademark)>20) and (('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0)))!=True): 
                major_trademark_name=list(set(trademark)&set(major_trademark_list))
                other_trademark_name=list(set(major_trademark_name)^set(trademark))
                replacement_map = {str(i1): str(i1) for i1 in (major_trademark_name)}
                coke_data['Product_Trademark'] = coke_data['Product_Trademark'].map(replacement_map)
                coke_data.Product_Trademark=coke_data.Product_Trademark.fillna('others')
                coke_data_o=coke_data[coke_data['Product_Trademark']=='others']
                coke_data=coke_data[coke_data['Product_Trademark']!='others']
                cd1_cols=coke_data.columns.tolist()
                if len(kpi)==2:
                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data.loc[numrange1:,'Product_Trademark'] = 'others'
                    coke_data=pd.concat([coke_data,coke_data_o])
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum() 
                elif len(kpi)==4:
                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data.loc[numrange1:,'Product_Trademark'] = 'others'
                    coke_data=pd.concat([coke_data,coke_data_o])
                    coke_data=coke_data.reset_index(drop=True)
                    coke_data=coke_data.groupby(cd1_cols[:-4]).sum()
                else:
                    pass
                coke_data.reset_index(inplace=True)
            else:
                pass
        
            if (len(manufacturer)>20 ) and (('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0)))!=True): 
                major_company_name=list(set(manufacturer)&set(major_company_list))
                other_company_name=list(set(manufacturer)^set(major_company_name))
                replacement_map = {str(i1): str(i1) for i1 in (major_company_name)}
                coke_data['Product_Company'] = coke_data['Product_Company'].map(replacement_map)
                coke_data.Product_Company=coke_data.Product_Company.fillna('others')
                cd1_cols=coke_data.columns.tolist()
                if len(kpi)==2:
                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum() 
                elif len(kpi)==4:      
                    coke_data=coke_data.groupby(cd1_cols[:-4]).sum()
                else:
                    pass
                if measure1==['share']:
                    manufacturer.append('others')
                else:
                    pass
                coke_data.reset_index(inplace=True) 
            else:
                pass
        else:
            pass

#            if (len(manufacturer)>20) and (('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0)))!=True):      # for creating other brands if i have more than 20 brands in my brand_name variable.
#                major_company_name=list(set(manufacturer)&set(major_company_list))
#                other_company_name=list(set(manufacturer)^set(major_company_name))
#                replacement_map = {str(i1): str(i1) for i1 in (major_company_name)}
#                coke_data['Product_Company'] = coke_data['Product_Company'].map(replacement_map)
#                coke_data.Product_Company=coke_data.Product_Company.fillna('others')
#                coke_data_o=coke_data[coke_data['Product_Company']=='others']
#                coke_data=coke_data[coke_data['Product_Company']!='others']
#                cd1_cols=coke_data.columns.tolist()
#                if len(kpi)==2:
#                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
#                    coke_data=coke_data.reset_index(drop=True)
#                    coke_data.loc[numrange:,'Product_Company'] = 'others'
#                    coke_data=pd.concat([coke_data,coke_data_o])
#                    coke_data=coke_data.reset_index(drop=True)
#                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum() 
#                elif len(kpi)==4:
#                    coke_data=coke_data.sort_values(kpi[0],ascending=False)
#                    coke_data=coke_data.reset_index(drop=True)
#                    coke_data.loc[numrange:,'Product_Company'] = 'others'
#                    coke_data=pd.concat([coke_data,coke_data_o])
#                    coke_data=coke_data.reset_index(drop=True)
#                    coke_data=coke_data.groupby(cd1_cols[:-4]).sum()
#                coke_data.reset_index(inplace=True)
#    
#            else:
#                pass 
#        else:
#            pass            
#        if cluster==['koreaonpremise', 'koreaoffpremise'] and cluster in input_fltr_lst:
#            cluster=['korea']
#        elif cluster==['saudiarabiaquarterly', 'saudiarabiahybrid'] and cluster in input_fltr_lst:
#            cluster=['saudiarabia']
#        else:
#            pass
        dim_dict={'Geography_Name':cluster,'Product_Company':manufacturer,'Product_Bev_Product':bev_name,'Product_Category':category,'Product_Trademark':trademark,
              'Product_Pack_Size':pack_size,'Channel_Name':channel,'Product_Single_Multi_Pack':packtype,'Product_Container_Type':container,
              'Product_Container_Material':material,'Product_Type_of_Consumption':consum_type,'Product_Refillable':refillable,
              'ROLLUP(Product_Company)':manufacturer,'ROLLUP(Product_Category)':category,'Period_Id':month_sql,
              'Product_Brand':brand_name,'Product_Flavours':flavour_name}
        
        # =============================================================================
        '''calculation'''#column identify
        # =============================================================================
#        if ('sale' in measure1 or (('size' in ques) and (('pack' not in ques) or ('packsize' not in ques)))) and (len(measure5)==0 and len(measure1)==0 and len(measure2)==0 and len(measure3)==0):
        if ('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0))) and (len(measure5)==0):
            if len(manufacturer)>20:
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            else:
                coke_data[kpi0[0]]=coke_data[kpi[0]] #need to check
    #        coke_data[kpi0[0]]=coke_data[kpi0[0]] 
    #        coke_data[kpi0[0]]=df[kpi[0]]
            if ('change' in measure2):
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                coke_data=coke_data.drop(kpi,axis=1)
            else:
                coke_data=coke_data.drop(kpi,axis=1) 
                
        elif ('growth' in measure1) and (len(measure5)==0):
            if coke_data[kpi[1]].sum()==0:
                details_dict["error"]="previous year value is zero,calculation of grouth is not possible."
            else:
                coke_data = coke_data[coke_data[kpi[1]] != 0]
                coke_data[kpi0[0]]= ((coke_data[kpi[0]]/coke_data[kpi[1]])-1)*100
                coke_data=coke_data.sort_values(kpi0[-1], ascending=False)
                coke_data=coke_data.drop(kpi,axis=1)
                test_coke_data1=coke_data##sat
            #    if 'accelerate' in ques:
            #        coke_data['Period']= coke_data['Period_Id'].str[-2:]
            #        d1=coke_data[coke_data['Period_Id'].str.contains(year_list[1])]
            #        d2=coke_data[coke_data['Period_Id'].str.contains(year_list[2])]
            #        colname=coke_data.columns.tolist()
            #        colname.remove('Period_Id')
            #        colname.remove(kpi0[0])
            #        coke_data=(pd.merge(d1, d2, on=colname, suffixes=('_py','')))
            ##        coke_data=(pd.merge(d4, d3, on=colname))
            #        coke_data['acceleration']=coke_data[kpi0[0]]-coke_data[kpi0[0]+'_py']
            #        coke_data.drop(['Period_Id_py',kpi0[0]+'_py','Period'], axis=1, inplace=True)
        
        elif (len(measure5)>0) and (measure1[0] not in ['share']) and measure3!=['mix']:
            l=[]
            for i in range(len(str3)-1):
                l.append(i)
            coke_data[kpi0[0]]= (df[kpi[0]]-df[kpi[1]])
            if dim3=='Geography_Name'  and (len(bu_cluster)==(len(bu_list)-1) or len(og_cluster)==(len(og_list)-1) or len(country_cluster)==(len(country_list)-2) or country_cluster in all_bu_lst or country_cluster in all_og_lst or top40_cluster in all_t40_bu_lst or top40_cluster in all_t40_og_lst ) and ('world' not  in ques and 't40 world' not  in ques):
#                if 't40' in ques or 'top40' in ques:
#                    coke_data=coke_data[coke_data.Geography_Name!='top40world']
#                    coke_data=coke_data[coke_data.Geography_Name!='world']                   
#                elif len(coke_data.Geography_Name.unique().tolist())==2 and 'world' in (coke_data.Geography_Name.unique().tolist()):
#                    pass
#                else:
#                    coke_data=coke_data[coke_data.Geography_Name!='world']
#                coke_data= coke_data.set_index(str3)
#                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
#                coke_data.reset_index(inplace=True)
#                coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]
#                coke_data=coke_data.drop(kpi,axis=1)

                if len(coke_data.Geography_Name.unique().tolist())==2 and 'world' in (coke_data.Geography_Name.unique().tolist()):
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('world')]
                    coke_data=df_1.set_index(list(set(df_1.columns.tolist()) - set([kpi0[0]]))).div(dftest.set_index(list(set(df_1.columns.tolist()) - set([kpi0[0]]))).sum())
                    coke_data.reset_index(inplace=True)
                    coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]*100
                    coke_data=coke_data.drop(kpi,axis=1)
                else:
                    if 't40' in ques or 'top40' in ques:
                        coke_data=coke_data[coke_data.Geography_Name!='top40world']
                        coke_data=coke_data[coke_data.Geography_Name!='world']
                    else:
                        coke_data=coke_data[coke_data.Geography_Name!='world']
                        
                    coke_data= coke_data.set_index(str3)
                    coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                    coke_data.reset_index(inplace=True)
                    coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]
                    coke_data=coke_data.drop(kpi,axis=1)

                
            elif dim3=='Geography_Name'  and((len(bu_cluster)<(len(bu_list)-1)) and (len(og_cluster)<(len(og_list)-1)) and (len(country_cluster)<(len(country_list)-2))):
                str_test=str3[:-1]
                if 't40' in ques or 'top40' in ques:
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('top40world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('top40world')]
                else:
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('world')]
                df_1.reset_index(drop=True,inplace=True)
                dftest.reset_index(drop=True,inplace=True)
    
                df_merge = pd.merge(df_1, dftest, left_on=str_test, right_on=str_test, how="left", validate="m:m")
                df_merge.columns = df_merge.columns.str.rstrip('_x')
                df_merge[kpi[0]]=df_merge[kpi0[0]]/df_merge[kpi0[0]+'_y']*100
    #            df_merge[kpi[1]]=df_merge[kpi[1]]/df_merge[kpi[1]+'_y']*100
                df_merge = df_merge.loc[:, ~df_merge.columns.str.endswith('_y')]
                coke_data=df_merge
                coke_data[kpi0[0]]=coke_data.loc[:,kpi[0]]
                coke_data=coke_data.drop(kpi,axis=1) 
    #            
    
            else:
                l=[]
                for i in range(len(str3)-1):
                    l.append(i)
                coke_data[kpi0[0]]= (df[kpi[0]]-df[kpi[1]])
                coke_data=coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
    #            coke_data_111=coke_data.copy()
                if (category==['nartd'] and manufacturer==['industry']) or (len(input_fltr_lst1)+len(input_Dim_lst1)>4):
                    coke_data=coke_data.drop(kpi,axis=1)                
                elif category==['nartd']:
                    coke_data=coke_data.drop(kpi,axis=1)
                    coke_data=coke_data[coke_data.Product_Company!='industry']
                    coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]*2     
                elif manufacturer==['industry']:
                    coke_data=coke_data.drop(kpi,axis=1)
                    coke_data=coke_data[coke_data.Product_Category!='nartd']
                    coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]*2
                else:
                    coke_data[kpi0[0]]=coke_data.loc[:,kpi0[0]]*2
                    coke_data=coke_data.drop(kpi,axis=1)
  
    #            
    
        elif 'price' in measure1:
            if coke_data[kpi[1]].sum()==0:
                details_dict["error"]="previous year value is zero,calculation of growth is not possible."
            else:
                coke_data = coke_data[coke_data[kpi[1]] != 0]
                coke_data = coke_data[coke_data[kpi[3]] != 0]
            if 'mix' in measure3:   
                coke_data["growth_value"]= (coke_data[kpi[0]]/coke_data[kpi[1]])-1
                coke_data["growth_volume"]= (coke_data[kpi[2]]/coke_data[kpi[3]])-1
                coke_data[kpi0[0]]= (coke_data["growth_value"]-coke_data["growth_volume"])*100 
                coke_data=coke_data.drop(kpi,axis=1)
                coke_data.drop(["growth_value","growth_volume"],axis=1,inplace=True)
                coke_data.reset_index(drop=True,inplace=True)
            else:
                coke_data[kpi0[0]]= (coke_data[kpi[0]]/coke_data[kpi[2]])
                coke_data[kpi0[0]+'_py']= (coke_data[kpi[1]]/coke_data[kpi[3]])
                coke_data=coke_data.drop(kpi,axis=1)
                if 'change' in measure2:
                    coke_data[kpi0[1]]=((coke_data[kpi0[0]]/coke_data[kpi0[0]+'_py'])-1)*100
                else:
                    pass
                coke_data.drop([kpi0[0]+'_py'],axis=1,inplace=True)
                coke_data.reset_index(drop=True,inplace=True)
         
        elif ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
            l=[]
            for i in range(len(str3)-1):
                l.append(i)
            coke_data_d=coke_data.copy()
            str4=str3.copy()
            str4.remove(dim3)
            
            if dim3=='Geography_Name' and any('world' in string for string in cluster):
                str_test=str3[:-1]
                coke_data_d=coke_data_d[coke_data_d.Geography_Name.str.contains(r'\bworld\b',case=False)]
                coke_data.reset_index(drop=True,inplace=True)
                coke_data_d.reset_index(drop=True,inplace=True)
                coke_data_d[kpi[0]]=coke_data_d[kpi[0]].sum()/2
                coke_data_d[kpi[1]]=coke_data_d[kpi[1]].sum()/2             
                df_merge = pd.merge(coke_data, coke_data_d, left_on=str_test, right_on=str_test, how="left", validate="m:m")
                df_merge.columns = df_merge.columns.str.rstrip('_x')
                df_merge[kpi[0]]=df_merge[kpi[0]]/df_merge[kpi[0]+'_y']*100
                df_merge[kpi[1]]=df_merge[kpi[1]]/df_merge[kpi[1]+'_y']*100
                df_merge = df_merge.loc[:, ~df_merge.columns.str.endswith('_y')]
                coke_data=df_merge.copy()
                coke_data.fillna(0, inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                coke_data=coke_data.drop(kpi,axis=1)  

            elif dim3=='Product_Category':
                str_test=str3[:-1]
                coke_data_d=coke_data_d[coke_data_d.Product_Category.str.contains(r'\bnartd\b',case=False)]
                coke_data.reset_index(drop=True,inplace=True)
                coke_data_d.reset_index(drop=True,inplace=True)          
    
                df_merge = pd.merge(coke_data, coke_data_d, left_on=str_test, right_on=str_test, how="left", validate="m:m")
                df_merge.columns = df_merge.columns.str.rstrip('_x')
                df_merge[kpi[0]]=df_merge[kpi[0]]/df_merge[kpi[0]+'_y']*100
                df_merge[kpi[1]]=df_merge[kpi[1]]/df_merge[kpi[1]+'_y']*100
                df_merge = df_merge.loc[:, ~df_merge.columns.str.endswith('_y')]
                coke_data=df_merge.copy()
                coke_data[kpi0[0]]=coke_data[kpi[0]]
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                coke_data=coke_data.drop(kpi,axis=1)        
            else: 
                del coke_data_d[dim3]
                coke_data_d= coke_data_d.set_index(str4) 
#                coke_data_d=(coke_data_d.groupby(level=l[:-2]).transform(sum)/2)
                coke_data_d=(coke_data_d.groupby(level=l[:-2]).transform(sum)/2)
                coke_data_d.reset_index(inplace=True)
                if len(coke_data_d.Product_Category.unique())>1:
                    coke_data_d[kpi[0]]=(coke_data_d[kpi[0]]).div(2)                 
                    coke_data_d[kpi[1]]=(coke_data_d[kpi[1]]).div(2)
                else:
                    pass
                
#                if dim3=='Product_Company':
#                    coke_data[kpi[0]]=(coke_data[kpi[0]].div(coke_data_d[kpi[0]], axis=0)*100)*2
#                    coke_data[kpi[1]]=(coke_data[kpi[1]].div(coke_data_d[kpi[1]], axis=0)*100)*2
#                else:
                coke_data[kpi[0]]=coke_data[kpi[0]].div(coke_data_d[kpi[0]], axis=0)*100
                coke_data[kpi[1]]=coke_data[kpi[1]].div(coke_data_d[kpi[1]], axis=0)*100                    
                coke_data[kpi0[0]]=coke_data[kpi[0]]
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                coke_data=coke_data.drop(kpi,axis=1)
            if dim3=='Geography_Name':
                coke_data=coke_data[coke_data["Geography_Name"]!='world']
                coke_data.reset_index(inplace=True)
                
        elif ('share' in measure1) or ('mix' in measure3):
            l=[]
            for i in range(len(str3)-1):
                l.append(i)
            if dim3=='Geography_Name'  and (len(bu_cluster)==(len(bu_list)-1) or len(og_cluster)==(len(og_list)-1) or len(country_cluster)==(len(country_list)-2) or country_cluster in all_bu_lst or country_cluster in all_og_lst or top40_cluster in all_t40_bu_lst or top40_cluster in all_t40_og_lst ) and ('world' not  in ques and 't40 world' not  in ques):
    #        if dim3=='Geography_Name'  and (len(bu_cluster)==(len(bu_list)-1) or len(og_cluster)==(len(og_list)-1) or len(country_cluster)==(len(country_list)-2) or country_cluster in all_bu_lst or country_cluster in all_og_lst or top40_cluster in all_t40_bu_lst or top40_cluster in all_t40_og_lst ):
                if len(coke_data.Geography_Name.unique().tolist())==2 and 'world' in (coke_data.Geography_Name.unique().tolist()):
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('world')]
                    coke_data=df_1.set_index(list(set(df_1.columns.tolist()) - set(kpi))).div(dftest.set_index(list(set(df_1.columns.tolist()) - set(kpi))).sum())
                    coke_data.reset_index(inplace=True)
                    coke_data[kpi]=coke_data[kpi]*100
                    coke_data[kpi0[0]]=coke_data[kpi[0]]
                else:
                    if 't40' in ques or 'top40' in ques:
                        coke_data=coke_data[coke_data.Geography_Name!='top40world']
                        coke_data=coke_data[coke_data.Geography_Name!='world']
                    else:
                        coke_data=coke_data[coke_data.Geography_Name!='world']
                        
                    coke_data= coke_data.set_index(str3)
                    coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                    coke_data.reset_index(inplace=True)
                    coke_data[kpi0[0]]=coke_data[kpi[0]]

            elif dim3=='Geography_Name'  and((len(bu_cluster)<=(len(bu_list)-1)) and (len(og_cluster)<=(len(og_list)-1)) and (len(country_cluster)<=(len(country_list)-2))):
                str_test=str3[:-1]
                if ('t40' in ques or 'top40' in ques) and (cluster!=['top40world','world']):
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('top40world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('top40world')]
                elif (cluster==['top40world','world']):
                    df_1=coke_data[coke_data.Geography_Name=='top40world']
                    dftest=coke_data[coke_data.Geography_Name=='world']
                else:
                    df_1=coke_data[~coke_data.Geography_Name.str.contains('world')]
                    dftest=coke_data[coke_data.Geography_Name.str.contains('world')]
                df_1.reset_index(drop=True,inplace=True)
                dftest.reset_index(drop=True,inplace=True)          
    
                df_merge = pd.merge(df_1, dftest, left_on=str_test, right_on=str_test, how="left", validate="m:m")
                df_merge.columns = df_merge.columns.str.rstrip('_x')
                df_merge[kpi[0]]=df_merge[kpi[0]]/df_merge[kpi[0]+'_y']*100
                df_merge[kpi[1]]=df_merge[kpi[1]]/df_merge[kpi[1]+'_y']*100
                df_merge = df_merge.loc[:, ~df_merge.columns.str.endswith('_y')]
                coke_data=df_merge
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            
            elif category==['nartd'] and manufacturer==['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and (dim3!='Product_Category' or dim3!='Product_Company'):
                coke_data= coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            elif category!=['nartd'] and manufacturer==['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Category': 
                coke_data= coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            elif category==['nartd'] and manufacturer!=['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Company': 
                coke_data= coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]    
            elif category!=['nartd'] and manufacturer!=['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Company' and dim3!='Product_Category':  #rahul3003
                coke_data= coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            elif category==['nartd'] and manufacturer==['industry']:
                if len(kpi)==2:
                    coke_data[kpi[0]]= coke_data[kpi[0]].transform(lambda x: (x / x.sum())*100)
                    coke_data[kpi[1]]= coke_data[kpi[1]].transform(lambda x: (x / x.sum())*100)
                else:
                    coke_data[kpi[0]]= coke_data[kpi[0]].transform(lambda x: (x / x.sum())*100)
                coke_data[kpi0[0]]=coke_data[kpi[0]]
            else:
                coke_data= coke_data.set_index(str3)
                coke_data= (coke_data/coke_data.groupby(level=l).transform(sum)*100)
                coke_data.reset_index(inplace=True)
                coke_data[kpi0[0]]=coke_data[kpi[0]]*2
              
            if ('change' in measure2) or len(measure5)>0:
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                if dim3=='Geography_Name'  and (len(bu_cluster)==(len(bu_list)-1) or len(og_cluster)==(len(og_list)-1) or len(country_cluster)==(len(country_list)-2) or country_cluster in all_bu_lst or country_cluster in all_og_lst or top40_cluster in all_t40_bu_lst or top40_cluster in all_t40_og_lst):
                    coke_data=coke_data.drop(kpi,axis=1)
                elif dim3=='Geography_Name'  and((len(bu_cluster)<(len(bu_list)-1)) and (len(og_cluster)<(len(og_list)-1)) and (len(country_cluster)<(len(country_list)-2))):
                    coke_data=coke_data.drop(kpi,axis=1)
                elif category==['nartd'] and manufacturer==['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and (dim3!='Product_Category' or dim3!='Product_Company'):
                    coke_data=coke_data.drop(kpi,axis=1)
                elif category!=['nartd'] and manufacturer==['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Category': 
                    coke_data=coke_data.drop(kpi,axis=1)
                elif category==['nartd'] and manufacturer!=['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Company': 
                    coke_data=coke_data.drop(kpi,axis=1)
                elif category!=['nartd'] and manufacturer!=['industry'] and (len(input_fltr_lst1+input_Dim_lst1))>=4 and dim3!='Product_Company' and dim3!='Product_Category':  #rahul3003
                    coke_data=coke_data.drop(kpi,axis=1)
                elif category==['nartd'] and manufacturer==['industry']:
                    coke_data=coke_data.drop(kpi,axis=1)    
                else:
                    coke_data[kpi0[1]]=coke_data[kpi0[1]]*2
                    coke_data=coke_data.drop(kpi,axis=1)
            elif ('swing' in measure2):
                coke_data[kpi0[1]]= coke_data[kpi[0]]-coke_data[kpi[1]]
                coke_data[kpi0[1]]=coke_data[kpi0[1]]*2
                for i in str3:
                    if i in dim_dict.keys():
                        n=dim_dict.get(i)
                        coke_data=coke_data[(coke_data[i].isin(n))]
                array = np.asarray(coke_data[kpi0[1]].tolist())-coke_data[coke_data.Product_Company==manufacturer2[0]][kpi0[1]].tolist()[0]
                coke_data[kpi0[1]] = array
                coke_data=coke_data.drop(kpi,axis=1)
            else:
                coke_data=coke_data.drop(kpi,axis=1)
    
        else:
            l=[]
            for i in range(len(str3)-1):
                l.append(i)
            coke_data= coke_data.set_index(str3)
            coke_data= (coke_data/coke_data.groupby(level=[l]).transform(sum)*100)
            coke_data.reset_index(drop=True,inplace=True)
    
        
#        if len(measure5)>0:
#            if len(brand_name)>20:      # for creating other brands if i have more than 20 brands in my brand_name variable.
#                major_brand_name=list(set(brand_name)&set(major_brand_list))
#                other_brand_name=list(set(major_brand_name)^set(brand_name))
#                replacement_map = {str(i1): str(i1) for i1 in (major_brand_name)}
#                coke_data['Product_Brand'] = coke_data['Product_Brand'].map(replacement_map)
#                coke_data.Product_Brand=coke_data.Product_Brand.fillna('others')
#                cd1_cols=coke_data.columns.tolist()
#                if len(kpi0)==1:
#                    coke_data=coke_data.groupby(cd1_cols[:-1]).sum()  
#                elif len(kpi)==2:      
#                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum()
#                coke_data.reset_index(inplace=True)
#            else:
#                pass
#                
#            if len(trademark)>20:      # for creating other trademark if i have more than 20 brands in my brand_name variable.
#                major_trademark_name=list(set(trademark)&set(major_trademark_list))
#                other_trademark_name=list(set(major_trademark_name)^set(trademark))
#                replacement_map = {str(i1): str(i1) for i1 in (major_trademark_name)}
#                coke_data['Product_Trademark'] = coke_data['Product_Trademark'].map(replacement_map)
#                coke_data.Product_Trademark=coke_data.Product_Trademark.fillna('others')
#                cd1_cols=coke_data.columns.tolist()
#                if len(kpi0)==1:
#                    coke_data=coke_data.groupby(cd1_cols[:-1]).sum()
#                elif len(kpi0)==2:      
#                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum()
#                coke_data.reset_index(inplace=True)
#            else:
#                pass
#        
#            if len(manufacturer)>20:      # for creating other company if i have more than 20 brands in my brand_name variable.
#                major_company_name=list(set(manufacturer)&set(major_company_list))
#                other_company_name=list(set(major_company_name)^set(manufacturer))
#                replacement_map = {str(i1): str(i1) for i1 in (major_company_name)}
#                coke_data['Product_Company'] = coke_data['Product_Company'].map(replacement_map)
#                coke_data.Product_Company=coke_data.Product_Company.fillna('others')
#                cd1_cols=coke_data.columns.tolist()
#                if len(kpi0)==1:
#                    coke_data=coke_data.groupby(cd1_cols[:-1]).sum() 
#                elif len(kpi)==2:      
#                    coke_data=coke_data.groupby(cd1_cols[:-2]).sum()
#                coke_data.reset_index(inplace=True)
#            else:
#                pass
#        else:
#            pass
#        
      
        if coke_data['Product_Category'].unique().tolist()!=['nartd']:
                coke_data=coke_data[coke_data.Product_Category!='nartd']
        else:
            pass
        if coke_data.Product_Company.unique().tolist()!=['industry']:
                coke_data=coke_data[coke_data.Product_Company!='industry']
        else:
            pass
        
        coke_data1=coke_data.copy()
    #    coke_data1=coke_data1[~coke_data1.isin([np.inf, -np.inf]).any(1)] 
    #    coke_data1=coke_data1[coke_data1[kpi0[-1]]!=inf]
        cd1_cols=coke_data1.columns.tolist()
        if len(kpi0)==1:
            coke_data1=coke_data1.groupby(cd1_cols[:-1]).sum()
            coke_data1=coke_data1.sort_values(kpi0[-1],ascending=False)
        elif len(kpi0)==2:
            coke_data1=coke_data1.groupby(cd1_cols[:-2]).sum()
            coke_data1=coke_data1.sort_values(kpi0[-1],ascending=False)
        coke_data1.reset_index(inplace=True)
    
        
        # =============================================================================
        '''DF_filter and dimention finding'''#This machine is used to extrat final DF from SQL
        # =============================================================================
        coke_data1_share_driver=coke_data1.copy()
#        print(coke_data1_share_driver)
        if ('swing' not in measure2):#total kpi is changed compared to 2341 line
            for i in str3:
                if i in dim_dict.keys():
    #                print(i)
                    n=dim_dict.get(i)
    #                print(n)
                    coke_data1=coke_data1[(coke_data1[i].isin(n))]
                    coke_data1.reset_index(drop=True,inplace=True)
            
            for i in str3:
                if len(coke_data1[i].unique())==1:
                    coke_data1.reset_index(drop=True,inplace=True)
#        print(coke_data1)
        
        if ('swing' not in measure2) and ('share' in measure1) and measure5==['driving'] and 'nartd' in ques and len(coke_data1.index)==0:#total kpi is changed compared to 2341 line
            try:
                del dim_dict['Product_Company']
            except KeyError:
                pass
        
        
            for i in str3:
                if i in dim_dict.keys():
    #                print(i)
                    n=dim_dict.get(i)
    #                print(n)
                    coke_data1=coke_data1_share_driver[(coke_data1_share_driver[i].isin(n))]
                    coke_data1.reset_index(drop=True,inplace=True)
            
            for i in str3:
                if len(coke_data1[i].unique())==1:
                    coke_data1.reset_index(drop=True,inplace=True)
        
        
        
    #    for absolute grouping
        if ('sale' in measure1 or (('size' in ques) and (len(pack_size) == 0))):
            cd1_cols=coke_data1.columns.tolist()
            if 'Product_Brand' in cd1_cols  and dim3=='Product_Brand':
                if len(coke_data1.Product_Brand.tolist())>20 :      # for creating other brands if i have more than 20 brands in my brand_name variable.
                    major_brand_name=list(set(brand_name)&set(major_brand_list))
                    other_brand_name=list(set(major_brand_name)^set(brand_name))
                    replacement_map = {str(i1): str(i1) for i1 in (major_brand_name)}
                    coke_data1['Product_Brand'] = coke_data1['Product_Brand'].map(replacement_map)
                    coke_data1.Product_Brand=coke_data1.Product_Brand.fillna('others')
                    
                    if len(kpi)==2:
                        coke_data1=coke_data1.groupby(cd1_cols[:-1]).sum() 
                    elif len(kpi)==4:      
                        coke_data1=coke_data1.groupby(cd1_cols[:-2]).sum()
                    coke_data1.reset_index(inplace=True)
                else:
                    pass
            elif 'Product_trademark' in cd1_cols and dim3=='Product_trademark':      
                if len(coke_data1.Product_Trademark.tolist())>20 :      # for creating other trademark if i have more than 20 brands in my brand_name variable.
                    major_trademark_name=list(set(trademark)&set(major_trademark_list))
                    other_trademark_name=list(set(major_trademark_name)^set(trademark))
                    replacement_map = {str(i1): str(i1) for i1 in (major_trademark_name)}
                    coke_data1['Product_Trademark'] = coke_data1['Product_Trademark'].map(replacement_map)
                    coke_data1.Product_Trademark=coke_data1.Product_Trademark.fillna('others')
                    if len(kpi)==2:
                        coke_data1=coke_data1.groupby(cd1_cols[:-1]).sum()  
                    elif len(kpi)==4:      
                        coke_data1=coke_data1.groupby(cd1_cols[:-2]).sum()
                    else:
                        pass
                    coke_data1.reset_index(inplace=True)
                else:
                    pass
            elif 'Product_Company' in cd1_cols and dim3=='Product_Company':
                if len(coke_data1.Product_Company.tolist())>20 :      # for creating other company if i have more than 20 brands in my brand_name variable.
                    major_company_name=list(set(manufacturer) & set(major_company_list))
                    other_company_name=list(set(major_company_name)^set(manufacturer))
                    replacement_map = {str(i1): str(i1) for i1 in (major_company_name)}
                    coke_data1['Product_Company'] = coke_data1['Product_Company'].map(replacement_map)
                    coke_data1.Product_Company=coke_data1.Product_Company.fillna('others')
                    if len(kpi)==2:
                        coke_data1=coke_data1.groupby(cd1_cols[:-1]).sum() 
                    elif len(kpi)==4:      
                        coke_data1=coke_data1.groupby(cd1_cols[:-2]).sum()
                    else:
                        pass
                    coke_data1.reset_index(inplace=True)
    
            else:
                pass
        else:
            pass 
        
                    
        if len(kpi0)==1:
            coke_data1=coke_data1.sort_values(kpi0[-1],ascending=False) 
            if len(coke_data1.index)>2:
                for cols in coke_data1.columns[:-1]:
                    excluded=coke_data1[coke_data1[cols].isin(['others','otherreportednartd'])] 
                    included=coke_data1[~coke_data1[cols].isin(['others','otherreportednartd'])]  
            else:
                pass
        elif len(kpi0)==2:
            coke_data1=coke_data1.sort_values(kpi0[-1],ascending=False) 
            if len(coke_data1.index)>2:
                for cols in coke_data1.columns[:-2]:
                    excluded=coke_data1[coke_data1[cols].isin(['others','otherreportednartd'])] 
                    included=coke_data1[~coke_data1[cols].isin(['others','otherreportednartd'])]  
            else:
                pass    
    
        if len(input_Dim_lst1)==2 and 'Period_Id' in input_Dim_lst1:
            if len(top_bottom)==0:
                top_num=101
            else:
                pass
    #        top_dim2=included.sort_values(by='Period_Id',ascending=False).head(top_num)[input_Dim_lst1[-1]].tolist()
            top_dim2=included[included.Period_Id==month_sql[-1]].sort_values(by=kpi0[0],ascending=False).head(top_num)[input_Dim_lst1[-1]].tolist()
            coke_data1=coke_data1[coke_data1[input_Dim_lst1[-1]].isin(top_dim2)]
            coke_data1.reset_index(drop=True,inplace=True)
        elif (len(top_bottom)>0) and ('dragging' in ques):
            if len(input_Dim_lst1)>=1 and len(coke_data1.index)>2:
                coke_data1=pd.concat([excluded,included]) 
            else:
                pass
            coke_data1.reset_index(drop=True,inplace=True)
            coke_data1=coke_data1.tail(top_num)
        elif (len(top_bottom)>0) and ('driving' in ques):
            if len(input_Dim_lst1)>=1 and len(coke_data1.index)>2:
                coke_data1=pd.concat([included,excluded]) 
            else:
                pass
            coke_data1.reset_index(drop=True,inplace=True)
            coke_data1=coke_data1.head(top_num)
        elif (len(top_bottom)>0) or ('driving' in ques) or ('drive' in ques) or('drives' in ques) or ('dragging' in ques):
            if 'top' in top_bottom:
                if len(input_Dim_lst1)>=1 and len(coke_data1.index)>2:
                    coke_data1=pd.concat([included,excluded]) 
                else:
                    pass
                coke_data1.reset_index(drop=True,inplace=True)            
                coke_data1=coke_data1.head(top_num)
            elif 'bottom' in top_bottom:
                if len(input_Dim_lst1)>=1 and len(coke_data1.index)>2:
                    coke_data1=pd.concat([excluded,included]) 
                else:
                    pass
                coke_data1=coke_data1.tail(top_num)
            else:
                coke_data1
    
        coke_data1.reset_index(drop=True,inplace=True) 
        coke_data1.update(coke_data1[kpi0].fillna(0))
    #    if 'otherreportednartd' in coke_data1.Product_Category.tolist():
    #        coke_data1['Product_Category'] = coke_data1['Product_Category'].replace( 'otherreportednartd', 'others')
    #    else:
    #        pass
        
        # =============================================================================
        '''part2 -prompt'''
        # =============================================================================
        promt_bu_dict={'aseanbusinessunit':asean_bu,'centralandeasterneuropebusinessunit':central_eastern_europe_bu,
              'greaterchinaandkoreabusinessunit':greater_china_korea_bu,'indiasouthwestasiabusinessunit':india_southwest_asia_bu,
              'latincenterbusinessunit':latin_centerbu,'middleeastnorthafricabusinessunit':middle_east_north_africa_bu,
              'southandeastafricabusinessunit':south_east_africa_bu,'southlatinbusinessunit':south_latin_bu}
        
        month_kpi3_sql=[]
        for i in range(0, len(month_kpi1)):
            month_str=['20'.join(month_kpi1[i].split('-'))]
            month_str=[date_dictid.get(w) if w in date_dictid.keys() else w for w in month_str]
            month_kpi3_sql.append(month_str)
            month_kpi3_sql=flatten(month_kpi3_sql)
        
        '''month_year_prompt'''#This engine is for creating month year for prompt
        if len(month_sql)>1:
            month_tag2="('"+(random.choice(month_kpi3_sql))+"')"
        elif month_sql[0] not in month_kpi3_sql:
            month_tag2="('"+(month_kpi3_sql[0])+"')"
        else:
            month_tag2="('"+(month_kpi3_sql[month_kpi3_sql.index(month_sql[0])])+"')"
    
        cluster_id2=[]
        if 'world' in cluster:
            country_tag2 = [random.choice(country_list[:-1])]
        elif len(bu_cluster)>0:
            if bu_cluster[0] in promt_bu_dict.keys():
                bu_cluster1=promt_bu_dict.get(bu_cluster[0])
                country_tag2 = [random.choice(bu_cluster1)]
            else:
                country_tag2 = [random.choice(bu_list[:-1])]
            
        elif len(og_cluster)>0:
            country_tag2 = [random.choice(og_list[:-1])]
        elif len(fo_cluster)>0:
            country_tag2 = [random.choice(fo_list[:-1])]
            cluster_id2=[fo_code_dict.get(w) if w in fo_code_dict.keys() else w for w in country_tag2]
            cluster_id2="('"+(cluster_id2[0])+"')"
        else:
            try:
                country_list.remove(cluster[0])
            except ValueError:
                pass
            country_tag2 = [random.choice(country_list[:-1])]
        
        if len(cluster_id2)==0:
            cluster_id2=[cluster_code_dict.get(w) if w in cluster_code_dict.keys() else w for w in country_tag2]
            cluster_id2="('"+(cluster_id2[0])+"')"
                
        #query for dataframe promt    
        sql_query2="SELECT LOWER(Geography_Name) as Geography_Name,LOWER(Product_Category) as Product_Category, LOWER(Product_Company) as Product_Company, lOWER(Product_Brand) as Product_Brand, Period_Id \
        FROM {} WHERE Period_Id in {} and Geography_Id in {} \
        GROUP BY Geography_Name, Product_Category, Product_Company,Product_Brand, Period_Id".format(tbl,month_tag2,cluster_id2)
         
        prompt_df=pd.read_sql_query(sql_query2, conn)
        prompt_df1=prompt_df[(prompt_df['Product_Company'].isin(major_company_list)) & (prompt_df['Product_Brand'].isin(major_brand_list))]

        excludewords=['unassigned','other']
        prompt_df1[~prompt_df1.stack().str.contains('|'.join(excludewords)).any(level=0)]
#        prompt_df1=prompt_df[(~prompt_df['Product_Company'].isin(['unassigned'])) & (~prompt_df['Product_Brand'].isin(['unassigned']))]             
        
        
        if (dim3=='Product_Category' or dim3=='Geography_Name') and (measure1==['share'] or measure3==['mix']):
            kpi_alt_lst[0]=='Value CTG'
        else:
            pass 
        
        if len(prompt_df1)>0:
            prompt_df=prompt_df1.sample(n=1)
            
            prompt_df.reset_index(drop=True,inplace=True)
            country_tag2 = [prompt_df['Geography_Name'].tolist()[0].replace(' ', '')]
            category_tag2 = [prompt_df['Product_Category'].tolist()[0].replace(' ', '')]
            company_tag2 = [prompt_df['Product_Company'].tolist()[0].replace(' ', '')]
            brand_tag2 = [prompt_df['Product_Brand'].tolist()[0].replace(' ', '')]
            
            #random questions  
            pmt_list=['for all company','for top 5 company','for all category', 'for top 5 category','for all brand', 'for top 5 brand']
            ques_v1=str('What is the '+kpi_alt_lst[0].title()+' for '+ company_tag2[0]+' Company  within '+category_tag2[0]+' ?')    
            ques_v2=str('What is the '+kpi_alt_lst[0].title()+' for '+ brand_tag2[0]+' Brand within '+category_tag2[0]+' ?')    
            ques_v3=str('What is the '+kpi_alt_lst[0].title()+' for '+ company_tag2[0]+' within '+category_tag2[0]+' ?')    
            ques_v4=str('What is the '+kpi_alt_lst[0].title() +' for top 5'+' Companies'+' for '+category_tag2[0]+' ?')
        #    ques_v5=str('What is the '+kpi_alt_lst[0].title() +' within '+brand_tag2[0].title()+' for top 5'+' Brand?')
            if kpi_alt_lst[0]=='Value mix':
                ques_v6=str('What is the '+kpi_alt_lst[0].title() +' for top 5 category in ' +country_tag2[0]+' ?')
            else:
                ques_v6=str('What is the '+kpi_alt_lst[0].title() +' for top 5 company in ' +country_tag2[0]+'?') 
            details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you also like to ask</b> : '+(random.choice([ques_v1,ques_v2,ques_v3,ques_v4,ques_v6])))))
    
        else:
            details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you also like to ask</b> : '+(random.choice(random_prompt)))))
           
    #    if 'nextset' not in ques and forpm>20:
    #        details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you also like to ask</b> : '+(random.choice(['Look at the next top '+ ques])))))
    #    else:
    #        details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you also like to ask</b> : '+(random.choice([ques_v1,ques_v2,ques_v3,ques_v4,ques_v6])))))
    
    
        # =============================================================================
        '''kind_engine'''#This engine is used for deciding the required graph
        # =============================================================================
        if len(kpi0)==1:
            if len(input_Dim_lst1)==0:
                kind= 'bar'
            elif len(input_Dim_lst1)==1:
                if len(measure5)>0: #  ormeasure1==['growth'] 
                    kind= 'bar'#'waterfall'
    #            elif (('share' in measure1) or ('mix' in measure3)) and (len(measure2)==0) and ((round(coke_data1[kpi0[0]].sum(), 1)<100.50 and round(coke_data1[kpi0[0]].sum(), 1)>99.50) or (measure3==['mix'] and 199 < round(coke_data1[kpi0[0]].sum(), 1) < 201)): 
    #                if category==['nartd'] and manufacturer==['industry']:
    #                    k_list= ['pie','doughnut']#,'treemap'
    #                    kind= random.choice(k_list)
    #                elif category==['nartd'] or manufacturer==['industry']:
    #                    kind= ['bar']
                elif (('share' in measure1) or ('mix' in measure3)) and (len(measure2)==0) and ((round(coke_data1[kpi0[0]].sum(), 1)<100.50 and round(coke_data1[kpi0[0]].sum(), 1)>99.50) or (measure3==['mix'] and 199 < round(coke_data1[kpi0[0]].sum(), 1) < 201)): 
                    if ('share' in measure1) and ('mix' not in measure3):
                        kind='bar'
                    else:
                        k_list= ['pie','doughnut']#,'treemap'
        #                k_list= ['treemap']
                        kind= random.choice(k_list)
                elif input_Dim_lst1==['Period_Id']:
                    kind= 'line'
                else:
                    kind= 'bar'
            elif len(input_Dim_lst1)==2:
                if len(measure5)>0: #  ormeasure1==['growth'] 
                    kind= 'bar'#'waterfall'
    #            elif (('share' in measure1) or ('mix' in measure1)) and (len(measure2)==0) and (round(coke_data1[kpi0[0]].sum() % 100, 1)<100.50 or round(coke_data1[kpi0[0]].sum() % 100, 1)>90.50):
    #                k_list= ['pie chart','treemap','doughnut']
    #                kind= random.choice(k_list)
                elif 'Period_Id' in input_Dim_lst1 :
                    kind= 'line'
                else:
                    kind= 'bar'
            else:
                details_dict["error"]="Please mention upto two dimensions"
            
        elif len(kpi0)==2:
            kind="bubblebar" 
        else:
            details_dict["error"]="Please mention a business question with a relevant KPI-Dimensions combination"
            
        charts=list(set(chart_list).intersection(set(ques_sm))) 
        if len(charts)>0:#user feed
            kind1 = charts[0]
        else:
            kind1=kind
    
    
    
            
        # =============================================================================
        # Json creation            
        # =============================================================================
        if len(coke_data1.index)>0:
            '''this is use to convert year and month format'''
            coke_data1=coke_data1.sort_values(by='Period_Id')
            if len(kpi0)==1:
                sales123=coke_data1[kpi0[0]].astype(float)
                sales123=sales123 #i have remove *100 Sales in lakhs
                coke_data1[kpi0[0]]=sales123
                coke_data1=coke_data1.round({kpi0[0]: 2})
            elif len(kpi0)==2:
                sales123=coke_data1[kpi0[0]].astype(float)
                sales123=sales123 #i have remove *100 Sales in lakhs
                coke_data1[kpi0[0]]=sales123
                coke_data1=coke_data1.round({kpi0[0]: 2})
                sales123=coke_data1[kpi0[1]].astype(float)
                sales123=sales123 #i have remove *100 Sales in lakhs
                coke_data1[kpi0[1]]=sales123
                coke_data1=coke_data1.round({kpi0[1]: 2})        
                
    
    
            filr_lst=[]
            caption_lst=[]
            if measure1!=['share'] and len(coke_data1.Geography_Name.unique().tolist())==2:
                if 'world' in (coke_data1.Geography_Name.unique().tolist()):
                    coke_data1=coke_data1[coke_data1.Geography_Name!='world']
                elif 'top40world' in (coke_data1.Geography_Name.unique().tolist()):
                    coke_data1=coke_data1[coke_data1.Geography_Name!='top40world']
            else:
                pass
            for i in range(len(input_fltr_lst1)):
               fltr_1=pd.unique(coke_data1[input_fltr_lst1[i]]).tolist()[0]
               filr_lst.append(fltr_1)
               if 'cluster' in input_fltr_lst2:
                   caption1=input_fltr_lst1[i]+":"+fltr_1
               else:
                   caption1=input_fltr_lst2[i]+":"+fltr_1
               caption_lst.append(caption1)
            if len(bu_country)==1:
               caption_lst.append("Business Unit:"+bu_country[0])
            elif len(og_country)==1:
               caption_lst.append("Operating Group:"+og_country[0])
            else:
               pass
       
        '''redefine_category'''#This engine is used for redefining the category list based on kpi
        if measure3_x==['mix']:
            index_list=['coresparkling','energydrinks','juicesnectarsstills','otherbeverages','otherreportednartd','plantbasedbeverages','rtdcoffee','rtdtea','sportsdrinks','dairy','water']
            index_list=sorted(set(index_list) & set(coke_data1.Product_Category.unique().tolist()), key = index_list.index)#maintain the order of available category
        else:
            index_list=['nartd','coresparkling','energydrinks','juicesnectarsstills','otherbeverages','otherreportednartd','plantbasedbeverages','rtdcoffee','rtdtea','sportsdrinks','dairy','water']
            index_list=sorted(set(index_list) & set(coke_data1.Product_Category.unique().tolist()), key = index_list.index)#maintain the order of available category
            
                
        '''Filter YTD & MTD in front of TP'''
        if 'Time period' in caption_lst[0]:
            caption_lst[0]=caption_lst[0].replace('Time period:',"Time Period:"+period[0]+' ')
        elif 'Period_Id' in caption_lst[0]:
            caption_lst[0]=caption_lst[0].replace('Period_Id:',"Time Period:"+period[0]+' ')
        else:
            pass
            
        
        ''' Checking KPI'''    
        if len(kpi0)==0 or len(measure1)==2:
            details_dict["error"]="Please ask one KPI at a time!"
            details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
            print(str(details_dict))
            
        elif len(kpi0)==1:
            title = str(period[0])+' '+str(kpi_lst[0]+' for '+str(month_tag[0]))
        
        
        
    
        # =============================================================================
        ''' identification for inference'''     ##rahul2403
        # =============================================================================
        fix_col=['index','Geography_Name','Period_Id','Product_Category','Product_Company']
        infer_cols=coke_data1.columns.tolist()
        for i in kpi0:
            infer_cols.remove(i)
        infer_id=[item for item in infer_cols if item not in fix_col]
    #    infer_id=input_Dim_lst1.copy()
        if ('mix' in measure3 or 'growth' in measure1) and (len(infer_id)==0):
            fill1='Product_Category'
            fill2='Product_Company'
            if 'within' in ques:
                x=re.compile(r"(within) (\w+)")
                z=re.search(x, ques)
                c=z.group(2)
                if c in coke_data[fill1].unique().tolist():
                    pass
                else:
                    fill3=fill1
                    fill1=fill2#rahul2403
                    fill2=fill3                
        elif ('mix' not in measure3) and ('share' in measure1 or 'growth' not in measure1) and (len(infer_id)==0):
            fill1='Product_Company'
            fill2='Product_Category'
            if 'within' in ques:
                x=re.compile(r"(within) (\w+)")
                z=re.search(x, ques)
                c=z.group(2)
                if c in coke_data[fill1].unique().tolist():
                    pass
                else:
                    fill3=fill1
                    fill1=fill2
                    fill2=fill3        
        elif len(infer_id)>0:
            if len(infer_id)==1:
                if infer_id[0] in input_Dim_lst1:
                    fill1=infer_id[0]
                    if ('mix' in measure3 or 'growth' in measure1):
                        fill2='Product_Company'
                    elif ('mix' not in measure3) and ('share' in measure1 or 'growth' not in measure1):
                        fill2='Product_Category'
                elif infer_id[0] in input_fltr_lst1:
                    fill2=infer_id[0]
                    if ('mix' in measure3 or 'growth' in measure1):
                        if 'within' in ques:
                            x=re.compile(r"(within) (\w+)")     #rahul2403
                            z=re.search(x, ques)
                            c=z.group(2)
                            if c in coke_data['Product_Company'].unique().tolist():
                                fill1='Product_Company'
                            elif c in coke_data['Geography_Name'].unique().tolist():
                                fill1='Geography_Name'
                            else:
                                fill1='Product_Category'
                        else:
                            fill1='Product_Category'
                    elif ('mix' not in measure3) and ('share' in measure1 or 'growth' not in measure1):
                        if 'within' in ques:
                            x=re.compile(r"(within) (\w+)")     #rahul2403
                            z=re.search(x, ques)
                            c=z.group(2)
                            if c in coke_data['Product_Category'].unique().tolist():
                                fill1='Product_Category'
                            elif c in coke_data['Geography_Name'].unique().tolist():
                                fill1='Geography_Name'
                            else:
                                fill1='Product_Company'
                        else:
                            fill1='Product_Company'
            elif len(infer_id)>=2:
                if infer_id[0] in input_Dim_lst1:
                    fill1=infer_id[0]
                    fill2=infer_id[1]
                elif infer_id[0] in input_fltr_lst1:
                    fill1=infer_id[1]
                    fill2=infer_id[0]
        
        if len(infer_id)==0 and len(input_Dim_lst1)==1:
            if fill1!=input_Dim_lst1[0]:
                fill1=input_Dim_lst1[0]
                if fill1==fill2:
                    if fill1==['Product_Company']:
                        fill2=['Product_Category']
                    elif fill1==['Product_Category']:
                        fill2=['Product_Company']
                else:
                    pass
        else:
            pass
    
        '''   
        field1=flatten(input_fltr_lst)
        field2=[]
        input_fltr_lst1copy=input_fltr_lst1.copy()
        if 'Period_Id' in input_fltr_lst1copy:
            input_fltr_lst1copy.remove('Period_Id') 
        else:
            pass
        if len([x for x in input_fltr_lst1copy if x not in ['Product_Category', 'Product_Company', 'Geography_Name']])>0:
            field2=[x for x in input_fltr_lst1copy if x not in ['Product_Category', 'Product_Company', 'Geography_Name']]
            field2=[field2[0]]
        else:
            if 'nartd' not in field1 and 'industry' in field1 and (('world' in field1) or ('t40' in field1)):
                field2=['Product_Category']
            elif 'nartd' in field1 and 'industry' not in field1 and (('world' in field1) or ('t40' in field1)):
                field2=['Product_Category']
            elif 'nartd' in field1 and 'industry' in field1 and (('world' not in field1) or ('t40' not in field1)):
                field2=['Geography_Name']
            elif 'nartd' not in field1 and 'industry' not in field1 and (('world' in field1) or ('t40' in field1)):
                if 'Product_Category'!= dim3:
                    field2=['Product_Category']
                else:
                    field2=['Product_Company']
            elif 'nartd' not in field1 and 'industry' in field1 and (('world' not in field1) or ('t40' not in field1)):
                if 'Product_Category'!= dim3:
                    field2=['Product_Category']
                else:
                    field2=['Geography_Name']
            elif 'nartd' in field1 and 'industry' not in field1 and (('world' not in field1) or ('t40' not in field1)):
                if 'Product_Company'!= dim3:
                    field2=['Product_Company']
                else:
                    field2=['Geography_Name']
            elif 'nartd' in field1 and 'industry' in field1 and (('world' in field1) or ('t40' in field1)):
                field2=['Product_Company']
            else:
                pass
        '''   
        ## =============================================================================
        '''Part3'''#This machine is used to extrat final DF from SQL
        ## =============================================================================
        '''k1error'''#IF dimension 0 filter 0 and dimension >2 then error will pop up 
        if len(kpi0)==1:
            if len(coke_data1.index)==0:
                details_dict['error']='Data not available for input combination, please try some different combination' #satish
                details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
                print(str(details_dict))
            elif (len(input_Dim_lst)>2) | ( len(input_fltr_lst)==0 and len(input_Dim_lst)==0):
                details_dict["error"]="Please mention upto two dimensions"
                details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
                ##print("\n dictionary is")
                print(str(details_dict))
                '''k1d0''' #KPI 1 dimension 0       
            elif len(input_Dim_lst)==0 and len(input_fltr_lst)>=4:
                if len(k1d0_remove)>0:      #rahul2603
                    if k1d0_remove==['t40']:
                        coke_data1=coke_data1[coke_data1['Geography_Name']!='top40world']
                    else:
                        coke_data1=coke_data1[coke_data1['Geography_Name']!='world']
                coke_data1.reset_index(drop=True,inplace=True)
                coke_pivot = coke_data1.pivot(index=coke_data1.columns[-2],columns=coke_data1.columns[-3],values=kpi_lst[0].lower())# creating DF basis of selection
                caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Source:- Global Compass Nielsen GTC ")
                if dim3=='Product_Trademark':
                    title_pass=str(coke_data1[dim3][0])+' TM'
                elif dim3=='Product_Brand':
                    title_pass=str(coke_data1[dim3][0])+' Brand'
                else:
                    title_pass=str(coke_data1[dim3][0])
                title = str(period[0])+' '+str(kpi_lst[0])+' for '+ str(title_pass) 
                if 'share' in measure1 or len(measure5)>0:
                    infer=str(title_pass)+' has '+'('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])             
                else:
                    infer=str(coke_data1[dim3][0])+' has '+'('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])             
                ylabel=str(period[0])+' '+str(kpi_lst[0]) + ' %'
                if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                    infer=infer.replace('%','')
                    title=title.replace('%','')
                    ylabel=ylabel.replace('%','')
                    if kpi_lst[0]=='Volume':
                        infer=infer.replace(")"," UC`s)")
                    elif kpi_lst[0]=='Price':
                        infer=infer.replace(")"," $/UC)")
                    else:
                        infer=infer.replace(")"," USD)")
                else:
                    pass 
                draw_graph(coke_pivot,title1=title,kind=kind1,xlabel=str(coke_data1.columns[-2]) ,ylabel=ylabel, caption=caption, infer=infer)
    #            ##print(details_dict)
                '''k1d1'''#KPI 1 dimension 1 
            elif len(input_fltr_lst)>0 and len(input_Dim_lst)==1:
                if measure3==['mix'] and coke_data1.Product_Category.unique().tolist()!=['nartd']:
                    coke_data1=coke_data1[coke_data1.Product_Category!='nartd']
                elif measure1==['share'] and coke_data1.Product_Company.unique().tolist()!=['industry']:
                    coke_data1=coke_data1[coke_data1.Product_Company!='industry']
                else:
                    pass
    #                coke_data1_max=
                if len(k1d0_remove)>0:      #rahul2603
                    if k1d0_remove==['t40']:
                        coke_data1=coke_data1[coke_data1['Geography_Name']!='top40world']
                    else:
                        coke_data1=coke_data1[coke_data1['Geography_Name']!='world']
                coke_data1.reset_index(drop=True,inplace=True)
                coke_data11=coke_data1.copy()
                if len(coke_data1.index)>2:
                    if len(kpi0)==1:
                        for cols in coke_data11.columns[:-1]:
                                if cols=='index':
                                    pass
                                else:
                                    coke_data11=coke_data11[(coke_data11[cols]!='others') & (coke_data11[cols]!='otherreportednartd')]
                    else:
                        for cols in coke_data11.columns[:-2]:
                                if cols=='index':
                                    pass
                                else:
                                    coke_data11=coke_data11[(coke_data11[cols]!='others') & (coke_data11[cols]!='otherreportednartd')]
                else:
                    pass
                
                coke_data1_max2=coke_data11.loc[coke_data11[kpi0[-1]].idxmax()]# to get maximum value foe which dim
    
                '''k1d1ntp'''#KPI 1 dimension 1 when time period not in dimension
                if ('Period_Id' in input_fltr_lst1):
                    coke_data1_new=coke_data1.filter([input_Dim_lst1[0],kpi0[0]],axis=1).sort_values(by=kpi0[0], ascending=False)# creating DF basis of selection
                    coke_data1_new = rc_ind(data=coke_data1_new)#prioritise_industry
                    if 'others' in coke_data1_new[input_Dim_lst1[0]].tolist():
                        condition = coke_data1_new[input_Dim_lst1[0]].isin(['others','otherreportednartd'])
                        excluded = coke_data1_new[condition]
                        included = coke_data1_new[~condition]
                        coke_data1_new=pd.concat([included,excluded]) 
                    coke_data1_new.reset_index(drop=True,inplace=True)
                    if ('Geography_Name' in input_Dim_lst1) and (len(bu_country)>0):# title differ with cluster eg.BU,country 
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" across "+ (', '.join(bu_country)))
                    elif ('Geography_Name' in input_Dim_lst1) and (len(og_country)>0):
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" across "+ (', '.join(og_country)))
                    elif ('Geography_Name'not in input_Dim_lst1) and len(top_bottom)>0:
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+" for "+str(top_bottom[0])+' '+str(top_num)+' '+str(input_Dim_lst1[0].split("_")[-1]))
                    elif  ('Geography_Name'not in input_Dim_lst1) and input_Dim_lst1[0]==dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+engine.plural(input_Dim_lst1[0])+' across '+str(cluster[0]))
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]==dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+engine.plural(input_Dim_lst1[0]))
                    elif  ('Geography_Name'not in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+str(dim1)+' across '+engine.plural(input_Dim_lst1[0]))
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 and len(coke_data1.Geography_Name.unique().tolist())==1:
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+str(dim1)+' in '+str(cluster[0]))
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 and len(coke_data1.Geography_Name.unique().tolist())!=1:
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+str(dim1)+' across '+engine.plural(input_Dim_lst1[0])) 
                    elif input_Dim_lst1[0]!=dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+str(dim1)+' across '+str(cluster[0]))
                
#                    if ('Geography_Name' in input_Dim_lst1) and (len(bu_country)>0):# title differ with cluster eg.BU,country 
#                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" across "+ (', '.join(bu_country)))
#                    elif ('Geography_Name' in input_Dim_lst1) and (len(og_country)>0):
#                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" across "+ (', '.join(og_country)))
#                    elif ('Geography_Name' in input_Dim_lst1):
#                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" across "+ engine.plural(str(cluster_name)))
#                    elif ('Geography_Name'not in input_Dim_lst1):
#                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+" for "+str(cluster[0])+' across '+engine.plural(input_Dim_lst1[0]))                            
#                    elif ('Geography_Name'not in input_Dim_lst1) and len(top_bottom)>0:
#                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+" for "+str(top_bottom[0])+' '+str(top_num)+' '+str(input_Dim_lst1[0].split("_")[-1]))
#                   
                    infer_dict={} 
                    if dim3!=input_Dim_lst1[0]:
                        fill_pass=input_Dim_lst1[0]
                        infer_dict['1']=str(coke_data1_max2[dim3]) + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%) within '+str(coke_data1_max2[fill_pass])
                    elif dim3==fill1 and dim3!=fill2:
                        fill_pass=fill2
                        infer_dict['1']=str(str(coke_data1_max2[dim3]) + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%) within '+str(coke_data1_max2[fill_pass]))
                    elif dim3==fill2 and dim3!=fill1:
                        fill_pass=fill1
                        infer_dict['1']=str(coke_data1_max2[dim3]) + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%) within '+str(coke_data1_max2[fill_pass])
                    else:
                        infer_dict['1']=str(coke_data1_max2[dim3]) + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'
                   
                    i = (list(infer_dict))
                    
                    infer=str(infer_dict.get(random.choice(i)))
                    ylabel=str(period[0])+' '+str(kpi_lst[0]) + ' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        infer=infer.replace('%','')
                        ylabel=ylabel.replace('%','')
                        title=title.replace('%','')
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace(")"," $/UC)")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Source:- Global Compass Nielsen GTC ")
                    coke_data1_new=coke_data1_new.set_index(input_Dim_lst1[0])
                    if('Product_Category' in input_Dim_lst1):
                        index_lst=coke_data1_new.index.tolist()
                        if 'nartd' in coke_data1_new.index:
                            index_lst.remove('nartd')
                            index_lst.insert(0,'nartd')
                            coke_data1_new=coke_data1_new.reindex(index_lst)
                        if measure3_x==['mix']:
                            coke_data1_new=coke_data1_new[coke_data1_new.index!='nartd']    
                        coke_data1_new=coke_data1_new.dropna()
                        
#                    coke_data1_new=coke_data1_new.set_index(input_Dim_lst1[0])
#                    if 'number' in ques:
#                        coke_data1_new['number']='number'
#                        if 'dragging' in measure5:
#                            coke_data3=coke_data[coke_data[kpi0[-1]]<0.0]
#                            caption1=str("There are "+ str(len(coke_data3[dim3].unique())) +" "+ unique_change.get(dim3) + " that are loosing growth.")
#                        else:
#                            coke_data3=coke_data[coke_data[kpi0[-1]]>=0.0]
#                            caption1=str("There are "+ str(len(coke_data3[dim3].unique())) +" "+ unique_change.get(dim3) + " that are gaining growth.")
#                        draw_graph(coke_data1_new, "","","",caption=caption1+ "<br>Source:- Global Compass Nielsen GTC ")
                    if ((kind == 'pie') or(kind == 'doughnut')) and ('bar' not in ques):
                        contrib=contri_percent(coke_data1_max2[kpi0][0]) #rahul
                        if len(contrib)>0:
                            infer_dict['5']=str(coke_data1_max2[input_Dim_lst1[0]]+ ' contributes to '+ str(contrib[0]) + ' of the '+ str(input_Dim_lst1[0]) + ' ' +kpi_lst[0].split(' ', 2)[0])
                            i = (list(infer_dict))
                            infer=str(infer_dict.get(random.choice(i)))
                        draw_graph(coke_data1_new,title1=title,kind=kind,xlabel="" ,ylabel="", caption=caption, infer=infer)            
                    else:
                        draw_graph(coke_data1_new,title1=title,kind=kind1,xlabel=str(input_Dim_lst2[0]) ,ylabel=ylabel, caption=caption, infer=infer)
           
                        
    #                if ((kind == 'pie') or(kind == 'doughnut')) and ('bar' not in ques):
    #                    contrib=contri_percent(coke_data1_max2[kpi0][0]) #rahul
    #                    if len(contrib)>0:
    #                        infer_dict['5']=str(coke_data1_max2[input_Dim_lst1[0]]+ ' contributes to '+ str(contrib[0]) + ' of the '+ str(input_Dim_lst1[0]) + ' ' +kpi_lst[0].split(' ', 2)[0])
    #                        i = (list(infer_dict))
    #                        infer=str(infer_dict.get(random.choice(i)))
    #                    draw_graph(coke_data1_new,title1=title,kind=kind,xlabel="" ,ylabel="", caption=caption, infer=infer)
    #                else:
    #                    draw_graph(coke_data1_new,title1=title,kind=kind1,xlabel=str(input_Dim_lst2[0]) ,ylabel=ylabel, caption=caption, infer=infer)
    #    
                    '''k1d1tp''' #KPI 1 dimension 1 when time period in dimension       
                else:#Here else condiion will consider month variable as in dim_lst
                    coke_data1=coke_data1.sort_values(by=['Period_Id'])#to sort month_year
        #            del coke_data1['month_year2']
                    filtr_lst1=input_fltr_lst[0:][0]
                    filtr_lst1=filtr_lst1[0] #for title
                    title=str(period[0])+' '+str(kpi_lst[0]+' %' +" trend for "+str(input_fltr_lst[1][0])+' within '+str(filtr_lst1)+' for '+str(input_fltr_lst[2][0]))
                    caption=str("Filter:- "+inference_comma(caption_lst)+" <br>Source:- Global Compass Nielsen GTC ")     
                    if 'Period_Id'==fill1 and 'Period_Id'!=fill2:
                        fill_pass=fill2
                        infer=str(str(period[0])+' ' + coke_data1_max2[(input_Dim_lst1[0])] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" within " +coke_data1_max2[fill_pass])                 
                    elif 'Period_Id'!=fill1 and 'Period_Id'==fill2:
                        fill_pass=fill1
                        infer=str(str(period[0])+' ' + coke_data1_max2[(input_Dim_lst1[0])] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" within " +coke_data1_max2[fill_pass])                 
                    elif dim3!=input_Dim_lst1[0]:
                        fill_pass=input_Dim_lst1[0]
                        infer= coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" for " +str(str(period[0])+' ' +coke_data1_max2[fill_pass])                 
                    else:
                        infer=str(str(period[0])+' ' + coke_data1_max2[(input_Dim_lst1[0])] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)')
                    
                    ylabel=str(kpi_lst[0])+ ' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        infer=infer.replace('%','')
                        title=title.replace('%','')
                        ylabel=ylabel.replace('%','')
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace(")"," $/UC)")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                    coke_data1_new=coke_data1.filter(['Period_Id',kpi0[0]],axis=1)
                    coke_data1_new=coke_data1_new.set_index('Period_Id')# creating DF basis of selection
                    if (kind == 'line') and ('bar' not in ques):
                        draw_graph(coke_data1_new,title1=title,kind='line',xlabel='trend',ylabel=ylabel, caption=caption, infer=infer)
                    else:
                        draw_graph(coke_data1_new,title1=title,kind=kind1,xlabel=str(input_Dim_lst2[0]) ,ylabel=str(period[0])+' '+ylabel, caption=caption, infer=infer)
        
                '''k1d2'''#KPI 1 dimension 2
            elif len(input_fltr_lst)>0 and len(input_Dim_lst)==2:
                if measure3==['mix'] and coke_data1.Product_Category.unique().tolist()!=['nartd']:
                    coke_data1=coke_data1[coke_data1.Product_Category!='nartd']
                elif measure1==['share'] and coke_data1.Product_Company.unique().tolist()!=['industry']:
                    coke_data1=coke_data1[coke_data1.Product_Company!='industry']
                    
                coke_pivot = coke_data1.pivot(index=input_Dim_lst1[0],columns=input_Dim_lst1[1],values=kpi0[0])# creating DF basis of selection
                coke_pivot.fillna(value=0, inplace=True) 
                coke_data11=coke_data1.copy()
                if len(coke_data1.index)>2:
                    for cols in coke_data11.columns[:-1]:
                        if cols=='index':
                                    pass
                        else:
                            coke_data11=coke_data11[(coke_data11[cols]!='others') & (coke_data11[cols]!='otherreportednartd')]
                else:
                    pass
                
                coke_data1_max2=coke_data11.loc[coke_data11[kpi0[-1]].idxmax()]# to get maximum value foe which dim
    
                    
                '''k1d2tp'''#KPI 1 dimension 2 when time period in dimension    
                if input_Dim_lst1[0]=='Period_Id':
                    coke_pivot=coke_pivot.sort_values(by=['Period_Id'])
                    coke_pivot = rc_ind(data=coke_pivot)#prioritise_industry
                    coke_data11=coke_data1.copy()
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Source:- Global Compass Nielsen GTC ")
    #                if len(field2)>0:
    #                    test_var=field2[0]
                    if dim3==fill2:
                        test_var=fill1
                    elif dim3==fill1:
                        test_var=fill2
                    else:
                        test_var=fill2
                        
                    infer_dict={}
                    if 'growth' in measure1:                     
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" vs. PY " + " in " +coke_data1_max2['Period_Id']) 
                        infer_dict['k1d2tp2']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp3']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                                   
                    else:  
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp2']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                   
                    i = (list(infer_dict))
                    infer=str(infer_dict.get(random.choice(i)))
                      
                    if ('Geography_Name' in input_Dim_lst1):# title differ with cluster eg.BU,country 
                        title1=str('Trend for '+kpi_lst[0]+' % across '+ engine.plural(str(cluster_name))) 
                    elif ('Geography_Name' not in input_Dim_lst1):
                        title1=str('Trend for '+kpi_lst[0]+' % across '+ engine.plural(input_Dim_lst2[1])) 
                    if('Product_Category' in input_Dim_lst1[0]):
                        coke_pivot=coke_pivot.reindex(index_list)#to index the category NART csd water
                        coke_pivot=coke_pivot.dropna()
                    elif('Product_Category' in input_Dim_lst1[1]):
                        coke_pivot=coke_pivot.dropna()
                        coke_pivot.max()
                    if('Product_Company' in input_Dim_lst1[0]) and (kind!='pie' and kind!='doughnut') and ('others' in coke_pivot.index):
                        coke_pivot=coke_pivot.drop('others',axis=0)
                    elif('Product_Company' in input_Dim_lst1[1]) and (kind!='pie' and kind!='doughnut') and ('others' in coke_pivot.columns) :
                        coke_pivot=coke_pivot.drop('others',axis=1)
                    ylabel=str(kpi_lst[0])+' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        infer=infer.replace('%','')
                        title1=title1.replace('%','')
                        ylabel=ylabel.replace('%','')
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace(")"," $/UC)")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                    if (kind == 'line')and ('bar' not in ques):
                        draw_graph(coke_pivot,title1=title1,kind='line',xlabel=str(input_Dim_lst2[0]),ylabel=ylabel, caption=caption, infer=infer)
                    else:
                        draw_graph(coke_pivot,title1=title1,kind=kind1,xlabel=str(input_Dim_lst2[0]),ylabel=ylabel, caption=caption, infer=infer)
        
                    '''k1d2ntp'''#KPI 1 dimension 2 when time period not in dimension
                elif input_Dim_lst1[0]!='Period_Id':
                    coke_pivot['sum1']=coke_pivot.sum(axis=1)
                    coke_pivot.sort_values('sum1',ascending=False)
                    coke_pivot.sort_values('sum1',ascending=False,inplace=True)
                    del coke_pivot['sum1']
                    coke_pivot = rc_ind(data=coke_pivot)
                    if 'others' in coke_pivot.index.tolist():
                        condition = coke_pivot.index =='others'
                        excluded = coke_pivot[condition]
                        included = coke_pivot[~condition]
                        coke_pivot=pd.concat([included,excluded]) 
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Source:- Global Compass Nielsen GTC ") 
                    
                    if dim3==input_Dim_lst1[0]:
                        test_var=input_Dim_lst1[1]
                    elif dim3==input_Dim_lst1[1]:
                        test_var=input_Dim_lst1[0]
                    else:
                        pass
                    infer_dict={}
    
                    if 'growth' in measure1:                     
                        infer_dict['k1d2ntp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" vs. PY " + " in " +coke_data1_max2['Period_Id']) 
                        infer_dict['k1d2ntp2']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2ntp3']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                                   
                    else:  
                        infer_dict['k1d2ntp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2ntp2']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                   
                    i = (list(infer_dict))
                    infer=str(infer_dict.get(random.choice(i)))
                 
                    if ('Geography_Name' in input_Dim_lst1):
                        title1=str(period[0])+' '+str(kpi_lst[0]+' %' + " of "+engine.plural(input_Dim_lst2[0])+ " across "+ engine.plural(str(cluster_name))) 
                    if ('Geography_Name' not in input_Dim_lst1):
                        title1=str(period[0])+' '+str(kpi_lst[0]+' %' + " of "+engine.plural(input_Dim_lst2[0])+ " across "+ engine.plural(input_Dim_lst1[1]))
                    if('Product_Category' in input_Dim_lst1[0]):
                        coke_pivot=coke_pivot.reindex(index_list)
                        coke_pivot=coke_pivot.dropna()
                    elif('Product_Category' in input_Dim_lst1[1]):
                        coke_pivot=coke_pivot[index_list]
                        coke_pivot=coke_pivot.dropna()
                    if('Product_Company' in input_Dim_lst1[0]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.index):
                        coke_pivot=coke_pivot.drop('others',axis=0)# to exclude others in bar chart and include others if kind = pie
                    elif('Product_Company' in input_Dim_lst1[1]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.columns) :
                        coke_pivot=coke_pivot.drop('others',axis=1)
                    ylabel=str(period[0])+' '+str(kpi_lst[0])+' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        infer=infer.replace('%','')
                        title1=title1.replace('%','')
                        ylabel=ylabel.replace('%','')
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace(")"," $/UC)")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                    draw_graph(coke_pivot,title1=title1,kind=kind1,xlabel=str(input_Dim_lst2[0]),ylabel=ylabel, caption=caption, infer=infer)
        
            '''k2error'''   #IF dimension 0 filter 0 and dimension >2 then error will pop up  
        elif len(kpi0)==2 and (len(measure2)==1 or (len(measure5)==1 and measure1==['share'])):
            if kind1=='barh':
                kind2='bubblebarh'
            else:
                kind2='bubblebar'
            if (len(input_Dim_lst)>2) | ( len(input_fltr_lst)==0 and len(input_Dim_lst)==0):
                details_dict["error"]="Please mention upto two dimensions"
                details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
                ##print("\n dictionary is")
                print(str(details_dict))
             
                '''k2d0'''#KPI 2 dimension 0
            elif len(input_Dim_lst1)==0:
                if len(input_fltr_lst)>=4:
                    if len(k1d0_remove)>0:      #rahul2603
                        if k1d0_remove==['t40']:
                            coke_data1=coke_data1[coke_data1['Geography_Name']!='top40world']
                        else:
                            coke_data1=coke_data1[coke_data1['Geography_Name']!='world'] 
                    coke_data1.reset_index(drop=True,inplace=True)
                    coke_pivot = coke_data1.pivot(index=coke_data1.columns[-3],columns=coke_data1.columns[-4],values=kpi_lst[0].lower())# creating DF basis of selection
                    coke_pivot1 = coke_data1.pivot(index=coke_data1.columns[-1],columns=coke_data1.columns[-4],values=kpi_lst[1].lower())# creating DF basis of selection
                    coke_pivot1.values[0][0]
                    details_dict['bubble_data']=[str(coke_pivot1.values[0][0])]
                    caption=str("Filter:- "+inference_comma(caption_lst) + "<br>Up/Down Arrow shown above the bar indicates change vs PY; GREEN: > 0.15 ppt RED: < -0.15 ppt and Rest is yellow" + "<br>Source:- Global Compass Nielsen GTC")#+(" for "+'and'.join(fltr_lst[1:][0]) if len(fltr_lst[1:])>0 else '')+" in "+month_year #"Sales across stores for Feb-18 and Fashion"
#                    caption.replace(u"\xa0", u" ")  # removed completely
                    caption = unidecode.unidecode(caption)

                    if dim3==fill1 and dim3!=fill2:
                        fill_pass=fill2
                    elif dim3==fill2 and dim3!=fill1:
                        fill_pass=fill1
                    else:
                        fill_pass=fill1
                        
                    if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                        if dim1=='nartd' and dim21=='nartd':
                            title_pass=str('nartd')
                        elif 'tccc' in coke_data1.Product_Company.tolist() and dim3!='Product_Company' and dim31!='Product_Company':
                            title_pass=str('tccc' +' '+coke_data1[dim3][0])
                        elif dim3!='Product_Company':
                            title_pass=str(coke_data1.Product_Company.tolist()[0] +' '+coke_data1[dim3][0])
                        elif dim31!='Product_Company':
                            title_pass=str(coke_data1.Product_Company.tolist()[0] +' '+coke_data1[dim31][0])
                        else:
                            title_pass=str(coke_data1[dim3][0] +' '+coke_data1[dim31][0])
                        fill_pass='Product_Category'
                    elif dim3=='Product_Trademark':
                        title_pass=str(coke_data1[dim3][0])+' TM'
                    elif dim3=='Product_Brand':
                        title_pass=str(coke_data1[dim3][0])+' Brand'
                    else:
                        title_pass=str(coke_data1[dim3][0])
                    title = str(period[0])+' '+str(kpi_lst[1])+' for '+ str(title_pass) 
    

                    if (round((coke_pivot1.values[0][0]),1))>0.15:
                        if measure1==['price']:
                            infer=str(title_pass)+' has ('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])+'% '+'within '+str(coke_data1[fill_pass][0])+ '; which increased by ' +str(abs(coke_data1[kpi0[1]][0]))+"% vs PY"
                        else:
                            infer=str(title_pass)+' has ('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])+'% '+'within '+str(coke_data1[fill_pass][0])+ '; which increased by ' +str(abs(coke_data1[kpi0[1]][0]))+" vs PY"
                    elif (round((coke_data1[kpi0[1]][0]),1))<-0.15:
                        if measure1==['price']:
                            infer=str(title_pass)+' has ('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])+'% '+'within '+str(coke_data1[fill_pass][0])+ '; which decreased by ' +str(abs(coke_data1[kpi0[1]][0]))+"% vs PY" 
                        else:
                            infer=str(title_pass)+' has ('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])+'% '+'within '+str(coke_data1[fill_pass][0])+ '; which decreased by ' +str(abs(coke_data1[kpi0[1]][0]))+" vs PY" 
                    else:
                        infer=str(title_pass)+' has ('+str(list(coke_data1[kpi0].iloc[0])[0])+"%) "+ str(kpi_lst[0])+'% '+'within '+str(coke_data1[fill_pass][0])+ '; with flat change vs PY'
                        
                    ylabel=str(period[0])+' '+str(kpi_lst[0]) + ' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        if measure1!=['price']:
                            infer=infer.replace('%','')
                            title=title.replace('%','')
                            ylabel=ylabel.replace('%','')
                        else:
                            pass
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace("%)"," $/UC)")
                            infer=infer.replace("Price%","Price")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                    draw_graph(coke_pivot,title1=title,kind=kind2,xlabel=str(coke_data1.columns[-3]) ,ylabel=ylabel, caption=caption, infer=infer)
                   
                '''k2d1''' #KPI 2 dimension 1   
            elif len(input_Dim_lst1)==1:
                    if len(k1d0_remove)>0:      #rahul2803
                        if k1d0_remove==['t40']:
                            coke_data1=coke_data1[coke_data1['Geography_Name']!='top40world']
                        else:
                            coke_data1=coke_data1[coke_data1['Geography_Name']!='world'] 
                    coke_data1=coke_data1.sort_values(kpi0[-1])
                    coke_data1.reset_index(drop=True,inplace=True)
                    if measure3==['mix'] and coke_data1.Product_Category.unique().tolist()!=['nartd']: #satish3103
                        coke_data3=coke_data1[coke_data1.Product_Category!='nartd']#satish3103
                    elif measure1==['share'] and coke_data1.Product_Company.unique().tolist()!=['industry']:
                        coke_data1=coke_data1[coke_data1.Product_Company!='industry']
                    else:
                        pass
                    
                    coke_data11=coke_data1.copy()
                    if len(coke_data1.index)>2:
                        for cols in coke_data11.columns[:-2]:
                            coke_data11=coke_data11[(coke_data11[cols]!='others') & (coke_data11[cols]!='otherreportednartd')]
                    else:
                        pass           
                    coke_data1_max2=coke_data11.loc[coke_data11[kpi0[-1]].idxmax()]# to get maximum value foe which dim
    
                    '''k2d1tp'''#KPI 2 dimension 1 when time period in dimension
                    if input_Dim_lst1[0]=='Period_Id':
                        coke_data1_new=coke_data1.filter([input_Dim_lst1[0],kpi0[0],kpi0[1]],axis=1)# creating DF basis of selection
                        coke_data1_new=coke_data1_new.sort_values(by=['Period_Id'])#to sort month_year
    
                    '''k2d1ntp'''#KPI 2 dimension 1 when time period not in dimension
                    if input_Dim_lst1[0]!='Period_Id':
                        coke_data1_new=coke_data1.filter([input_Dim_lst1[0],kpi0[0],kpi0[1]],axis=1).sort_values(by=kpi0[-1], ascending=False)
                    coke_data1_new = rc_ind(data=coke_data1_new)#prioritise_industry
                    if 'others' in coke_data1_new[input_Dim_lst1[0]].tolist():
                        condition = coke_data1_new[input_Dim_lst1[0]].isin(['others','otherreportednartd'])
                        excluded = coke_data1_new[condition]
                        included = coke_data1_new[~condition]
                        coke_data1_new=pd.concat([included,excluded])       
    
                    if('Product_Category' in input_Dim_lst1):
                        coke_data1_new['Product_Category']=pd.Categorical(coke_data1_new['Product_Category'], index_list, ordered=True)
                        coke_data1_new=coke_data1_new.dropna()
                        coke_data1_new.sort_values(kpi0[-1],inplace=True,ascending=False)
                    bubble_data=coke_data1_new[kpi0[1]].tolist()#passing bubble data in list
                    bubble_data = [ '%.2f' % elem for elem in bubble_data]
                    details_dict['bubble_data']=bubble_data
                    del coke_data1_new[kpi0[1]]
                     
                    
                    if ('Geography_Name' in input_Dim_lst1) and (len(bu_country)>0):# title differ with cluster eg.BU,country 
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ (', '.join(bu_country)))
                    elif ('Geography_Name' in input_Dim_lst1) and (len(og_country)>0):
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ (', '.join(og_country)))
                    
                    elif ('Geography_Name'not in input_Dim_lst1) and len(top_bottom)>0:
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+str(top_bottom[0])+' '+str(top_num)+' '+str(input_Dim_lst1[0].split("_")[-1]))
                    
                    elif  ('Geography_Name'not in input_Dim_lst1) and input_Dim_lst1[0]==dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+engine.plural(input_Dim_lst1[0])+' across '+str(cluster[0]))
                    
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]==dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +' across '+engine.plural(input_Dim_lst1[0]))
                    
                    elif  ('Geography_Name'not in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 :
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+str(dim1)+' across '+engine.plural(input_Dim_lst1[0]))
                    
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 and len(coke_data1.Geography_Name.unique().tolist())==1:
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+str(dim1)+' in '+str(cluster[0]))
                    
                    elif  ('Geography_Name' in input_Dim_lst1) and input_Dim_lst1[0]!=dim3 and len(coke_data1.Geography_Name.unique().tolist())!=1:
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" + " with change vs PY" +" for "+str(dim1)+' across '+engine.plural(input_Dim_lst1[0])) 
                    elif ('Period_Id' in input_Dim_lst1):     
                        title=str(period[0])+' '+str('Trend of '+kpi_lst[0]+' %'+ " with change vs PY")
                    elif ('Period_Id' not in input_Dim_lst1) and ('Geography_Name' not in input_Dim_lst1):     
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ engine.plural(input_Dim_lst2[0]))
                    elif input_Dim_lst1[0]!=dim3:
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+str(dim1)+' across '+str(cluster[0]))
                    elif ('Geography_Name'not in input_Dim_lst1):
                        title=str(period[0])+' '+str(kpi_lst[0]+'%'+ " with change vs PY" +" for "+str(cluster[0])+' across '+engine.plural(input_Dim_lst1[0]))      
                    '''
                    if ('Geography_Name' in input_Dim_lst1) and (len(bu_country)>0):# title differ with cluster eg.BU,country 
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ (', '.join(bu_country)))
                    elif ('Geography_Name' in input_Dim_lst1) and (len(og_country)>0):
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ (', '.join(og_country)))
                    elif ('Geography_Name' in input_Dim_lst1):
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ engine.plural(str(cluster_name)))
                    elif ('Period_Id' in input_Dim_lst1):     
                        title=str(period[0])+' '+str('Trend of '+kpi_lst[0]+' %'+ " with change vs PY")
                    elif ('Period_Id' not in input_Dim_lst1) and ('Geography_Name' not in input_Dim_lst1):     
                        title=str(period[0])+' '+str(kpi_lst[0]+' %'+ " with change vs PY" + " across "+ engine.plural(input_Dim_lst2[0]))
                    '''
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Up/Down Arrow shown above the bar indicates change vs PY; GREEN: > 0.15 ppt RED: < -0.15 ppt and Rest is yellow" + "<br>Source:- Global Compass Nielsen GTC")#+(" for "+'and'.join(fltr_lst[1:][0]) if len(fltr_lst[1:])>0 else '')+" in "+month_year #"Sales across stores for Feb-18 and Fashion"
#                    caption.replace(u"\xa0", u" ")   # removed completely
                    caption = unidecode.unidecode(caption)
    
                    if  dim3!=fill2 and dim3!=fill1 and dim3 not in input_Dim_lst1:
                        fill_pass=input_Dim_lst1[0]
                    elif dim3==fill2:
                        fill_pass=fill1
                    elif dim3==fill1:
                        fill_pass=fill2
                    else:
                        fill_pass=fill2 


                    if ('share' in measure1) and measure5==['driving'] and 'nartd' in ques:
                        
                        if dim1=='nartd' and dim21=='nartd':
                            title_pass=str('nartd')
                        elif 'tccc' in coke_data1.Product_Company.tolist() and dim3!='Product_Company' and dim31!='Product_Company':
                            title_pass=str('tccc' +' '+coke_data1_max2[dim3])
                        elif dim3!='Product_Company':
                            title_pass=str(coke_data1.Product_Company.tolist()[0] +' '+coke_data1_max2[dim3])
                        elif dim31!='Product_Company':
                            title_pass=str(coke_data1.Product_Company.tolist()[0] +' '+coke_data1_max2[dim31])
                        else:
                            title_pass=str(coke_data1_max2[dim3] +' '+coke_data1_max2[dim31])
                        fill_pass='Product_Category'
                    elif dim3=='Product_Trademark':
                        title_pass=str(coke_data1_max2[dim3])+' TM'
                    elif dim3=='Product_Brand':
                        title_pass=str(coke_data1_max2[dim3])+' Brand'
                    else:
                        title_pass=str(coke_data1_max2[dim3])

                        
                    if (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))>0.15: 
                        if dim3 not in input_Dim_lst1:
                            if measure1==['price']:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY within "+coke_data1_max2[fill_pass])
                            else:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY within "+coke_data1_max2[fill_pass])
                        else:
                            if measure1==['price']:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY")
                            else:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY")
                        
                    elif (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))<-0.15:
                        if dim3 not in input_Dim_lst1:
                            if measure1==['price']:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0])+' '+ coke_data1_max2['Period_Id']+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY within "+coke_data1_max2[fill_pass])
                            else:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0])+' '+ coke_data1_max2['Period_Id']+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY within "+coke_data1_max2[fill_pass])
                        else:
                            if measure1==['price']:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0])+' '+ coke_data1_max2['Period_Id']+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY")
                            else:
                                infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0])+' '+ coke_data1_max2['Period_Id']+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY")
                    else:
                        if dim3 not in input_Dim_lst1:
                            infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+"with flat change vs PY within "+coke_data1_max2[fill_pass])
                        else:
                            infer=str(title_pass + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" in "+ str(period[0]) +' '+ coke_data1_max2['Period_Id']+'; '+"with flat change vs PY")
    
                    ylabel=str(period[0])+' '+str(kpi_lst[0])+' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        if measure1!=['price']:
                            infer=infer.replace('%','')
                            title=title.replace('%','')
                            ylabel=ylabel.replace('%','')
                        else:
                            pass
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace("%)"," $/UC)")
                            infer=infer.replace("Price%","Price")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                    
                    coke_data1_new=coke_data1_new.set_index(input_Dim_lst1[0])
#                    if 'number' in ques:
#                        coke_data1_new['number']='number'
#                        if 'dragging' in measure5:
#                            coke_data3=coke_data[coke_data[kpi0[-1]]<0.0]
#                            caption1=str("There are "+ str(len(coke_data3[dim3].unique())) +" "+ unique_change.get(dim3) + " that are loosing shares.")
#                        else:
#                            coke_data3=coke_data[coke_data[kpi0[-1]]>=0.0]
#                            caption1=str("There are "+ str(len(coke_data3[dim3].unique())) +" "+ unique_change.get(dim3) + " that are gaining shares.")
#                        del details_dict["bubble_data"]
#                        draw_graph(coke_data1_new, "","","",caption=caption1)
#                    else:
                    draw_graph(coke_data1_new,title1=title,kind=kind2,xlabel=str(input_Dim_lst2[0]) ,ylabel=ylabel, caption=caption, infer=infer)
        
                    '''k2d2'''#KPI 2 dimension 2
            elif len(input_fltr_lst)>0 and len(input_Dim_lst)==2:
                if measure3==['mix'] and coke_data1.Product_Category.unique().tolist()!=['nartd']:
                    coke_data1=coke_data1[coke_data1.Product_Category!='nartd']
                elif measure1==['share'] and coke_data1.Product_Company.unique().tolist()!=['industry']:
                    coke_data1=coke_data1[coke_data1.Product_Company!='industry']
                else:
                    pass
    
                coke_data1_max2=coke_data1.loc[coke_data1[kpi0[0]].idxmax()] # to get maximum value foe which dim
                coke_pivot = coke_data1.pivot(index=input_Dim_lst1[0],columns=input_Dim_lst1[1],values=kpi0[0])
                coke_pivot.fillna(value=0, inplace=True)#creating DF basis of selection 
                coke_pivot_change = coke_data1.pivot(index=input_Dim_lst1[0],columns=input_Dim_lst1[1],values=kpi0[1])#creating DF basis of selection for change
                coke_pivot_change.fillna(value=0, inplace=True)
                
                coke_data11=coke_data1.copy()
                if len(coke_data1.index)>2:
                    for cols in coke_data11.columns[:-2]:
                        coke_data11=coke_data11[(coke_data11[cols]!='others') & (coke_data11[cols]!='otherreportednartd')]
                else:
                    pass
                
                coke_data1_max2=coke_data11.loc[coke_data11[kpi0[-1]].idxmax()]# to get maximum value foe which dim
    
                if('Product_Category' in input_Dim_lst1[0]):   
                    coke_pivot_change=coke_pivot_change.reindex(index_list)                   
                    coke_pivot_change=coke_pivot_change.dropna()
                elif('Product_Category' in input_Dim_lst1[1]):
                    coke_pivot_change=coke_pivot_change[index_list]
                    coke_pivot_change=coke_pivot_change.dropna()
    
        
                '''k2d2tp''' #KPI 2 dimension 2 when time period in dimension       
                if input_Dim_lst1[0]=='Period_Id':
                    coke_pivot=coke_pivot.sort_values(by=['Period_Id'])#to sort month year
                    coke_pivot = rc_ind(data=coke_pivot)#prioritise_industry
                    coke_pivot_change=coke_pivot_change.sort_values(by='Period_Id')
                    coke_pivot_change=coke_pivot_change.reindex(coke_pivot.index.tolist())
                    coke_pivot_change = rc_ind(data=coke_pivot_change)#prioritise_industry
                    bubble_data=[]#passing bubble data in list
                    for i in range(0,len(coke_pivot_change.T.columns)):
                        xtest=coke_pivot_change.T
                        lst=xtest[xtest.columns[i]].tolist()
                        bubble_data.append(lst)
                    bubble_data = [item for sublist in bubble_data for item in sublist]
                    bubble_data = [ '%.1f' % elem for elem in bubble_data]
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Up/Down Arrow shown above the bar indicates change vs PY; GREEN: > 0.15 ppt RED: < -0.15 ppt and Rest is yellow" + "<br>Source:- Global Compass Nielsen GTC")#+(" for "+'and'.join(fltr_lst[1:][0]) if len(fltr_lst[1:])>0 else '')+" in "+month_year #"Sales across stores for Feb-18 and Fashion"
#                    caption.replace(u"\xa0", u" ")   # removed completely
                    caption = unidecode.unidecode(caption)
    
    #                if len(field2)>0:
    #                    test_var=field2[0]
                    if dim3==fill2:
                        test_var=fill1
                    elif dim3==fill1:
                        test_var=fill2
                    else:
                        test_var=fill2
                    
                    infer_dict={}
                    if 'growth' in measure1:                     
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" vs. PY " + " in " +coke_data1_max2['Period_Id']) 
                        infer_dict['k1d2tp2']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp3']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                                   
                    else:  
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp2']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                   
                    i = (list(infer_dict))
                    infer=str(infer_dict.get(random.choice(i)))                
    
                    if (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))>0.15:
                        if measure1==['price']:
                            infer=infer+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY "
                        else:
                            infer=infer+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY "
                    elif (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))<-0.15:
                        if measure1==['price']:
                            infer=infer+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY "
                        else:
                            infer=infer+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY "
                    else:
                        infer=infer+'; '+ 'with flat change vs PY '
    
                    details_dict['bubble_data']=bubble_data
                    if ('Geography_Name' in input_Dim_lst1):
                        title1=str('Trend for '+kpi_lst[0]+' %' +" with change vs PY across "+ engine.plural(str(cluster_name))) 
                    elif ('Geography_Name' not in input_Dim_lst1):
                        title1=str('Trend for '+kpi_lst[0]+' %' +" with change vs PY across "+ engine.plural(input_Dim_lst2[1])) 
                    if('Product_Category' in input_Dim_lst1[0]):
                        coke_pivot=coke_pivot.reindex(index_list)
                        coke_pivot=coke_pivot.dropna()
                    elif('Product_Category' in input_Dim_lst1[1]):
                        coke_pivot=coke_pivot[index_list]
                        coke_pivot=coke_pivot.dropna()
                    if('Product_Company' in input_Dim_lst1[0]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.index) :
                        coke_pivot=coke_pivot.drop('others',axis=0)
                    elif('Product_Company' in input_Dim_lst1[1]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.columns) :
                        coke_pivot=coke_pivot.drop('others',axis=1)
                        
                    ylabel=str(period[0])+' '+str(kpi_lst[0])+' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        if measure1!=['price']:
                            infer=infer.replace('%','')
                            title=title.replace('%','')
                            ylabel=ylabel.replace('%','')
                        else:
                            pass
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace("%)"," $/UC)")
                            infer=infer.replace("Price%","Price")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
    
                    draw_graph(coke_pivot,title1=title1,kind=kind2,xlabel=str(input_Dim_lst2[0]),ylabel=ylabel, caption=caption, infer=infer)
        
                    '''k2d2ntp'''#KPI 2 dimension 2 when time period not in dimension
                elif input_Dim_lst1[0]!='Period_Id':
                    coke_pivot['sum1']=coke_pivot.sum(axis=1)
                    coke_pivot.sort_values('sum1',ascending=False,inplace=True)
                    del coke_pivot['sum1']
                    coke_pivot = rc_ind(data=coke_pivot)
                    if 'others' in coke_pivot.index.tolist():
                        condition = coke_pivot.index =='others'
                        excluded = coke_pivot[condition]
                        included = coke_pivot[~condition]
                        coke_pivot=pd.concat([included,excluded])
                        
                    if('Product_Category' not in input_Dim_lst1[0]):
                        coke_pivot_change=coke_pivot_change.reindex(coke_pivot.index.tolist())#for change data
                    coke_pivot_change = rc_ind(data=coke_pivot_change)#prioritise_industry
                    bubble_data=[]#passing bubble data in list
                    for i in range(0,len(coke_pivot_change.T.columns)):
                        xtest=coke_pivot_change.T
                        lst=xtest[xtest.columns[i]].tolist()
                        bubble_data.append(lst)
                    bubble_data = [item for sublist in bubble_data for item in sublist]
                    bubble_data = [ '%.1f' % elem for elem in bubble_data]
    
                    caption=str("Filter:- "+inference_comma(caption_lst)+ "<br>Up/Down Arrow shown above the bar indicates change vs PY; GREEN: > 0.15 ppt RED: < -0.15 ppt and Rest is yellow" + "<br>Source:- Global Compass Nielsen GTC")#+(" for "+'and'.join(fltr_lst[1:][0]) if len(fltr_lst[1:])>0 else '')+" in "+month_year #"Sales across stores for Feb-18 and Fashion"
#                    caption.replace(u"\xa0", u" ")   # removed completely
                    caption = unidecode.unidecode(caption)
                    if dim3==input_Dim_lst1[0]:
                        test_var=input_Dim_lst1[1]
                    elif dim3==input_Dim_lst1[1]:
                        test_var=input_Dim_lst1[0]
                    else:
                        pass                
                    
                    infer_dict={}
                    if 'growth' in measure1:                     
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+" vs. PY " + " in " +coke_data1_max2['Period_Id']) 
                        infer_dict['k1d2tp2']=str(coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp3']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has the highest '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                                   
                    else:  
                        infer_dict['k1d2tp1']=str(coke_data1_max2[dim3] + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id']+ ', within '+coke_data1_max2[test_var])
                        infer_dict['k1d2tp2']=str('Within '+coke_data1_max2[test_var]+', '+coke_data1_max2[dim3] + ' has '+ kpi_lst[0]+'% '+'('+str(coke_data1_max2[kpi0][0])+'%)'+ " in " +str(period[0])+' '+coke_data1_max2['Period_Id'])                   
                    i = (list(infer_dict))
                    infer=str(infer_dict.get(random.choice(i)))                
    
                    if (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))>0.15:
                        if measure1==['price']:
                            infer=infer+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY "
                        else:
                            infer=infer+'; '+'which increased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY "
                    elif (float("{0:0.1f}".format(coke_data1_max2[kpi0][1])))<-0.15:
                        if measure1==['price']:
                            infer=infer+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+"% vs PY "
                        else:
                            infer=infer+'; '+'which decreased by ' +str(float("{0:0.1f}".format(abs(coke_data1_max2[kpi0][1]))))+" vs PY "
                    else:
                        infer=infer+'; '+ 'with flat change vs PY '
    
                    details_dict['bubble_data']=bubble_data
                    if ('Geography_Name' in input_Dim_lst1):
                        title1=str(period[0])+' '+str(kpi_lst[0]+' %' +" with change vs PY"+ " of "+engine.plural(input_Dim_lst2[0])+ " across "+ engine.plural(str(cluster_name))) 
                    if ('Geography_Name' not in input_Dim_lst1):
                        title1=str(period[0])+' '+str(kpi_lst[0]+' %' +" with change vs PY"+ " of "+engine.plural(input_Dim_lst2[0])+ " across "+ engine.plural(input_Dim_lst2[1])) 
                    if('Product_Category' in input_Dim_lst1[0]):
                        coke_pivot=coke_pivot.reindex(index_list)
                        coke_pivot=coke_pivot.dropna()
                    elif('Product_Category' in input_Dim_lst1[1]):
                        coke_pivot=coke_pivot[index_list]
                        coke_pivot=coke_pivot.dropna()
                    if('Product_Company' in input_Dim_lst1[0]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.index) :
                        coke_pivot=coke_pivot.drop('others',axis=0)
                    elif('Product_Company' in input_Dim_lst1[1]) and (kind!='pie' and kind!='doughnut')  and ('others' in coke_pivot.columns) :
                        coke_pivot=coke_pivot.drop('others',axis=1)
                    ylabel=str(period[0])+' '+str(kpi_lst[0])+' %'
                    if (measure1==['sale'] or measure1==['price']) and measure3!=['mix'] and len(measure5)==0:
                        if measure1!=['price']:
                            infer=infer.replace('%','')
                            title=title.replace('%','')
                            ylabel=ylabel.replace('%','')
                        else:
                            pass
                        if kpi_lst[0]=='Volume':
                            infer=infer.replace(")"," UC`s)")
                        elif kpi_lst[0]=='Price':
                            infer=infer.replace("%)"," $/UC)")
                            infer=infer.replace("Price%","Price")
                        else:
                            infer=infer.replace(")"," USD)")
                    else:
                        pass 
                        pass 
                    draw_graph(coke_pivot,title1=title1,kind=kind2,xlabel=str(input_Dim_lst2[0]),ylabel=ylabel, caption=caption, infer=infer)
                    
            else:
                details_dict["error"]="Please mention atleast one dimensions"
                details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
                #print("\n dictionary is")
                print(str(details_dict))
             
#    t10=time.time()
#    print('\n Final1 time lap-:')
#    print(t10-t1)
                
except:
    print(traceback.print_exc())#comment
    try:
        if len(df)>0:
            if len(coke_data1)==0:
                details_dict['error']='Data not available for input combination, please try some different combination' #satish
            else:
                details_dict["error"]=random.choice(error_list)
        elif len(df)==0:
            details_dict['error']='Data not available for input combination, please try some different combination' #satish
        else:
            details_dict["error"]=random.choice(error_list)
    except:
        details_dict['error']='Data not available for input combination, please try some different combination' #satish

    details_dict['prompt_ques']=re.sub(' +',' ',(str('<b>Would you also like to ask</b> : '+(random.choice(random_prompt)))))
    details_dict = str(details_dict).replace('"',"*").replace("'",'"').replace("*","'")
    print(str(details_dict))