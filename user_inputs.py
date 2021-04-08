# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 18:39:40 2021

@author: Admin
"""

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
from unilever_main import *
from segments_generate import *

# =============================================================================
# ### Taking Input
# =============================================================================

country="Lebanon"
category="DEODORANT"
channel='TOTAL LEBANON'
format_1="PRODUCT FORM"
format_2=""
format_3=""
# SDESC_1=""




# =============================================================================
# ### Taking Input2
# =============================================================================

# calling functions 
user_interest = ["1 - Segment_generater", "2 - Run Main Code"]
print("\n \n Your choice", user_interest)
user_input = int(input("Enter Your choice:  "))

if user_input==1:
    segment_generater_fun(country,category,channel,format_1,format_2,format_3) 
else:    
    main_fun(country,category,channel,format_1,format_2,format_3)

# import unilever_seg_creation