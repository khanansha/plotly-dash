# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 14:55:59 2021

@author: Anjum Khan
"""
# QUESTION 2
import random
import array 
from random import randrange
import math
max_len=12
digits=['0','1','2','3','4','5','6','7','8','9']
lower_char=['a','b','c','d','e','f','h','g','i']
uper_char=['A','B','C','D','E','F','G']
Symbol=['@','#','$']
C= digits + lower_char + uper_char

rand_digit=random.choice(digits)
rand_lower_char=random.choice(lower_char)
rand_uper_char=random.choice(uper_char)
rand_symbol= random.choice(Symbol)


temp_var= rand_digit + rand_lower_char + rand_uper_char + rand_symbol

for x in range(max_len- 4):
    temp_var=temp_var + random.choice(C)
    temp_var_list=array.array('u',temp_var)
    random.shuffle(temp_var_list)

password=""    
for x in temp_pass_list:
    
    password=password+x

print(password) 


# QUESTION 1
print("1-ROCK")
print("2-PAPER")
print("3-SCISSOR")

while True:
    player_1=int(input("BOB:"))
    if player_1 >3 or player_1 <1:
        print("invalid input")
        break
    if player_1==1:
        player_input="ROCK"
    elif player_1 ==2:
        player_input="PAPER"
    else:
        player_input="SCISSOR"
    print("BOB SELECTED:" + player_input)   
    
    player_2= int(input("ALICE"))
    if player_1 == player_2:
        print("you both cannot win...")
        break
    if player_2==1:
        player_input_2="ROCK"
    else player_2==2:
        player_input_2="PAPER"
    else:
        player_input_2="SCISSOR"
    print("ALICE SELECTED:" + player_input_2)   
    if((player_1==1 and player_2==2) or (player_1==2 and player_2==1)):
        result="paper"
    elif((player_1==1 and player_2==3) or (player_1==3 and player_2==1)):
        result="Rock"
    else:
        result="Scissor"    
    if result==player_input:
        print("bob won...")
    else:
        print("Alice won :")
    print("wanna play again?(Y/N")   
    again=input()
    if again =='n' or again=='N':
        break
print("EXIT")    
        
 

#  QUESTION 3
lower_inpt= int(input("enter lower: -"))
uper_inpt= int(input("enter upper: -"))

a= random.randint(lower,upper)





