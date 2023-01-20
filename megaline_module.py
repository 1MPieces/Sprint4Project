#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Loading all the libraries
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import statistics as st
from scipy import stats as ss
import calendar


# Load the data files into different DataFrames
calls_df = pd.read_csv('./megaline_calls.csv')
internet_df = pd.read_csv('./megaline_internet.csv')
messages_df = pd.read_csv('./megaline_messages.csv')
plans_df = pd.read_csv('./megaline_plans.csv')
users_df = pd.read_csv('./megaline_users.csv')

#Rename columns as per above:
plans_df = plans_df.rename(columns={
    'messages_included' : 'msgs_incl',
    'mb_per_month_included' : 'mb_incl',
    'minutes_included' : 'min_incl',
    'usd_monthly_pay' : 'plan_usd',
    'usd_per_gb' : 'gb_usd',
    'usd_per_message' : 'msg_usd',
    'usd_per_minute' : 'min_usd',
    'plan_name' : 'plan'
})




#Update data types as per above review:
plans_df['plan_usd'] = plans_df['plan_usd'].astype('float')
plans_df['gb_usd'] = plans_df['gb_usd'].astype('float')




# Add column names and functions:
plans_df['gb_incl'] = plans_df['mb_incl']/1024
plans_df['gb_incl'] = plans_df['gb_incl'].astype('int')


# Create new dataframe without extranious columns:
plans = plans_df.drop(columns = 'mb_incl')
plans.set_index('plan', drop=True, inplace=True)


#Update data types as per above review:
users_df['user_id'] = users_df['user_id'].astype('object')
users_df['reg_date'] = pd.to_datetime(users_df['reg_date'])
users_df['churn_date'] = pd.to_datetime(users_df['churn_date'])




# Add column to hold True if customer lives in New York or New Jersey:
# Create boolean column True if user lives in NY-NJ area

users_df['ny_nj'] = users_df['city'].str.contains('NY','NJ')


# Create new dataframe with only columns that will be needed for this project:
users = users_df.drop(columns = ['first_name', 'last_name', 'reg_date', 'churn_date', 'age', 'city'])


#Rename columns as per above review:
calls_df = calls_df.rename(columns={
    'id':'call_id',
})


#Update data types as per above review:
calls_df['user_id'] = calls_df['user_id'].astype('object')
calls_df['call_date'] = pd.to_datetime(calls_df['call_date'])




#Adding a rounded-up call duration:
calls_df['min_spent'] = np.ceil(calls_df['duration'])
calls_df['min_spent'] = calls_df['min_spent'].astype('int')

#Adding a month of call column:
calls_df['month'] = calls_df['call_date'].dt.month


# Create a new dataframe without extranious columns:
calls = calls_df.drop(columns = ['call_date', 'call_id'])


#Rename columns as per above:
messages_df = messages_df.rename(columns={'id':'msg_id', 'message_date':'msg_date'})


#Update data types as per above review:
messages_df['msg_id'] = messages_df['msg_id'].astype('object')
messages_df['user_id'] = messages_df['user_id'].astype('object')
messages_df['msg_date'] = pd.to_datetime(messages_df['msg_date'])




#Adding a month of text column:
messages_df['month'] = messages_df['msg_date'].dt.month

# Creating new dataframe and dropping columns:
messages = messages_df.drop(columns = ['msg_date'])


#Rename columns as per above:
internet_df = internet_df.rename(columns={'id':'web_id'})


#Update data types as per above review:
internet_df['web_id'] = internet_df['web_id'].astype('object')
internet_df['user_id'] = internet_df['user_id'].astype('object')
internet_df['session_date'] = pd.to_datetime(internet_df['session_date'])



# Add column names and formulas:
internet_df['gb_used'] = (internet_df['mb_used'])/1024
internet_df['month'] = internet_df['session_date'].dt.month


# Create new dataframe without extraneous columns:
internet = internet_df.drop(columns = ['web_id','session_date', 'mb_used'])


# Group calls made and the total duration by each user per month. Save the result.

# Isolate min_spent column to count and set index, rename columns
call_data = calls.pivot_table(values=['duration', 'min_spent'], index = ['user_id', 'month'], aggfunc = ['count', 'sum', 'mean'])
call_data = call_data.reset_index()

call_data.columns = ['user_id', 'month', 'ttl_calls', 'drop_me', 'sum_dur', 'sum_minspent', 'mean_dur', 'mean_minspent' ]
call_data = call_data.drop(columns = {'drop_me'})


# Calculate the number of messages sent by each user per month. Save the result.
#Isolate message information, count, index, rename columns
msg_data = messages.pivot_table(values=['msg_id'], index = ['user_id', 'month'], aggfunc = ['count'])
msg_data.columns = ['ttl_msgs']
msg_data['sum_msgspent'] = msg_data['ttl_msgs']
msg_data = msg_data.reset_index()



# Calculate the volume of internet traffic used by each user per month. Save the result.

# Isolate the gb_used data to sum over indexes user_id and month, rename columns
web_data = internet.pivot_table(values='gb_used', index = ['user_id', 'month'], aggfunc = ['count','sum'])
web_data.columns = ['ttl_sesn', 'sum_gbused']

# Create new column for chargeable GB, Round monthly GB total up to next integer:
web_data['sum_gbspent'] = np.ceil(web_data['sum_gbused'])
web_data['sum_gbspent'] = web_data['sum_gbspent'].astype('int')
web_data = web_data.reset_index()

# Reduce user data to only id, plan and region information 


user_data = users[['user_id', 'plan', 'ny_nj']]
user_data.sort_values('user_id')



# Merge the data for calls, minutes, messages, internet based on user_id and month

# merge call and text data on user_id and month
calls_and_msg = pd.merge(call_data, msg_data, how = 'outer', on = ['user_id', 'month'])
calls_and_msg.sort_values(['user_id', 'month'])


# Merge call and text dataframe with web_data on user_id and month
services_used = pd.merge(calls_and_msg, web_data, how = 'outer', on = ['user_id', 'month'])

services_used.sort_values(['user_id', 'month'])


# Add the user/plan information to the services_used dataframe
monthly_user = services_used.join(user_data.set_index('user_id'), on = ['user_id'], how = 'left', sort = True)


monthly_user = monthly_user.fillna(0)




# reset datatypes
monthly_user['sum_minspent'] = monthly_user['sum_minspent'].astype('int')
monthly_user['sum_msgspent'] = monthly_user['sum_msgspent'].astype('int')
monthly_user['sum_gbspent'] = monthly_user['sum_gbspent'].astype('int')
monthly_user['ttl_calls']= monthly_user['ttl_calls'].astype('int')
monthly_user['ttl_sesn'] = monthly_user['ttl_sesn'].astype('int')
monthly_user['ttl_msgs'] = monthly_user['ttl_msgs'].astype('int')


def revenue(row):
    
    # Information to pull from row
    
    min_spent = row['sum_minspent']
    gb_spent = row['sum_gbspent']
    msgs_spent = row['sum_msgspent']
    plan = row['plan']
    
    
    
    # Plan Details:
    
    # Plan limits:
    min_incl = plans.loc[plan, 'min_incl']
    msgs_incl = plans.loc[plan, 'msgs_incl']
    gb_incl = plans.loc[plan, 'gb_incl']
    
    # USD for plan and extras:
    plan_usd = plans.loc[plan, 'plan_usd']
    min_usd = plans.loc[plan, 'min_usd']
    msg_usd = plans.loc[plan, 'msg_usd']
    gb_usd = plans.loc[plan,'gb_usd']
    
    
    
    # Overage:
    
    x_min = (min_spent - min_incl)*min_usd
    x_msg = (msgs_spent - msgs_incl)*msg_usd
    x_gb = (gb_spent - gb_incl)*gb_usd
    
    rev = plan_usd + x_min.clip(0) + x_msg.clip(0) + x_gb.clip(0)
    
    return rev
    
# Apply the function and get the revenue.
monthly_user['revenue'] = monthly_user.apply(revenue, axis=1)