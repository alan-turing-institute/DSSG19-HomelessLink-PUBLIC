#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import datetime
import matplotlib.patches as mpatches
from datetime import datetime


# ## Exploratory data analysis
# 
# Aims for the project:
# 1. Improve referral process for connecting rough sleepers to service providers and users
# 2. Better identification of duplicate notifications
# 3. understand trends over time and space 
# 

# In[3]:


#read file
path = 'xxxxx'
file = pd.read_csv(path, sep = ';', error_bad_lines = False)


# ### Challenge for Streetlink
# 
# #### Number of alerts in various region (London, coastline)

# In[4]:


# scatter plot of longitude and latitude with alpha controlling weight/density
get_ipython().run_line_magic('matplotlib', 'inline')
file.plot(kind = 'scatter', x = 'geo_location_longitude', y = 'geo_location_latitude', alpha = .05, figsize = (10,7))
plt.legend()


# In[10]:


#region
region = pd.DataFrame(file.groupby(by = 'region')['Alert ID'].count())
region['pct'] = region['Alert ID']/sum(region['Alert ID']) * 100
print(region)


# ### On an average day in London, Streetlink receive....on a peak day, they receive...

# In[20]:


file['alert_open_date_formulaT'] = pd.to_datetime(file['alert_open_date_formula'])
london = file[file['region'] == 'London']
y = pd.DataFrame(london.groupby(london['alert_open_date_formulaT'].dt.date)['Alert ID'].count())
y.describe()


# In[21]:


#across the entire dataset: What is the max, min, mean, standard deviation, etc? 
y = pd.DataFrame(file.groupby(file['alert_open_date_formulaT'].dt.date)['Alert ID'].count())
y.describe()


# ### Duplicate alerts by region
# * West Midlands and Wales have the most duplicates alerts detected at ~16%
# * East of England and South West have the lowest percent of duplicate alerts at ~10%

# In[75]:


dupCases = file.loc[file['duplication_status'] == 'Duplicate',]
display(pd.DataFrame(pd.value_counts(dupCases.region)/region['Alert ID']))


# ## Part 1: Descriptive statistices of the variables
# 
# we have unique indexes for the alerts, variables related to the rough sleeper(age, gender, location, time), duplicate status. We don't have the parent child node relationship between the referrals yet, therefore, duplicate cases are included in the descriptive statistics.

# #### We have 256,547 alerts in total in the processed file received on July 2nd, 2019

# In[26]:


file.shape[0]


# ### Continuous variables: how many days an alert remain in open ?

# In[76]:


display(pd.DataFrame(file.cases_days_open.describe()))


# ### Categorical variables
# historic alert: alert before 2017, age group is not fine grained

# In[81]:


print('There are {} types of alert record, they include:'. format(len(file.alert_record_type.unique())), file.alert_record_type.unique())
print('\n')
print('There are {} types of status, they include:'. format(len(file.status.unique())), file.status.unique()) 
print('\n')
print('There are {} types of Streetlink users, they include:'. format(len(file.referrer_type.unique())), file.referrer_type.unique()) 
print('\n')
print('There are {} regions, they include:'. format(len(file.region.unique())), file.region.unique()) 
print('\n')
print('There are {} age groups, they include:'. format(len(file.age_group.unique())), file.age_group.unique()) 


# ### Now we see how many NAs in each variable
# Most of alerts are created by individuals, because more than 98% of group alert are NA, nearly 30% of alerts do not have appearance description, this could be a problem for our deduplication process. Only 40% of users agree streetlink staff to contact them, among which 10 percent is from self referrer. 

# In[30]:


# calculate percent missing for each column
percent_missing = file.isnull().sum() * 100 / len(file)
missing_value_df = pd.DataFrame({'column_name' : file.columns,
                               'percent_missing' : percent_missing})
missing_value_df


# how many contacts give consent to be contacted?

# In[34]:


display(file.groupby('contact_consent').size())
p = 90371/(18436+90371) *100
print(f'Since changing the process to ask for consent, {p:.2f}% of people consent to be contacted')


# In[82]:


#age group
agegroup = pd.DataFrame(file.groupby(by = 'duplication_status')['Alert ID'].count())
agegroup['pct'] = agegroup['Alert ID']/sum(agegroup['Alert ID']) * 100
display(agegroup)


# ### Age
# There are 4 age groups, nearly 60% of the cases do not have age information, 30% of the rough sleepers in this sample are 25-50 years old

# In[60]:


#age group
agegroup = pd.DataFrame(file.groupby(by = 'age_group')['Alert ID'].count())
agegroup['pct'] = agegroup['Alert ID']/sum(agegroup['Alert ID']) * 100
display(pd.DataFrame(agegroup.sort_values(by='pct',ascending=False)))             


# ### Gender
# * Almost 46% of alerts do not have gender information. 
# * Only 10% of them indicate the rough sleeper is a female 

# In[59]:


#gender
gender = pd.DataFrame(file.groupby(by = 'gender')['Alert ID'].count())
gender['pct'] = gender['Alert ID']/sum(gender['Alert ID']) * 100
display(pd.DataFrame(gender.sort_values(by='pct', ascending=False)))


# ###  Percentage of alerts by local authority

# In[61]:


#LA check LA alert by time 
def checkVarPer(file1, groupName):
    group = pd.DataFrame(file1.groupby(by = groupName)['Alert ID'].count())
    group['pct'] = group['Alert ID']/sum(group['Alert ID']) * 100
    display(pd.DataFrame(group.sort_values(by='pct',ascending=False)))
    
checkVarPer(file, 'local_authority_name')


# ### Insights:
# 
# * There are about 30% of alerts without appearance description
# * 50% without gender information
# * 60% without age information. 
# 
# Cases with multiple NA fields *could* be low quality cases. These cases also cause problems in the deduplicate process

# ### Referral type
# Nearly 90% of the alerts are created by members in the public compared to self-reported alerts. 

# In[62]:


#referral type
referrertype = pd.DataFrame(file.groupby(by = 'referrer_type')['Alert ID'].count())
referrertype['pct'] = referrertype['Alert ID']/sum(referrertype['Alert ID']) * 100
display(pd.DataFrame(referrertype))


# ### Outcome
# Nearly 37% of the alert outcomes are 'person not found' , only 12% have 'Action taken',that's why we need to predict the likelihood of an alert having person not found as outcome.

# In[64]:


#outcome
outcome = pd.DataFrame(file.groupby(by = 'outcome_summary')['Alert ID'].count())
outcome['pct'] = outcome['Alert ID']/sum(outcome['Alert ID']) * 100
display(pd.DataFrame(outcome).sort_values(ascending=False,by='pct'))


# #### How long are the cases open in each region
# More than 50% of the alerts close in 1 day, 75% of alerts in London close in 1 day. The London team works very hard, but they have a lot of person not found cases

# In[43]:


def describeAll(Var):
    casesStats = pd.DataFrame(file.groupby(by = 'region')[Var].describe())
    med = pd.DataFrame(file.groupby(by = 'region')[Var].median())
    casesStats['med'] = med
    return casesStats
describeAll('cases_days_open')


# Number of alerts in different regions, London has the highest number of alerts

# Averaged number of alerts created in different times of a day 

# In[70]:


# how many days in this data set
df = file.groupby('alert_open_date_formula').nunique()
print('There are {} days in this data set'. format(df.shape[0]))


# #### Alerts throughout the day by region
# * There is a pattern of when people report by region (i.e. ~10am and ~8pm in London)

# In[69]:


def plotRegionHour1(Region):
    regionData = file[file.region == Region]
    hour = pd.DataFrame(regionData.groupby(by = 'hour_of_day_alert_raised')['Alert ID'].count())
    plt.plot(hour.index, hour['Alert ID']/2330)
    plt.xlabel('Hour', fontsize = 18)
    plt.ylabel('Number of alerts', fontsize = 18)
    plt.title(Region, fontsize = 20)


# In[47]:


def plotRegionHour(Region):
    regionData = file[file.region == Region]
    hour = pd.DataFrame(regionData.groupby(by = 'hour_of_day_alert_raised')['Alert ID'].count())
    plt.plot(hour.index, hour['Alert ID']/2330)
    plt.xlabel('Hour', fontsize = 10)
    plt.ylabel('Number of alerts', fontsize = 10)
    plt.title(Region, fontsize = 12)


# In[48]:


figure(num=None, figsize=(12,12), dpi = 100, facecolor = 'w', edgecolor = 'k')
plt.subplots_adjust(hspace = 0.8)
plt.subplot(5,3,1)
plotRegionHour('London')

plt.subplot(5,3,2)
plotRegionHour('Wales')

plt.subplot(5,3,3)
plotRegionHour('East Midlands')

plt.subplot(5,3,4)
plotRegionHour('East of England')

plt.subplot(5,3,5)
plotRegionHour('North East')

plt.subplot(5,3,6)
plotRegionHour('North West')

plt.subplot(5,3,7)
plotRegionHour('South East')

plt.subplot(5,3,8)
plotRegionHour('South West')

plt.subplot(5,3,9)
plotRegionHour('West Midlands')

plt.show()


# ### Temporal Statistics 

# In[49]:


file['alert_open_date_formula'] = pd.to_datetime(file['alert_open_date_formula'])


# In[50]:


yd = pd.DataFrame(file.groupby(file['alert_open_date_formulaT'].dt.year)['Alert ID'].count())


# ### Why do we need dedup? 
# The busiest day in a year in london shows almost ~6000 alerts coming in one day. De-duping could offset the bottlenecke experienced by Homeless Link Staff during peak season. 

# In[71]:


def PlotAlertsMonth(Origin):
   # timeMask = (file['alert_open_date_formula'].dt.year >= fromYear) & (file['alert_open_date_formula'].dt.year <= toYear)
   # yearData = file[timeMask]
    yearData = file[file['alert_origin'] == Origin]
    yearData['alert_open_date_formula'] = sorted(yearData['alert_open_date_formula'], reverse = True)
    yd = pd.DataFrame(yearData.groupby(yearData['alert_open_date_formulaT'].dt.month)['Alert ID'].count())
    plt.plot(yd.index, yd['Alert ID'])
    plt.xlabel('Month', fontsize = 10)
    plt.xticks(rotation=60)
    #plt.ylim (0,26000)
    plt.ylabel('Number of alerts', fontsize = 10)
    plt.title('Number of alert aggregated by month from 2012-2019', fontsize = 12)


# In[72]:


figure(num=None, figsize=(12,3), dpi = 100, facecolor = 'w', edgecolor = 'k')
blue = mpatches.Patch(color='blue', label='Phone line')
orange = mpatches.Patch(color='orange', label='Phone App')
green = mpatches.Patch(color='green', label='Website')
plt.legend(handles=[blue,orange,green])
plt.subplots_adjust(hspace = 1)
PlotAlertsMonth('Phone')
PlotAlertsMonth('App')
PlotAlertsMonth('Web')
plt.show()


# #### Most of the alerts appear to be coming in early in the year and late in the year when the weather is cold. 

# In[53]:


def PlotAlertsYear(Origin):
    timeMask = (file['alert_open_date_formula'].dt.year >= 2012) & (file['alert_open_date_formula'].dt.year <= 2018)
    yearData = file[timeMask]
    yearData = yearData[yearData['alert_origin'] == Origin]
    yearData['alert_open_date_formula'] = sorted(yearData['alert_open_date_formula'], reverse = True)
    yd = pd.DataFrame(yearData.groupby(yearData['alert_open_date_formula'].dt.year)['Alert ID'].count())
    plt.plot(yd.index, yd['Alert ID'])
    plt.xlabel('Year', fontsize = 10)
    plt.xticks(rotation=60)
    plt.ylabel('Number of alerts', fontsize = 10)
    plt.title('Number of alert aggregated by year from 2012-2018', fontsize = 12)


# In[54]:


figure(num=None, figsize=(12,3), dpi = 100, facecolor = 'w', edgecolor = 'k')
blue = mpatches.Patch(color='blue', label='Phone line')
orange = mpatches.Patch(color='orange', label='Phone App')
green = mpatches.Patch(color='green', label='Website')
plt.legend(handles=[blue,orange,green])
plt.subplots_adjust(hspace = 1)
PlotAlertsYear('Phone')
PlotAlertsYear('App')
PlotAlertsYear('Web')
plt.show()


# ### The number of website alerts appears to be increasing over time.  
# * Phone line alerts appear stable over time. 
# * Phone App alerts are stronger than phone line alerts, but show a year over year drop between 2017 and 2018. 
