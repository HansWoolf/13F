#%%
import datetime as dt
import numpy as np
import os
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup as Soup
import fuzzywuzzy

#%%
#Define Function get Infotable
def Get_Infotable(WebPage):
    TempHyperlink = []
    for a in WebPage.find_all('a', href = True):
        if re.match(r'\/Archives\/edgar\/data\/\d{7}\/\d{18}\/.*\.xml',a['href'])!= None: TempHyperlink.append(a['href'])
    return ("https://www.sec.gov"+ TempHyperlink[2]) 
#%%
#Get Portfolio Date
def Get_Portfolio_Date(WebPage):
    Portfolio_Date = WebPage.find_all('div',{"class":"info"})[3].text
    return Portfolio_Date

#%%
#Get Fund Terms
ReleaseDelay = 45
ManagerConfig = pd.read_excel(r'D:\\Documents\\ASI\\17F\\ManagerConfig.xlsx')
EDGAR_13F_Template = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=00000000&type=13F&dateb=&owner=exclude&count=40'
FundTerm = pd.DataFrame()
for row in ManagerConfig.index:
    ManagerConfig.loc[row,'URL'] = EDGAR_13F_Template.replace("00000000",str(ManagerConfig.loc[row,'CIK']))
    TempFundTerm = pd.read_html(ManagerConfig.loc[row,'URL'])[-1]
    for a in TempFundTerm.index: TempFundTerm.loc[a,'Filing Date'] = dt.datetime.strptime(TempFundTerm.loc[a,'Filing Date'],'%Y-%m-%d')
    #TempFundTerm['Portfolio Date'] = Get_Portfolio_Date(TempFundTerm['Filing Date'],ReleaseDelay)
    SubPageLink = []
    WebPage = Soup(requests.get(ManagerConfig.loc[row,'URL']).content,"html.parser")
    for a in WebPage.find_all('a', href = True, id = True): SubPageLink.append("https://www.sec.gov" + a['href'])
    TempFundTerm['SubPageLink'] = SubPageLink
    TempFundTerm = TempFundTerm.loc[TempFundTerm['Filing Date']>=dt.datetime(2014,12,20)]
    Doc_Link,Portfolio_Date = [],[]
    for a in TempFundTerm.index: 
        WebPage1 = Soup(requests.get(TempFundTerm.loc[a,'SubPageLink']).content,"html.parser")
        Doc_Link.append(Get_Infotable(WebPage1))
        Portfolio_Date.append(Get_Portfolio_Date(WebPage1))
    TempFundTerm['Document_Link'],TempFundTerm['Fund'], TempFundTerm['Portfolio Date']= pd.DataFrame(Doc_Link),ManagerConfig.loc[row,'Name'],pd.DataFrame(Portfolio_Date)
    FundTerm = pd.concat([FundTerm,TempFundTerm[['Fund','Portfolio Date','Document_Link']]],axis = 0)
    print(row)
FundTerm.reset_index(drop = True, inplace = True)

#%%
#Get Portfolios Holdings

Portfolio = pd.DataFrame()
for row in FundTerm.index:
    TempPortfolio = pd.read_html(FundTerm.loc[row,'Document_Link'])[-1].loc[3:][[0,1,2,3,4,5,6,7]]
    TempPortfolio.columns = ["IssuerName","ShareClass","CUSIP","Value","Shares","SH/PRN","Description","Other Manager"]
    TempPortfolio['Fund'],TempPortfolio['Date'] = FundTerm.loc[row,'Fund'], FundTerm.loc[row,'Portfolio Date']
    Portfolio = pd.concat([Portfolio,TempPortfolio], axis = 0)
    print(row)
Portfolio['Value'] = Portfolio['Value'].astype(int)
Portfolio['Shares'] = Portfolio['Shares'].astype(int)

Portfolio.reset_index(drop = True, inplace = True)

# %%
#Top N Portfolio Filtering
N = 5
TopNPortfolio = pd.DataFrame()
Fund_Date_Index = Portfolio.drop_duplicates(subset = ['Fund','Date'],keep = 'first',inplace = False)[['Fund','Date']]
for row in Fund_Date_Index.index:
    DuplicateCheck = N
    while True:
        TempPortfolio = Portfolio.loc[(Portfolio['Fund']==Fund_Date_Index.loc[row,'Fund'])&(Portfolio['Date']==Fund_Date_Index.loc[row,'Date'])].drop_duplicates().nlargest(DuplicateCheck,'Value')
        if (len(TempPortfolio.drop_duplicates(subset = 'IssuerName'))==N): break
        DuplicateCheck += 1
    SumPortfolio = TempPortfolio['Value'].sum()
    TempPortfolio['Percentage'] = TempPortfolio['Value']/SumPortfolio
    TempPortfolio['Date'] = Fund_Date_Index.loc[row,'Date']
    TopNPortfolio = pd.concat([TopNPortfolio,TempPortfolio], axis = 0)
TopNPortfolio.reset_index(drop = True, inplace = True)
TopNPortfolio = TopNPortfolio[['Fund','Date','IssuerName','Value','Shares','Percentage','CUSIP','ShareClass','SH/PRN','Description','Other Manager']]

# %%
