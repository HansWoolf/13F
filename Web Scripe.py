#%%
import datetime as dt
import numpy as np
import os
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup as Soup
import ffn
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
ManagerConfig = pd.read_excel(r'D:\\Documents\\ASI\\17F\\ManagerConfig.xlsx', index_col = 'Name')
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
    TempFundTerm['Document_Link'],TempFundTerm['Fund'], TempFundTerm['Portfolio Date']= pd.DataFrame(Doc_Link),row,pd.DataFrame(Portfolio_Date)
    FundTerm = pd.concat([FundTerm,TempFundTerm[['Fund','Portfolio Date','Filing Date','Document_Link']]],axis = 0)
    print(row)
FundTerm.reset_index(drop = True, inplace = True)

#%%
#Get Portfolios Holdings

Portfolio = pd.DataFrame()
for row in FundTerm.index:
    TempPortfolio = pd.read_html(FundTerm.loc[row,'Document_Link'])[-1].loc[3:][[0,1,2,3,4,5,6,7,8,9,10,11]]
    TempPortfolio.columns = ["IssuerName","ShareClass","CUSIP","Value","Shares","SH/PRN","Put/Call","Discretion","Other Manager","VotingSole","VotingShared","VotingNone"]
    TempPortfolio['Fund'],TempPortfolio['Date'],TempPortfolio['Filing Date'] = FundTerm.loc[row,'Fund'], FundTerm.loc[row,'Portfolio Date'],FundTerm.loc[row,'Filing Date']
    Portfolio = pd.concat([Portfolio,TempPortfolio], axis = 0)
    print(row)
Portfolio['Value'] = Portfolio['Value'].astype(int)
Portfolio['Shares'] = Portfolio['Shares'].astype(int)

Portfolio.reset_index(drop = True, inplace = True)
#Portfolio = Portfolio.replace('Anchro Bolt','Anchor Bolt')
#%%
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + dt.timedelta(n)
# %%
#Top N Portfolio Filtering
N = 5
DuplicateCheck = True
TopNPortfolio = pd.DataFrame()
Fund_Date_Index = Portfolio.drop_duplicates(subset = ['Fund','Date'],keep = 'first',inplace = False)[['Fund','Date']]
Portfolio = Portfolio.drop_duplicates(subset = ['Fund','Date','IssuerName','Value'],keep = 'first')
for row in Fund_Date_Index.index:
    DuplicateCheckNum = N
    while DuplicateCheck:
        TempPortfolio = Portfolio.loc[(Portfolio['Fund']==Fund_Date_Index.loc[row,'Fund'])&(Portfolio['Date']==Fund_Date_Index.loc[row,'Date'])].drop_duplicates().nlargest(DuplicateCheckNum,'Value')
        if (len(TempPortfolio.drop_duplicates(subset = 'IssuerName'))==N): break
        DuplicateCheckNum += 1
    SumPortfolio = TempPortfolio['Value'].sum()
    TempPortfolio['Percentage'] = TempPortfolio['Value']/SumPortfolio
    #As one entire portfolio
    #TempPortfolio['Percentage'] = TempPortfolio['Value']/SumPortfolio*ManagerConfig.loc[TempPortfolio['Fund'].iloc[0],'Proportion']
    TempPortfolio['Date'] = Fund_Date_Index.loc[row,'Date']
    TopNPortfolio = pd.concat([TopNPortfolio,TempPortfolio], axis = 0)
TopNPortfolio = TopNPortfolio.sort_values(by = ['Date','Fund'], ascending = [1,1])
TopNPortfolio.reset_index(drop = True, inplace = True)
TopNPortfolio = TopNPortfolio[['Date','Filing Date','Fund','IssuerName','Percentage','Value','Shares','CUSIP','ShareClass','SH/PRN','Put/Call','Discretion','Other Manager','VotingSole','VotingShared','VotingNone']]
IssuerNameTicker = {'LEAR CORP':'LEA US','HUNTSMAN CORP':'HUN US','GENERAL MTRS CO':'GM US','AMERICAN AIRLS GROUP INC':'AAL US','DANA HLDG CORP':'DAN US','SYNOVUS FINL CORP':'SNV US','WELLS FARGO & CO NEW':'WFC US','SQUARE 1 FINL INC':'SQBK US','HUDSON VALLEY HLDG CORP':'HVB US','US BANCORP DEL':'USB US','APPLE INC':'AAPL US','AVAGO TECHNOLOGIES LTD':'1373183D US','LINKEDIN CORP':'LNKD US','BAIDU INC':'BIDU US','NETFLIX INC':'NFLX US','MALLINCKRODT PUB LTD CO':'MNK US','GILEAD SCIENCES INC':'GILD US','PFIZER INC':'PFE US','VALEANT PHARMACEUTICALS INTL':'BHC US','AGILENT TECHNOLOGIES INC':'A US','ULTA SALON COSMETCS & FRAG I':'ULTA US','DOLLAR TREE INC':'DLTR US','BEST BUY INC':'BBY US','BURLINGTON STORES INC':'BURL US','LOWES COS INC':'LOW US','TERRAFORM PWR INC':'TERP US','ALLERGAN INC':'1284849D US','SPDR S&P 500 ETF TR':'SPY US','NORTHSTAR ASSET MGMT GROUP I':'NSAM US','NORTHSTAR RLTY FIN CORP':'NRF US','GENERAL MTRS CO':'GM US','BERRY PLASTICS GROUP INC':'BERY US','LEAR CORP':'LEA US','INGERSOLL-RAND PLC':'IR US','TYSON FOODS INC':'TSN US','SYNOVUS FINL CORP':'SNV US','WELLS FARGO & CO NEW':'WFC US','BELDEN INC':'BDC US','CENTERSTATE BANKS INC':'CSFL US','HUDSON VALLEY HOLDING CORP':'HVB US','APPLE INC':'AAPL US','AVAGO TECHNOLOGIES LTD':'1373183D US','NETFLIX INC':'NFLX US','CHARTER COMMUNICATIONS INC D':'9876540D US','FACEBOOK INC':'FB US','HORIZON PHARMA PLC':'HZNP US','MALLINCKRODT PUB LTD CO':'MNK US','GILEAD SCIENCES INC':'GILD US','LABORATORY CORP AMER HLDGS':'LH US','BIOGEN INC':'BIIB US','ULTA SALON COSMETCS & FRAG I':'ULTA US','PRICELINE GRP INC':'BKNG US','LOWES COS INC':'LOW US','BURLINGTON STORES INC':'BURL US','CITIGROUP INC':'C US','ACTAVIS PLC':'AGN US','CHENIERE ENERGY INC':'LNG US','WILLIAMS COS INC DEL':'WMB US','NORTHSTAR ASSET MGMT GROUP I':'NSAM US','APPLE INC':'AAPL US','LYONDELLBASELL INDUSTRIES N':'LYB US','TYSON FOODS INC':'TSN US','MAGNA INTL INC':'MG US','GENERAL MTRS CO':'GM US','DELTA AIR LINES INC DEL':'DAL US','SYNOVUS FINANCIAL CORP.':'SNV US','WELLS FARGO & CO':'WFC US','ASTORIA FINANCIAL CORP.':'AF US','CENTERSTATE BANKS, INC.':'CSFL US','HUDSON VALLEY HOLDING CORP':'HVB US','APPLE INC':'AAPL US','FACEBOOK INC':'FB US','AVAGO TECHNOLOGIES LTD':'1373183D US','JD COM INC':'JD US','CHARTER COMMUNICATIONS INC D':'9876540D US','EDWARDS LIFESCIENCES CORP':'EW US','HCA HOLDINGS INC':'HCA US','TENET HEALTHCARE CORP':'THC US','MALLINCKRODT PUB LTD CO':'MNK US','HORIZON PHARMA PLC':'HZNP US','ULTA SALON COSMETCS & FRAG I':'ULTA US','CTRIP COM INTL LTD':'TCOM US','RYDER SYS INC':'R US','LYONDELLBASELL INDUSTRIES N':'LYB US','JD COM INC':'JD US','ALLERGAN PLC':'AGN US','WILLIAMS COS INC DEL':'WMB US','APPLE INC':'AAPL US','CHENIERE ENERGY INC':'LNG US','NORTHSTAR RLTY FIN CORP':'NRF US','BERRY PLASTICS GROUP INC':'BERY US','SOUTHWEST AIRLS CO':'LUV US','TYSON FOODS INC':'TSN US','MAGNA INTL INC':'MG US','DELTA AIR LINES INC DEL':'DAL US','ASTORIA FINL CORP':'AF US','SYNOVUS FINL CORP':'SNV US','US BANCORP':'USB US','WELLS FARGO & CO':'WFC US','STERLING BANCORP':'STL US','APPLE INC':'AAPL US','FACEBOOK INC':'FB US','NETFLIX INC':'NFLX US','AVAGO TECHNOLOGIES LTD':'1373183D US','CHARTER COMMUNICATIONS INC D':'9876540D US','ALLERGAN PLC':'AGN US','EDWARDS LIFESCIENCES CORP':'EW US','STERICYCLE INC':'SRCL US','BARD C R INC':'BCR US','ABBOTT LABS':'ABT US','ULTA SALON COSMETCS & FRAG I':'ULTA US','CITIGROUP INC':'C US','PRICELINE GRP INC':'BKNG US','FACEBOOK INC':'FB US','POLARIS INDS INC':'PII US','VALEANT PHARMACEUTICALS INTL':'BHC US','APPLE INC':'AAPL US','BIOGEN INC':'BIIB US','ALLERGAN PLC':'AGN US','NORTHSTAR RLTY FIN CORP':'NRF US','BERRY PLASTICS GROUP INC':'BERY US','SOUTHWEST AIRLS CO':'LUV US','TYSON FOODS INC':'TSN US','DELTA AIR LINES INC DEL':'DAL US','FORD MTR CO DEL':'F US','WELLS FARGO & CO NEW':'WFC US','REGIONS FINL CORP':'RF US','CITIZENS FINL GROUP INC':'CFG US','BRIDGE BANCORP INC':'BDGE US','US BANCORP DEL':'USB US','ALPHABET INC':'GOOG US','APPLE INC':'AAPL US','MICROSOFT CORP':'MSFT US','NETFLIX INC':'NFLX US','FACEBOOK INC':'FB US','BARD C R INC':'BCR US','LILLY ELI & CO':'LLY US','MEDIVATION INC':'MDVN US','BAXALTA INC':'BXLT US','LABORATORY CORP AMER HLDGS':'LH US','FACEBOOK INC':'FB US','CITIGROUP INC':'C US','PRICELINE GRP INC':'BKNG US','ULTA SALON COSMETCS & FRAG I':'ULTA US','MCDONALDS CORP':'MCD US','LEVEL 3 COMMUNICATIONS INC':'LVLT US','MICROSOFT CORP':'MSFT US','MICROSOFT CORP':'MSFT US','COMCAST CORP NEW':'CMCSA US','ALPHABET INC':'GOOG US','ALLERGAN PLC':'AGN US','SOUTHWEST AIRLS CO':'LUV US','BERRY PLASTICS GROUP INC':'BERY US','DELTA AIR LINES INC DEL':'DAL US','HONEYWELL INTL INC':'HON US','GENERAL MTRS CO':'GM US','CITIZENS FINANCIAL GROUP INC.':'CFG US','WELLS FARGO & CO NEW':'WFC US','REGIONS FINANCIAL CORP. NEW':'RF US','BRIDGE BANCORP, INC.':'BDGE US','US BANCORP DEL':'USB US','FACEBOOK INC':'FB US','MICROSOFT CORP':'MSFT US','NETFLIX INC':'NFLX US','JD COM INC':'JD US','ALPHABET INC':'GOOG US','ZIMMER BIOMET HLDGS INC':'ZBH US','QUEST DIAGNOSTICS INC':'DGX US','DEXCOM INC':'DXCM US','LABORATORY CORP AMER HLDGS':'LH US','MALLINCKRODT PUB LTD CO':'MNK US','ULTA SALON COSMETCS & FRAG I':'ULTA US','FACEBOOK INC':'FB US','HOME DEPOT INC':'HD US','COACH INC':'TPR US','PVH CORP':'PVH US','APPLE INC':'AAPL US','EXPEDIA INC DEL':'EXPE US','LEVEL 3 COMMUNICATIONS INC':'LVLT US','ALPHABET INC':'GOOG US','DOLLAR TREE INC':'DLTR US','BERRY PLASTICS GROUP INC':'BERY US','SOUTHWEST AIRLS CO':'LUV US','GENERAL MTRS CO':'GM US','HONEYWELL INTL INC':'HON US','EQT CORP':'EQT US','REGIONS FINL CORP NEW':'RF US','WELLS FARGO & CO':'WFC US','CITIZENS FINL GROUP INC':'CFG US','US BANCORP DEL':'USB US','BRIDGE BANCORP INC':'BDGE US','ACTIVISION BLIZZARD INC':'ATVI US','LIBERTY BROADBAND CORP':'LBRDK US','FACEBOOK INC':'FB US','NETFLIX INC':'NFLX US','JD COM INC':'JD US','EDWARDS LIFESCIENCES CORP':'EW US','LABORATORY CORP AMER HLDGS':'LH US','HOLOGIC INC':'HOLX US','AETNA INC NEW':'AET US','ZIMMER BIOMET HLDGS INC':'ZBH US','HOME DEPOT INC':'HD US','PVH CORP':'PVH US','TJX COS INC NEW':'TJX US','ULTA SALON COSMETCS & FRAG I':'ULTA US','BURLINGTON STORES INC':'BURL US','EXPEDIA INC DEL':'EXPE US','LEVEL 3 COMMUNICATIONS INC':'LVLT US','WILLIAMS COS INC DEL':'WMB US','APPLE INC':'AAPL US','DOLLAR TREE INC':'DLTR US','GOODYEAR TIRE & RUBR CO':'GT US','BERRY PLASTICS GROUP INC':'BERY US','SOUTHWEST AIRLS CO':'LUV US','HONEYWELL INTL INC':'HON US','LEAR CORP':'LEA US','REGIONS FINL CORP NEW':'RF US','CITIZENS FINANCIAL GROUP INC.':'CFG US','WELLS FARGO & CO':'WFC US','US BANCORP':'USB US','JP MORGAN CHASE & CO.':'JPM US','FACEBOOK INC':'FB US','APPLE INC':'AAPL US','LIBERTY BROADBAND CORP':'LBRDK US','ACTIVISION BLIZZARD INC':'ATVI US','NETFLIX INC':'NFLX US','ZIMMER BIOMET HLDGS INC':'ZBH US','CIGNA CORPORATION':'9999945D US','BIOGEN INC':'BIIB US','EDWARDS LIFESCIENCES CORP':'EW US','LABORATORY CORP AMER HLDGS':'LH US','PVH CORP':'PVH US','CAVIUM INC':'CAVM US','VAIL RESORTS INC':'MTN US','BURLINGTON STORES INC':'BURL US','MGM RESORTS INTERNATIONAL':'MGM US','EXPEDIA INC DEL':'EXPE US','APPLE INC':'AAPL US','EBAY INC':'EBAY US','WILLIAMS COS INC DEL':'WMB US','ENCANA CORP':'ECA US','SOUTHWEST AIRLS CO':'LUV US','SEALED AIR CORP NEW':'SEE US','BERRY PLASTICS GROUP INC':'BERY US','NOBLE ENERGY INC':'NBL US','EQT CORP':'EQT US','REGIONS FINANCIAL CORP.':'RF US','ASTORIA FINANCIAL CORP.':'AF US','BRIDGE BANCORP, INC.':'BDGE US','WELLS FARGO & CO':'WFC US','CITIZENS FINANCIAL GROUP INC.':'CFG US','FACEBOOK INC':'FB US','LIBERTY BROADBAND CORP':'LBRDK US','NVIDIA CORP':'NVDA US','ACTIVISION BLIZZARD INC':'ATVI US','ALPHABET INC':'GOOG US','CELGENE CORP':'CELG US','LABORATORY CORP AMER HLDGS':'LH US','INTUITIVE SURGICAL INC':'ISRG US','AETNA INC NEW':'AET US','QUEST DIAGNOSTICS INC':'DGX US','DOLLAR TREE INC':'DLTR US','HOME DEPOT INC':'HD US','ADVANCE AUTO PARTS INC':'AAP US','BURLINGTON STORES INC':'BURL US','MGM RESORTS INTERNATIONAL':'MGM US','EXPEDIA INC DEL':'EXPE US','MARATHON PETE CORP':'MPC US','WELLS FARGO & CO NEW':'WFC US','APPLE INC':'AAPL US','TESORO CORP':'ANDV US','BERRY PLASTICS GROUP INC':'BERY US','GOODYEAR TIRE & RUBR CO':'GT US','ALASKA AIR GROUP INC':'ALK US','GENERAL MTRS CO':'GM US','UNITED CONTL HLDGS INC':'UAL US','BRIDGE BANCORP INC':'BDGE US','WELLS FARGO & CO NEW':'WFC US','JPMORGAN CHASE & CO':'JPM US','US BANCORP DEL':'USB US','REGIONS FINL CORP NEW':'RF US','FACEBOOK INC':'FB US','LIBERTY BROADBAND CORP':'LBRDK US','BROADCOM LTD':'AVGO US','ACTIVISION BLIZZARD INC':'ATVI US','BANK AMER CORP':'BAC US','BIOVERATIV INC':'BIVV US','AETNA INC NEW':'AET US','CELGENE CORP':'CELG US','LABORATORY CORP AMER HLDGS':'LH US','EDWARDS LIFESCIENCES CORP':'EW US','COHERENT INC':'COHR US','DOLLAR TREE INC':'DLTR US','LOWES COS INC':'LOW US','ADVANCE AUTO PARTS INC':'AAP US','BURLINGTON STORES INC':'BURL US','EBAY INC':'EBAY US','EBAY INC':'EBAY US','MICROSOFT CORP':'MSFT US','WIX COM LTD':'WIX US','MICROSOFT CORP':'MSFT US','MARATHON PETE CORP':'MPC US','WIX COM LTD':'WIX US','ENCANA CORP':'ECA US','SOUTHWEST AIRLS CO':'LUV US','ALASKA AIR GROUP INC':'ALK US','TURQUOISE HILL RES LTD':'TRQ US','UNITED CONTL HLDGS INC':'UAL US','NOBLE ENERGY INC':'NBL US','ALLY FINL INC':'ALLY US','BRIDGE BANCORP INC':'BDGE US','WELLS FARGO CO NEW':'WFC US','JPMORGAN CHASE & CO':'JPM US','US BANCORP DEL':'USB US','FACEBOOK INC':'FB US','LIBERTY BROADBAND CORP':'LBRDK US','ALIBABA GROUP HLDG LTD':'BABA US','ACTIVISION BLIZZARD INC':'ATVI US','BROADCOM LTD':'AVGO US','BIOVERATIV INC':'BIVV US','CELGENE CORP':'CELG US','LABORATORY CORP AMER HLDGS':'LH US','EDWARDS LIFESCIENCES CORP':'EW US','AETNA INC NEW':'AET US','COHERENT INC':'COHR US','PVH CORP':'PVH US','PARKER HANNIFIN CORP':'PH US','HOME DEPOT INC':'HD US','MCDONALDS CORP':'MCD US','MICROSOFT CORP':'MSFT US','AT&T INC':'T US','EXPEDIA INC DEL':'EXPE US','FLEETCOR TECHNOLOGIES INC':'FLT US','IAC INTERACTIVECORP':'IAC US','TYSON FOODS INC':'TSN US','NOBLE ENERGY INC':'NBL US','LEAR CORP':'LEA US','GENERAL MTRS CO':'GM US','DANA INCORPORATED':'DAN US','ALLY FINL INC':'ALLY US','WELLS FARGO CO NEW':'WFC US','BRIDGE BANCORP INC':'BDGE US','JPMORGAN CHASE & CO':'JPM US','US BANCORP DEL':'USB US','FACEBOOK INC':'FB US','APPLE INC':'AAPL US','ALIBABA GROUP HLDG LTD':'BABA US','LIBERTY BROADBAND CORP':'LBRDK US','ACTIVISION BLIZZARD INC':'ATVI US','SPDR S&P 500 ETF TR':'SPY US','LABORATORY CORP AMER HLDGS':'LH US','BIOVERATIV INC':'BIVV US','ZIMMER BIOMET HLDGS INC':'ZBH US','AETNA INC NEW':'AET US','PVH CORP':'PVH US','DOLLAR TREE INC':'DLTR US','UNITED RENTALS INC':'URI US','COHERENT INC':'COHR US','HOME DEPOT INC':'HD US','SPDR S&P 500 ETF TR':'SPY US','MICROSOFT CORP':'MSFT US','FLEETCOR TECHNOLOGIES INC':'FLT US','ACTIVISION BLIZZARD INC':'ATVI US','IAC INTERACTIVECORP':'IAC US','CHENIERE ENERGY INC':'LNG US','DELTA AIR LINES INC DEL':'DAL US','GENERAL MTRS CO':'GM US','CATERPILLAR INC DEL':'CAT US','SOUTHWEST AIRLS CO':'LUV US','ALLY FINL INC':'ALLY US','WELLS FARGO CO NEW':'WFC US','JPMORGAN CHASE & CO':'JPM US','US BANCORP DEL':'USB US','ONEMAIN HLDGS INC':'OMF US','BROADCOM LTD':'AVGO US','FACEBOOK INC':'FB US','NVIDIA CORP':'NVDA US','ALIBABA GROUP HLDG LTD':'BABA US','LIBERTY BROADBAND CORP':'LBRDK US','MERCK & CO INC':'MRK US','BIOVERATIV INC':'BIVV US','ZIMMER BIOMET HLDGS INC':'ZBH US','CELGENE CORP':'CELG US','LABORATORY CORP AMER HLDGS':'LH US','DOLLAR TREE INC':'DLTR US','PVH CORP':'PVH US','MICRON TECHNOLOGY INC':'MU US','FACEBOOK INC':'FB US','SHERWIN WILLIAMS CO':'SHW US','SPDR S&P 500 ETF TR':'SPY US','FLEETCOR TECHNOLOGIES INC':'FLT US','MICROSOFT CORP':'MSFT US','CHENIERE ENERGY INC':'LNG US','ALPHABET INC':'GOOG US','CHENIERE ENERGY INC':'LNG US','DELTA AIR LINES INC DEL':'DAL US','GENERAL MTRS CO':'GM US','LEAR CORP':'LEA US','ANDEAVOR':'ANDV US','ALLY FINL INC':'ALLY US','WELLS FARGO CO NEW':'WFC US','ONEMAIN HLDGS INC':'OMF US','JPMORGAN CHASE & CO':'JPM US','US BANCORP DEL':'USB US','TWITTER INC':'TWTR US','FACEBOOK INC':'FB US','SHOPIFY INC':'SHOP US','ELECTRONIC ARTS INC':'EA US','BROADCOM LTD':'AVGO US','ZIMMER BIOMET HLDGS INC':'ZBH US','REGENERON PHARMACEUTICALS':'REGN US','MERCK & CO INC':'MRK US','LABORATORY CORP AMER HLDGS':'LH US','DENTSPLY SIRONA INC':'XRAY US','GLOBAL PMTS INC':'GPN US','PVH CORP':'PVH US','LUMENTUM HLDGS INC':'LITE US','ELDORADO RESORTS INC':'ERI US','LULULEMON ATHLETICA INC':'LULU US','FLEETCOR TECHNOLOGIES INC':'FLT US','MICROSOFT CORP':'MSFT US','WIX COM LTD':'WIX US','CSX CORP':'CSX US','FIRST DATA CORP NEW':'FDC US','CHENIERE ENERGY INC':'LNG US','GENERAL MTRS CO':'GM US','HESS CORP':'HES US','LEAR CORP':'LEA US','AMERICAN AXLE & MFG HLDGS IN':'AXL US','ALLY FINL INC':'ALLY US','ONEMAIN HLDGS INC':'OMF US','WELLS FARGO CO NEW':'WFC US','BRIDGE BANCORP INC':'BDGE US','JPMORGAN CHASE & CO':'JPM US','FACEBOOK INC':'FB US','TWITTER INC':'TWTR US','ELECTRONIC ARTS INC COM STK':'EA US','ACTIVISION BLIZZARD INC COM STK':'ATVI US','SHOPIFY INC OTTAWA ON COM':'SHOP US','MERCK & CO INC':'MRK US','REGENERON PHARMACEUTICALS':'REGN US','ZIMMER BIOMET HLDGS INC':'ZBH US','LABORATORY CORP AMER HLDGS':'LH US','ANTHEM INC':'ANTM US','PVH CORP':'PVH US','ELDORADO RESORTS INC':'ERI US','UNITED RENTALS INC':'URI US','CATERPILLAR INC DEL':'CAT US','BURLINGTON STORES INC':'BURL US','SPDR S&P 500 ETF TR':'SPY US','FIRST DATA CORP NEW':'FDC US','MICROSOFT CORP':'MSFT US','WIX COM LTD':'WIX US','FLEETCOR TECHNOLOGIES INC':'FLT US','CHENIERE ENERGY INC':'LNG US','CATERPILLAR INC DEL':'CAT US','DANA INCORPORATED':'DAN US','UNITED TECHNOLOGIES CORP':'UTX US','CROWN HOLDINGS INC':'CCK US','ALLY FINL INC':'ALLY US','ONEMAIN HLDGS INC':'OMF US','JPMORGAN CHASE & CO':'JPM US','BRIDGE BANCORP INC':'BDGE US','WELLS FARGO CO NEW':'WFC US','LIBERTY BROADBAND CORP':'LBRDK US','ACTIVISION BLIZZARD INC':'ATVI US','FACEBOOK INC':'FB US','MICROSOFT CORP':'MSFT US','NETFLIX INC':'NFLX US','ZIMMER BIOMET HLDGS INC':'ZBH US','MERCK & CO INC':'MRK US','MEDTRONIC PLC':'MDT US','REGENERON PHARMACEUTICALS':'REGN US','STRYKER CORP':'SYK US','THERMO FISHER SCIENTIFIC INC':'TMO US','WORLDPAY INC':'WP US','WYNN RESORTS LTD':'WYNN US','FIDELITY NATL INFORMATION SV':'FIS US','TRACTOR SUPPLY CO':'TSCO US','FIRST DATA CORP NEW':'FDC US','WIX COM LTD':'WIX US','IQVIA HLDGS INC':'IQV US','CHENIERE ENERGY INC':'LNG US','ANDEAVOR':'ANDV US','CHENIERE ENERGY INC':'LNG US','BERRY GLOBAL GROUP INC':'BERY US','UNITED RENTALS INC':'URI US','CATERPILLAR INC DEL':'CAT US','AMERICAN AIRLS GROUP INC':'AAL US','ALLY FINL INC':'ALLY US','JPMORGAN CHASE & CO':'JPM US','ONEMAIN HLDGS INC':'OMF US','BRIDGE BANCORP INC':'BDGE US','WELLS FARGO CO NEW':'WFC US','ALIBABA GROUP HLDG LTD':'BABA US','AMAZON COM INC':'AMZN US','LIBERTY BROADBAND CORP':'LBRDK US','FACEBOOK INC':'FB US','MICROSOFT CORP':'MSFT US','SPDR S&P 500 ETF TR':'SPY US','MERCK & CO INC':'MRK US','REGENERON PHARMACEUTICALS':'REGN US','ZIMMER BIOMET HLDGS INC':'ZBH US','INTUITIVE SURGICAL INC':'ISRG US','GLOBAL PMTS INC':'GPN US','FIDELITY NATL INFORMATION SV':'FIS US','ELDORADO RESORTS INC':'ERI US','BRUNSWICK CORP':'BC US','INTERCONTINENTAL EXCHANGE IN':'ICE US','SPDR S&P 500 ETF TR':'SPY US','FIRST DATA CORP NEW':'FDC US','IQVIA HLDGS INC':'IQV US','WILLIAMS COS INC DEL':'WMB US','MARATHON PETE CORP':'MPC US','BERRY GLOBAL GROUP INC':'BERY US','CHENIERE ENERGY INC':'LNG US','DANA INCORPORATED':'DAN US','HESS CORP':'HES US','CABOT OIL & GAS CORP':'COG US','JPMORGAN CHASE & CO':'JPM US','WELLS FARGO CO NEW':'WFC US','BRIDGE BANCORP INC':'BDGE US','ONEMAIN HLDGS INC':'OMF US','US BANCORP DEL':'USB US','LIBERTY BROADBAND CORP':'LBRDK US','FACEBOOK INC':'FB US','SERVICENOW INC':'NOW US','NETFLIX INC':'NFLX US','AMAZON COM INC':'AMZN US','DANAHER CORPORATION':'DHR US','UNITEDHEALTH GROUP INC':'UNH US','ZIMMER BIOMET HLDGS INC':'ZBH US','MEDTRONIC PLC':'MDT US','NEUROCRINE BIOSCIENCES INC':'HZNP US','BIO RAD LABS INC':'BIO US','FOOT LOCKER INC':'FL US','LULULEMON ATHLETICA INC':'LULU US','BRUNSWICK CORP':'BC US','ADVANCE AUTO PARTS INC':'AAP US','FIRST DATA CORP NEW':'FDC US','WILLIAMS COS INC DEL':'WMB US','CHENIERE ENERGY INC':'LNG US','MICROSOFT CORP':'MSFT US','MARATHON PETE CORP':'MPC US','UNITED RENTALS INC':'URI US','BERRY GLOBAL GROUP INC':'BERY US','CHENIERE ENERGY INC':'LNG US','GENERAL MTRS CO':'GM US','GRAFTECH INTL LTD':'EAF US','JPMORGAN CHASE & CO':'JPM US','ONEMAIN HLDGS INC':'OMF US','BRIDGE BANCORP INC':'BDGE US','WELLS FARGO CO NEW':'WFC US','US BANCORP DEL':'USB US','LIBERTY BROADBAND CORP':'LBRDK US','FACEBOOK INC':'FB US','SERVICENOW INC':'NOW US','MICROSOFT CORP':'MSFT US','NETFLIX INC':'NFLX US','NEUROCRINE BIOSCIENCES INC':'HZNP US','HUMANA INC':'HUM US','EDWARDS LIFESCIENCES CORP':'EW US','MERCK & CO INC':'MRK US','MADRIGAL PHARMACEUTICALS INC':'MDGL US','FIDELITY NATL INFORMATION SV':'FIS US','BURLINGTON STORES INC':'BURL US','LULULEMON ATHLETICA INC':'LULU US','ELDORADO RESORTS INC':'ERI US','BIO RAD LABS INC':'BIO US','CITIGROUP INC':'C US','MARATHON PETE CORP':'MPC US','MICROSOFT CORP':'MSFT US','BANK AMER CORP':'BAC US','CHENIERE ENERGY INC':'LNG US','CHENIERE ENERGY INC':'LNG US','UNITED TECHNOLOGIES CORP':'UTX US','GENERAL MTRS CO':'GM US','BERRY GLOBAL GROUP INC':'BERY US','NEWMONT GOLDCORP CORPORATION':'NEM US','ONEMAIN HLDGS INC':'OMF US','WELLS FARGO CO NEW':'WFC US','BRIDGE BANCORP INC':'BDGE US','US BANCORP DEL':'USB US','JPMORGAN CHASE & CO':'JPM US','LIBERTY BROADBAND CORP':'LBRDK US','ALIBABA GROUP HLDG LTD':'BABA US','FACEBOOK INC':'FB US','GLOBAL PMTS INC':'GPN US','SERVICENOW INC':'NOW US','EDWARDS LIFESCIENCES CORP':'EW US','NEUROCRINE BIOSCIENCES INC':'HZNP US','MERCK & CO INC':'MRK US','BIOGEN INC':'BIIB US','INTUITIVE SURGICAL INC':'ISRG US','FIDELITY NATL INFORMATION SV':'FIS US','LOWES COS INC':'LOW US','DOLLAR TREE INC':'DLTR US','BURLINGTON STORES INC':'BURL US','VAIL RESORTS INC':'MTN US','CITIGROUP INC':'C US','MICROSOFT CORP':'MSFT US','FIDELITY NATL INFORMATION SV':'FIS US','BANK AMER CORP':'BAC US','MARATHON PETE CORP':'MPC US',}
TopNPortfolio[['Date']] = pd.to_datetime (TopNPortfolio['Date'],infer_datetime_format=True)
TopNPortfolio[['Filing Date']] = pd.to_datetime (TopNPortfolio['Filing Date'],infer_datetime_format=True)
TopNPortfolio = TopNPortfolio.loc[TopNPortfolio['Date']<=dt.datetime(2019,12,30)]
TopNPortfolio['IssuerTicker'] = TopNPortfolio['IssuerName'].map(IssuerNameTicker)

#%%
def Return_Calculation(returns,weights,DateDelta):
    returns = returns+1
    WeightMatrix = pd.DataFrame(index = returns.index, columns = returns.columns)
    WeightsDate = weights.index.drop_duplicates()
    weights = WeightsByTicker
    for FilingDate in range(len(WeightsDate)):
        startdate = WeightsDate[FilingDate] + dt.timedelta(days = 1)
        enddate = WeightsDate[FilingDate+1] + dt.timedelta(days = 1) if FilingDate+1<len(WeightsDate) else startdate+dt.timedelta(days = DateDelta)
        Holdings = weights.loc[startdate-dt.timedelta(days = 1)].groupby('IssuerTicker').sum()
        for single_date in daterange(startdate, enddate): 
            for Ticker in Holdings.index: WeightMatrix.loc[single_date,Ticker] = Holdings.loc[Ticker][0]
    WeightMatrix = WeightMatrix.dropna(axis = 0, how = 'all').replace(np.nan,0)
    Performance = pd.DataFrame(index = WeightMatrix.index,columns = ['Performance'])
    for CalculationDate in range(0,len(WeightMatrix.index)):
        DailyPerformance = sum(returns.loc[WeightMatrix.index[CalculationDate]].mul(WeightMatrix.loc[WeightMatrix.index[CalculationDate]]/sum(WeightMatrix.loc[WeightMatrix.index[CalculationDate]])))
        if WeightMatrix.index[CalculationDate] == WeightMatrix.index[0]: Performance.loc[WeightMatrix.index[CalculationDate],'Performance'] = DailyPerformance*sum(WeightMatrix.loc[WeightMatrix.index[CalculationDate]])
        else: Performance.loc[WeightMatrix.index[CalculationDate],'Performance'] = DailyPerformance*Performance.loc[WeightMatrix.index[CalculationDate-1],'Performance']
    return Performance.iloc[:,0]


        




#%%
#Calculate Return
DateDelta = 45
ReturnByTicker = pd.read_excel(r'D:\\Documents\\ASI\\17F\\Return Data by Ticker.xlsx', index_col = 'Date').dropna()
TopNPortfolio['Simulation Date'] = TopNPortfolio['Date']+dt.timedelta(days = DateDelta)
TopNPortfolio = TopNPortfolio.set_index('Simulation Date')

Performance = pd.DataFrame()
for Manager in ManagerConfig.index:
    #Manager = 'Steadfast'
    TopNPortfolioWithTicker = TopNPortfolio.loc[TopNPortfolio['Fund'] == Manager]
    WeightsByTicker = TopNPortfolioWithTicker[['IssuerTicker','Percentage']]
    Performance = pd.concat([Performance,Return_Calculation(ReturnByTicker,WeightsByTicker,DateDelta).rename(Manager)], axis = 1)
Performance = pd.concat([Performance,Return_Calculation(ReturnByTicker,TopNPortfolio[['IssuerTicker','Percentage']],DateDelta).rename('Portfolio')], axis = 1)






#%%

# %%
def portfolio_performance(returns,weights):
    """
    Input component weights and returns. Output portfolio performance
    Paremeters:
    weights: A time series or single-row matrix/vector containing asset weights, as decimal
    percentages, treated as beginning of period weights. See
    """
    print('Calculating Portfolio Performance')
    # returns=target_asset_port_data_attributes['component_returns']
    # weights =target_asset_port_data_attributes['effective_weights']
    returns = ReturnByTicker
    weights = WeightsByTicker
    
    component_returns= returns
    compnent_weights = pd.DataFrame(data=np.nan,index= component_returns.index,columns=component_returns.columns)

    for date in compnent_weights.index:
        for IssuerTicker in compnent_weights.columns:
            if date in weights.loc[weights['IssuerTicker']==IssuerTicker].index:
                compnent_weights.loc[date,IssuerTicker] = weights.loc[weights['IssuerTicker']==IssuerTicker].loc[date].iloc[0,2]

    portfolio_dates = component_returns.index
    components = component_returns.columns

    # pre-allocate
    BoP_df = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=components)
    EoP_df = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=components)
    PnL_df = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=components)
    portfolio_BoP = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=['Portfolio BoP'])
    portfolio_EoP = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=['Portfolio EoP'])
    portfolio_PnL = pd.DataFrame(data=np.nan,index=portfolio_dates,columns=['Portfolio PnL'])
        
    portfolio_index =  pd.DataFrame(data=np.nan,index=portfolio_dates,columns=['Index'])
    previous_index_value = np.int64(1)

    pre_date = portfolio_dates[0]
    # set BoP to start weights
    for date,row in component_returns.iterrows():
        # print(date)
        # 1st date
        if date == portfolio_dates[0]:
            BoP_df.loc[date] = compnent_weights.iloc[0,:]
            EoP_df.loc[date] = BoP_df.loc[date] * (1+component_returns.loc[date])
            PnL_df.loc[date] = EoP_df.loc[date].subtract(BoP_df.loc[date])

            portfolio_BoP.loc[date] = BoP_df.loc[date].sum()
            portfolio_EoP.loc[date] = EoP_df.loc[date].sum()
            portfolio_PnL.loc[date] = PnL_df.loc[date].sum()

            portfolio_index.loc[date] = np.nansum([previous_index_value,portfolio_PnL.loc[date].values])
            previous_index_value =  portfolio_index.loc[date]
            pre_date = date

        # after first date
        else:
            BoP_df.loc[date] = EoP_df.loc[pre_date]
            # weights override
            if date in compnent_weights.index:
                none_NaN_index = ~compnent_weights.loc[date].isnull()
                if  not compnent_weights.loc[date][none_NaN_index].empty:
                    tmp_sum = BoP_df.loc[date].sum()
                    BoP_df.loc[date][none_NaN_index.values] = (compnent_weights.loc[date][none_NaN_index.values].values)*tmp_sum

                    
            EoP_df.loc[date] = BoP_df.loc[date] * (1+component_returns.loc[date])
            PnL_df.loc[date] = EoP_df.loc[date].subtract(BoP_df.loc[date])

            portfolio_BoP.loc[date] = BoP_df.loc[date].sum()
            portfolio_EoP.loc[date] = EoP_df.loc[date].sum()
            portfolio_PnL.loc[date] = PnL_df.loc[date].sum()
            
            portfolio_index.loc[date] = np.nansum([previous_index_value,portfolio_PnL.loc[date].values])
            previous_index_value =  portfolio_index.loc[date]
            pre_date = date


    portfolio_returns = portfolio_index.pct_change(1)    
    portfolio_returns.columns = ['Returns']

    portfolio_index
    perf = portfolio_index.calc_stats()
   
    output = pd.Series(data = [perf,PnL_df,portfolio_index,portfolio_BoP,portfolio_EoP,BoP_df], index=['Portfolio Perf','Component PnL','portfolio_index','portfolio_BoP','portfolio_EoP','BoP_df'])
    return output

# %%
