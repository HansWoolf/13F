#%%
import  ffn
import pandas as pd
import numpy as np
#%%

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

    component_returns= returns
    compnent_weights = pd.DataFrame(data=np.nan,index= component_returns.index,columns=component_returns.columns)
    compnent_weights.loc[weights.index,:] = weights

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


#%%
data_path = r'T:\London File1 Group\InvestmentProducts\Systematic Strategies\13F\Python Test Data.xlsx'
return_data = pd.read_excel(data_path,sheet_name='Return') 
return_data.set_index('Date',inplace = True)
weight_data = pd.read_excel(data_path,sheet_name='Weight')   
weight_data.set_index('Date',inplace = True)

#%%o


output = portfolio_performance(returns=return_data,weights=weight_data)

perf= output['Portfolio Perf']

port_index = output['portfolio_index']

perf.prices
perf.stats

pnl = output['Component PnL']

#%%

pnl.to_excel(r'T:\London File1 Group\InvestmentProducts\Systematic Strategies\13F\Python Output.xlsx')
port_index.to_excel(r'T:\London File1 Group\InvestmentProducts\Systematic Strategies\13F\Python Output.xlsx')

output['BoP_df'].to_excel(r'T:\London File1 Group\InvestmentProducts\Systematic Strategies\13F\Python Output.xlsx')
# %%
