
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: functions.py : python script with general functions                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

# basic
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta

# data amd processing
import MetaTrader5 as mt5


# -------------------------------------------------------------------------- MT5: INITIALIZATION / LOGIN -- #
# ------------------------------------------------------------------------------------------------------ -- #

def f_init_login(param_acc, param_pass, param_exe):
    """
    Initialize conexion and Login into a Meta Trader 5 account in the local computer where this code is executed, using the MetaTrader5 python package.

    Parameters
    ----------

    param_acc: int
        accout number used to login into MetaTrader5 Web/Desktop App (normally is a 8-9 digit integer number)
        param_acc = 41668916
    param_pass: str
        accout trader's password (or just password) to login into MetaTrader5 Web/Desktop App 
        (normally alphanumeric include uppercase and sometimes symbols). If the investor's password 
        is provided, the some actions do not work like open trades.
        param_pass = "n2eunlnt"
    
    param_direxe: str
        Route in disk where is the executable file of the MetaTrader5 desktop app which will be used 
        param_direxe = 'C:\\Program Files\\MetaTrader 5\\terminal64.exe'
    
    Return
    ------

        if connection is succesful then returns connected client object and prints message,
        if connection is not succesful then returns error message and attempts a shutdown of connection.
    
    References
    ----------
    https://www.mql5.com/en/docs/integration/python_metatrader5/mt5login_py
    
    
    """
    
    # server name (as it is specified in the terminal)
    mt5_ser = "MetaQuotes-Demo"
    # timeout (in miliseconds)
    mt5_tmo = 10000

    # Perform initialization handshake
    ini_message = mt5.initialize(param_exe, login=param_acc, password=param_pass, server=mt5_ser,
                                 timeout=mt5_tmo, portable=False)

    # resulting message
    if not ini_message:
        print(" **** init_login failed, error code =", mt5.last_error())
        mt5.shutdown()
    else: 
        print(" ++++ init_login succeded, message = ", ini_message)
    
    # returns an instance of a connection object (or client)
    return mt5


# ------------------------------------------------------------------------------------ MT5: ACCOUNT INFO -- #
# ------------------------------------------------------------------------------------------------------ -- #

def f_acc_info(param_ct):
    """
    Get the info of the account associated with the initialized client param_ct

    Params
    ------

    param_ct: MetaTrader5 initialized client object
        this is an already succesfully initialized conexion object to MetaTrader5 Desktop App

    Returns
    -------

    df_acc_info: pd.DataFrame 
        Pandas DataFrame with the account info         

    References
    ----------
    https://www.mql5.com/en/docs/integration/python_metatrader5/mt5login_py

    """

    # get the account info and transform it into a dataframe format
    acc_info = param_ct.account_info()._asdict()
    
    # select especific info to display
    df_acc_info = pd.DataFrame(list(acc_info.items()), columns=['property','value'])

    # return dataframe with the account info
    return df_acc_info


# ------------------------------------------------------------------------------- MT5: HISTORICAL ORDERS -- #
# ------------------------------------------------------------------------------------------------------ -- #

def f_hist_trades(param_ct, param_ini, param_end):
    """
    Get the historical executed trades in the account associated with the initialized MetaTrader5 client

    Params
    ------

    param_ct: MetaTrader5 initialized client object
        This is an already succesfully initialized conexion object to MetaTrader5 Desktop App
    
    param_ini: datetime
        Initial date to draw the historical trades
        
        param_ini = datetime(2021, 2, 1)
    param_end: datetime
        Final date to draw the historical trades
        
        param_end = datetime(2021, 3, 1)

    Returns
    -------
        df_hist_trades: pd.DataFrame

    References
    ----------
    https://www.mql5.com/en/docs/integration/python_metatrader5/mt5historydealsget_py
    https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties  
    
    https://www.mql5.com/en/docs/integration/python_metatrader5/mt5historyordersget_py
    https://www.mql5.com/en/docs/constants/tradingconstants/orderproperties#enum_order_property_integer

    """

    # get historical info of deals in the account
    history_deals = param_ct.history_deals_get(param_ini, param_end)

    # get historical info of orders in the account
    history_orders = param_ct.history_orders_get(param_ini, param_end)

    # check for returned results
    if (len(history_orders) > 0) & (len(history_deals) > 0):
        print(" ++++ Historical orders retrive: OK")
        print(" ++++ Historical deals retrive: OK")
    else:
        print("No orders and/or deals returned")

    # historical deals of the account
    df_deals = pd.DataFrame(list(history_deals), columns=history_deals[0]._asdict().keys())
    
    # historical orders of the account
    df_orders = pd.DataFrame(list(history_orders), columns=history_orders[0]._asdict().keys())
   
    # useful columns from orders
    df_hist_trades = df_orders[['time_setup', 'symbol', 'position_id', 'type', 'volume_current',
                           'price_open', 'sl', 'tp']]

    # rename columns 
    df_hist_trades.columns = ['OpenTime', 'Symbol', 'Ticket', 'Type', 'Volume', 'OpenPrice', 'S/L', 'T/P']

    # choose only buy or sell transactions (ignore all the rest, like balance ...)
    df_hist_trades = df_hist_trades[(df_hist_trades['Type'] == 0) | (df_hist_trades['Type'] == 1)]
    df_hist_trades['OpenTime'] = pd.to_datetime(df_hist_trades['OpenTime'], unit='s')
    
    # unique values for position_id
    uni_id = df_hist_trades['Ticket'].unique()
    
    # first and last index for every unique value of position_id
    ind_opens = [df_hist_trades.index[df_hist_trades['Ticket'] == i][0] for i in uni_id]
    ind_closes = [df_hist_trades.index[df_hist_trades['Ticket'] == i][-1] for i in uni_id]
    
    # generate lists with values to add
    cts = df_hist_trades['OpenTime'].loc[ind_closes]
    cps = df_hist_trades['OpenPrice'].loc[ind_closes]

    # resize dataframe to have only the first value of every unique position_id
    df_hist_trades = df_hist_trades.loc[ind_opens]

    # add close time and close price as a column to dataframe
    df_hist_trades['CloseTime'] = cts.to_list()
    df_hist_trades['ClosePrice'] = cps.to_list()
    df_hist_trades['Profit'] = df_deals['profit'].loc[df_deals['position_id'].isin(uni_id) & 
                                                      df_deals['entry'] == 1].to_list()
  
    return df_hist_trades

# ------------------------------------------------------------------------------- MT5: HISTORICAL PRICES -- #
# ------------------------------------------------------------------------------------------------------ -- #

def f_hist_prices(param_ct, param_sym, param_tf, param_ini, param_end):
    """
    Historical prices retrival from MetaTrader 5 Desktop App.

    Parameters
    ----------

    param_ct: MetaTrader5 initialized client object
        This is an already succesfully initialized conexion object to MetaTrader5 Desktop App
      
    param_sym: str
        The symbol of which the historical prices will be retrieved
        
        param_sym = 'EURUSD'
    
    param_tf: str
        The price granularity for the historical prices. Check available timeframes and nomenclatures from 
        the references. The substring 'TIMEFRAME_' is automatically added.
        
        param_tf = 'M1'
    param_ini: datetime
        Initial date to draw the historical trades
        
        param_ini = datetime(2021, 2, 1)
    param_end: datetime
        Final date to draw the historical trades
        
        param_end = datetime(2021, 3, 1)
    
    **** WARNINGS ****
    
    1.- Available History
    
        MetaTrader 5 terminal provides bars only within a history available to a user on charts. The number of # bars available to users is set in the "Max.bars in chart" parameter. So this must be done
        manually within the desktop app to which the connection is made.
    
    2.- TimeZone
        When creating the 'datetime' object, Python uses the local time zone, 
        MetaTrader 5 stores tick and bar open time in UTC time zone (without the shift).
        Data received from the MetaTrader 5 terminal has UTC time.
        Perform a validation whether if its necessary to shift time to local timezone.

    **** ******** ****

    References
    ----------
    https://www.mql5.com/en/docs/integration/python_metatrader5/mt5copyratesfrom_py#timeframe

    """


    # get hour info in UTC timezone (also GMT+0)
    hour_utc = datetime.datetime.now().utcnow().hour
    # get hour info in local timezone (your computer)
    hour_here = datetime.datetime.now().hour
    # difference (in hours) from UTC timezone
    diff_here_utc = hour_utc - hour_here
    # store the difference in hours
    tdelta = datetime.timedelta(hours=diff_here_utc)
    # granularity 
    param_tf = getattr(param_ct, 'TIMEFRAME_' + param_tf)
    # dictionary for more than 1 symbol to retrieve prices
    d_prices = {}

    # retrieve prices for every symbol in the list
    for symbol in param_sym:
        # prices retrival from MetaTrader 5 Desktop App
        prices = pd.DataFrame(param_ct.copy_rates_range(symbol, param_tf,
                                                         param_ini - tdelta,
                                                         param_end - tdelta))
        # convert to datetime 
        prices['time'] = [datetime.datetime.fromtimestamp(times) for times in prices['time']]

        # store in dict with symbol as a key
        d_prices[symbol] = prices
    
    # return historical prices
    return d_prices
