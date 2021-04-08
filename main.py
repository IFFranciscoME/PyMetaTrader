
"""
# -- --------------------------------------------------------------------------------------------------- -- #
# -- project: A SHORT DESCRIPTION OF THE PROJECT                                                         -- #
# -- script: main.py : python script with the main functionality                                         -- #
# -- author: YOUR GITHUB USER NAME                                                                       -- #
# -- license: GPL-3.0 License                                                                            -- #
# -- repository: YOUR REPOSITORY URL                                                                     -- #
# -- --------------------------------------------------------------------------------------------------- -- #
"""

# -- import libraries
import pandas as pd
import datetime

# -- import project scripts
import functions as fn

# ------------------------------------------------------------------------------------------ LOCAL FILES -- #
# ------------------------------------------------------------------------------------------ ----------- -- #

# --------------------------------------------------------------------------------- MetaTrader5 CONEXION -- #
# --------------------------------------------------------------------------------- -------------------- -- #

# -- ----------------------------------------------------------------------- INITIALIZE AND LOGIN TO MT5 -- #

# 1. Open locally MetaTrader5 Desktop App
# 2. Check everything is working fine and the connection to the account is ok
# 3. Run the following

# local direction of executable file
local_exe = 'C:\\Program Files\\MetaTrader 5\\terminal64.exe'

# account number *** USE YOURS INSTEAD ***
mt5_acc = 41668916

# account Password Traders/Investors *** USE YOURS INSTEAD ***
# mt5_trd_pas = "n2eunlnt" 
mt5_inv_pas = "1pfmhwne"

# try initialization and login
mt5_client = fn.f_init_login(param_acc=mt5_acc, param_pass=mt5_inv_pas, param_exe=local_exe)

# -- -------------------------------------------------------------------------------------- ACCOUNT INFO -- # 

# get the account info of the already initialized mt5 client
df_acc_info = fn.f_acc_info(param_ct=mt5_client)

# ---------------------------------------------------------------------- HISTORICAL ACCOUNT ORDERS/DEALS -- # 

# construct a datetime that is explicitly aware of the difference 
ini_date = datetime.datetime(2011, 2, 1, 0, 0)
end_date = datetime.datetime(2021, 2, 26, 0, 0)

# get historical prices
df_hist = fn.f_hist_trades(param_ct=mt5_client, param_ini=ini_date, param_end=end_date)

# ------------------------------------------------------------------------------------ HISTORICAL PRICES -- # 

# get the historical trades of the account (using orders + deals info)

# construct init time from the OpenTime - minute of the first trade
ini_date = df_hist['OpenTime'].iloc[0] - datetime.timedelta(minutes=76288)
# construct init time from the OpenTime + minute of the last trade
end_date = df_hist['CloseTime'].iloc[-1] + datetime.timedelta(minutes=1)
# get all the symbols traded in order to download prices for all of them
symbols = list(df_hist['Symbol'].unique())

# get historical prices using UTC time
df_prices = fn.f_hist_prices(param_ct=mt5_client, param_sym=symbols, param_tf='M1',
                             param_ini=ini_date, param_end=end_date)

# in case a timeshift is necessary to re-express the time column for local use
# diff_here_server = 8
# in case of the need to display times in local timezone (your computer)
# df_prices['time'] = df_prices['time'] + pd.Timedelta(hours=diff_here_server)
