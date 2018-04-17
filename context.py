
"""
All parameters determining the context for data acquisition, analytics, and plotting
"""
import sys
import string
import logging, logging.handlers
import os
import typing as tg
import pandas as pd
import numpy as np

MILLISECONDS: int = 5000                        #time out parameter for Bloomberg Bcon()

LOG_FILENAME = 'C:/Users/ndominguez/OneDrive/Research Program/log/development.log'

# set up logging to file
fmt = '%(asctime)s : %(name)-12s:%(lineno)-4d %(funcName)20s() : %(levelname)8s : %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format= fmt,
                    datefmt='%m-%d %H:%M',
                    filename= LOG_FILENAME,
                    filemode='wt')

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler(stream=sys.stderr)     #logging output
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter(fmt)
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)


VALID_FREQ = ['daily', 'monthly', 'weekly']

industry_group = 'gics_industry_group_name'             #Bloomberg field names
sector_group = 'gics_sector_name'
sectype = 'security_typ'
mktcap = 'crncy_adj_mkt_cap'
secname = 'security_name'

DEFAULT_FUNDAMENTAL_FIELDS = ['security_name','security_typ','eqy_init_po_dt','gics_sector_name','gics_industry_group_name','crncy_adj_mkt_cap']

common = 'Common Stock'
closed_end= 'Closed-End Fund'
reit = 'REIT'
mlp = 'MLP'
unit = 'Unit'
etp = 'ETP'
security_types = [common, closed_end, reit, mlp, unit, etp]

VALID_DATA_SOURCE = ['Bloomberg', 'Quandl']
px_last = 'px_last'
HLOC_FIELDS = [px_last, 'px_open', 'px_high', 'px_low']

S = tg.List[str]
D = tg.Union[None,pd.DataFrame]


# --------- parameters-------------------------
data_directory = "C:/Users/ndominguez/OneDrive/Research Program/Industry/Industry Discrimination/Data/"
DEFAULT_SHEET = 'Sheet1'
in_file = "BBTickers5000V"

tickers_file = data_directory + in_file + ".xlsx"
ser_tickers_file = data_directory + in_file + ".pickle"

working_file = data_directory + 'working_sample' + '.pickle'

out_file = "TickerList"
ticker_output_file = data_directory + ".CSV"
DEFAULT_DATA_PATH = data_directory
SERIALIZED_TICKER_UNIVERSE = DEFAULT_DATA_PATH + 'ticker_universe.pickle'
working_ticker_file = ticker_output_file

file_out = 'augmented_universe.json'


#St. Louis FRB
API_KEY = '710a71358b167fe5cbe596a8d33537cf'


#----------------------------------------------------------------------------
#          Utilities
#----------------------------------------------------------------------------

