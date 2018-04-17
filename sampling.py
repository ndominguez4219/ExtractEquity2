# Samples n tickers from file; writes small ticker file of size sample_size
import typing as tg
import typecheck as tc
import re

import datetime
import math
import sys
import pickle
import string

from collections import OrderedDict
from pathlib import Path
import os.path

import logging
import pprint
import math

import numpy as np
import pandas as pd
import pdblp
import json
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import deque
import context

# add the handler to the root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# -------------- utility ------------------------------
def beautify(name:tg.Union[str,tg.List[str]])->tg.Optional[tg.Union[str, tg.List[str]]]:
    '''takes either a string or list(str) and squeezes out extra whitespace between tokens (leaving 1)
        Also removes '\n' '\t' and others.
        Returns same structure as was input (str or list) or None if error'''

    try:
        if isinstance(name,str):
            return " ".join(name.split()).lower()
        elif isinstance(name,list):
            for s in name:
                if not isinstance(s,str):
                    raise Exception('invalid base string [{}] in list [{}]'.format(s,name))
            for i,s in enumerate(name):
                name[i] = ' '.join(s.split()).lower()
            return name
        else:
            raise Exception('invalid input [{}]'.format(name))
    except Exception as e:
        logger.debug(e)
        return None



def toDateCls(dt:str)->datetime.date:
    '''Takes a bloomberg format date string and returns a python date class.  Assumes that dt string is in YYYYMMDD format.
    '''
    if isinstance ( dt, str ) == False:
        print ( "Error in toBloobergDt routine" )
        exit ()
    y = int ( dt[ :4 ] )
    m = int ( dt[ 5:6 ] )
    d = int ( dt[ 7: ] )
    return datetime.date ( year=y, month=m, day=d )

def toBloombergDt(d: datetime.date) ->str:
    '''Takes a date class and returns a bloomberg format date string: YYYYMMDD
    '''
    n = d.year * 10000 + d.month * 100 + d.day
    return str(n)

def isValidTicker(ticker:str,source:str)->bool:
    """Validate ticker format, assuming the 'source' style
    Returns True if valid.
    """
    try:
        assert(source in context.VALID_DATA_SOURCE)
        if source == 'Bloomberg':
            assert(len(ticker) > 0)
            matchStr = '^(?P<sym>[A-Z]{1,5}?)\s+?(?P<exchange>US|LN|GR)\s+?(?P<sectype>equity|cmdty|index)'
            pattern = re.compile(matchStr,flags=re.IGNORECASE)
            match = pattern.fullmatch(ticker)
            if match is None:
                logger.error('Invalid ticker {} and source {} combination'.format(ticker,source))
                return False
            else:
                matches = match.groupdict()
                logger.debug('Parsed symbol as [{sym}] exchange as [{exchange}] security type as [{sectype}]'.format(**matches))
                return True   
        elif source == 'Quandl':
            logger.error('Data source not connected yet: {}',format(source))
            exit(1)
        else:
            logger.error('Data source not connected yet: {}',format(source))
            exit(1)
    except AssertionError as a:
        print(a)
        logger.error('Invalid source {} detected with ticker: {}'.format(source,ticker))
        return False
    except Exception as e:
        print(e)
        return False


def checkTickers(data:pd.DataFrame, source:str = 'Bloomberg')->tg.Tuple[pd.DataFrame,pd.DataFrame]:
    """Checks a dataframe 'data' of tickers to check the validity of each one.  The format is determined by 'source'
    It returns a tuple of dataframes: one is the original minus errors 'clean', the other one is error records, 'errors'.
    Requires that a columns label 'ticker' exist in 'data'
    """
    assert(len(data.index) > 0), "input dataframe is empty"

    clean = pd.DataFrame()
    errors = pd.DataFrame()
    for index, row in data.iterrows():
        if isValidTicker(row['ticker'],source):
            clean = clean.append(data.iloc[index,0:],ignore_index=True)
        else:
            errors = errors.append(data.iloc[index,0:],ignore_index=True)

    return clean, errors


def isValidField(field: str = 'PX_LAST') -> bool:
    """Checks if field is one of the valid series fields in the store as a Series price record
    """
    assert(field in context.VALID_SERIES_FIELDS)
    return True

def isValidFundamentalField(field:str) -> bool:
    """Checks if field is one of the valid series fields in the store as a Series price record
    """
    assert(field in SampleTickers)
    return True
 
def isSourceOk(source: str)->bool:
    if not source in context.VALID_DATA_SOURCE:
        logger.error("Bad data source: {}".format(string))
    return True


def save(data:pd.DataFrame,full_filename:str)->bool:
    """Saves the dataframe, 'data', in the 'full_filename' file as either a readable json file or a serialized 
        pickle file depending on whether the 'filename_out' has a txt or 'pickle' extension.
        The 'full_filename' must include the 'drive:path/filename.ext'--normal windows format.
        The dataframe is written in 'orient=index' orientation.
        The json file is human-readable while pickle file is not.
        Returns True if completed successfully, otherwise False if erred for any reason.
    """
    assert(full_filename != '')
        
    (path,filename) = os.path.split(Path(full_filename))
    (path,ext) = os.path.splitext(Path(full_filename))

    try:
        if (ext == '.txt') or (ext == '.json'):
            with open(full_filename, 'w') as f:
                jsonStr = data.to_json(orient='index')
                f.write(json.dumps(json.loads(jsonStr), indent=4))
                    
        elif ext == '.pickle':
            with open(full_filename, 'wb') as f:
                pickle.dump(data, f)
        else:
            logger.error('Unknown file extension: [{}] in file : \n {:40s}'.format(ext, full_filename))
            raise Exception('Unknown file extension')
            return False
 
        logger.debug('file serialized to: \n {:80s}'.format(full_filename))
        logger.debug('wrote {:6d} rows'.format(len(data)))
        return True
    except OSError:
            logger.critical('cannot open file: \n {:80s}', full_filename)        
    except Exception as e:
        print(e)
    return False
   

def read(full_filename:str)->tg.Union[None,pd.DataFrame]:
    """read into memory the data in 'filename' which is just the short filename.  The procedure to read is
    determined by the file extension (i.e., txt or json, pickle, etc).
    The full path+filename is forced to be the 'data_directory' in the context.py file.
    The dataframe file is assumed to have been created in 'orient=index' orientation.
    """

    assert(full_filename != '')

    (path,filename) = os.path.split(Path(full_filename))
    (path,ext) = os.path.splitext(Path(full_filename))

    if Path(full_filename).is_file() == False:
        raise Exception('Path name: \n {:^120s} \n does not exist'.format(full_filename))

    try:
        if (ext == '.txt') or (ext == '.json'):
            with open(full_filename, 'rt') as f:
                # jsonStr = json.loads(f.read())
                df = pd.read_json(full_filename,orient='index')
        elif ext == '.pickle':
            with open(full_filename, 'rb') as f:
                df = pickle.load(f)
        else:
            logger.error('Unknown file extension: {}'.format(ext))
            return None
        logger.info('{:6d} records read from file: \n {:^120}'.format(len(df.index),full_filename))
        return df
    except Exception as e:
        print(e)
        exit(1)



class SampleABC(object,metaclass=ABCMeta):
    """Base class for all Sample types.
    Upon creating this init method checks for a file called 'full_filename.pickle'; it it exists, use this file.  If
    it does not exist, it assumpes that there exists a file called 'full_filename.xlsx' which has simple a column of
    tickers in Bloomberg format.  It reads this file, and creates 'full_filename.pickle', which the single column
    of the dataframe called 'tickers'.
    """
    universe: pd.DataFrame = pd.DataFrame()
    serialized_tickers_file: str

    @abstractmethod
    def __init__(self,full_filename:str):
        assert(full_filename != '')
        try:
            if Path(full_filename).is_file == False:
                raise Exception('file: \n {:120s} \n does not exist'.format(full_filename))
            (path,filename) = os.path.split(Path(full_filename))
            (root,ext) = os.path.splitext(Path(full_filename))

            #check if the serialized version of filename exists
            if ext != '.pickle':
                serialized_filename :str = root + str('.pickle')
                if Path(serialized_filename).is_file():
                    self.universe = read(serialized_filename)
                else:    #create it
                    self.universe = pd.read_excel ( full_filename, header=0, sheet_name=context.DEFAULT_SHEET )
                    self.universe.loc[:,'ticker'] = self.universe.loc[:,'ticker'].str.lower()
                    self.universe.columns = self.universe.columns.str.lower()
                    logger.info('{:6d} records read from: \n {:120s}'.format(len(self.universe.index),full_filename))
                    ok:bool = save(self.universe,serialized_filename)
                    if ok == True:
                        logger.info('Unverse file serialized')
                    else:
                        logger.error('Error serializing: \n {:120s}'.format(serialized_filename))
                        raise Exception('File serialization error')
                self.serialized_tickers_file = serialized_filename
            else:
                if Path(full_filename).is_file():
                    self.universe = read(full_filename)
                    logger.info('{:6d} records read from: \n {:120s}'.format(len(self.universe.index),full_filename))
                    self.serialized_tickers_file = full_filename
                else:
                    raise Exception('Invalid file: {:120s}')
        except Exception as e:
            print(e)


class Sample(SampleABC):
    nrows: int = 0
    sample: pd.DataFrame = pd.DataFrame()
    def __init__(self,full_filename=context.tickers_file):
        super().__init__(full_filename)

    def randomSample(self, data:tg.Optional[pd.DataFrame] = None,
                        securityType:tg.List[str] = [context.common],               # common, REIT, etc
                        byKey:tg.Optional[str] = context.industry_group,            # various fields in the data
                        fraction:float = .15)->tg.Optional[pd.DataFrame]:
        ''' Randomly sample records from 'data' by 'from subsample of 'securitypType' which are defined in 'context.py'.
            Note: 'securityType' is a list of security types.
            If data=None, then use 'self.universe' which is held in memory.
            if byKey = none then sample from all groups, applying 'fraction' to all matching 'securityType'
            Otherwise, sample by groups, applying 'fraction' to each 'byKey' within 'securityType'
            :rtype: dataframe or None
        '''

        if data is None:
            working = self.universe.copy(deep=True)
            logger.debug("Using self.universe")
        else:
            working = data.copy(deep=True)
            logger.debug('Using data input as parameter.')
        nrows = len(working.index)
        assert(nrows>0)
        
        #contrained to use fraction to neareast .01, which is 1%
        fraction = round(fraction,2)
        logger.debug('Rounded sampling fraction is {:4.3f}'.format(fraction))
        adj_fraction = int(math.floor(fraction*100.0))
        assert(adj_fraction in range(1,100))
        
        try:
            security_sample = working.loc[working[context.sectype].isin(securityType)].copy()
            
            #select subsample by securityType
            seed = datetime.datetime.now().microsecond
            if byKey is None:   
                result_sample = security_sample.sample(n = None, frac=fraction, replace=False, weights=None, random_state=seed)
            else:       #sample by groups
                grouped = security_sample.groupby(by=byKey)
                counts = grouped.size().reset_index(name='raw counts')

                result_sample = grouped.apply(lambda x: x.sample(frac=fraction, weights=None, random_state=seed))
                result_sample.reset_index(drop=True,inplace=True)

                sampled_grouped = result_sample.groupby(by=byKey)
                sampled_counts = sampled_grouped.size().reset_index(name='sampled counts')
                all_counts = pd.merge(counts, sampled_counts, on=byKey, how='outer')

            logger.debug('{} rows selected from [{}] group(s)'.format(len(result_sample.index),','.join(securityType)))
            if byKey:
                logger.debug('\n {:^80}'.format(all_counts.to_string()))

        except AssertionError as a:
            logger.critical('number to records to sample from is {} sample size to sample is '.format(nrows,fraction))
            print(a)
        except Exception as e:
            logger.critical('Data has {} rows, sample fraction {} on key {} and group(s) [{}]'
                            .format(nrows,fraction,byKey,','.join(securityType)))
            print(e)
            
        return result_sample

    def refdata(self,ticker:str, connection:object, request:tg.List[str] = context.DEFAULT_FUNDAMENTAL_FIELDS,
                source:str ='Bloomberg' )->pd.DataFrame:
        """Retrives fundamental fields from 'source' (setup up from Bloomberg right now').  Returns a dataframe.
           The dataframe returns has a column called 'ticker' which is the ticker requested, and a column for each request field.
        """
        try:
            assert(isValidTicker(ticker,source))
            result = connection.ref (ticker, request)
            return result
        except ConnectionError as e:
                logger.critical('Failed to start Bloomberg session: confirm terminal is started.')
                exit(1)
        except AssertionError as a:
            logger.critical('Bad ticker input, {} : not consistent with source, error:{}'.format(ticker,a))
        except Exception as bloombergException:
            logger.critical('Bloomberg access error, ticker {}, {}'.format(ticker,bloombergException))

class DataABC(object,metaclass=ABCMeta):
    """Base class for all Data structures.
    """
    @abstractmethod
    def __init__(self):
        pass
    def beautify(self, ticker:str)->str:
        return " ".join(ticker.split()).lower()

class BSeries(DataABC):
    """Retreives a record of fields (PX_LAST, PX_LOW, ...) from the Bloomberg data base, starting at begin_date to end_date.
    If begin_date if before the data begins, it will return (according to Bloomberg) beginning at the first data date.
    It will return data to end_date, or the most recently avaialable data.
    It expects dates in python format and will convert to bloomberg format.
    Note: this class returns a pandas dataframe with two levels of columns (ticker,field)
    """
    timeseries: pd.DataFrame

    def __init__(self,ticker:tg.List[str],
                    begindate:str,
                    enddate:str,
                    fields:tg.List[str] = context.HLOC_FIELDS,
                    freq:str ='DAILY'):
        super().__init__()

        try:
            conn = pdblp.BCon ( debug=True, port=8194, timeout = context.MILLISECONDS)
            
            conn.start ()

            if begindate > enddate:
                temp = enddate
                enddate = begindate
                begindate = temp

            for n in range(len(ticker)):
                ticker[n] = self.beautify(ticker[n])

            for t in ticker:
                if not isValidTicker(t,'Bloomberg'):
                    logger.critical('invalid ticker [{}] in request list [{}]'.format(t,ticker))
                    raise

            self.timeseries = conn.bdh (ticker,fields,end_date=enddate,start_date=begindate,
                                    ovrds=[ ('currency', 'usd') ],
                                    longdata=False )
    
        except ConnectionError as c:
            logger.critical('connection error when attemption to start Bloomberg \n {}'.format(c))
        except Exception as e:
            logger.critical('Bad ticker input, {} : not consistent with source'.format(ticker))
            conn.stop ()
            exit(e)

        conn.stop()

    def getseries(self)->pd.DataFrame:
        return self.timeseries


def seriesToToday(data:pd.DataFrame, source:str = 'Bloomberg',overwrite:bool = False)->tg.Optional[pd.DataFrame]:
    '''updates the time series in 'data' to the most recent close.
    Assumes that 'data' is a two-level column index with ticker as level 0 and fields as level 1.  Updates only those
    fields that already exist.
    data is assumed to include series on exactly one ticker
    '''
    try:
        if data.empty:
            raise Exception('dataframe is empty')
    
        ticker, fields = zip(*list(data.columns))
        ticker = [i for i in set(ticker)]
        fields = [i for i in set(fields)]

        conn = pdblp.BCon(timeout = context.MILLISECONDS)
        conn.start()
        logger.info('Bloomberg started')

        today = datetime.datetime.now().date().strftime('%Y%m%d')
        mostRecent = data.index.max().date().strftime('%Y%m%d')
        logger.info('reading historical daily for ticker {} and fields {} from {} to {}'.format(ticker,fields,today,mostRecent))

        result = conn.bdh(ticker,fields,mostRecent,today)
        logger.info('read {} records from Bloomberg for ticker {}'.format(len(result.index),ticker))
        if overwrite:
            combined = result.combine_first(data)
        else:
            combined = data.combine_first(result)

        conn.stop()
        logger.info('Bloomberg stopped')
        return combined

    except Exception as e:
        logger.error(e)
    return None