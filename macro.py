

"""This module interfaces with the FRED database of economic variables from St. Louis Federal Reserve.
"""
import sys
import typing as tg
import typecheck as tc

import json
from fred import Fred

import logging
import warnings

import pandas as pd
import pprint
import datetime
from abc import ABCMeta, abstractmethod, abstractproperty
import context

# add the handler to the root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


series_description = {
'T5YIFR'    : '5-Year, 5-Year Forward Inflation Expectation Rate; %; D; NSA',
'T5YIFRM'   : '5-Year, 5-Year Forward Inflation Expectation Rate; %; M; NSA',
'T10Y2Y'    : '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity; %; D; NSA',
'T10Y2YM'   : '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity; %; M; NSA',
'TEDRATE'   : 'TED Spread; %; D; NSA'
}

DEfAULT_PARAMS = {'limit': 10000,
                'observation_start': '1940-01-01'
            }
url_root = 'https://api.stlouisfed.org/fred'

def pp_json(json_thing, sort=True, indents=4):
    if isinstance(json_thing,str) == True:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None

class Series(object,metaclass=ABCMeta):
    """Base class for all Series types.
    """
    @abstractmethod
    def __init__(self,in_params):
        obs = pd.DataFrame(index=[],columns=[],dtype=float)
        obs_freq = None
        params = {}
        series_list= None
        fredSeries = fredCategories = None

        self.fredSeries = Fred(api_key = context.API_KEY, response_type = 'df')
        self.fredDetails = Fred(api_key = context.API_KEY, response_type = 'json')
        if any(in_params.values()) == False:
            self.params = DEfAULT_PARAMS
        else:
            self.params = in_params
   
    @abstractproperty
    def getSeries(self,seriesList,join_type): pass



class macroSeries(Series):
    """Manages a single time series indexed by 'date'
    """
    
    @tc.typecheck
    def __init__(self,in_params:dict = {}):
        super(macroSeries,self).__init__(in_params)
    

    @tc.typecheck
    def getSeries(self, seriesList:tc.list_of(str),join_type:str = 'inner')->tc.optional(pd.DataFrame):
        """Returns a dataframe with one or more series for all data available.  Series are merged by date index,
        union of dates.
        Does not check for consistent frequency.
        Input: accepts a list of variable nmemonics.
        """
        try:
            if not seriesList:
                seriesList = [seriesList]
                
            assert(len(seriesList))

            x = self.fredSeries.series.observations(seriesList[0],params=self.params).drop(['realtime_end','realtime_start'], axis=1)
            series = x.set_index('date').rename(columns={'value': seriesList[0]})

            for name in seriesList[1:]:
                x = self.fredSeries.series.observations(name,params=self.params).drop(['realtime_end','realtime_start'],axis=1) 
                y = x.set_index('date').rename(columns={'value': name})
                series = pd.merge(left=series,right=y,how = join_type, left_index = True, right_index = True)
            group = 'macroseries'
            combos = [(i,i) for i in series.columns]
            series.columns = pd.MultiIndex.from_tuples(combos)
            self.obs = series

        except AssertionError as a:
            logger.error(a)
        return self.obs

    @tc.typecheck
    def seriesDetails(self,name:str)->dict:
        """Return a JSON string of all available parameters for the 'series'.
        """
        assert(str)
        info = {}
        info = self.fredDetails.series.details(name,response_type = 'json')
        return json.loads(info)

    @tc.typecheck
    def title(self,name:str)->str:
        j = seriesDetails(name)
        return j['seriess'][0]['title']

    @tc.typecheck
    def frequency(self,name:str)->str:
        j = seriesDetails(name)
        return j['seriess'][0]['frequency']
   
    @tc.typecheck
    def lastUpdated(self,name:str)->datetime.datetime:
        j = seriesDetails(name)
        return j['seriess'][0]['last_updated']
    
    @tc.typecheck
    def recessionRaw(self,series:str = 'USRECQP')->pd.DataFrame:
        fred_series = self.fredSeries.series.observations(series,params=self.params)
        fred_series.drop(['realtime_end','realtime_start'],axis=1,inplace=True)
        fred_series.rename(columns={'value': series},inplace = True)
        return fred_series
    
    @tc.typecheck
    def recessionSeries(self,series:str = 'USRECQP', in_params:dict = {})->tc.optional(pd.DataFrame):
        """Returns the FRED recession indicator (0s or 1s) which indicates the recssion periods from peak to trough acorrding to
        data provider.  The default series is the NBER quarterly series.
        Also converts the FRED format for recession 0-1 indicators to a "beginning_date" and "end_date" format.
        Assumes input is a date index and an indicator column (FRED format).
        """
        recession_series = pd.DataFrame(columns=['beginning_date','ending_date'],dtype='datetime64[ns]')
        try:
            fred_series = self.recessionRaw(series)
            if fred_series.loc[0,series] == 1:
                beginning_date = fred_series.loc[0,'date']
                n = 1
            else:
                n = 0
            for i in range(1,len(fred_series)):
                if fred_series.loc[i,series] == 1:             #in recession
                    if fred_series.loc[i-1,series] == 0:       #new recession
                        b = fred_series.loc[i,'date']
                        beginning_date = b #datetime.date.strptime(b,'%Y-%m-%d')
                        n=n+1
                elif fred_series.loc[i,series] == 0:           #no recession, or end of recession
                    if fred_series.loc[i-1,series] == 1:       #new recession
                        e = fred_series.loc[i-1,'date']
                        ending_date = e #datetime.date.strptime(e,'%Y-%m-%d')

                        begin_end = {'beginning_date': beginning_date, 'ending_date': ending_date}
                        recession_series = recession_series.append(begin_end,ignore_index=True)
                        logger.debug('i-index:{0:5d}, n-index:{1:5d} begin-date:{2:{dfmt}}, end-date:{3:{dfmt}}'
                                      .format(i,n,beginning_date, ending_date, dfmt='%Y-%m-%d'))
                else:
                    raise Exception("Invalid data from FRED.")
        except Exception as e:
            print(e)
            return None
        return recession_series


class macroReleases(Series):
    """
        Returns the releases for 
    """
    
    @tc.typecheck
    def __init__(self,in_params:dict = {}):
        super(macroReleases,self).__init__(in_params)
    
    @tc.typecheck
    def getSeries(self, seriesList:tc.list_of(str),join_type:str = 'inner')->tc.optional(pd.DataFrame):
        try:
            if not seriesList:
                seriesList = [seriesList]
                
            assert(len(seriesList))

            x = self.fredSeries.series.observations(seriesList[0],params=self.params) #.drop(['realtime_end','realtime_start'], axis=1)
            #series = x.set_index('date').rename(columns={'value': seriesList[0]})

            #for name in seriesList[1:]:
            #    x = self.fredSeries.series.observations(name,params=self.params).drop(['realtime_end','realtime_start'],axis=1) 
            #    y = x.set_index('date').rename(columns={'value': name})
            #    series = pd.merge(left=series,right=y,how = join_type, left_index = True, right_index = True)
            #group = 'macroseries'
            #combos = [(i,i) for i in series.columns]
            #series.columns = pd.MultiIndex.from_tuples(combos)
            self.obs = series

        except AssertionError as a:
            logger.error(a)
        return self.obs