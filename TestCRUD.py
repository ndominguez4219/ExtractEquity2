"""Test CRUD and ODM layers for extracting Bloomberg Data
"""
import sys
import datastore as store
import sampling as sp
import datetime
import pandas as pd
import logging, logging.handlers
import context
import macro

# add the handler to the root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
pd.set_option('display.width',160)

def sampling_test():
    #sample = sp.Sample()
    #groupList = [context.reit,context.common]
    #result = sample.randomSample(securityType=groupList,byKey = context.industry_group,fraction = .1)

    # test data
    #testresult = pd.DataFrame({'month': [1, 4, 7, 10],'year': [2012, 2014, 2013, 2014],'sale':[55, 40, 84, 31]})
    #save_filename = context.DEFAULT_DATA_PATH+filename
    #cleanSample, errorsSample = sp.checkTickers(data=result)
    #saveOK = sp.save(data=testresult,full_filename = save_filename)
    #readOk = sp.read(full_filename = save_filename)

    

    return None

def test_get_series():
    begin_date = '20180110'
    end_date = '20180115'
    tickerList = ['ibm us    equity','wmt us equity','   qqq us    equity']
    Bts = sp.BSeries(ticker = tickerList, begindate=begin_date,enddate=end_date)
    ts = Bts.getseries()
    logger.debug('data from sampling.get_series() \n {} '.format(ts))
    result = sp.save(ts , context.working_file)
    ts = sp.read(context.working_file)
    logger.debug('data after sampling.save(series) and sampling.read(series) {} \n'.format(ts))
    return ts

def test_install_into_database():
    logger.debug('calling test_install_into_database()')
    ts = sp.read(context.working_file)
    db = store.Series(collection='asset')
    installed = db.install(data=ts)
    logger.debug('Exiting test_install_into_database()')
    return installed

def test_read_from_db():
    logger.debug('Calling test_read_from_db()')
    db = store.Series(collection='asset')
    ticker = 'wmt us equity'
    result, notReadList = db.read(ticker)
    logger.debug('Result of single asset string request {}, output is \n {} \n\n {} not read'.format(ticker,result,notReadList))
    tickerList = ['wmt us equity', 'ibm us equity', 'qqq us equity']
    result, notReadList = db.read(tickerList)
    logger.debug('Result of list request {}, output is \n {} \n\n {} not read'.format(ticker,result,notReadList))
    errorList = ['wmt', 'wmt us equity','ibm']
    result, notReadList = db.read(errorList)
    logger.debug('Result of error list request {}, output is \n {} \n\n {} not read'.format(ticker,result,notReadList))
    logger.debug('Exiting test_read_from_db())')


def test_beautify():
    logger.debug('Calling test_beautify()')
    strSingle = '   wmt      us     eQuity  '
    logger.debug('Result of single string: {} is {}'.format(strSingle,sp.beautify(strSingle)))
    tickerList = ['   IBM us     equity   ']
    logger.debug('Result of single element list: {} is {}'.format(tickerList,sp.beautify(tickerList)))
    longList = ['   IBM us     equity   ', 'wmT us       Equity', 'QQQ us equity'   ]
    logger.debug('Result of multiple element list: {} is {}'.format(tickerList,sp.beautify(longList)))
    error1:int = 999
    logger.debug('Result of single error: {} is {}'.format(error1,sp.beautify(error1)))
    errorList = ['IBM us Equity   ',-999]
    logger.debug('Result of error list: {} is {}'.format(errorList,sp.beautify(errorList)))
    logger.debug('Exiting test_beautify())')

def test_slice_series():
    logger.debug('Calling test_slice_series()')
    db = store.Series(collection='asset')
    assetList = ['wmt us equity', 'ibm us equity', 'qqq us equity']
    data,errorList = db.read(assetList)
    begindate = datetime.date(2018,1,10)
    enddate = datetime.date(2018,1,12) 
    limits = {'maxnumber' : 10}
    fields = 'px_high'
    slice, errorList = db.slice(data,fields=fields,begindate=begindate, enddate=enddate, params=limits)
    logger.debug('Result of single request field {}, begin [{}], end [{}] and slice \n {} \n\n error list {}'.
                 format(fields,begindate,enddate,slice,errorList))
    fields = ['px_high', 'px_last']
    slice, errorList = db.slice(data,fields=fields,begindate=begindate, enddate=enddate, params=limits)
    logger.debug('Result of multiple request field {} begin [{}], end [{}] and slice \n {} \n\n error list {}'.
                 format(fields,begindate,enddate,slice,errorList))
    logger.debug('Exiting test_slice_series()')


def test_update_database():
    logger.debug('Calling test_update_database()')
    db = store.Series(collection='asset')

    update_data = {'px_open': [-1.0,-1.0,-1.0], 'px_last': [-9.0,-9.0,-9.0]}
    beg = datetime.datetime(2018,1,11)
    end = datetime.datetime(2018,1,13)
    fields = list(update_data.keys())
    ticker = 'xyz us equity'
    cols = [(ticker,f) for f in fields]
    up = pd.DataFrame(data=update_data, index=pd.date_range(beg,end))
    up.columns = pd.MultiIndex.from_tuples(cols)
    logger.debug('Hypothetical update dataframe \n {} \n\n'.format(up))
    limits = {}

    updated = db.update(data = up)
    logger.debug('list of assets updated is {}'.format(updated))
    for name in updated:
        data_read, _ = db.read(name)
        logger.debug('updated data for [{}] is \n {} \n\n'.format(name,data_read))
    logger.debug('Exiting test_update_database()')

def test_udpate_db_for_newest_data():
    logger.debug('Calling test_udpate_db_for_newest_data()')
    # get all securities in database
    db = store.Series(collection='asset')
    result = store.recent_data()
    logger.debug('Exiting test_udpate_db_for_newest_data()')


def test_read_macro_from_source():
    logger.debug('Calling test_read_macro_from_source()')
    ticker = ['T5YIFR']
    pullResult = macro.macroSeries().getSeries(ticker)
    if logger.isEnabledFor(logging.DEBUG):
        pd.set_option('max_rows', 10)
        logger.debug('{}'.format(pullResult.info()))
        logger.debug('series read {} \n {}'.format(ticker,pullResult))
        pd.reset_option('max_rows')

    tickers = ['T5YIFR','T10Y2Y']
    p = {'observation_start': '2018-01-01'}
    pullMultiple = macro.macroSeries(p).getSeries(tickers)
    if logger.isEnabledFor(logging.DEBUG):
        pd.set_option('max_rows', 10)
        logger.debug('{}'.format(pullMultiple.info()))
        logger.debug('series read {} \n {}'.format(ticker,pullMultiple))
        pd.reset_option('max_rows')

    logger.debug('Exiting test_read_macro_from_source()')


def test_install_macro_series():
    logger.debug('Calling test_install_macro_series()')
    fields = ['T5YIFR','T10Y2Y']
    pullMultiple = macro.macroSeries({'observation_start': '2018-01-01'}).getSeries(fields) # read macro series
    if logger.isEnabledFor(logging.DEBUG):
        pd.set_option('max_rows', 10)
        logger.debug('{}'.format(pullMultiple.info()))
        logger.debug('series read {} \n {}'.format(fields,pullMultiple))
        pd.reset_option('max_rows')
    db = store.Series(collection='asset')
    db_update = db.installMacro(data = pullMultiple)
    logger.debug('Exiting test_install_macro_series()')

def test_read_macro_series():
    logger.debug('Calling test_read_macro_series()')
    assets = ['T5YIFR','T10Y2Y']
    db = store.Series(collection='asset')
    readResult, unread = db.readMacro(assets)
    if logger.isEnabledFor(logging.DEBUG):
        pd.set_option('max_rows', 10)
        logger.debug('{}'.format(readResult.info()))
        logger.debug('series read {} \n {}'.format(assets,readResult))
        logger.debug('series not read {}'.format(unread))
        pd.reset_option('max_rows')
    logger.debug('Exiting test_read_macro_series()')

def test_update_dates():
    logger.debug('calling test_update_dates()')
    series = ['T5YIFRM','T10Y2YM']
    params = {}
    mc = macro.macroReleases({'observation_start': '2018-01-01'}).getSeries(series)

    logger.debug('exiting test_update_dates()')




if __name__ == '__main__':


 #  sampling_test()

    #data = test_get_series()

   
    #installed = test_install_into_database()
    #resultList = test_beautify()
    #resultdf, notFoundlist = test_read_from_db()

  # test_slice_series()
   # test_update_database()
   # test_udpate_db_for_newest_data()
   # test_seriesToToday()
   # test_read_macro_from_source()
   # test_install_macro_series()
   # test_read_macro_series()
    test_update_dates()
pass