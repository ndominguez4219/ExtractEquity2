
# Create, Read, Update, and Delete actions for MondoDB
# using the pymodm wrapper for python


import sys
import logging
import logging.handlers


import typing as tg
import typecheck as tc




import pprint
import datetime
from dateutil.parser import parse

from abc import ABCMeta, abstractmethod, abstractproperty
from collections import deque

import pandas
import pymongo
import pymodm
import pickle

import context


MAX_BYTES = 2000
MAXVERSIONS = 5

# add the handler to the root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#-----------------------------------------------------------------------------------------------------------
#                                                 Data Models
#-----------------------------------------------------------------------------------------------------------


def isVersioneOk(integer:int)->bool:
    if not (0 <= integer <= MAXVERSIONS):
        logger.error("Bad version number: {}".format(integer))
        raise ValidationError("Invalid version number when validating.")
    return True



class TimeSeries(pymodm.EmbeddedMongoModel):
    timeseries = pymodm.fields.BinaryField()
    class Meta:
        final = True

class Asset(pymodm.MongoModel):
    ticker = pymodm.fields.CharField(primary_key=True,required=True)  #add validator later if necessary
    source = pymodm.fields.CharField(required=True)
    version = pymodm.fields.IntegerField(min_value=0,max_value=MAXVERSIONS, validators=[isVersioneOk])
    updated = pymodm.fields.DateTimeField(required = True)
    series = pymodm.fields.EmbeddedDocumentField(TimeSeries)
    class Meta:
        # Read from secondaries.
        read_preference = pymongo.read_preferences.ReadPreference.PRIMARY
        write_preference = pymongo.write_concern.WriteConcern.acknowledged
        final = True

    def save(self) -> object:
        try:
            pk = super(Asset,self).save()
            if pk is None:
                raise Exception('Returned None from attempted save')
            logger.debug('Saved asset [{0}:{1}] from update {2:{dfmt} {tfmt}} at {3:{dfmt} {tfmt}}'
                                .format(pk.ticker,
                                pk.source,
                                pk.updated,
                                datetime.datetime.now(),
                                dfmt='%Y-%m-%d',
                                tfmt='%H:%M:%S,%f'))
        except Exception as e:
            print(e)
        return pk

    def remove(self)->object:
        try:
            super(Asset,self).delete()
            logger.debug('Removed asset [{0}] of source: [{1}]'.format(self.ticker, self.source))
        except Exception as e:
            logger.critical(e)

class Fundamentals(pymodm.MongoModel):
    ticker = pymodm.fields.ReferenceField(Asset)
    metadata = pymodm.fields.BinaryField()
    class Meta:
        final = True





    
    

#---------------------------------------------------------------------------------------------------

class Series(object):
    """Base class for all timeseries objects
    """
    # creating connectioons for communicating with Mongo DB
    host:str = 'localhost'
    port:int = 27017
    collection:str = '_default'
    database:str = 'timeseries'
    collection = 0
    db = 0
    validator = None

    def __init__(self, collection='series', database='testODM', validator=None, validationlevel='strict'):
        super().__init__()
        self.database = database
        self.collection = collection
        self.validator = None
        self.validationlevel = validationlevel
        self.db = None
        self.dbColl = None

        try:
            client = pymongo.MongoClient(host = self.host, port=self.port)
            dbnames = client.database_names()
            if self.database in dbnames:
                pymodm.connect("mongodb://localhost:27017/"+self.database)
                logger.debug("Requested database {0} exists".format(self.database))
            else:
               raise Exception(self.database)
        except Exception as e:
            logger.critical("Connection error, requested database [{0}] does not exist".format(e))
            exit(e)

        try:
            self.db = pymongo.database.Database(client=client,name=self.database,write_concern=None,read_concern=None)
            collectionNames = self.db.collection_names()
            if self.collection in collectionNames:
                self.dbColl = pymongo.collection.Collection(self.db,name=self.collection,create=False)
                logger.info('Collection [{}] found'.format(self.dbColl.name))
            else:
                self.dbColl = pymongo.collection.Collection(self.db,name=self.collection,create=True)
                logger.info('Collection [{}] created'.format(self.dbColl.name))
            
            info = self.db.command('collstats',self.collection)
            logger.info('database.collection [{ns:}] with document count {count:6d} and storage size {storageSize:12d} bytes'.format(**info))
            logger.info('{nindexes:3d} indicies have been created'.format(**info))

        except pymongo.errors.ConnectionFailure as e:
           logger.critical('Could not connect to database [{}]'.format(self.database))
           print ( e )

    def install(self,data:pandas.core.frame.DataFrame, fundamental:object=None, source:str = 'Bloomberg'):
        """ 'installs' a new dataseries.
            'data' is the input dataframe which is multilevel pandas dataframe.  The uppermost index level[0] is considered the 'asset'
             which is a general nmemonic.
        """
        try:
            assert(isinstance(data,pandas.core.frame.DataFrame))
            tickers = list(data.columns.levels[0])
            nassets = len(tickers)

            for i, ticker in enumerate(tickers):
                q = {'_id': ticker, 'source' : source}              #all versions
                series = pymodm.queryset.QuerySet(model=Asset,query=q)
                logger.debug('Documents returned by query: {:>3}, remove existing document(s) before install'.format(series.count()))
                if series.count() > 0:
                    for s in series.raw(q):
                        s.remove()
                
                result = data.xs(ticker, axis=1, level=0, drop_level=False)   #get each series
                model = Asset(ticker = ticker,
                            source = source,
                            version = 0,
                            updated = datetime.datetime.utcnow(),
                            series = TimeSeries(pickle.dumps(result, protocol = pickle.DEFAULT_PROTOCOL)) 
                            ).save()
            logger.debug('{:4d} assets installed in collection {}'.format(nassets,self.dbColl.name))
            return nassets
            
        except AssertionError as a:
            logger.critical('critical error: {}'.format(a))

        except Exception as e:
            logger.error("There was a problem with document: {}".format(e))

        return None

            
    def read(self,assets:tg.List[str]=[], source:str = 'Bloomberg',
                    version:int = 0
                    )->tg.Tuple[tg.Optional[pandas.core.frame.DataFrame],tg.List[str]]:
        """ Reads the data for the 'asset' in its entirety.
            Does no validation checking for 'asset' or 'source'
        """
        if not isinstance(assets,list):
            assets = [assets]

        assert(assets)
        nassets = len(assets)

        result = pandas.DataFrame()
        notFoundList = []

        for _, ticker in enumerate(assets):
            try:         
                q = {'_id': ticker, 'source' : source, 'version': version}
                asset = pymodm.queryset.QuerySet(model=Asset).get(q)
                ts = pickle.loads(asset.series.timeseries)
                _ , fields = zip(*list(ts.columns))
                fields = list(fields)
                logger.info('read {} records from asset {} with fields {}, version {}'.format(len(ts.index),ticker,fields,version))
                result = pandas.concat([result, ts], axis=1)

            except Asset.DoesNotExist as e:
                logger.error('In [{}], asset [{_id}] source [{source}] with verison [{version}] not found by query'.
                             format(self.dbColl.name,**q))
                notFoundList.append(ticker)
                continue
            except Asset.MultipleObjectsReturned as many:
                logger.error(many)
                return None, None
            except AssertionError as a:
                logger.error(a)
                return None, None
            except Exception as e:
                logger.debug('found exception: {}',format(e))
                return None, assets
        return result, notFoundList

    def slice(self,data:pandas.DataFrame,
                            begindate:datetime.date,
                            enddate:datetime.date,
                            fields:tg.Union[str,tg.List[str]] = 'all',
                            params:dict={},
                            source:str = 'Bloomberg',
                            version:int = 0
                           )->tg.Union[tg.Optional[pandas.core.frame.DataFrame],tg.List[str]]:
        """ Creates a slice by date across the list of all assets in dataframe for selected fields
        """
        assert(not data.empty)

        if not isinstance(fields,list):
            fields = [fields]

        errorList = []
        try:
            assetList, fieldList = zip(*list(data.columns))
            assetList = [i for i in set(assetList)]
            fieldList = [i for i in set(fieldList)]

            if 'all' in fields:
                fields = fieldList

            logger.debug('field request for slice is [{}] field list in dataframe is [{}]'.format(fields,fieldList))

            for f in fields:   
                if not f in fieldList:
                    errorList.append(f)
            if errorList:
                raise Exception('the field(s} {} are not the dataframe fields {}'.format(errorList,fieldList))

            if enddate < begindate:
                temp = enddate
                enddate = begindate
                begindate = temp

            beg:str = begindate.strftime('%Y-%m-%d')
            end:str = enddate.strftime('%Y-%m-%d')

            #begin slicing
            
            if len(fields) == 1:
                slice = data.xs(fields[0],axis=1, level=numLevels-1,drop_level=False).loc[beg:end]
            else:
                slice = pandas.DataFrame()
                for _, a in enumerate(assetList):
                    for _, f in enumerate(fields):
                        s = data.xs((a,f), axis=1, level=[0,1],drop_level=False).loc[beg:end]
                        slice = pandas.concat([slice, s], axis=1)

            return slice, []

        except Exception as e:
            logger.error(e)
            return None, errorList


    def update(self,data:pandas.DataFrame,
                            source:str = 'Bloomberg',
                            version:int = 0,
                            overwrite:bool = False
                            )->tg.Optional[tg.List[str]]:
        """ Updates the assets in 'data' by date across the list of all assets; if the asset does
            not exist in the database, it is installed.
            If 'overwrite = True', the new data is given precedence; if 'False' the existing data has
            precedence.

        """
        try:
            assert(not data.empty)

            assetList, fieldList = zip(*list(data.columns))
            assetList = [i for i in set(assetList)]
            fieldList = [i for i in set(fieldList)]

            logger.debug('asset list is {} and field list is {}'.format(assetList,fieldList))
            updated = []

            for ticker in assetList:
                update = data.xs(ticker,axis=1, level=0,drop_level=False)
                if logger.isEnabledFor(logging.DEBUG):
                    pandas.set_option('max_rows', 10)
                    logger.debug('update data is: [{}] \n {} \n\n'.format(ticker,update))
                    pandas.reset_option('max_rows')
                try:
                    q = {'_id': ticker, 'source' : source, 'version': version}
                    asset = pymodm.queryset.QuerySet(model=Asset).get(q)
                except Asset.DoesNotExist as notExist:
                    logger.debug('[{}:{}] not found in collection {}'.format(ticker,source,self.dbColl.name))
                    model = Asset(ticker = ticker,
                                source = source,
                                version = 0,
                                updated = datetime.datetime.utcnow(),
                                series = TimeSeries(pickle.dumps(update, protocol = pickle.DEFAULT_PROTOCOL)) 
                                ).save()
                    updated.append(model.ticker)
                    continue
                except Asset.MultipleObjectsReturned as many:
                    logger.error(many)
                    return None
                
                logger.info('[{}:{}] found in collection {}'.format(asset.ticker,asset.source,self.dbColl.name))
                ts = pickle.loads(asset.series.timeseries)
                if overwrite == False:
                    result = ts.combine_first(update)
                else:
                    result = update.combine_first(ts)

                model = Asset(ticker = asset.ticker,
                            source = asset.source,
                            version = 0,
                            updated = datetime.datetime.utcnow(),
                            series = TimeSeries(pickle.dumps(result, protocol = pickle.DEFAULT_PROTOCOL)) 
                            ).save()
                updated.append(model.ticker)
            logger.info('[{}] have been updated in collection [{}]'.format(model.ticker,self.dbColl.name))
            return updated

        except AssertionError as a:
            logger.error(a)
        except Exception as e:
            logger.error(e)
   
    def installMacro(self,data:pandas.DataFrame,
                            source:str = 'StLouisFRB',
                            version:int = 0,
                            overwrite:bool = False
                            )->tg.Optional[tg.List[str]]:

        return self.update(data,source,version,overwrite)

    def readMacro(self,assets:tg.List[str]=[],
                    source:str = 'StLouisFRB',
                    version:int = 0
                    )->tg.Tuple[tg.Optional[pandas.core.frame.DataFrame],tg.List[str]]:

        readResult, notFoundResult =  self.read(assets,source,version)
        return readResult, notFoundResult

    def recent_data(self,query:dict = {'source': 'Bloomberg','version': 0})->tg.Optional[pandas.DataFrame]:
        """retrieves the date of the last data in the database relative to yesterday's close
        query = {} means that all securities will be tested.
        returns a dataframe with the columns '_id', 'source', 'last data date'.
        Assumes query = {'source': Bloomberg, 'version': 0} which return all '_id' for 'Bloomberg' and version 0.
        """
        try:
            if '_id' in query:
                logger.info('query is {p[_id]} with source {p[source]} and version {p[version]}'.format(p = query))
            else:
                logger.info('query is for ALL with source {p[source]} and version {p[version]}'.format(p = query))
            asset = pymodm.queryset.QuerySet(model=Asset).raw(query)
            
            if asset:
                logger.info('{:5d} assets matched query {}'.format(asset.count(), asset.raw_query))
            else:
                raise Exception('no response to query {}'.format(query))

            for _ , ticker in enumerate(asset):
                ts = pickle.loads(ticker.series.timeseries)
                recent_date = ts.index.max()

        except Asset.DoesNotExist as notExist:
            logger.info('no assets found from query in collection {}'.format(self.dbColl.name))
            return None
        except Asset.MultipleObjectsReturned as many:
            logger.info('{:5d} identical ids returned from query'.format(asset.count))
            
            return None
        except Exception as e:
            logger.error(e)
#---------------------------------------------------------------------------------------------------------------

