# -- Load libraries for script
import numpy as np
import pandas as pd
import time as time
import datetime as dt
import math

from os import listdir, path, replace
from os import path
from os.path import isfile, join
from rich import print, inspect

from PyProcess.Processing.ob_processing import format_orderbooks
from PyProcess.Processing.pt_processing import format_publictrades

from abc import ABCMeta, abstractmethod

# -- Load other scripts
# from PyProcess.Processing import ob_processing
# from PyProcess.Processing import pt_processing

# -- Script's Functionality References 
# [1] https://docs.python.org/3/library/abc.html

# -- -------------------------------------------------------------------------- ABSTRACT CLASS (TEMPLATE) -- #
# -- ---------------------------------------------------------------------------------------------------- -- #
# -- ---------------------------------------------------------------------------------------------------- -- #

class AbstractExchange(metaclass=ABCMeta):

    def __init__(self, class_id = '', exchange_id:str = '', market_type:str = 'spot', symbol_id:str = '',
                       data:dict = {}, logs:bool = None):

        """ Instantiation of a AbstractExchange Abstract Class """

        self.class_id = class_id
        self.exchange_id = exchange_id
        self.symbol_id = symbol_id
        self.data = data
        self.logs = logs

        self.market_type = market_type

    @abstractmethod
    def get_data(self, class_id:str, ini_ts:str = '', end_ts:str = '', source=object, 
                       category='raw',  category_params:dict = {}, 
                       table_source:str = '', output_format='dict', inplace=False) -> dict:

        """

        Get data is going to re-direct to an API-based controller so it will handle the request to the
        specified source object.
    
        NOTE: A generic code that will apply to all instantiations of this abstract class, any particularity
        in each case, either in the input, processing or output, will be defined in the instantiated
        class method and its respective processing script located in the Processing folder.

        Parameters
        ----------

        class_id:str, (default='')
            A string indicating the class identification for other data processing and visualization
            decisions accross sub-classes.

        ini_ts:str, (default='')
            Initial timestamp, in format

        end_ts:str, (default='')
            Ending timestamp, in format

        source=object, (default=None)
            An instantiation of the Source class, which needs to contain a connection object to a source
            for querying and posting data. 
            'tinybird'
            'bitquery'

        category:str, (default='raw')
            The category of the type of the data to be returned, used in conjunction with category_params.
            'raw'
            'metrics'

        category_params:dict, (default={})
            Used in conjunction with category, is a dictionary with the parameters to be used in order
            to produce the result according to the specified form/calculations of data to be returned. This
            is also dependant of the class_id.

            class_id='OrderBooks' and category='raw': (As is) OrderBooks data
                category_params: {'time-sampled', 'price-aggregated'} a set of 2 options to chose from, 
                for 'time-sampled' means getting OrderBooks "Ordered in time" by taking the closest one to 
                each of the points of a regular time period list of values. For 'price-aggregated' means
                to summ the volumes of two or more levels that are grouped into a "bin".
            
            class_id='OrderBooks' and category='metrics' A set of calculated metrics using OrderBooks data
                and using category_params: ['metric_1', 'metric_2', ... 'metric_id'] which is a list of the 
                metrics that are required to be returned from the call.
            
            class_id='PublicTrades' and category='raw'
                category_params: {'time-sampled', 'volume-aggregated'} a set of two options to chose from,
                for 'time-sampled' means to aggregate the publictrades within a time-based block and produces
                sum of volume, weighted-price, count of trades. For 'volume-aggregated' means to have 
                a volume-based block, producing buckets of volume and thus having volume, weighted-price, 
                count of trades.
            
            class_id='PublicTrades' and category='metrics' a set of calculated metrics using PublicTrades data 
                and using category_params: ['metric_1', 'metric_2', ... 'metric_id'] which is a list of the 
                metrics that are required to be returned from the call.

        table_source:str, (default='tracy')
            An indication of a explicit table source from where to get the data from, this is going
            to be used in a specific way depending on the class_id. 

        output_format:str, (default='dict')
            The format of the result, options are 'dict', 'pd.dataframe' and 'ps.dataframe' with this last
            one as the spark-based dataframe.

        inplace:bool, (default=False)
            To specify if the resulting data is ought to be stored within the class in the data attribute, 
            or, not.

        Returns
        -------

        if inplace==True:
            result = dict, pd.dataframe or ps.dataframe 
        else
            result = {'result': 'Ok'}
        
        """
        
        # Endpoint parameters are specific to the source
        endpoint_params = {'exchange_id': self.exchange_id, 'symbol_id': self.symbol_id,
                           'market_type': self.market_type,
                           'p_start': ini_ts, 'p_end': end_ts, 'p_source': table_source}

        # Call parameters can be either specific to the source or generic
        call_params = {'class_id': class_id, 'category': category, 'category_params': category_params,
                       'output_format': output_format, 'drop_empty': True}

        # Data from source
        response_data = source.get_endpoint(e_params=endpoint_params, c_params=call_params,
                                            verbose='DEBUG', logger=None)
        
        if inplace:
            self.data = response_data
        else:
            return response_data
         
    @abstractmethod
    def post_data(self) -> dict:
        pass

    @abstractmethod
    def read_data(self):
        pass
    
    @abstractmethod
    def write_data(self) -> dict:
        pass          


# -- ---------------------------------------------------------------------------------- ----------------- -- #
# -- ---------------------------------------------------------------------------------- CLASS: ORDERBOOKS -- #
# -- ---------------------------------------------------------------------------------- ----------------- -- #

class OrderBooks(AbstractExchange):
    """

    A subclass from the AbstractExchange class template. The OrderBooks class is used to store, 
    organize, describe, plot and transform Orderbook data.

    """

    def __str__(self):
        class_name = self.__class__.__name__
        message = "\n{}(ini_ts='{}', end_ts='{}', exchange_id='{}', market_type='{}', symbol_id='{}')\n"
        return message.format(class_name, self.ini_ts, self.end_ts,
                              self.exchange_id, self.market_type, self.symbol_id)
    
    def get_data(self, class_id:str = 'OrderBooks', ini_ts:str = '', end_ts:str = '', source=object, 
                       category='raw',  category_params:dict = {}, table_source:str = '', 
                       output_format='dict', inplace=False) -> dict:

        """
        
        'time-sampled': get the closes OrderBook before each point in time given subsampling frequency
            requires 'freq': int in category_params
            returns a single orderbook

        'price-aggregated': aggregate price-volume levels using the specified bin-size
            requires 'bintype' : ['bps', 'count'] and 'binsize' : (according to bintype) in category_params
            returns a single orderbook (same as input, only with either more or with less levels)
        
        'metrics': Get a list of metrics as specified in a list (these should be already implemented)
            requires 'metrics' : ['metric_0', 'metric_1' ... 'metric_id'] in category_params

        output_format = ['dict', 'array', 'pd.dataframe', 'ps.dataframe']

        """
        
        # Always will be a call either 'raw' data, or, 'metrics' data. The rest of transformations are 
        # conducted inside this class with the aid of other helper functions and scripts.

        # Using the source, call the endpoint and get the results delivered back
        raw_data = super().get_data(class_id, ini_ts, end_ts, source, category, category_params,
                                    table_source, output_format, inplace)

        # print(unformatted_data)
        
        # Format output
        ob_data = format_orderbooks(ob_data=raw_data, processing_params=category_params, verbose=True)
        
        if inplace:
            self.data = ob_data
        else:
            return ob_data
        
    def post_data(self):
        return super().post_data()
    
    def read_data(self):
        return super().read_data()
    
    def write_data(self):
        return super().write_data()

    # Light description of data
    def describe_data():
        pass
    

# -- ----------------------------------------------------------------------------- ---------------------- -- #
# -- ----------------------------------------------------------------------------- -- CLASS: PUBLICTRADES -- #
# -- ----------------------------------------------------------------------------- ---------------------- -- #

class PublicTrades(AbstractExchange):

    """ Python Class to store, organize, describe, plot and transform PublicTrades data. """

    def __str__(self):
        
        """ Internal method to print the class with its current values """

        class_name = self.__class__.__name__
        message = "\n{}(ini_ts='{}', end_ts='{}', exchange_id='{}', symbol_id='{}')\n"
        return message.format(class_name, self.ini_ts, self.end_ts, self.exchange_id, self.symbol_id)
        
    # -- ------------------------------------------------------------------------- ---------------------- -- #

    def get_data(self, class_id:str='PublicTrades', ini_ts:str='', end_ts:str='', source=object, 
                       category='raw',  category_params:dict={}, table_source:str='',
                       output_format='dict', inplace=False) -> dict:
        """
        
        category:

            'raw'
                Do not require params, no transformation just formating.

            'time-sampled'
                requires category_params: 'frequency' (in minutes)
                returns ts, 'traded_volume', 'traded_count', 'price'

            'volume-aggregated' 
                requires category_params: 'bucket_volume', 'bucket_price', 
                returns ['init_ts', 'end_ts'], 'bucket_volume', 'bucket_price', 'bucket_count'
        
        """

        # Using the source, call the endpoint and get the results delivered back
        raw_data = super().get_data(class_id, ini_ts, end_ts, source, category, category_params,
                                    table_source, output_format, inplace)

        # Verify match between output format and current data
        if not isinstance(raw_data, dict) & output_format == 'dict' :
            pass
            
        # Format output data
        pt_data = format_publictrades(pt_data=raw_data, processing_params=category_params, verbose=True)

        return pt_data

    def post_data(self):
        return super().post_data()
    
    def read_data(self):
        return super().read_data()
    
    def write_data(self):
        return super().write_data()

    # Light description of data
    def describe_data():
        pass
