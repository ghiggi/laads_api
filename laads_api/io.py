#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 15:07:38 2020

@author: ghiggi
"""
import os 
os.chdir('/home/ghiggi/laads_api')
import subprocess
import concurrent.futures
import datetime
import modapsclient as m
import numpy as np
import pandas as pd 
from tqdm import tqdm
from itertools import chain
from laads_api.utils.utils_string import str_replace
# from laads_api.utils.utils_string import str_extract
# from laads_api.utils.utils_string import str_subset
# from laads_api.utils.utils_string import str_sub 
# from laads_api.utils.utils_string import str_pad 
# from laads_api.utils.utils_string import str_detect
# from laads_api.utils.utils_string import subset_list_by_boolean

##----------------------------------------------------------------------------.
## Extend subset to hourly/minutes/seconds
## Remove need specify satellite and instrument from find_*, download_*
##----------------------------------------------------------------------------.
## Not available via OpenDAP
# 450 OLCI and SLSTR (Sentinel 3 A B)
# 490 MERSI (Envisat)
# Accept conditions ... 
# https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/450/
# https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/490/

# https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/lws-classic/api.php#searchForFiles
##----------------------------------------------------------------------------. 
def _modaps_sat_acronym(satellite, instrument):
    if ((satellite == "Terra") & (instrument == "MODIS")):
        sat = "AM1M"
    elif ((satellite == "Aqua") & (instrument == "MODIS")):
        sat = 'PM1M'
    elif ((satellite == "Terra_Aqua") & (instrument == "MODIS")):
        sat = 'AMPM'
    elif ((satellite == "SNPP") & (instrument == "VIIRS")):
        sat = 'NPP'
    else:
        raise ValueError("There is a bug")
    return(sat)
##----------------------------------------------------------------------------.      
def get_satellites():
    satellites = ["Terra","Aqua", "Terra_Aqua", "SNPP", "Terra_Aqua"] # JPSS-1 , Envisat, Sentinel-3
    return satellites

def get_instruments(satellite):
    if (satellite in ["Terra", "Aqua", "Terra_Aqua"]):
        return ["MODIS"]
    elif (satellite in ["SNPP"]): # JPSS-1, JPSS-2
        return ["VIIRS"]
    else: 
        # MERIS --> EnviSat
        # OLCI, SLSTR --> Sentinel 3A,B,C
        raise ValueError("BUG. Currently not implemented") 

##----------------------------------------------------------------------------.
def get_products(satellite, instrument):
    modaps_inst = _modaps_sat_acronym(satellite, instrument)
    # Retrieve products 
    proxy = m.ModapsClient()
    products = proxy.listProductsByInstrument(modaps_inst)
    return products

##----------------------------------------------------------------------------.
def get_collections_VIIRS():
    collections = {'5000': 'VIIRS Collection 1 - Level 1, Land (Archive Set 5000)',
                   '5110': 'VIIRS Collection 1 - Level 1, Atmosphere (Archive Set 5110)',
                   '5111': 'VIIRS Collection 1.1 - Level 1, Atmosphere (Archive Set 5111)',
                   '5200': 'VIIRS Collection 2 - Level 1, Atmosphere, Land (Archive Set 5200)'}
    return collections

def get_collections_MODIS():
    collections = {'404': 'MODIS Collection 4 - NACP Vegetation Indexes, Phenology (Archive Set 404)',
                   '55': 'MODIS Collection 5.5 - Terra Selected Land (Archive Set 55)',
                   '6': 'MODIS Collection 6 - Level 1, Atmosphere, Land (Archive Set 6)',
                   '61': 'MODIS Collection 6.1 - Level 1, Atmosphere, Land (Archive Set 61)'}
    return collections 

def get_collections_AVHRR():
    collections = {'464': 'AVHRR Collection 4 - Long Term Data Record Level 1 (Archive Set 464)',
                   '465': 'AVHRR Collection 5 - Long Term Data Record Level 1 (Archive Set 465)'}
    
def get_collections(product): 
    proxy = m.ModapsClient()
    collections = proxy.getCollections(product)
    return collections

def get_collection_most_recent(product):
    proxy = m.ModapsClient()
    collections = proxy.getCollections(product)
    collections = [int(v) for v in collections.keys()]
    most_recent_collection = str(max(collections))
    return most_recent_collection 
 
##----------------------------------------------------------------------------.
def get_connection_types():
    return ["https","opendap"]

def get_LAADS_base_url(connection_type="https"):
    if server_type == 'opendap':
        base_url = "https://ladsweb.modaps.eosdis.nasa.gov/opendap/allData"
    else: # https or http
        base_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData"
    return base_url

def convert_to_OPeNDAP_filepaths(filepaths): 
    filepaths = str_replace(filepaths, "archive", "opendap")    
    return filepaths

def get_LAADS_filepaths(product, 
                        start_time,
                        end_time, 
                        collection,
                        bbox=None, connection_type="https"):
    #-------------------------------------------------------------------------.  
    # Check bbox 
    if (bbox is None): 
        bbox = check_bbox(bbox)
        coordsOrTiles = "global"
    else: 
        coordsOrTiles = "coords"
    #-------------------------------------------------------------------------.   
    # Process start_time and end_time 
    # TODO: enhance modaps to accept also hour and minute 
    # start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    # end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    start_time_str = start_time.strftime('%Y-%m-%d') # %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d') # %H:%M:%S')
    #-------------------------------------------------------------------------.
    # Retrieve fileIDs
    proxy = m.ModapsClient()
    fileIDs = proxy.searchForFiles(products=product, 
                                   startTime=start_time_str, 
                                   endTime=end_time_str, 
                                   west=bbox[0],
                                   south=bbox[1], 
                                   east=bbox[2], 
                                   north=bbox[3], 
                                   coordsOrTiles = coordsOrTiles,
                                   collection=collection)
    #-------------------------------------------------------------------------.
    if ((len(fileIDs) == 1) and (fileIDs[0] == 'No results')):
        return 
    #-------------------------------------------------------------------------.
    # Check file is online for direct download 
    # TODO: Log which fail is not online !  
    fileIDs_formatted = ",".join(fileIDs)
    l_properties = proxy.getFileProperties(fileIDs_formatted)
    idx_online = [properties['online'] == "true" for properties in l_properties]
    fileIDs = list(np.array(fileIDs)[idx_online])
    #-------------------------------------------------------------------------.
    # Retrieve file urls 
    fileIDs_formatted = ",".join(fileIDs)
    filepaths = proxy.getFileUrls(fileIDs_formatted)  
    #-------------------------------------------------------------------------.
    # Change to opendap if specified 
    if (connection_type == "opendap"):
        filepaths = convert_to_OPeNDAP_filepaths(filepaths)   
    #-------------------------------------------------------------------------.
    # Return file paths 
    return(filepaths)

def get_filepaths_timepaths(filepaths):
    timepaths = []
    for filepath in filepaths:
        dirname = os.path.dirname(filepath)
        doy = os.path.basename(dirname)
        year = os.path.basename(os.path.dirname(dirname))
        timepaths.append(os.path.join(year, doy))
    return timepaths

def define_disk_filepaths(filepaths,
                          base_DIR,
                          satellite,
                          instrument,
                          product, 
                          collection):
    """Define disk filepath from server filepath."""
    ##------------------------------------------------------------------------.
    # Retrieve filenames
    filenames = [os.path.basename(filepath) for filepath in filepaths]
    # Retrieve timepath /year/doy
    timepaths = get_filepaths_timepaths(filepaths)
    # Retrieve folder paths <instrument>/.../<product>/<year>/<doy>
    disk_files_dir = get_disk_files_dir(base_DIR = base_DIR,
                                        satellite = satellite,
                                        instrument = instrument,
                                        product = product, 
                                        collection = collection,
                                        timepaths = timepaths)
    # Retrieve disk filepaths 
    disk_filepaths = [os.path.join(d,fnm) for d, fnm in zip(disk_files_dir, filenames)]
    # Create the necessary directories
    dir_uniques = np.unique(disk_files_dir)
    for d in dir_uniques:
        if not os.path.exists(d):
            os.makedirs(d)
    # Return filepaths 
    return(disk_filepaths)

def get_disk_base_path(base_DIR, instrument, satellite, collection):
    # MODIS/Aqua/<Collection>/<products> 
    # OLCI/Sentinel3A/<Collection>/<products>
    base_path = os.path.join(base_DIR, instrument, satellite, collection)
    return base_path

def get_disk_product_dir(base_DIR, instrument, satellite, collection, product):
    # MODIS/Aqua/<Collection>/<products>
    base_path = get_disk_base_path(base_DIR, instrument, satellite, collection)
    folder_path = os.path.join(base_path, product)
    return folder_path  

def get_disk_files_dir(base_DIR, instrument, satellite, collection, product, timepaths):
    # Check timepaths is a list (and not a string)
    if isinstance(timepaths, str):
        timepaths = [timepaths]
    # Retrieve product dir 
    product_dir = get_disk_product_dir(base_DIR, instrument, satellite, collection, product)
    # Retrieve folder dir 
    files_dir = [os.path.join(product_dir, timepath) for timepath in timepaths]
    return files_dir   

def listdir_filepaths(directory):
    filenames = os.listdir(directory)
    filepaths = [os.path.join(directory, filename) for filename in filenames]
    return filepaths

def get_disk_filepaths(base_DIR, instrument, satellite, product, 
                       collection, timepaths):
    folder_paths = get_disk_files_dir(base_DIR, instrument, satellite, collection, product, timepaths)
    filepaths = [listdir_filepaths(folder_path) for folder_path in folder_paths]
    filepaths = list(chain.from_iterable(filepaths))
    return filepaths   



def get_blocks_of_time(start_time, end_time):
    """Split the time interval in blocks of 20 days."""
    # Blocks of 20 days
    l_DatetimeIndex = pd.date_range(start=start_time, end=end_time, freq='20D')
    l_datetime = list(l_DatetimeIndex.to_pydatetime())
    l_datetime.append(end_time)
    l_datetime = list(np.unique(l_datetime))
    start_times, end_times = zip(*[(l_datetime[i], l_datetime[i+1]) for i in range(0, len(l_datetime)-1)])
    start_times = list(start_times)
    end_times = list(end_times)
    return start_times, end_times
                
def get_idx_within_range(s_a, e_a, start, end):
    # Return index of elements included in start-end range
    idx_select1 = np.logical_and(s_a <= start, e_a > start) # first granule
    idx_select2 = np.logical_and(s_a >= start, s_a < end) # all the rest
    idx_select = np.logical_or(idx_select1, idx_select2)
    return(idx_select)

#----------------------------------------------------------------------------.
################
#### Checks ####
################
def not_in(list1, list2):
    # Test wheter list1 element are not in list2
    return [elem not in list2 for elem in list1]

def is_not_empty(x):
    return(not not x)

def is_empty(x):
    return( not x)

def check_bbox(bbox):
    if (bbox is None):
       bbox = [-180, -90, 180, 90] 
    else: 
        if not isinstance(bbox, list):
            raise TypeError('Provide bbox as a [W S E N] list')
        if (len(bbox) != 4):
            raise ValueError('Provide bbox as a [W S E N] list') 
        if (bbox[2] > 180):
            bbox[0] = bbox[0] - 180 
            bbox[2] = bbox[2] - 180 
    return(bbox)   
     
def check_satellite(satellite):
    if not isinstance(satellite, str):
        raise TypeError('Provide satellite name as a string')
    if (satellite not in get_satellites()):
        raise ValueError("Specify a valid satellite name! --> get_satellites()")
        
def check_instrument(instrument, satellite):
    if not isinstance(instrument, str):
        raise TypeError('Provide instrument name as a string')
    if (instrument not in get_instruments(satellite)):
        raise ValueError("Specify a valid instrument! --> get_instrument(satellite)")

def check_products(products, satellite, instrument):
    # If products are not specified, retrieve all 
    if (products is None):
        raise ValueError("Specify at least 1 product") 
    ##------------------------------------------------------------------------. 
    # If product is specified, silently convert to list if string
    if isinstance(products, str):
        products = [products]
    ##------------------------------------------------------------------------. 
    # Check that all specified products are valid 
    if any(not_in(products, get_products(satellite, instrument))):
        raise ValueError('Specify valid products !')     
    ##------------------------------------------------------------------------. 
    return products  

def check_collections(collections, products):
    # If collections not specified, set to the most recent
    if (collections is None): 
        collections = [get_collection_most_recent(product) for product in products]
    # Else check validity of provided
    else: 
        # Check input is str or list
        if isinstance(collections, str):
            collections = [collections]
        if not isinstance(collections, list):
            raise TypeError("Provide 'collections' as a string or list")
        ##---------------------------------------------------------------------.    
        # If collections has length 1 --> Recycle 
        if (len(collections) == 1):
            collections = list(np.repeat(collections, len(products)))
        ##---------------------------------------------------------------------.
        # Check that collections and products have same length
        if (len(collections) != len(products)):
            raise ValueError("'products' and 'collections' must have same size")
        ##---------------------------------------------------------------------.
        # Check validity of collections 
        idx_not_valid = [c not in get_collections(p) for c, p in zip(collections, products)]
        # TODO : Print more detailed error 
        if any(idx_not_valid):
            raise ValueError("Specify valid collection for each product")
    return(collections)

def check_connection_type(connection_type):
    if not isinstance(connection_type, str):
        raise TypeError('Provide connection_type as a string')
    if connection_type not in get_connection_types():
        raise ValueError("Specify valid 'connection_type'. 'https' or 'opendap'")
    
def check_Datetime(Datetime):
    if not isinstance(Datetime, (datetime.datetime, datetime.date)):
        raise ValueError("Please provide a datetime object")
    if not isinstance(Datetime, datetime.datetime):
        Datetime = datetime.datetime(Datetime.year, Datetime.month, Datetime.day,0,0,0)
    return(Datetime)

def check_start_end_time(start_time, end_time):
    start_time = check_Datetime(start_time)
    end_time = check_Datetime(end_time)
    # Check start_time and end_time are chronological  
    if (start_time > end_time):
        raise ValueError('Provide start_time occuring before of end_time') 
    # Check start_time and end_time are in the past
    if (start_time > datetime.datetime.now()):
        raise ValueError('Provide a start_time occuring in the past') 
    if (end_time > datetime.datetime.now()):
        raise ValueError('Provide a end_time occuring in the past') 
    return (start_time, end_time)

def check_base_DIR(base_DIR):
    if (base_DIR is not None):
        if not os.path.exists(base_DIR):
            raise ValueError("The path", base_DIR, 'does not exist on disk')
        if not os.path.isdir(base_DIR):
            raise ValueError(base_DIR, 'is not a folder path')

def check_transfer_tool(transfer_tool, force_download):
    if not isinstance(transfer_tool, str):
        raise TypeError("Specify 'transfer_tool' as a string")
    if (transfer_tool not in ['curl','wget']):
        raise ValueError("Specify valid 'transfer_tool'. 'curl' or 'wget'")
    if (force_download is True):
        transfer_tool = "curl"
        print("Since force_download is set to True, curl is used as transfer_tool")
    return transfer_tool
#-----------------------------------------------------------------------------.
##############################
### File search functions ####
##############################
def find_server_filepaths(satellite, 
                          instrument,
                          products,
                          start_time, 
                          end_time,
                          bbox = None, 
                          collections = None,
                          base_DIR = None,
                          connection_type = "https",
                          output_tuple = False):
 
    # AWS_connection = "https","opendap","base_DIR" 
    # base_DIR only if data where downloaded 
    ##------------------------------------------------------------------------.
    # Checks 
    check_satellite(satellite)
    check_instrument(instrument, satellite)
    check_connection_type(connection_type)
    check_base_DIR(base_DIR=base_DIR)
    # bbox = check_bbox(bbox)
    products = check_products(products, satellite, instrument)
    collections = check_collections(collections, products)
    start_time, end_time = check_start_end_time(start_time, end_time)
    #-------------------------------------------------------------------------.
    # Checks to output a tuple (server_path, disk_path)
    if (output_tuple is True) and (base_DIR is None):
        raise ValueError("If output_tuple is True, base_DIR must be specified")
    ##------------------------------------------------------------------------.
    # Iterate over block of 20 days to avoid API query limits
    # - max 6000 filepaths returned every time 
    # - 5 min granules over 20 days = 60*24/5*20
    l_start_time, l_end_time = get_blocks_of_time(start_time, end_time)
    #---------------------------------------------------------------------.
    ## Loop over products and timesteps
    # Initialize filepaths 
    all_filepaths = []
    all_disk_filepaths = []
    for product, collection in zip(products, collections): 
        #---------------------------------------------------------------------. 
        for start_time, end_time in zip(l_start_time, l_end_time):  
            #-----------------------------------------------------------------.
            # Retrieve filepaths
            if (base_DIR is None) or (output_tuple is True):
                # Get paths using Web Services 
                filepaths = get_LAADS_filepaths(product = product, 
                                                start_time = start_time,
                                                end_time = end_time,
                                                collection = collection, 
                                                bbox = bbox, 
                                                connection_type=connection_type)
            #-----------------------------------------------------------------.
            # Skip to next folder if no data available
            if (is_empty(filepaths)):
                continue
            #-----------------------------------------------------------------.
            # Define also filepaths on disk (for download)
            if (output_tuple is True):  
                disk_filepaths = define_disk_filepaths(filepaths=filepaths,
                                                       base_DIR=base_DIR,
                                                       satellite=satellite,
                                                       instrument=instrument,
                                                       product=product, 
                                                       collection=collection)                                    
                # Attach to existing ones
                all_disk_filepaths.extend(disk_filepaths)
            #-----------------------------------------------------------------.
            # Attach filepaths to existing ones
            all_filepaths.extend(filepaths)
    #-------------------------------------------------------------------------.
    # Return filepaths
    if (output_tuple is True):
        return (all_filepaths, all_disk_filepaths)  
    else: 
        return all_filepaths

##----------------------------------------------------------------------------.
def find_disk_filepaths(base_DIR,
                        satellite,
                        instrument, 
                        products, 
                        start_time,
                        end_time,
                        collections=None,
                        bbox = None):
    ## TODO : extend to HH/MM/SS 
    ##------------------------------------------------------------------------.
    # Checks 
    check_satellite(satellite)
    check_instrument(instrument, satellite)
    check_base_DIR(base_DIR=base_DIR)
    # bbox = check_bbox(bbox)
    products = check_products(products, satellite, instrument)
    collections = check_collections(collections, products)
    start_time, end_time = check_start_end_time(start_time, end_time)
    #-------------------------------------------------------------------------.
    # Define timepaths into which to search 
    list_DateTime = pd.date_range(start=start_time, end=end_time, freq='D')
    timepaths = list_DateTime.strftime('%Y/%j').to_list()
    ## Loop over products and timesteps
    # Initialize filepaths 
    all_disk_filepaths = []
    for product, collection in zip(products, collections): 
        #---------------------------------------------------------------------. 
        # Retrieve disk filepaths
        disk_filepaths = get_disk_filepaths(base_DIR = base_DIR,
                                            instrument = instrument,
                                            satellite = satellite,
                                            collection = collection,
                                            product = product, 
                                            timepaths = timepaths) 
        #---------------------------------------------------------------------.
        # Skip to next folder if no data available
        if (is_empty(disk_filepaths)):
            continue
        #---------------------------------------------------------------------.
        # Attach to existing ones
        all_disk_filepaths.extend(disk_filepaths)
    #-------------------------------------------------------------------------.  
    # If bbox is provided, query LAADS for filenames and then intersect with those on disk 
    if (bbox is not None): 
        _, disk_filepaths = find_server_filepaths(base_DIR = base_DIR,
                                                  satellite = satellite,
                                                  instrument = instrument, 
                                                  products = products, 
                                                  start_time = start_time,
                                                  end_time = end_time,
                                                  bbox = bbox,
                                                  connection_type = "https",
                                                  output_tuple=True)
        all_disk_filepaths = all_disk_filepaths[np.isin(all_disk_filepaths, disk_filepaths)]
    #-------------------------------------------------------------------------.
    # Return filepaths
    return all_disk_filepaths

#-----------------------------------------------------------------------------.   
##########################
### Download functions ###
##########################
def curl_cmd(server_path, disk_path, APP_KEY):
    """Create curl command to download data."""
    #-------------------------------------------------------------------------.
    # Check disk directory exists (if not, create)
    disk_dir = os.path.dirname(disk_path)
    if not os.path.exists(disk_dir):
        os.makedirs(disk_dir)
    #-------------------------------------------------------------------------.
    ## Define command to run
    # - v : verbose
    # --fail : fail silently on server errors. Allow to deal better with failed attemps
    #           Return error > 0 when the request fails
    # --silent: hides the progress and error
    # --retry 10: retry 10 times 
    # --retry-delay 5: with 5 secs delays 
    # --retry-max-time 60*10: total time before it's considered failed
    # --connect-timeout 20: limits time curl spend trying to connect ot the host to 20 secs
    # --get url: specify the url 
    # -o : write to file instead of stdout 
    
    # cmd = "".join(["curl ",
    #                "-v ",
    #                "-H 'Authorization: Bearer ", APP_KEY,"' ",
    #                server_path, " > ", disk_path])
    cmd = "".join(["curl ",
                   "-v ",
                   "--connect-timeout 20 ",
                   "--retry 100 ", 
                   "--retry-delay 5 ",
                   "--tlsv1.2 ", # TSL/SSL
                   "-H 'Authorization: Bearer ", APP_KEY,"' ",
                   "--get ", server_path, " ",
                   "-o ", disk_path])
    #-------------------------------------------------------------------------.
    return cmd    

def wget_cmd(server_path, disk_path, APP_KEY):
    """Create curl command to download data."""
    #-------------------------------------------------------------------------.
    # Check disk directory exists (if not, create)
    disk_dir = os.path.dirname(disk_path)
    if not os.path.exists(disk_dir):
        os.makedirs(disk_dir)
    #-------------------------------------------------------------------------.
    ## Define command to run
    cmd = "".join(["wget ",
                 "-e robots=off ",  # allow wget to work ignoring robots.txt file 
                 "-np ",           # prevents files from parent directories from being downloaded
                 "-R .html,.tmp ", # comma-separated list of rejected extensions
                 "-nH ",           # don't create host directories
                 "--secure-protocol=TLSv1_2 ", # TLS v1.2
                 "--header 'Authorization: Bearer ", APP_KEY,"' ", # identification
                 "-c ",            # continue from where it left 
                 "--read-timeout=60 ", # if no data arriving for 5 seconds, retry
                 "--tries=0 ",     # retry forever
                 "-O ", disk_path," ",
                 server_path])
    #-------------------------------------------------------------------------.
    return cmd    

def run(commands, n_threads = 10):
    """
    Run bash commands in parallel using multithreading.

    Parameters
    ----------
    commands : list
        list of commands to execute in the terminal.
    n_threads : int, optional
        Number of parallel download. The default is 10.
        LAADS currently accept max 10 parallel download per APP KEY.
        
    Returns
    -------
    List of commands which didn't complete. 

    """
    n_threads = min(n_threads, 10) 
    n_cmds = len(commands)
    with tqdm(total=n_cmds) as pbar: 
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            dict_futures = {executor.submit(subprocess.check_call, cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL): cmd for cmd in commands}
            # List cmds that didn't work 
            l_cmd_error = []
            for future in concurrent.futures.as_completed(dict_futures.keys()):
                pbar.update(1) # Update the progress bar 
                # Collect all commands that caused problems 
                if future.exception() is not None:
                    l_cmd_error.append(dict_futures[future])
    
    return l_cmd_error         


##----------------------------------------------------------------------------.
def download(base_DIR, 
             APP_KEY,
             instrument,
             satellite,
             products,
             start_time,
             end_time,
             bbox = None, 
             collections = None,
             force_download = False,
             n_threads = 10, 
             transfer_tool = "curl"):
    #-------------------------------------------------------------------------.  
    transfer_tool = check_transfer_tool(transfer_tool, force_download=force_download)
    # Retrieve filepaths 
    server_filepaths, disk_filepaths = find_server_filepaths(base_DIR = base_DIR, 
                                                             satellite = satellite,
                                                             instrument = instrument,
                                                             products = products, 
                                                             start_time = start_time,
                                                             end_time = end_time,
                                                             bbox = bbox, 
                                                             collections = collections,
                                                             connection_type = 'https',
                                                             output_tuple = True)
    #-------------------------------------------------------------------------.
    # Decide if to redownload data already present on disk 
    if (force_download is False):
        ## Remove from the filepath list the already existing files on disk 
        idx_not_exist = [not os.path.exists(filepath) for filepath in disk_filepaths]
        disk_filepaths = list(np.array(disk_filepaths)[idx_not_exist]) 
        server_filepaths = list(np.array(server_filepaths)[idx_not_exist]) 
    #-------------------------------------------------------------------------.
    # Retrieve commands
    if (transfer_tool == "curl"):
        list_cmd = [curl_cmd(server_path, disk_path, APP_KEY) for server_path, disk_path in zip(server_filepaths,disk_filepaths)]
    else:    
        list_cmd = [wget_cmd(server_path, disk_path, APP_KEY) for server_path, disk_path in zip(server_filepaths,disk_filepaths)]
    ##-------------------------------------------------------------------------.
    # Run download commands in parallel
    # - Return a list of commands that didn't complete
    bad_cmds = run(list_cmd, n_threads = n_threads)
    return bad_cmds 
       
##----------------------------------------------------------------------------.





    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
 
 
    


  
    
### TODO Parallel download   
# https://stackoverflow.com/questions/14533458/python-threading-multiple-bash-subprocesses    
    
### TODO LOGGING
# import logging
## LOG.info("what you want")
# LOG = logging.getLogger(__name__)
# OUT_HDLR = logging.StreamHandler(sys.stdout)
# OUT_HDLR.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
# OUT_HDLR.setLevel(logging.INFO)
# LOG.addHandler(OUT_HDLR)
# LOG.setLevel(logging.DEBUG)    
    
    
     
 

 