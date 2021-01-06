#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 14:05:47 2020

@author: ghiggi
"""
import os 
os.chdir('/home/ghiggi/Packages/laads_api')

from laads_api.io import find_server_filepaths 
from laads_api.io import find_disk_filepaths
##########################
### Define data query ####
##########################
# Define base directory where to save data
base_DIR = "/home/ghiggi/Data"

# Define start_time and end_time
start_time = datetime.datetime(2020,1,1,0,0,0)
end_time = datetime.datetime(2020,1,2,0,0, 0)

# Have a look at satellites, instruments and products available 
instrument = "MODIS" 
satellite = "Aqua"

# Define products to retrieve
products = ['MYD021KM'] # 'MYD03'
products = ['MYD021KM', 'MYD03']
collection = '61'

# Define bounding box
bbox = [21, -73.2, 28, -71] # [W S E N] 

##----------------------------------------------------------------------------.
### Retrieve and read files via OPeNDAP using xarray  
opendap_filepaths = find_server_filepaths(satellite = satellite,
                                          instrument = instrument, 
                                          products = products, 
                                          start_time = start_time,
                                          end_time = end_time,
                                          bbox = bbox,
                                          connection_type = "opendap")

### Read data directly from servers via opendap 
import xarray
remote_data = xr.open_dataset(opendap_filepaths[0])

##----------------------------------------------------------------------------.
### Retrieve filepaths on disk 
filepaths = find_disk_filepaths(base_DIR = base_DIR,
                                satellite = satellite,
                                instrument = instrument, 
                                products = 'MYD03', 
                                start_time = start_time,
                                end_time = end_time)



