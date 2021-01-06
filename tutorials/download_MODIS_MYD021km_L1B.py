#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 18:14:02 2020

@author: ghiggi
"""
# spatial --> cartopy 

import os 
os.chdir('/home/ghiggi/Packages/laads_api')          # CHANGE HERE  !!!
import datetime 
from laads_api.io import download 
from laads_api.io import run
#-----------------------------------------------------------------------------.
### Define query 

# Define base directory where to save data
base_DIR = "/home/ghiggi/Data"                       # CHANGE HERE  !!!

start_time = datetime.datetime.strptime("2020-08-09 15:00:00", '%Y-%m-%d %H:%M:%S')
end_time = datetime.datetime.strptime("2020-08-09 16:00:00", '%Y-%m-%d %H:%M:%S')
start_time = datetime.datetime(2020,1,1,0,0,0)
end_time = datetime.datetime(2020,1,2,0,0, 0)

# Have a look at satellites, instruments and products available 
instrument = "MODIS" 
satellite = "Aqua"

# Define products to retrieve
products = ['MYD021KM', 'MYD03']
products = 'MYD03'
collections = '61'

# Define bounding box
bbox = [21, -73.2, 28, -71] # [W S E N]   # [xmin ymin xmax ymax]

# Define APP_KEY to access LAADS DAAC servers
APP_KEY = '19412E7E-0BF1-11EB-9E3A-A1B02ADBF251'

#----------------------------------------------------------------------------.
### Download data 
# - Update libssl-dev ro the most recent version !
l_cmds_err = download(base_DIR = base_DIR,
                      APP_KEY = APP_KEY, 
                      instrument = instrument,
                      satellite = satellite,
                      products = products,
                      start_time = start_time, 
                      end_time = end_time,
                      bbox = bbox, 
                      collections = collections,
                      transfer_tool = "wget",
                      n_threads = 10)
 
# Try to rerun commands that had errors
run(l_cmds_err, n_threads = 10)