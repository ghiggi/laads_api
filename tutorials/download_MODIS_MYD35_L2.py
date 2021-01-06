#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 21:17:16 2020

@author: ghiggi
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 18:14:02 2020

@author: ghiggi
"""
import os 
os.chdir('/home/ghiggi/Packages/laads_api')          # CHANGE HERE  !!!
import datetime 
from laads_api.io import download 
from laads_api.io import run
from laads_api.io import get_products

# from laads_api.io import find_server_filepaths 
# from laads_api.io import find_disk_filepaths

#-----------------------------------------------------------------------------.
### Define query 

# Define base directory where to save data
base_DIR = "/home/ghiggi/Data"               # CHANGE HERE  !!!

# Define APP_KEY to access LAADS DAAC servers
APP_KEY = '19412E7E-0BF1-11EB-9E3A-A1B02ADBF251'

# Select satellite and instrument 
instrument = "MODIS" 
satellite = "Aqua"

# Look at the product availables
print(get_products(satellite=satellite, instrument=instrument))

# Define products to retrieve
products = 'MYD35_L2'
collections = '61'

# Define region bounding box to be contained in the granule
bbox = [23, -73.2, 25, -71] # [W S E N]   # [xmin ymin xmax ymax]
##-----------------------------------------------------------------------------.


days = [11, 12, 13, 21, 22]
for day in days:
    start_time = datetime.datetime(2019,12, day,8,0,0)
    end_time = datetime.datetime(2019,12, day,20,0, 0)
    #-------------------------------------------------------------------------.
    ### Download data 
    l_cmds_err = download(base_DIR = base_DIR,
                          APP_KEY = APP_KEY, 
                          instrument = instrument,
                          satellite = satellite,
                          products = products,
                          start_time = start_time, 
                          end_time = end_time,
                          bbox = bbox, 
                          collections = collections,
			              force_download = False, 
                          progress_bar = False,
                          verbose = True, 
                          transfer_tool = "wget", # "curl",
                          n_threads = 1)
    #-------------------------------------------------------------------------.
    # Try to rerun commands that had errors
    run(l_cmds_err, n_threads = 10)


days = [1, 2]
for day in days:
    start_time = datetime.datetime(2020,1,day,8,0,0)
    end_time = datetime.datetime(2020,1,day,20,0, 0)
    #-------------------------------------------------------------------------.
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
                          progress_bar = False,
                          verbose = True, 
                          n_threads = 1)
    #-------------------------------------------------------------------------.
    # Try to rerun commands that had errors
    run(l_cmds_err, n_threads = 10)


 
