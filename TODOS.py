#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 15:09:44 2021

@author: ghiggi
"""

### TODO LOGGING
# import logging
## LOG.info("what you want")
# LOG = logging.getLogger(__name__)
# OUT_HDLR = logging.StreamHandler(sys.stdout)
# OUT_HDLR.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
# OUT_HDLR.setLevel(logging.INFO)
# LOG.addHandler(OUT_HDLR)
# LOG.setLevel(logging.DEBUG)    

# how to recover corrupted files? 
# - Update libssl-dev ro the most recent version !

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