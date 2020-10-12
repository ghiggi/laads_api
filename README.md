# laads_api

Experimental API to retrieve MODIS, VIIRS, OLCI and SLSTR data from LDAAS DAAC NASA archive.

script_download.py shows how to perfom the download. 

Currently only data from VIIRS and MODIS can be downloaded.

laads_api currently relies on modapsclient to interface the LAADS Web Service Classic API.
laads_api is not yet a package, so please change the path in the laads_api/io.py and in the scripts.

Packages required: 
pip install modapsclient
pip install numpy
pip install pandas  