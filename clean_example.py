#!/usr/bin/env python
from import_ais_data import clean_ais
from modules.operating_system import OperatingSystem

# Variables
os = OperatingSystem()
FOLDERPORTS = "Data"
PORTFILENAME = "Gatehouse_locode.csv"
FILE_NAME_POLYGON = "Oestersoe_polygon.csv"
PICKLE_FOLDER = os.path('Pickle_data','Oestersoe')
PICKLE_INTERPOLATED_FILE = "pickle_interpolated_10m.pkl"
PICKLE_ROUTE_FILE = "pickle_routes_10m.pkl"
PICKLE_WAYPOINTS_FILE = "pickle_waypoints.pkl" 
FOLDERAIS = "AIS"
GEOAREA = "Oestersoe_dtu_casper"
SHIPTYPE = "cargo"
FILES = -1 #-1 means all files


# Setup class
ais_class = clean_ais(verbose = True)

# Import AIS data from csv files
ais_class.import_ais(
    folder_name = FOLDERAIS,
    geoarea = GEOAREA,
    shiptype = SHIPTYPE,
    file_amount = FILES
)

# Import the port polygons
ais_class.import_ports(FOLDERPORTS, PORTFILENAME)

# Import area of interrest polygon
ais_class.import_polygon(GEOAREA, FOLDERAIS, FILE_NAME_POLYGON)

# Create trips
ais_class.create_routes(speed_limit = 3)

# Remove tracks outside polygon
ais_class.remove_routes_outside_polygon()

# Interpolate routes with an frequency of 10 minutes:
ais_class.interpolate_routes(interval_s = 10*60)

# Cleaning ais data
ais_class.clean_data(threshold = 10, interval_s = 24*60*60, speed = 0.5)

# Create waypoints for each trip
ais_class.create_waypoints(waypoint_amount = 100)

# Export data as pickle files
ais_class.save_routes(PICKLE_FOLDER, PICKLE_ROUTE_FILE)
ais_class.save_interpolated(PICKLE_FOLDER, PICKLE_INTERPOLATED_FILE)
ais_class.save_waypoints(PICKLE_FOLDER, PICKLE_WAYPOINTS_FILE)

# Import data from pickle files
ais_class_v2 = clean_ais(verbose = True)
ais_class_v2.import_routes(PICKLE_FOLDER, PICKLE_ROUTE_FILE)
ais_class_v2.import_interpolated(PICKLE_FOLDER, PICKLE_INTERPOLATED_FILE)
ais_class_v2.import_waypoints(PICKLE_FOLDER, PICKLE_WAYPOINTS_FILE)

# Access data
ais_class.routes
ais_class.interpolated_routes
ais_class.waypoints







