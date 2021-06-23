# Fetching and cleaning AIS data
This project is a small part of a Bachelor project made by Casper Bang-Hansen and Mathias Jensen.
The findings of the report can be found [here](https://drive.google.com/file/d/1B9XpTJCqlNLRMJ-1MbzLPj7wPqQ2w-yt/view?usp=sharing)

In the following a description on how to use the code will be made:

## Download dataset
### Step 1
Open the file "database_details.py" and change the login details for the database.
### Step 2
The first step is finding the desired polygon on the database.
### Step 3
Secondly import the file "_1_get_ports_polygon.py", and change the constant "GEO_AREA" to the name of the desired polygon found on the database. Furthermore the file path should be changed to the desired file path.
The following code shows how to get the polygon from the database
```python
from _1_get_ports_polygon.py import save_polygons
from modules.operating_system import OperatingSystem

# Setting up variables
os = OperatingSystem()
GEO_AREA = 'Oestersoe_dtu_casper'
FILE_PATH = os.path('AIS','Oestersoe_dtu_casper', 'Oestersoe_polygon.csv)

# Function call
save_polygons(geo_area = GEO_AREA, file_path = FILE_PATH)
```
Note: Make sure "FILE_PATH" exists as a path
### Step 4
The third step is to fetch the AIS data. This is done by selecting a ship type and a range of dates.\\
The following code shows how to fetch the AIS data. Import the file "_2_get_ais_data.py" :
```python
from _2_get_ais_data import get_ship_ais

# Setup variables
FOLDER_NAME = "AIS"
SHIPTYPE = 'cargo'
START_DATE = '2021-04-03'
END_DATE = '2021-04-03'
GEO_AREA = 'Oestersoe_dtu_casper'

# Call function
get_ship_ais(
    shiptype=SHIPTYPE,
    start_date=START_DATE,
    end_date=END_DATE,
    geo_area=GEO_AREA,
    folder_name = FOLDER_NAME,
    verbose=True
)
```
Note: Make sure the following path exists "{FOLDER_NAME}/{GEO_AREA}/{SHIPTYPE}"

## Clean the AIS data
### Step 1
Setup the initial variables for file handling:
```python
from _3_import_ais_data import clean_ais
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
```
### Step 2
Setup the ais data handling class:
```python
ais_class = clean_ais(verbose = True)
```
### Step 3
Import the AIS data from the folder where all the .csv files are located:
```python
ais_class.import_ais(
    folder_name = FOLDERAIS,
    geoarea = GEOAREA,
    shiptype = SHIPTYPE,
    file_amount = FILES
)
```
### Step 4
Import all the other needed data files:
```python
#Import the port polygons
ais_class.import_ports(FOLDERPORTS, PORTFILENAME)

#Import area of interrest polygon
ais_class.import_polygon(GEOAREA, FOLDERAIS, FILE_NAME_POLYGON)
```

### Step 5
Creating trips:
```python
ais_class.create_routes(speed_limit = 3)
```
### Step 6
Remove trips which travels outside the polygon:
```python
ais_class.remove_routes_outside_polygon()
```
### Step 7
Interpolate routes with an frequency of 10 minutes:
```python
ais_class.interpolate_routes(10*60)
```
### Step 8
Removing trips which doesnt move more then 10 km within 24 hours.
Furthermore the trips which includes datapoints with speeds less then 0.5 knots are removed.
```python
ais_class.clean_data(threshold = 10, interval = 24*60*60, speed = 0.5)
```
### Step 9
Save the data sets:
```python
ais_class.save_routes(PICKLE_FOLDER, PICKLE_ROUTE_FILE)
ais_class.save_interpolated(PICKLE_FOLDER, PICKLE_INTERPOLATED_FILE)
```
### Step 10
To acces the interpolated or raw data at any time use the following code:
```python
routes = ais_class.routes
interpolated_routes = ais_class.interpolated_routes
```
### Step 11
Whenever a dataset has been saved it can be import back into the class by using the following code:
```python
ais_class.import_routes(PICKLE_FOLDER, PICKLE_ROUTE_FILE)
ais_class.import_interpolated(PICKLE_FOLDER, PICKLE_INTERPOLATED_FILE)
```

## Creating waypoints
Go through step 1-9 for [Clean the AIS data](#clean-the-ais-data) and then go through the following steps:
### Step 1
Create trips with 100 waypoints per trip:
```python
ais_class.create_waypoints(waypoint_amount = 100)
```
### Step 2
Save the data set:
```python
ais_class.save_waypoints(PICKLE_FOLDER, PICKLE_WAYPOINTS_FILE)
```
### Step 3
To acces the waypoint data at any time use the following code:
```python
waypoints = ais_class.waypoints
```
### Step 4
Whenever a dataset has been saved it can be import back into the class by using the following code:
```python
ais_class.import_waypoints(PICKLE_FOLDER, PICKLE_WAYPOINTS_FILE)
```

## License
[MIT](License)
