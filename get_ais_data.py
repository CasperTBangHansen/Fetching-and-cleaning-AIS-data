#!/usr/bin/env python
"""
Module for getting ais fata from the Gatehouse database
"""
#Import
import database_details
from datetime import timedelta, datetime, date
from typing import Union, Generator
import pandas as pd                                     # type: ignore
from modules.errors import WrongArguments, PathError
from modules.database import Database
from modules.operating_system import OperatingSystem

# database_details should contain the following information:
# login_info = {
#     'host' : "**********",
#     'database_name' : "**********",
#     'port' : "**********",
#     'username' : "**********",
#     'password' : "**********",
# }

def daterange(
    start_date : date,
    end_date : date
    ) -> Generator[date, None, None]:
    """
    Calculate the daterange between to dates
    """
    for i in range(int((end_date - start_date).days + 1)):
        yield start_date + timedelta(i)

def get_ship_ais(
    shiptype : Union[str, int] = 'cargo',
    start_date : str = '2019-04-01',
    end_date : str = '2019-04-02',
    geo_area : str = 'N_Norway',
    folder_name : str = 'AIS',
    verbose : bool = True
    ) -> None:
    '''
    Function to fetch ship data from the database
    '''

    # Converting string to datetime
    try:
        start_date_count = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date_count = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as exc:
        raise WrongArguments(
            "start_date and end_date should be formatted as\
            (YYYY-MM-DD) but was {0} and {1}".format(
                start_date,
                end_date
                )
        ) from exc

    if verbose:
        print("{0} Days are queried.".format((end_date_count - start_date_count).days + 1))

    # Ship types
    ship_type_lookup = {
        'cargo' : 70,
        'tanker' : 80,
        'other type' : 90,
        'passenger' : 60,
        'fishing' : 30,
        'towing' : 31,
        'tailing': 36,
        'pleasure craft': 37,
        'tug' : 52,
        'law enforcement': 55,
        'search and rescue vessel' : 51,
        'high speed craft' : 40,
        'hsc' : 40,
        'military ops' : 35,
        'military' : 35
    }
    
    # Converting to shiptype (str -> int)
    if isinstance(shiptype, str):
        try:
            shiptype = ship_type_lookup[shiptype]
        except KeyError as exc:
            raise WrongArguments(
                "shiptype was {0}, but should be one of the following types {1}".format(
                    shiptype,
                    list(ship_type_lookup)
                )
            ) from exc
    
    # Special case of towing ship type (idk why)
    if shiptype == 32:
        shiptype = 31

    # Sql query for getting the data from the database
    plain_sql = """\
        with cte as \
        (\
        select mmsi , pgt_pointsm(track) as p from track.tbl_daily where day = '{date}' \
        and mmsi in (select mmsi from dbserver.mat_statvoy where shiptype = '{ship_type}' \
        and track && (select polygon from dbserver.tbl_shapes where name = '{geo_area}'))\
        ) select mmsi , (p).stamp , st_x((p).pos::geometry), st_y((p).pos::geometry), (p).sog, (p).cog from cte where (p).bits = 1\
        """

    # For saving files later
    operating_system = OperatingSystem()

    try:
        # Connect to database
        database = Database(
            **database_details.login_info
        )
        database.connect()
        
        # Init results
        results = None

        # Getting results from the dates
        for current_date in daterange(start_date_count, end_date_count):
            if verbose:
                print(f"Fetching results from {current_date}")

            # Getting results
            result = database.execute_sql(
                plain_sql.format(
                    date = current_date,
                    ship_type = shiptype,
                    geo_area = geo_area
                )
            )

            # Check answer
            if result is not None:
                df_results = pd.DataFrame(
                    result,
                    columns = [
                        'mmsi',
                        'datatime',
                        'lon',
                        'lat',
                        'sog',
                        'cog'
                    ]
                )
                # Saves the csv file to AIS/geo_area/ship_type/date
                ship_string = {v: k for k, v in ship_type_lookup.items()}[shiptype]
                pandas_to_csv(
                    file_path=operating_system.path(folder_name, geo_area, ship_string),
                    file_name=current_date.strftime('%Y-%m-%d'),
                    data_frame=df_results
                )

    # Disconnect
    finally:
        database.disconnect()

def pandas_to_csv(
    file_path : str,
    file_name : str,
    data_frame: pd.DataFrame
    ) -> None:
    """ Save data_frame to csv file."""
    # Make file_name into a csv file and get the path to the file
    file_name = file_name if file_name.endswith(
            '.csv'
    ) else file_name + '.csv'

    operating_system = OperatingSystem()

    # Check if the directory exists
    if not operating_system.check_file(file_path):
        raise PathError(file_path)

    # Save data frame
    data_frame.to_csv(
        operating_system.path(
            file_path,
            file_name
        ),
        date_format='%Y-%m-%d %H:%M:%S',
        sep=';',
        header=False,
        index=False
    )
if __name__ == "__main__":
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
