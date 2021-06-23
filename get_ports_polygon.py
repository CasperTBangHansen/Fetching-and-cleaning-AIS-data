#!/usr/bin/env python
"""
This file is for getting port polygons from the database
"""
from modules.database import Database
import pandas as pd                     # type: ignore
import sys
import database_details

# database_details should contain the following information:
# login_info = {
#     'host' : "**********",
#     'database_name' : "**********",
#     'port' : "**********",
#     'username' : "**********",
#     'password' : "**********",
# }

def save_polygons(geo_area : str, file_path : str):
    # Sql query for getting the port polygons from the database
    plain_sql = """\
        SELECT substring(left(St_astext(polygon),-2),10) FROM dbserver.tbl_shapes where name = '{geo_area}'
        """
    result = None
    try:
        # Setup Database class with the valid information
        database = Database(
            **database_details.login_info
        )

        # Connecting to the database
        database.connect()

        # Getting results
        result = database.execute_sql(
            plain_sql.format(
                geo_area = geo_area
            )
        )
    except:
        # Print error
        e = sys.exc_info()
        print("FAILED : {0}".format(e))
    finally:
        # Disconnect
        database.disconnect()

    # Save the data to a csv file
    if result is not None:
        result = pd.DataFrame([[float(y) for y in x.split(' ')] for x in result[0][0].split(',')])[[1,0]]
        result.to_csv(
            file_path,
            sep=';',
            header=False,
            index=False
        )


if __name__ == "__main__":
    # Setting variables
    GEO_AREA = "Oestersoe_dtu_casper"
    FILE_PATH = "AIS\\Oestersoe_dtu_casper\\Oestersoe_polygon.csv"

    # Function call
    save_polygons(geo_area = GEO_AREA, file_path = FILE_PATH)
    

