#!/usr/bin/env python
"""
Module for importing AIS data and cleaning the data.
"""
import csv
import time
from typing import Union
import numpy as np                                                              # type: ignore
import pandas as pd                                                             # type: ignore
from haversine import haversine_vector                                          # type: ignore
from modules.operating_system import OperatingSystem
from modules.points_in_polygons.points_in_polygons import mask_from_polygons
from modules.centroid import find_centroid
from modules.errors import NotdefinedError

# TO DO:
# Use dask instead of pandas
# Make import_ais faster
# Implement getters and setters

class clean_ais():
    """
    Model for import AIS data and sorting it
    """
    def __init__(
        self,
        verbose : bool = True
    ) -> None:
        # Setting default values
        self.ports : Union[pd.DataFrame, None] = None
        self.routes : Union[pd.DataFrame, None] = None
        self.interpolated_routes : Union[pd.DataFrame, None] = None
        self.polygon : Union[pd.DataFrame, None] = None
        self.ais_data : Union[pd.DataFrame, None] = None
        self.waypoints : Union[pd.DataFrame, None] = None
        self.waypoint_amount : Union[int, None] = None
        
        # verbose
        self.verbose = verbose
        
        # Operationg system for files
        self.os = OperatingSystem()
    
    def save_routes(self, folder_name : str, file_name : str) -> None:
        """
        Save routes to specified path.
        """
        self.__save_pickle(self.routes, "routes", folder_name, file_name)

    def save_interpolated(self, folder_name : str, file_name : str) -> None:
        """
        Save interpolated routes to specified path.
        """
        self.__save_pickle(self.interpolated_routes, "interpolated_routes", folder_name, file_name)
    
    def save_waypoints(self, folder_name : str, file_name : str) -> None:
        """
        Save waypoint routes to specificed path.
        """
        self.__save_pickle(self.waypoints, "waypoints", folder_name, file_name)

    def __save_pickle(self, dataframe : pd.DataFrame, variable_name : str, folder_name : str, file_name : str) -> None:
        """
        Save pickle file to specified path.
        """
        if dataframe is not None:
            start = time.time()
            pd.to_pickle(
                dataframe,
                self.os.path(folder_name, file_name)
            )
            if self.verbose:
                print("Saved {0} ({1:.2f}s)".format(variable_name, time.time() - start), flush=True)
        else:
            raise NotdefinedError(variable_name)
    
    def import_routes(self, folder_name : str, file_name : str) -> None:
        """
        Imports routes from specified path.
        """
        self.routes = self.__import_pickle("routes", folder_name, file_name)

    def import_interpolated(self, folder_name : str, file_name : str) -> None:
        """
        Imports interpolated routes from specified path.
        """
        self.interpolated_routes = self.__import_pickle("interpolated_routes", folder_name, file_name)

    def import_waypoints(self, folder_name : str, file_name : str) -> None:
        """
        Imports waypoint routes from specified path.
        """
        self.waypoints = self.__import_pickle("waypoints", folder_name, file_name)
    
    def __import_pickle(self, variable_name : str, folder_name : str, file_name : str) -> pd.DataFrame:
        """
        Import pickle file from specified path.
        """
        start = time.time()
        dataframe = pd.read_pickle(self.os.path(folder_name, file_name))
        if self.verbose:
            print('Imported {0} \
from pickle files ({1:.2f}s)'.format(variable_name, time.time()-start), flush=True)
        return dataframe

    def import_ports(self, folder_name : str, file_name : str) -> None:
        """
        Import datafile for all polygon ports.
        """
        start = time.time()
        self.ports = pd.read_csv(
            self.os.check_path(
                folder_name,
                file_name
            ),
            sep=';',
            header=None,
            names=[
                'name',
                'locode',
                'polygon',
            ]
        )
        if self.verbose:
            print("Imported all port polygons ({0:.2f}s)".format(time.time()-start), flush=True)
        
        # Split str to list and convert to float
        self.ports['polygon'] = self.ports['polygon'].str.split(' |,')
        self.ports['polygon'] = self.ports['polygon'].apply(
            lambda x: np.fliplr(
                np.reshape(
                    np.array(
                        np.asfarray(x,float)
                    ),
                    (-1,2)
                )
            )
        )
        self.ports = self.ports.reset_index(drop=True)
    def import_polygon(
        self,
        geoarea : str,
        folder_name : str,
        file_name : str
    ) -> None:
        """Imports polygon from datafile."""
        start = time.time()
        self.polygon = pd.read_csv(
            self.os.check_path(
                folder_name,
                geoarea,
                file_name
            ),
            sep=';',
            header=None,
            names=[
                'lat',
                'long',
            ]
        )
        if self.verbose:
            print("Imported polygon of geoarea ({0:.2f}s)".format(time.time()-start), flush=True)

    def import_ais(
        self,
        folder_name : str,
        geoarea : str,
        shiptype : str,
        file_amount : int = -1
    ) -> None:
        """
        Function for import AIS data from csv files
        """
        # Check if the directory exists
        folder_path = self.os.check_path(
            folder_name,
            geoarea,
            shiptype
        )

        start = time.time()
        # Save pandas dataframes in list
        ship_data = []
        files = self.os.get_files(folder_path)
        files = files if file_amount == -1 else files[:file_amount]
        for idx, file in enumerate(files):
            if self.verbose:
                print(
                    "\rProgress = {0:.2f}%".format(
                        (idx)/len(files) * 100
                    ),
                    end= '',
                    flush=True
                )

            # Import data using csv.reader for higher performance then pd.read_csv
            with open(self.os.check_path(folder_path, file)) as csvfile:
                ship_data += list(csv.reader(csvfile, delimiter=';'))
        if self.verbose:
            print("\rProgress = 100.00% ({0:.2f}s)\n\
Converting to dataframe".format(time.time() - start), flush=True)

        # Create dataframe from list
        self.ais_data = pd.DataFrame(
            ship_data,
            columns=[
                    "mmsi",
                    "time",
                    "long",
                    "lat",
                    "sog",
                    "cog"
            ],
        ).set_index('mmsi')

        # Change types from string to float/datetime
        col = ["lat", "long", "sog", "cog"]
        self.ais_data[col] = self.ais_data[col].apply(pd.to_numeric, errors='coerce')
        self.ais_data["time"] = pd.to_datetime(self.ais_data["time"], format='%Y-%m-%d %H:%M:%S')
        if self.verbose:
            print("Converting to dataframe ({0:.2f}s)".format(time.time() - start), flush=True)
        self.ais_data = self.ais_data.sort_index()

    def create_routes(
        self,
        speed_limit : float = 3
    ) -> None:
        """
        Find which ship has been in which port at what time
        and the tracks between ports.
        """
        # Check if the data has been imported
        if self.ais_data is None:
            raise NotdefinedError("ais_data")
        if self.ports is None:
            raise NotdefinedError("ports")
            
        # Setup data in the right format for the function in polygon function
        start = time.time()
        ships = self.ais_data.copy().sort_values(
            by = ['mmsi','time']
        )
        lat = ships['lat'].values
        long = ships['long'].values
        polygon_tuple = list(self.ports['polygon'].apply(lambda x: [list(zip(x.T[0,:],x.T[1,:]))]))
        polygon_in = [[False]*len(y) for y in polygon_tuple]
        args = [lat, long, polygon_tuple , polygon_in]
        if self.verbose:
            print("Variables for inside polygon function is made ({0:.2f}s)".format(time.time() - start), flush=True)

        # Find ships inside polygons
        start = time.time()
        masks = mask_from_polygons(*args, include_holes=False)
        if self.verbose:
            print("Inside polygon ({0:.2f}s)".format(time.time() - start), flush=True)


        # Get ships in- and out-side polygon with data
        start = time.time()
        # Convert to dataframe
        masks = pd.DataFrame(masks, columns = ["index", "polygon_index"])
        exploded_masks = masks.explode('index')
        # If there is duplicates (polygons inside polygons, remove first duplicates)
        exploded_masks = exploded_masks.drop_duplicates('index')

        # Get data about the exploded masks (indexes)
        ships = ships.reset_index().copy()
        constraint = ships.index.isin(exploded_masks['index'])
        locode_in_polygon = exploded_masks.merge(
            self.ports,
            left_on='polygon_index',
            right_index=True
        ).drop(
            [
                'polygon_index'
            ],
            axis=1
        )
        
        # Ships inside port
        in_port = ships[constraint]
        in_port = in_port.merge(
            locode_in_polygon,
            left_index=True,
            right_on='index'
        ).set_index(in_port.index)

        # Outside port
        out_port = ships[~constraint].copy()
        out_port['index'] = out_port.index
        if self.verbose:
            print("Setup variables for everything else ({0:.2f}s)".format(
                time.time() - start
                ),
                flush=True
            )
        start = time.time()

        # Find arrived and departure times (of the polygon)
        times = in_port.groupby(
            [
                'mmsi',
                (
                    in_port['locode'] != in_port['locode'].shift()
                ).cumsum()
            ]
        ).agg(
            {
            'time': ['first','last'],
            'lat': ['first','last'],
            'long': ['first','last'],
            'index': list
            }
        ).reset_index().drop('locode', axis = 1, level=0)

        # Check if the ships are just passing by the port or if the ship docs
        # Distance inside polygon
        delta_time = times['time']['last'] - times['time']['first']
        haversine_distance = times.apply(
            lambda row: haversine_vector(
                (row['lat']['first'], row['long']['first']),
                (row['lat']['last'], row['long']['last'])
            )[0],
            axis = 1
        )

        # Minimum speed inside polygon
        # Speed in knots (1 km/h = 0.53996 knots)
        speed = haversine_distance/(delta_time.dt.total_seconds()/3600) * 0.53996
        index = speed <= speed_limit
        time_in_port = times[index].drop(['index', 'lat', 'long'], axis = 1, level=0)
        time_out_port = times[~index]

        time_out_port_idx = [j for i in time_out_port['index']['list'].to_list() for j in i]
        time_in_port.columns = [
            time_in_port.columns.values[0][0]
        ] + [column[1] for column in time_in_port.columns.values[1:]]

        # Calculates centroid of ports
        centroid = in_port['polygon'].apply(find_centroid)
        in_port['port_lat'] = centroid.apply(lambda x: x[0])
        in_port['port_long'] = centroid.apply(lambda x: x[1])
        in_port = in_port.drop('polygon', axis = 1)

        # Find routes outside of ports

        # Had a high enough avg speeds inside the port polygon
        time_out_port_values = ships[ships.index.isin(time_out_port_idx)].copy()
        time_out_port_values['index'] = time_out_port_values.index

        # Create routes with an id
        routes = pd.concat([out_port, time_out_port_values]).sort_index().copy()
        routes['id'] = routes.groupby(
            [
                'mmsi',
                (
                    routes['index'] != routes['index'].shift() + 1
                ).cumsum(),
            ]
        ).grouper.group_info[0]

        # Remove routes where there is less then 10 points
        point_count = routes['id'].value_counts()
        route_port = routes[routes['id'].isin(point_count[point_count >= 10].index)]
        routes = routes.drop('index', axis=1).copy()

        # Remove the first and last trip each ship has sailed
        remove_idx = route_port.groupby('mmsi')['id'].agg(['first','last']).values.flatten()
        route_port = route_port[~route_port['id'].isin(remove_idx)]

        # Get from port and to port
        from_to = route_port.groupby('id')['index'].agg(['first','last'])
        
        # Setup columns for from and to
        from_to['from_locode'] = in_port.loc[from_to['first']-1]['locode'].values
        from_to['from_lat'] = in_port.loc[from_to['first']-1]['port_lat'].values
        from_to['from_long'] = in_port.loc[from_to['first']-1]['port_long'].values

        from_to['to_locode'] = in_port.loc[from_to['last']+1][['locode']].values
        from_to['to_lat'] = in_port.loc[from_to['last']+1]['port_lat'].values
        from_to['to_long'] = in_port.loc[from_to['last']+1]['port_long'].values
        from_to['to_time'] = in_port.loc[from_to['last']+1]['time'].values
        
        # Merge with the port routes
        route_port = route_port.merge(
            from_to.drop(['first','last'], axis = 1),
            left_on='id',
            right_index=True
        ).drop('index', axis=1)
        if self.verbose:
            print("Done setting up routes ({0:.2f}s)".format(time.time() - start), flush=True)
        self.routes = route_port.copy()

    def remove_routes_outside_polygon(
        self,
    ) -> None:
        """
        Removes points outside polygon
        """
        # Check if the data has been imported
        if self.polygon is None:
            raise NotdefinedError("polygon")
        if self.routes is None:
            raise NotdefinedError("routes")

        # Setup variables
        lat = self.routes['lat'].values
        long = self.routes['long'].values
        polygon = self.polygon.values
        polygon_tuple = [[list(zip(polygon.T[0,:], polygon.T[1,:]))]]
        polygon_in = [[False]*len(y) for y in polygon_tuple]
        args = [lat, long, polygon_tuple , polygon_in]

        # Find ships inside polygons
        start = time.time()
        masks = mask_from_polygons(*args, include_holes=False)
        if self.verbose:
            print("Within function for country ({0:.2f}s)".format(time.time() - start), flush=True)

        # Removes ids outside of polygon
        remove_id = self.routes[
            ~self.routes.reset_index()['id'].index.isin(masks[0][0])
        ]['id'].unique()

        # Overwrite self.routes
        self.routes = self.routes[~self.routes['id'].isin(remove_id)].copy()

    def __interpolate(
        self,
        interval_s: int,
        dataframe : pd.DataFrame
    ) -> pd.DataFrame:
        """
        Interpolate routes with interval
        """
        start = time.time()
        # Convert to string
        resamling_interval = '{0}s'.format(interval_s)

        # For merging later on
        id_locode = dataframe[
            [
                'id', 'from_locode', 'to_locode', 'from_lat',
                'from_long', 'to_lat', 'to_long' , 'to_time'
            ]
        ].value_counts().reset_index().drop(0, axis=1)

        if self.verbose:
            print("Variables setup ({0:.2f}s)".format(time.time() - start), flush=True)
        
        # Interpolating
        start = time.time()
        interpolated = dataframe[
            ['mmsi', 'time', 'lat', 'long', 'sog', 'cog', 'id']
        ].set_index(
            'time'
        ).groupby(
            ['id','mmsi']
        ).resample(
            resamling_interval
        ).mean().interpolate(
            'linear'
        ).drop(
            'id', axis=1
        ).reset_index()

        if self.verbose:
            print("Interpolated data ({0:.2f}s)".format(time.time() - start), flush=True)
        return interpolated.merge(
            id_locode,
            left_on='id',
            right_on='id'
        )
        
    def interpolate_routes(
        self,
        interval_s: int,
    ) -> None:
        """
        Interpolate routes with interval
        """
        # Check if the data has been imported
        if self.routes is None:
            raise NotdefinedError("routes")
        start = time.time()
        self.interpolated_routes = self.__interpolate(interval_s, self.routes)

    def clean_data(self, threshold: int = 10, interval_s: int = 24*60*60, speed : float = 0.5) -> None:
        """
        Removes data which havent moved threshold km interval time
        and removes ships which have a speed under speed knots at any given time.
        """
        # Check if the data has been imported
        if self.routes is None:
            raise NotdefinedError("routes")
        if self.interpolated_routes is None:
            raise NotdefinedError("interpolated_routes")
        
        # Interpolate with a given interval
        ais_inter_day = self.__interpolate(interval_s, self.interpolated_routes)
        
        # Shiftes the four columns
        ais_inter_day_diff = ais_inter_day[
            ['lat','long','time','id']
        ].groupby('id').shift(1).add_prefix('next_')
        
        # Merges dataframes
        ais_inter_day_diff = pd.merge(
            ais_inter_day,
            ais_inter_day_diff,
            left_index=True,
            right_index=True
        ).dropna()
        
        # Calculates the distance between the current point and
        # the next point
        ais_inter_day_diff['distance'] = ais_inter_day_diff.apply(
            lambda row: haversine_vector(
                (row['lat'], row['long']),
                (row['next_lat'], row['next_long'])
            )[0],
            axis = 1
        )
        # Remove trips which have points inside the threshold
        remove_id = ais_inter_day_diff[ais_inter_day_diff['distance'] < threshold]['id'].unique()
        
        # Overrid dataframes
        self.routes = self.routes[~self.routes['id'].isin(remove_id)].copy()
        self.interpolated_routes = self.interpolated_routes[
            ~self.interpolated_routes['id'].isin(remove_id)
        ].copy()
        
        # Remove trips which have points which is less the speed amount
        remove_id = self.interpolated_routes[self.interpolated_routes['sog'] <= speed]['id'].unique()
        
        # Overrid dataframes
        self.routes = self.routes[~self.routes['id'].isin(remove_id)].copy()
        self.interpolated_routes = self.interpolated_routes[
            ~self.interpolated_routes['id'].isin(remove_id)
        ].copy()

    def create_waypoints(self, waypoint_amount : int):
        """
        Creates a dataframe with waypoints
        """
        # Check if the data has been imported
        if self.interpolated_routes is None:
            raise NotdefinedError("interpolated_routes")
        if self.routes is None:
            raise NotdefinedError("routes")

        # Setting waypoints
        self.waypoint_amount = waypoint_amount

        # Interpolate again with a higher frequency to get the routes
        # back with less then 100 waypoints

        # Find trips with less then waypoint amount and resample
        port_route_copy = self.interpolated_routes.copy()
        point_amount = port_route_copy.groupby('id')['lat'].count().reset_index()
        missing_points_data = point_amount[point_amount['lat'] < self.waypoint_amount]['id'].values
        missing_points_data = self.routes[self.routes['id'].isin(missing_points_data)]
        
        # Find trip length (in time) of the missing points
        time_last = missing_points_data.groupby('id')['time'].last().reset_index()
        time_first = missing_points_data.groupby('id')['time'].first().reset_index()
        delta_time = time_last.copy()
        delta_time['length'] = (time_last['time'] - time_first['time']).astype('timedelta64[m]')
        
        # Remove trips under 30 minutes and interpolate all the routes with the smallest
        # frequency for getting waypoint amount
        delta_time = delta_time.drop('time', axis=1)
        port_route_copy = port_route_copy.merge(delta_time[['id','length']], left_on='id', right_on='id')
        remove_id = port_route_copy['id'].unique()
        port_route_copy = port_route_copy[port_route_copy['length'] >= 30].reset_index()
        smallest_time = port_route_copy['length'].min()
        new_inter = self.__interpolate(int(smallest_time*60/self.waypoint_amount), port_route_copy)
        
        # Remove duplicate interpolations
        interpolated_local = self.interpolated_routes[~self.interpolated_routes['id'].isin(remove_id)]
        # Concatenate with interpolated dataframe
        interpolated_local = pd.concat( [new_inter, interpolated_local] )
        
        # Create waypoints
        start = time.time()
        self.waypoints = self.__waypoints(interpolated_local, self.waypoint_amount)
        if self.verbose:
            print("Created waypoints ({0:.2f}s)".format(time.time() - start), flush=True)

    def __waypoints(
        self,
        route : pd.DataFrame,
        points : int = 10
    ) -> pd.DataFrame:

        # Save data about the waypoint
        port_info = route.drop(['time','lat','long','sog','cog'], axis = 1)
        port_info = port_info.drop_duplicates()

        # Make a copy of the route dataframe to work on
        z = route[['id','time','lat','long','sog','cog']].copy()

        # Calculate frequency f in seconds for each id
        # and t0 as the midnight of the first day of the group
        g = z.groupby('id')['time']
        z['f'] = (g.transform('max') - g.transform('min')).astype(np.int64) / (points - 1) // 10**9
        z['t0'] = g.transform('min').dt.floor('d').astype(np.int64) // 10**9

        # Calculate seconds since t0
        # This is what .resample(...) operates on
        z['s_since_t0'] = z['time'].astype(np.int64) // 10**9 - z['t0']

        # Get grouped seconds since t0
        # In the same way that .resample(...) does
        z['s_group'] = z['t0'] + z['s_since_t0'] // z['f'] * z['f']

        # Convert grouped seconds to datetime
        z['time_group'] = pd.to_datetime(z['s_group'], unit='s')

        # Calculate mean
        z = z.groupby(['id', 'time_group'])[['lat', 'long','sog','cog']].mean().reset_index()

        # Merge with port_info
        return z.merge(
            port_info,
            left_on='id',
            right_on='id'
        ).rename(
            columns = {
                'time_group' : 'time'
            }
        )
