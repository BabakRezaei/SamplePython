import numpy as np
import pandas as pd
import math
from time import time
from datetime import datetime
import os
from dotenv import load_dotenv

from modules.interpolation_funcs import create_metamodel
from modules.calcDistance import distance
from modules.connection_manager import read_from_db

from logger import logger


class Interpolation:
    def __init__(self, conjunto_feeders, start_date, pred_date):
        load_dotenv()

        self.has_error = False
        self.conjunto_feeders = conjunto_feeders
        self.start_date = start_date
        self.pred_date = pred_date
        self.neighbor_states = ['BA', 'RJ', 'SP', 'MS', 'GO', 'DF', 'MG']
        path_to_interpolation = os.getenv("PATH_TO_INTERPOLATION")

        read_from_database = os.getenv("READ_FROM_DATABASE").lower()
        if read_from_database == "yes":
            # READ FROM DATABASE
            table = "inmet_autostations"
            try:
                self.neighbor_stations = read_from_db(table_name=table)
                if self.neighbor_stations.empty:
                    logger.info(
                        "There is no autostation to start interpolation process")
                    self.has_error = True

            except Exception as ex:
                logger.info(
                    f"An exception is occured in reading {table}, {ex}")
                self.has_error = True

            # READ FROM DATABASE
            table = 'geo_lat_long_alimentador'
            try:
                column_list = ["ctmt", "longitude", "latitude"]
                if len(self.conjunto_feeders) == 1:
                    condition_str = f"WHERE ctmt='{self.conjunto_feeders[0]}'"
                else:
                    condition_str = f"WHERE ctmt IN {tuple(self.conjunto_feeders)}"

                self.conjunto_feeders_coords = read_from_db(table_name=table,
                                                            condition_str=condition_str,
                                                            column_list=column_list)
                self.code = "ctmt"
                if self.conjunto_feeders_coords.empty:
                    logger.info(f"An exception is occured in reading {table}")
                    self.has_error = True

            except Exception as ex:
                logger.info(
                    f"An exception is occured in reading {table}, {ex}")
                self.has_error = True

        else:
            # reading autostation file to filter metereological stations inside the listed states
            autostation_filename = os.getenv("AUTOSTATIONS_FILENAME")
            path = os.path.join(path_to_interpolation, autostation_filename)
            try:
                stations = pd.read_csv(path, header=0)
                if not stations.empty:
                    self.neighbor_stations = stations[(
                        stations['state_initials'].isin(self.neighbor_states))]
                else:
                    logger.info(
                        "There is no autostation to start interpolation process")
                    self.has_error = True

            except Exception as ex:
                logger.info(f"An exception is occured, {ex}")
                self.has_error = True

            # reading feeder coordinates and filter the required feeders for interpolation
            feeders_coords_filename = os.getenv("FEEDER_COORDS_FILENAME")
            path = os.path.join(path_to_interpolation, feeders_coords_filename)
            try:
                all_feeders = pd.read_csv(path)
                if not all_feeders.empty:
                    self.conjunto_feeders_coords = all_feeders[all_feeders["code"].isin(
                        self.conjunto_feeders)]
                    self.code = "code"
                else:
                    logger.info(
                        "There is no feeder coordinates to start interpolation process")
                    self.has_error = True

                del all_feeders

            except Exception as ex:
                logger.info(f"An exception is occured, {ex}")
                self.has_error = True

        ##########################################################
        # WE NEED TO READ THE HISTORY OF AUTOSATIONS FROM DATABASE
        self.total_stations_history = pd.DataFrame()
        path_to_state_stations = os.getenv("PATH_TO_STATE_STATIONS")
        for state in self.neighbor_states:
            read_path = f"{state}/All_Stations_{state}.csv"
            path = os.path.join(path_to_state_stations, read_path)
            tmp_df = pd.read_csv(path, header=0)
            if not tmp_df.empty:
                self.total_stations_history = pd.concat([self.total_stations_history, tmp_df],
                                                        ignore_index=True)
            else:
                break

        # # READ FROM DATABASE
        # table = "METEOROLOGICAL_HISTORICAL_DATA"
        # column_list = []
        # condition_str = f"WHERE state IN {self.neighbor_states}"
        # self.total_stations_history = read_from_db(table_name=table,
        #                                            condition_str=condition_str,
        #                                            column_list=column_list)

    def run_interpolation(self):
        logger.info("Interpolation in process ...")
        cols = ['cod_alim', 'year', 'month', 'temp_max', 'temp_min',
                'temp_mean', 'precipitation', 'humidity_min',
                'humidity_mean', 'windvelo_mean']
        final_total_df = pd.DataFrame(columns=cols)

        for feeder in self.conjunto_feeders:
            tmp_df = self.interpolation_process(feeder)
            if not tmp_df.empty:
                final_total_df = pd.concat(
                    [final_total_df, tmp_df], ignore_index=True)

        logger.info("Interpolation process is finished.")
        del self.neighbor_stations
        del self.conjunto_feeders_coords
        del self.total_stations_history
        del self.conjunto_feeders
        return final_total_df

    def interpolation_process(self, feeder):
        cols = ['cod_alim', 'year', 'month', 'temp_max', 'temp_min',
                'temp_mean', 'precipitation', 'humidity_min',
                'humidity_mean', 'windvelo_mean']

        columns = self.neighbor_stations['station_code'].unique()

        sel_points = self.conjunto_feeders_coords[self.conjunto_feeders_coords[self.code] == feeder]
        feeder_points_count = sel_points.shape[0]

        min_lon = min(sel_points['longitude'])
        max_lon = max(sel_points['longitude'])

        min_lat = min(sel_points['latitude'])
        max_lat = max(sel_points['latitude'])

        UL_lon = min_lon
        UL_lat = max_lat

        UR_lon = max_lon
        UR_lat = max_lat

        LL_lon = min_lon
        LL_lat = min_lat

        LR_lon = max_lon
        LR_lat = min_lat

        C_lon = (max_lon - min_lon) / 2 + min_lon
        C_lat = (max_lat - min_lat) / 2 + min_lat

        lon_distance = distance(UL_lon, UL_lat, UR_lon, UR_lat)
        lat_distance = distance(UL_lon, UL_lat, LL_lon, LL_lat)
        dia_distance = math.sqrt(
            math.pow(lon_distance, 2) + math.pow(lat_distance, 2))

        prime_search_radius = math.ceil(dia_distance / 2)

        Xp = int((lon_distance // 10) + 1)
        Yp = int((lat_distance // 10) + 1)
        total_points = Xp * Yp

        if total_points <= 4:
            if feeder_points_count < 100:
                total_points = 1
            else:
                total_points = 4

        # a list of tuples containing longitudes and latitudes
        calculated_points = []

        if total_points == 1:
            calculated_points.append((C_lon, C_lat))
        elif total_points == 4:
            mean_lon = (max_lon - min_lon) / 2 + min_lon
            mean_lat = (max_lat - min_lat) / 2 + min_lat

            # adding 4 centers of rectangle sides
            calculated_points.append((mean_lon, max_lat))
            calculated_points.append((min_lon, mean_lat))
            calculated_points.append((max_lon, mean_lat))
            calculated_points.append((mean_lon, min_lat))
            calculated_points.append((C_lon, C_lat))
        else:
            # create a rectangle by Xp * Yp points
            lon_interval = (UR_lon - UL_lon) / (Xp - 1)
            lat_interval = (LL_lat - UL_lat) / (Yp - 1)

            for j in range(Yp):
                lat = UL_lat + (j * lat_interval)

                for i in range(Xp):
                    lon = UL_lon + (i * lon_interval)
                    # logger.info(f"lon: {lon}, lat: {lat}")
                    calculated_points.append((lon, lat))

            calculated_points.append((C_lon, C_lat))

        dist_mat = []
        for _, station in self.neighbor_stations.iterrows():
            lon2 = station['longitude']
            lat2 = station['latitude']

            dist_mat.append(distance(C_lon, C_lat, lon2, lat2))

        close_stations = set()
        for iter in range(len(columns)):
            if dist_mat[iter] <= prime_search_radius:
                close_stations.add(columns[iter])

        current_added_stations = len(close_stations)
        search_step_size = 10
        initial_search_radius = prime_search_radius
        while len(close_stations) < current_added_stations + 3:
            next_search_radius = initial_search_radius + search_step_size
            for iter in range(len(columns)):
                if initial_search_radius < dist_mat[iter] <= next_search_radius:
                    close_stations.add(columns[iter])

            initial_search_radius = next_search_radius

        # extract the locations of the closest stations
        found_stations = self.neighbor_stations[self.neighbor_stations['station_code'].isin(
            close_stations)]

        start_date = self.start_date
        end_date = self.pred_date
        date_range = pd.date_range(start=start_date, end=end_date,
                                   freq=pd.DateOffset(months=1))
        feeder_periods = pd.DataFrame(columns=cols)
        periods_wo_value = []
        for period in date_range:
            year = period.year
            month = period.month

            coords = []
            temp_max = []
            temp_min = []
            temp_mean = []
            precipitation = []
            humidity_min = []
            humidity_mean = []
            windvelo_mean = []

            for _, row in found_stations.iterrows():
                station_code = row['station_code']
                lon = row['longitude']
                lat = row['latitude']

                hist_values = self.total_stations_history[
                    ((self.total_stations_history['station_code'] == station_code) &
                     (self.total_stations_history['year'] == year) &
                     (self.total_stations_history['month'] == month))
                ]

                hist_values.reset_index(drop=True, inplace=True)
                if not hist_values.empty:
                    tmp_max = hist_values.iloc[0]['temp_max']
                    tmp_min = hist_values.iloc[0]['temp_min']
                    tmp_mean = hist_values.iloc[0]['temp_mean']
                    precip = hist_values.iloc[0]['precipitation']
                    hum_min = hist_values.iloc[0]['humidity_min']
                    hum_mean = hist_values.iloc[0]['humidity_mean']
                    wind_mean = hist_values.iloc[0]['windvelo_mean']

                    if not pd.isna(tmp_max) and not pd.isna(tmp_min) and \
                            not pd.isna(tmp_mean) and not pd.isna(precip) and \
                            not pd.isna(hum_min) and not pd.isna(hum_mean) and \
                            not pd.isna(wind_mean):

                        temp_max.append([tmp_max])
                        temp_min.append([tmp_min])
                        temp_mean.append([tmp_mean])
                        precipitation.append([precip])
                        humidity_min.append([hum_min])
                        humidity_mean.append([hum_mean])
                        windvelo_mean.append([wind_mean])
                        # distances.append(dist_mat[list(columns).index(station_code)])
                        coords.append([lon, lat])
                else:
                    # logger.info(f"station_code: {station_code}")
                    # logger.info(f"hist_values: {hist_values}")
                    continue

            if len(temp_max) < 1 or len(temp_min) < 1 or len(temp_mean) < 1 or \
                    len(precipitation) < 1 or len(humidity_min) < 1 or \
                    len(humidity_mean) < 1 or len(windvelo_mean) < 1:

                # tmp = [feeder, year, month]
                # tmp.extend([-100] * 7)
                # feeder_periods.loc[len(feeder_periods)] = tmp
                periods_wo_value.append((year, month))
                continue

            try:
                tmp = [feeder, year, month]
                vars = [temp_max, temp_min, temp_mean, precipitation,
                        humidity_min, humidity_mean, windvelo_mean]
                for var in vars:
                    if all(item[0] == 0.0 for item in var):
                        tmp.append(0.0)
                        continue

                    model = create_metamodel(coords, var)
                    if model:
                        pred_points = [[x[0], x[1]] for x in calculated_points]
                        pred_values = model(pred_points)
                        tmp.append(np.mean(pred_values))
                    else:
                        logger.info(
                            f"there is no MetModel for prediction {var}.")

                feeder_periods.loc[len(feeder_periods)] = tmp

            except:
                # tmp = [feeder, year, month]
                # tmp.extend([-100] * 7)
                # feeder_periods.loc[len(feeder_periods)] = tmp
                periods_wo_value.append((year, month))
                continue

            if len(periods_wo_value) > 0:
                # filter feeder_period data frame to find the same month from other years
                for period_pair in periods_wo_value:
                    filtered_df = feeder_periods[feeder_periods['month']
                                                 == period_pair[1]]
                    tmp = [feeder, period_pair[0], period_pair[1]]
                    tmp.append(np.mean(filtered_df['temp_max']))
                    tmp.append(np.mean(filtered_df['temp_min']))
                    tmp.append(np.mean(filtered_df['temp_mean']))
                    tmp.append(np.mean(filtered_df['precipitation']))
                    tmp.append(np.mean(filtered_df['humidity_min']))
                    tmp.append(np.mean(filtered_df['humidity_mean']))
                    tmp.append(np.mean(filtered_df['windvelo_mean']))
                    feeder_periods.loc[len(feeder_periods)] = tmp
                feeder_periods.sort_values(
                    by=['year', 'month'], ignore_index=True, inplace=True)

        return feeder_periods
