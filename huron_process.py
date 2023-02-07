import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
from IPython.utils.tz import utcfromtimestamp
from constants import *
from ftp_utils import FtpUtils
import time


class HuronProcess:

    def __init__(self, logger):
        self.logger = logger

    def __to_ist_from_timestamp(self, x):
        """To IST Time

        Args:
            x (datetime): Date Time to be converted

        Returns:
            DateTime: In IST time
        """
        return utcfromtimestamp(x) + timedelta(minutes=330)

    def generate_csv_from_json(self, json_data):
        weather_data = []
        inverter_data = []
        mfm_data = []
        scb_data = []
        extra_data = []

        for item in json_data['node_list']:
            if item['node_type'] == 'WS':
                weather_data.append(item)
            elif item['node_type'] == 'SOLAR_EM':
                mfm_data.append(item)
            elif item['node_type'] == 'Inverter':
                inverter_data.append(item)
            elif item['node_type'] == 'SCB':
                scb_data.append(item)
            else:
                extra_data.append(item)

        plant_name = json_data['network_id']
        timestamp_fmt = self.__to_ist_from_timestamp(json_data['packet_timestamp']).strftime("%Y%m%d")
        current_file_name = timestamp_fmt + '.csv'
        # os.mkdir(plant_name)
        if inverter_data:
            inverter_df = self.__get_inverter_df(inverter_data)
            self.__get_csv(inverter_df, 'inverter', plant_name, current_file_name)
        if weather_data:
            weather_df = self.__get_weather_df(weather_data, plant_name)
            self.__get_csv(weather_df, 'WS', plant_name, current_file_name)
        if mfm_data:
            mfm_df = self.__get_mfm_data_df(mfm_data)
            self.__get_csv(mfm_df, 'MFM', plant_name, current_file_name)
        if scb_data:
            scb_df = self.__get_scb_data_df(scb_data)
            self.__get_csv(scb_df, 'SCB', plant_name, current_file_name)

    def __get_local_file(self, dir_name, current_file_name):
        # input => 'Athenese/Inverter_1'
        # data/Athenese/
        # data/Athenese/Inverter_1/
        plant_name = dir_name.split('/')[0]
        try:
            if current_file_name in os.listdir('data/' + dir_name):
                return True
        except:
            try:
                os.mkdir('data/' + plant_name)
            except:
                try:
                    os.mkdir('data/' + dir_name)
                except:
                    pass

        return False

    def __get_csv(self, df, comp, plant_name, current_file_name):
        try:
            total_time_list = []
            down_time_list = []
            for i in range(0, df.shape[0]):
                dir_name = self.__get_ftp_directory(plant_name, comp, i)
                if plant_name in One_Min_Interval_Data_Stations:
                    frequency = 1
                else:
                    frequency = 5
                data = [
                    {'Date': df['Date'].iloc[-1], 'Time': df['Time'].iloc[-1], 'Project_name': plant_name,
                     'Device': dir_name.split('/')[1], 'Status_code': 1, 'frequency': frequency}]
                # self.check_data_received_or_not(data)
                current_file_with_path = 'data/' + dir_name + '/' + current_file_name
                if self.__get_local_file(dir_name, current_file_name):  # Append Data to Existing File
                    df_remote = pd.read_csv(current_file_with_path)
                    time_dt = df.iloc[[i]].iloc[0][1]  # Get Time Data
                    if df_remote[df_remote['Time'] == time_dt].shape[0] == 0:  # Check if data exists
                        df_remote = pd.concat([df_remote, df.iloc[[i]]], axis=0, ignore_index=True)
                        df_remote = df_remote.sort_values(by='Time')
                        if comp == 'inverter':
                            total_time = len(df_remote[df_remote['node_id'].astype(float) > 0].index) * 5
                            down_time = total_time - len(df_remote[df_remote['ac_power'].astype(float) > 0].index) * 5
                            total_time_list.append(total_time)
                            down_time_list.append(down_time)

                        if comp == 'inverter' and i == (df.shape[0] - 1):
                            try:
                                total_time = int(max(total_time_list))
                                down_time = int(sum(down_time_list))
                                df_remote['plant_availability'] = (1 - (
                                        (1003.2 * down_time) / (1003.2 * total_time))) * 100
                                df_remote['grid_availability'] = ((total_time - down_time) / total_time) * 100
                            except Exception as e:
                                self.logger.error(f'Error in Availability of plant {plant_name} :{e}')

                        if comp == 'WS':
                            df_remote.insert(7, 'node_id', df_remote.pop('node_id'))
                            df_remote['Daily POA Energy'] = df_remote['POA Instantaneous Energy'].astype(
                                'float').cumsum()
                            if 'timestamp' in df_remote.columns:
                                df_remote = df_remote.drop(['timestamp'], axis=1)
                            if 'node_type' in df_remote.columns:
                                df_remote.drop(['node_type'], axis=1, inplace=True)
                            wms_columns = ['Date', 'Time', 'irradience', 'ambient_temp', 'ghi', 'irradiance_tdf',
                                           'temperature', 'node_id', 'POA Instantaneous Energy', 'Daily POA Energy']
                            df_remote = pd.concat([pd.DataFrame(columns=wms_columns), df_remote])

                        if Time_Now >= Condition_Time:
                            if plant_name in One_Min_Interval_Data_Stations:
                                frequency = 1
                            else:
                                frequency = 5
                            # pass hold parameters up-to midnight
                            if comp == 'inverter':
                                hold_list = Hold_Parameter_List_Before_Mid_Night_Inverter
                                hold_list1 = Hold_Parameter_After_Mid_Night_Inverter

                            elif comp == 'MFM':
                                hold_list = Hold_Parameter_List_Before_Mid_Night_MFM
                                hold_list1 = Hold_Parameter_After_Mid_Night_MFM

                            elif comp == 'WS':
                                hold_list = Hold_Parameter_List_Before_Mid_Night_WS
                                hold_list1 = Hold_Parameter_After_Mid_Night_WS

                            elif comp == 'SCB':
                                hold_list = Hold_Parameter_List_Before_Mid_Night_InverterMPPT
                                hold_list1 = Hold_Parameter_After_Mid_Night_InverterMPPT

                            df_remote = self.__up_to_midnight(df_remote, hold_list, hold_list1, frequency)
                            df_remote.to_csv(current_file_with_path, index=False)
                        else:
                            df_remote.to_csv(current_file_with_path, index=False)

                        cnt = 0
                        while cnt < 3:
                            if FtpUtils(self.logger).save_file_to_ftp(current_file_with_path, dir_name):
                                cnt = 3
                            else:
                                cnt += 1
                                self.logger.info('Retrying after 1 sec')
                                time.sleep(1)
                else:
                    df.iloc[[i]].to_csv(current_file_with_path, index=False)
                    cnt = 0
                    while cnt < 3:
                        if FtpUtils(self.logger).save_file_to_ftp(current_file_with_path, dir_name):
                            cnt = 3
                        else:
                            cnt += 1
                            self.logger.info('Retrying after 1 sec')
                            time.sleep(1)
        except Exception as e:
            self.logger.error(f'Error in adding time stamp:{e}')

    def __get_ftp_directory(self, plant_name, comp, count):
        if comp == 'inverter':
            dir = plant_name + '/Inverter_' + str(count + 1)
        elif comp == 'MFM':
            dir = plant_name + '/MFM'
        elif comp == 'WS':
            dir = plant_name + '/WS'
        elif comp == 'SCB':
            dir = plant_name + '/SCB_' + str(count + 1)
        else:
            dir = ''
        return dir

    def __generate_flat_data(self, final_items, item, parent_name):
        if type(item) is list:
            for i in range(len(item)):
                child_name = str(i) if parent_name == "" else parent_name + '/' + str(i)
                final_items = self.__generate_flat_data(final_items, item[i], child_name)
        elif type(item) is str:
            final_items[parent_name] = item
        elif type(item) is int:
            final_items[parent_name] = item
        else:
            for key, value in item.items():
                child_name = key if parent_name == "" else parent_name + '/' + key
                final_items = self.__generate_flat_data(final_items, value, child_name)
        return final_items

    def __get_inverter_df(self, inverter_data):
        col_inv = ["Date", "Time", "node_id", "ac_power", "apparant_power", "dc_bus_voltage", "dc_current", "dc_power",
                   "dc_power_smb", "efficiency", "energy", "energy_5min", "energy_today", "grid_curnt_1",
                   "grid_curnt_2", "grid_curnt_3", "grid_down", "grid_frequency", "grid_voltage_v12",
                   "grid_voltage_v23", "grid_voltage_v31", "inv_down", "inv_temp", "inverter_loss", "pf",
                   "reactive_power", "alarm/01", "alarm/02", "alarm/03", "alarm/04", "alarm/05", "alarm/06", "alarm/07",
                   "alarm/08", "alarm/09", "alarm/10", "alarm/11", "alarm/12", "dc_bus_voltages/0", "dc_bus_voltages/1",
                   "dc_bus_voltages/2", "dc_bus_voltages/3", "dc_bus_voltages/4", "dc_bus_voltages/5",
                   "dc_bus_voltages/6", "dc_bus_voltages/7", "dc_bus_voltages/8", "dc_bus_voltages/9",
                   "dc_bus_voltages/10", "dc_bus_voltages/11", "dc_bus_voltages/12", "dc_bus_voltages/13",
                   "dc_bus_voltages/14", "dc_bus_voltages/15", "dc_bus_voltages/16", "dc_bus_voltages/17",
                   "dc_bus_voltages/18", "dc_bus_voltages/19", "dc_currents/0", "dc_currents/1", "dc_currents/2",
                   "dc_currents/3", "dc_currents/4", "dc_currents/5", "dc_currents/6", "dc_currents/7", "dc_currents/8",
                   "dc_currents/9", "dc_currents/10", "dc_currents/11", "dc_currents/12", "dc_currents/13",
                   "dc_currents/14", "dc_currents/15", "dc_currents/16", "dc_currents/17", "dc_currents/18",
                   "dc_currents/19", "mppt_currents/0", "mppt_currents/1", "mppt_currents/2", "mppt_currents/3",
                   "mppt_currents/4", "mppt_currents/5", "mppt_currents/6", "mppt_currents/7", "mppt_currents/8",
                   "mppt_currents/9", "mppt_powers/0", "mppt_powers/1", "mppt_powers/2", "mppt_powers/3",
                   "mppt_powers/4", "mppt_powers/5", "mppt_powers/6", "mppt_powers/7", "mppt_powers/8", "mppt_powers/9",
                   "mppt_voltages/0", "mppt_voltages/1", 'mppt_voltages/2', 'mppt_voltages/3', 'mppt_voltages/4',
                   'mppt_voltages/5', 'mppt_voltages/6', 'mppt_voltages/7', 'mppt_voltages/8', 'mppt_voltages/9']
        df1 = pd.DataFrame(columns=col_inv)
        inverter_info = []
        for inverter in inverter_data:
            final_items = {}
            final_items = self.__generate_flat_data(final_items, item=inverter, parent_name="")
            inverter_info.append(final_items)
        df = pd.DataFrame(inverter_info)
        df['Date'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%Y/%m/%d")
        df['Time'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%H:%M:%S")
        cols = list(df.columns[-2:]) + list(df.columns[:-3])
        df = df[cols]
        df.drop(['node_type'], axis=1, inplace=True)
        df.columns = [c.replace('sensors/', '') for c in df.columns]
        df1 = pd.concat([df1, df], axis=0)
        return df1

    def __get_weather_df(self, weather_data, plant_name):
        col_ws = ['Date', 'Time', 'irradience', 'ambient_temp', 'ghi', 'irradiance_tdf', 'temperature']
        df1 = pd.DataFrame(columns=col_ws)
        weather_info = []
        for weather in weather_data:
            final_items = {}
            final_items = self.__generate_flat_data(final_items, item=weather, parent_name="")
            weather_info.append(final_items)
        df = pd.DataFrame(weather_info)
        df['Date'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%Y/%m/%d")
        df['Time'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%H:%M:%S")
        cols = list(df.columns[-2:]) + list(df.columns[:-3])
        df = df[cols]
        if 'node_type' in df.columns:
            df.drop(['node_type'], axis=1, inplace=True)
        df.columns = [c.replace('sensors/', '') for c in df.columns]
        df1 = pd.concat([df1, df], axis=0)
        if 'timestamp' in df1.columns:
            df1 = df1.drop(['timestamp'], axis=1)
        if plant_name in One_Min_Interval_Data_Stations:
            df1['POA Instantaneous Energy'] = df1['irradience'].astype('float') / 60
        else:
            df1['POA Instantaneous Energy'] = df1['irradience'].astype('float') / 12
        return df1

    def __get_mfm_data_df(self, mfm_data):
        col_mfm = ['Date', 'Time', 'freq', 'fwd_run_secs', 'i_b', 'i_r', 'i_y', 'kvah_f',
                   'kvah_r', 'kvarh_f', 'kvarh_r', 'kwh_f', 'kwh_f_5min', 'kwh_f_today',
                   'kwh_r', 'pf', 'pow_act', 'pow_app', 'pow_react', 'rev_run_secs',
                   'v_br', 'v_ry', 'v_yb']
        df1 = pd.DataFrame(columns=col_mfm)
        mfm_info = []
        for mfm in mfm_data:
            final_items = {}
            final_items = self.__generate_flat_data(final_items, item=mfm['sensors'], parent_name="")
            mfm_info.append(final_items)

        final_mfm_info = {}
        for m in mfm_info:
            for key, item in m.items():
                final_mfm_info[key] = item
        final_mfm_info['timestamp'] = mfm_data[0]['timestamp']
        df = pd.DataFrame([final_mfm_info])
        df['Date'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%Y/%m/%d")
        df['Time'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%H:%M:%S")
        df = df[col_mfm]
        df1 = pd.concat([df1, df], axis=0)
        return df1

    def __get_scb_data_df(self, scb_data):
        col_scb = ['Date', 'Time', 'Switch', 'Power', 'SPD', 'temperature', 'total_current', 'voltage', 'current/0',
                   'current/1', 'current/2', 'current/3', 'current/4', 'current/5', 'current/6', 'current/7',
                   'current/8', 'current/9', 'current/10', 'current/11', 'current/12', 'current/13']
        df1 = pd.DataFrame(columns=col_scb)
        scb_info = []
        for scb in scb_data:
            final_items = {}
            final_items = self.__generate_flat_data(final_items, item=scb, parent_name="")
            scb_info.append(final_items)
        df = pd.DataFrame(scb_info)
        df['Date'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%Y/%m/%d")
        df['Time'] = df['timestamp'].apply(self.__to_ist_from_timestamp).dt.strftime("%H:%M:%S")
        cols = list(df.columns[-2:]) + list(df.columns[:-3])
        df = df[cols]
        df.drop(['node_type'], axis=1, inplace=True)
        df.columns = [c.replace('sensors/', '') for c in df.columns]
        df1 = pd.concat([df1, df], axis=0)
        return df1

    def __fill_missing_values(self, df, freq):
        """
        Fills missing timestamp values by NAN value

        :param df: Dataframe
        :return: Filled missing values dataframe
        """
        try:
            first_time = df.Time.min()
            last_time = df.Time.max()
            sr1 = pd.Series(pd.date_range(first_time, last_time, freq=f'{int(freq)} min'))
            df2 = pd.DataFrame(dict(time_stamps=sr1))
            y = pd.to_datetime(df2['time_stamps'])
            df2['Date'] = y.dt.strftime("%Y/%m/%d")
            df2['Time'] = y.dt.strftime("%H:%M:%S")
            df2 = df2.drop(['time_stamps'], axis=1)
            result = pd.merge(df, df2, how='left', on=['Date', 'Time'])
            result.replace(np.nan, 'NaN', inplace=True)
            return result
        except Exception as e:
            self.logger.error(f"Error in fill missing value {e}")

    def __after_midnight(self, df, hold_list, freq):
        try:
            temp = df.Date.iloc[-1]
            if freq == 1.0:
                periods = 360
            else:
                periods = 72
            curr_date_temp = datetime.datetime.strptime(temp, "%Y/%m/%d")
            new_date = curr_date_temp + datetime.timedelta(days=1)
            sr1 = pd.Series(pd.date_range(new_date, periods=periods, freq=f'{freq} min'))
            df2 = pd.DataFrame(dict(time_stamps=sr1))
            y = pd.to_datetime(df2['time_stamps'])
            df2['Date'] = y.dt.strftime("%Y/%m/%d")
            df2['Time'] = y.dt.strftime("%H:%M:%S")
            df2 = df2.drop(['time_stamps'], axis=1)

            none_value_list = []
            for i in df.columns:
                if df[i].iloc[-1] == 'NaN':
                    none_value_list.append(i)

            for i in none_value_list:
                df2[i] = df[i].iloc[-1]

            for i in hold_list:
                if i not in ['Date', 'Time']:
                    df2[i] = df[i].iloc[-1]
            zero_cols = [x for x in df.columns if x not in hold_list + none_value_list]
            df2[zero_cols] = 0
            #  df = df2.join(pd.DataFrame(0, df2.index, zero_cols))
            df = pd.concat([df, df2])
            return df
        except Exception as e:
            self.logger.error(f"Error in after midnight {e}")

    def __up_to_midnight(self, df, hold_list, hold_list1, frequency):
        """
        Up-to midnight add time stamp

        :param hold_list1:
        :param df: Dataframe
        :param hold_list: List of column tags which you want to hold
        :return: Dataframe
        """
        try:
            time = df.Time.iloc[-1]

            df = self.__fill_missing_values(df, frequency)
            last_time = datetime.datetime.strptime(time, "%H:%M:%S") + datetime.timedelta(minutes=frequency)
            last_time = datetime.datetime.strftime(last_time, "%H:%M:%S")
            sr1 = pd.Series(pd.date_range(last_time, '23:59:00', freq=f'{frequency} min'))
            df2 = pd.DataFrame(dict(time_stamps=sr1))
            y = pd.to_datetime(df2['time_stamps'])
            df2['Date'] = y.dt.strftime("%Y/%m/%d")
            df2['Time'] = y.dt.strftime("%H:%M:%S")
            df2 = df2.drop(['time_stamps'], axis=1)
            none_value_list = []
            for i in df.columns:
                if df[i].iloc[-1] == 'NaN':
                    none_value_list.append(i)

            for i in none_value_list:
                df2[i] = df[i].iloc[-1]

            for i in hold_list:
                if i not in ['Date', 'Time']:
                    df2[i] = df[i].iloc[-1]
            zero_cols = [x for x in df.columns if x not in hold_list + none_value_list]
            df2[zero_cols] = 0
            # df2 = df2.join(pd.DataFrame(0, df2.index, zero_cols))

            data_df1 = pd.concat([df, df2])

            # after 12
            data_df = self.__after_midnight(data_df1, hold_list1, frequency)
            return data_df
        except Exception as e:
            self.logger.error(f"Error in up-to midnight {e}")

    def check_data_received_or_not(self, data):
        try:
            file_name = 'check_data.csv'
            check_df = pd.DataFrame(data)
            if os.path.exists(file_name):
                check_df.to_csv(file_name, index=False, mode='a', header=False)
            else:
                check_df.to_csv(file_name, index=False)
            FtpUtils(self.logger).save_csv_dir(file_name, destination_dir="check_data")
        except Exception as e:
            self.logger.error('Exception in check data received or not process')
            self.logger.error(e)
