import datetime

# Time condition for
Time_Now = datetime.datetime.utcnow()
Condition_Time = datetime.datetime(Time_Now.year, Time_Now.month, Time_Now.day, 13, 00, 00)

# for Inverter
Hold_Parameter_List_Before_Mid_Night_Inverter = ['energy', 'energy_5min', 'energy_today', 'Date', 'Time']
Hold_Parameter_After_Mid_Night_Inverter = ['energy', 'Date', 'Time']

# for WS
Hold_Parameter_List_Before_Mid_Night_WS = ['Date', 'Time']
Hold_Parameter_After_Mid_Night_WS = ['Date', 'Time']

# for MFM
Hold_Parameter_List_Before_Mid_Night_MFM = ['kvah_f', 'kvah_r', 'kvarh_f', 'kvarh_r', 'kwh_f', 'kwh_f_5min',
                                            'kwh_f_today',
                                            'kwh_r', 'Date', 'Time']

Hold_Parameter_After_Mid_Night_MFM = ['kvah_f', 'kvah_r', 'kvarh_f', 'kvarh_r', 'kwh_f', 'kwh_r', 'Date', 'Time']

# for Inverter_MPPT
Hold_Parameter_List_Before_Mid_Night_InverterMPPT = ['Date', 'Time']
Hold_Parameter_After_Mid_Night_InverterMPPT = ['Date', 'Time']

One_Min_Interval_Data_Stations = ['EMCO', 'HCIL', 'LUMAX001', 'IVL', 'Diwana', 'RMU056']
