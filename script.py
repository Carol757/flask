from calendar import weekday
import csv
import os
import pandas as pd
from datetime import datetime, timedelta,time
import pytz
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

def process_csv(df1,df2,df3):
    
    status_df = pd.read_csv(df1)
    hours_df = pd.read_csv(df2)
    timezone_df = pd.read_csv(df3)
    current_timestamp = pd.to_datetime(status_df['timestamp_utc']).max()
    status_df['timestamp'] = pd.to_datetime(status_df['timestamp_utc'])
    status_df['store_timezone'] = timezone_df.set_index('store_id')['timezone_str']
    status_df['timestamp'] = status_df.apply(lambda row: pytz.timezone(row['store_timezone']).localize(row['timestamp']), axis=1)
    merged_df = pd.merge(status_df, hours_df, on='store_id', how='outer')
    merged_df.fillna({'start_time_local': '00:00:00', 'end_time_local': '23:59:59'}, inplace=True)
    merged_df['start_time'] = pd.to_datetime(merged_df['start_time_local'])
    merged_df['end_time'] = pd.to_datetime(merged_df['end_time_local'])
    merged_df['is_business_hour'] = (merged_df['timestamp'].dt.day == merged_df['dayOfWeek']) & \
                                    (merged_df['timestamp'].dt.time >= merged_df['start_time'].dt.time) & \
                                    (merged_df['timestamp'].dt.time <= merged_df['end_time'].dt.time)
    merged_df['is_active'] = (merged_df['status'] == 'active')
    merged_df['uptime_last_week'] = merged_df['is_active'] & merged_df['is_business_hour']
    merged_df['downtime_last_week'] = ~merged_df['is_active'] & merged_df['is_business_hour']
    merged_df['downtime_last_hour'] =pd.to_datetime(merged_df['downtime_last_week']& merged_df['timestamp'].dt.min).max()
    merged_df['uptime_last_hour'] = pd.to_datetime(merged_df['uptime_last_week'] & merged_df['timestamp'].dt.min).max()
    merged_df['downtime_last_day'] =pd.to_datetime(merged_df['downtime_last_week']& merged_df['timestamp'].dt.day ==weekday())*1000
    merged_df['uptime_last_day'] = pd.to_datetime(merged_df['uptime_last_week']& merged_df['timestamp'].dt.day ==weekday())*1000
    store_groups = merged_df.groupby('store_id')
    extrapolated_df = pd.DataFrame()
    for store_id, store_data in store_groups:
        store_data['time_delta'] = store_data['timestamp'].diff().fillna(pd.Timedelta(seconds=0))
        start_time = store_data['start_time'].iloc[0]
        end_time = store_data['end_time'].iloc[0]
        time_index = pd.date_range(start=start_time, end=end_time, freq='1min')
        resampled_data = store_data.set_index('timestamp').resample('1min').asfreq().fillna(method='ffill')
        resampled_data['uptime_extrapolated'] = resampled_data['uptime'].cumsum().reindex(time_index, method='ffill')


    return time.time()
`store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)`