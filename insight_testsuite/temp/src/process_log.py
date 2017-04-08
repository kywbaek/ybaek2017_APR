#!/usr/bin/env python

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generateDicts(f):
    for line in f:
        temp = line.split(' ')
        Dict = {"host": temp[0], "timestamp": line.split('[')[1].split(']')[0], "request": line.split('"')[1],
                "HTTP_reply_code": temp[-2], "bytes": temp[-1][:-1]}
        yield Dict


def feature1_hosts(df, file_path):
    hosts = df['host'].value_counts()[:10]
    hosts.to_csv(file_path)


def feature2_resources(df, file_path):
    df['sum_bytes'] = df.groupby('request')['bytes_int'].transform(np.sum)
    requests = df[['request', 'sum_bytes']].sort_values(by='sum_bytes', ascending=False)
    unique_requests = requests.drop_duplicates(subset='request')[['request', 'sum_bytes']][:10]
    unique_requests['resource'] = unique_requests['request'].apply(lambda x: x.split()[1])
    unique_requests['resource'].to_csv(file_path, header=False, index=False)


def feature3_hours(df, file_path):
    hours = df[['bytes_int', 'pd_datetime']].rolling('3600s', on='pd_datetime').count().sort_values(by='bytes_int', ascending=False)
    hours['bytes_int'] = hours['bytes_int'].astype('int')
    unique_hours = hours.drop_duplicates(subset='pd_datetime')[['pd_datetime', 'bytes_int']][:10]
    unique_hours['pd_datetime'] = unique_hours['pd_datetime'].apply(lambda x: x - timedelta(seconds=3600))
    unique_hours['timestamp_start'] = unique_hours['pd_datetime'].dt.strftime(date_format='%d/%b/%Y:%H:%M:%S').apply(lambda x: x + ' -0400')
    unique_hours[['timestamp_start', 'bytes_int']].to_csv(file_path, header=False, index=False)


def login_to_int(x):
    if x == '200':
        return 1
    else:
        return -1


def get_blocked_index(df, third_failed_login_index):
    blocked_index = []
    for index in third_failed_login_index:
        i = 1
        pd_host = df[index:][(df['host'] == df.ix[index]['host'])]
        end_time = pd_host.iloc[0]['pd_datetime'] + timedelta(seconds=300)
        num_row = pd_host.shape[0]
        while (i < num_row) and (pd_host.iloc[i]['pd_datetime'] < end_time):
            blocked_index.append(pd_host.iloc[i].name)
            i += 1
    return blocked_index


def get_blocked_list(df, blocked_index):
    blocked_list = []
    for index in blocked_index:
        row = df.ix[index]
        line = row['host'] + ' - - [' + row['timestamp'] + '] "' + row['request'] + '" ' + row['HTTP_reply_code'] + ' ' + row['bytes']
        blocked_list.append(line)
    return blocked_list


def feature4_blocked(df, login_to_int, get_blocked_index, get_blocked_list, file_path):
    df_login = df[(df.request == 'POST /login HTTP/1.0')]

    df_login['login_check'] = df_login['HTTP_reply_code'].apply(login_to_int)
    df_login_by_host = df_login[['login_check', 'host', 'pd_datetime']].groupby('host').rolling('20s', on='pd_datetime').sum().reset_index(0, drop=True)
    third_failed_login_index = list(df_login_by_host[(df_login_by_host.login_check == -3)].index)

    blocked_index = get_blocked_index(df, third_failed_login_index)
    blocked_list = get_blocked_list(df, blocked_index)

    blocked_text = '\n'.join(blocked_list) + '\n'
    with open(file_path, 'w+') as out_f:
        out_f.write(blocked_text)


def main():

    log_path, hosts_path, resources_path, hours_path, blocked_path = sys.argv[1:]

    with open(log_path) as f:
        Dict_list = list(generateDicts(f))

    df = pd.DataFrame(Dict_list)

    df['bytes_int'] = df['bytes'].replace('-', 0)
    df['bytes_int'] = df['bytes_int'].astype('int')

    df['pd_datetime'] = pd.to_datetime(df['timestamp'].apply(lambda x: x[:-6]), format='%d/%b/%Y:%H:%M:%S')

    feature1_hosts(df, hosts_path)
    feature2_resources(df, resources_path)
    feature3_hours(df, hours_path)
    feature4_blocked(df, login_to_int, get_blocked_index, get_blocked_list, blocked_path)

if __name__ == "__main__":

    main()
