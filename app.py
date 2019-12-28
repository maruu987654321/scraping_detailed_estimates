import requests   #import library
from bs4 import BeautifulSoup
import pandas as pd
import datetime 
import os.path
from os import path
import sqlalchemy
import mysql.connector
import pymysql
import yaml
from IPython.display import display, HTML
import re
from json import loads

def load_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def write_data_database(df, name_stock):
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()


    df.to_sql(con=database_connection, name='{}'.format(name_stock), if_exists='replace',chunksize=100, index=True)
    database_connection.close()

def create_database(df, name_stock):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user=database_username,
            passwd=database_password,
            )

        mycursor = mydb.cursor()

        mycursor.execute("CREATE DATABASE {}".format(database_name)) 
        write_data_database(df, name_stock)

    except mysql.connector.errors.DatabaseError:

        write_data_database(df, name_stock)

new_zacks_current_week = []
zacks_current_week = []
yahoo_current_week = []
all_value_zack = []
all_value_yahoo = []
all_value_zack.append('')
all_value_yahoo.append('')
title_first_column = ['Earning Estimate', 'Zack Consensus Estimate', 'Zack Most Recent Consensus', 'Zacks Most Accurate Estimate', 'No of Analyst / Estimates', 'Avg. Estimate', 'Low Estimate', 'High Estimate', 'Year Ago EPS', 'Year over Year Growth Est.', 'Revenue / Sales Estimate', 'Zack Consensus Estimate', 'No. of Analyst', 'Avg. Estimate', 'Low Estimate', 'High Estimate', 'Year Ago Sales', 'Sales Growth (year/est)', 'Earning history', 'EPS Est.', 'EPS Actual', 'Difference', 'Surprise %', 'EPS Trend', 'Current Estimate', 'Up 7 Days Ago', 'Up 30 Days Ago', 'Up 60 Days Ago', 'Up 90 Days Ago', 'EPS Revision', 'Up Last 7 Days', 'Up Last 30 Days', 'Up Last 60 Days', 'Down Last 7 Days', 'Down Last 30 Days', 'Down Last 60 Days', 'Growth Estimates', 'Current Qtr.', 'Next Qtr.', 'Current Year', 'Next Year', 'Next 5 Years (per annum)', 'Past 5 Years (per annum)', 'Zacks Upside', 'Zacks Most Accurate Estimate', 'Zacks Consensus Estimate', 'Earning Expected Surprise','Zacks Rank', 'Yahoo Rank']

def read_stocks(name_file_with_stock):
    df = pd.read_excel (name_file_with_stock) 
    list_symbol = df[df.columns[0]].to_list()
    return list_symbol

def parse_html_table(table):
    n_columns = 0
    n_rows=0
    column_names = []
    
    for row in table.find_all('tr'):
                
        td_tags = row.find_all('td')
        if len(td_tags) > 0:
            n_rows+=1
            if n_columns == 0:
                n_columns = len(td_tags)
                        
        th_tags = row.find_all('th') 
        if len(th_tags) > 0 and len(column_names) == 0:
            for th in th_tags:
                column_names.append(th.get_text())
    
    if len(column_names) > 0 and len(column_names) != n_columns:
        raise Exception("Column titles do not match the number of columns")
    
    columns = column_names if len(column_names) > 0 else range(0,n_columns)
    df = pd.DataFrame(columns = columns, index= range(0,n_rows))
    row_marker = 0
    for row in table.find_all('tr'):
        column_marker = 0
        columns = row.find_all('td')
        for column in columns:
            df.iat[row_marker,column_marker] = column.get_text()
            column_marker += 1
        if len(columns) > 0:
            row_marker += 1
                    
    for col in df:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass
            
    return df

def main_info_zacks():
    try:
        rank = soup_zack.find_all("p", {"class":"rank_view"})
        r = rank[0].text
        r = r.strip()
        r = int(r[0])
    except IndexError:
        r = ''
    all_value_zack = []
    lst = df.iloc[:,1].to_list()
    all_value_zack.append(' ')
    all_value_zack.append(lst[0])
    all_value_zack.append(lst[2])
    all_value_zack.append(df_mst_acc_est_zack.iloc[:,1].to_list()[0])
    all_value_zack.append(lst[1])
    all_value_zack.append(' ')
    all_value_zack.append(lst[4])
    all_value_zack.append(lst[3])
    all_value_zack.append(lst[5])
    all_value_zack.append(lst[-1])
    all_value_zack.extend([' ' for i in range(1)])
    lst_revenue_zack = get_sales_zacks(soup_zack)
    val_revenue_zack = lst_revenue_zack[0].replace('B', '')
    all_value_zack.append(val_revenue_zack)
    all_value_zack.append(lst_revenue_zack[1])
    all_value_zack.append(' ')
    all_value_zack.append(lst_revenue_zack[3].replace('B', ''))
    all_value_zack.append(lst_revenue_zack[2].replace('B', ''))
    all_value_zack.append(lst_revenue_zack[4].replace('B', ''))
    all_value_zack.append(lst_revenue_zack[5].replace('%', ''))
    all_value_zack.append(' ')
    lst_consensus_trend = get_reported_earnings_history(soup_zack)
    all_value_zack.append(lst_consensus_trend[1])
    all_value_zack.append(lst_consensus_trend[0])
    all_value_zack.append(lst_consensus_trend[2])
    all_value_zack.append(lst_consensus_trend[3].replace('%', ''))
    all_value_zack.append(' ')
    lst_magnitude_second = get_magnitude_consensus_estimate_trend(soup_zack)
    all_value_zack.extend(lst_magnitude_second)
    all_value_zack.append(' ')
    lst_revision_zack = get_revision_zack(soup_zack)
    lst_revision_zack_second = [i for i in lst_revision_zack]
    all_value_zack.extend(lst_revision_zack_second)
    all_value_zack.append(' ')
    res_earning_lst_row_second = earning_estimates_last_row(soup_zack)
    all_value_zack.extend(res_earning_lst_row_second)
    all_value_zack.extend([' ' for i in range(3)])
    upside = [i.replace('%', '') for i in df_mst_acc_est_zack.iloc[:,1].to_list()]
    all_value_zack.extend(upside)
    all_value_zack.append(r)
    all_value_zack.append(' ')
    return all_value_zack

def main_info_yahoo():
    all_value_yahoo = []
    all_value_yahoo.extend([' ' for i in range(4)])
    all_value_yahoo.append(No_analyst_estimates[0])
    all_value_yahoo.append(No_analyst_estimates[1])
    all_value_yahoo.append(No_analyst_estimates[2])
    all_value_yahoo.append(No_analyst_estimates[3])
    all_value_yahoo.append(No_analyst_estimates[4])
    all_value_yahoo.extend([' ' for i in range(3)])
    lst_revenue_yahoo = get_revenue_estimate_yahoo(soup_yahoo)
    lst_revenue_yahoo2 = []
    for i in lst_revenue_yahoo:
        i = i.replace('B', '')
        i = i.replace('%', '')
        lst_revenue_yahoo2.append(i)
    all_value_yahoo.extend(lst_revenue_yahoo2) 
    all_value_yahoo.extend(' ')
    lst_history2 = get_earning_history2(soup_yahoo)
    lst_history2 = [i.replace('%', '') for i in lst_history2]
    all_value_yahoo.extend(lst_history2)
    all_value_yahoo.append(' ')
    lst_eps = get_eps_trend(soup_yahoo)
    all_value_yahoo.extend(lst_eps)
    all_value_yahoo.extend([' ' for i in range(1)])
    lst_rev = get_revision(soup_yahoo)
    lst_rev.insert(2, ' ')
    all_value_yahoo.extend(lst_rev)
    all_value_yahoo.extend([' ' for i in range(2)])
    growth_yahoo = get_growth_yahoo(soup_yahoo)
    growth_yahoo = [(i.replace('%', '')) for i in growth_yahoo]
    all_value_yahoo.extend(growth_yahoo)
    all_value_yahoo.extend([' ' for i in range(5)])
    try:
        rank = get_rank_yahoo(soup_yahoo)
        all_value_yahoo.append(rank)
    except KeyError:
        rank = ''
    return all_value_yahoo

def get_earning_estimate (name_stock):
    url_zacks = 'https://www.zacks.com/stock/quote/{}/detailed-estimates'.format(name_stock)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    main_info = requests.get(url_zacks, headers=headers).text
    soup_zack = BeautifulSoup(main_info, 'lxml')
    try:
        toRemove = soup_zack.findAll('table')
        table = toRemove[9]
        df = parse_html_table(table)
        today_list = list(df.columns.values)[1]
        date = today_list
        date_zack = date + ' ' + 'Zacks Current Week'
        date_zack = date_zack.replace(')', '')
        date_zack = date_zack.replace('(', '')
        table_most_acc_est_zack = toRemove[12]
        df_mst_acc_est_zack = parse_html_table(table_most_acc_est_zack)
    except (AttributeError, IndexError):
        soup_zack = []
        date_zack = 'Current Quarter Zacks Current Week'
        df = []
        df_mst_acc_est_zack = []
    return soup_zack, df, date_zack, df_mst_acc_est_zack


def get_info_for_yahoo(name_stock):
    url_yahoo = 'https://finance.yahoo.com/quote/{}/analysis?p={}&.tsrc=fin-srch'.format(name_stock, name_stock)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    main_info = requests.get(url_yahoo, headers=headers).text
    soup_yahoo = BeautifulSoup(main_info, 'lxml')
    table = soup_yahoo.find("table",{"class":"W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    try:
        table1 = parse_html_table(table)
        No_analyst_estimates = table1.iloc[:,1].to_list()
        today_list_yahoo = list(table1.columns.values)[1]
        date_yahoo = today_list_yahoo + ' ' + 'Yahoo Current Week'
        date_yahoo = date_yahoo.replace(')', '')
        date_yahoo = date_yahoo.replace('(', '')
    except AttributeError:
        soup_yahoo = []
        No_analyst_estimates = []
        date_yahoo = 'Current Quarter Yahoo Current Week'
    return soup_yahoo, No_analyst_estimates, date_yahoo

def get_reported_earnings_history(soup_zack):
    toRemove = soup_zack.findAll('table')
    tb = parse_html_table(toRemove[13])
    lst_consensus_trend  =  tb.iloc[:,1].to_list()
    return lst_consensus_trend

def get_rank_yahoo(soup_yahoo):
    script = soup_yahoo.find("script",text=re.compile("root.App.main")).text
    data = loads(re.search("root.App.main\s+=\s+(\{.*\})", script).group(1))
    stores = data["context"]["dispatcher"]["stores"]
    rank = stores[u'QuoteSummaryStore']['financialData']['recommendationMean']['raw']
    return rank

def earning_estimates_last_row(soup_zack):
    toRemove = soup_zack.findAll('table')
    tb = parse_html_table(toRemove[9])
    res_earning_lst_row = tb.values[-1].tolist()
    res_earning_lst_row_second = []
    for i in res_earning_lst_row[1:]:
        i = i.replace('%', '')
        res_earning_lst_row_second.append(i)
    return res_earning_lst_row_second


def get_magnitude_consensus_estimate_trend(soup_zack):
    toRemove = soup_zack.findAll('table')
    tb = parse_html_table(toRemove[11])
    lst_magnitude_first  =  tb.iloc[:,1].to_list()
    lst_magnitude_second = []
    for i in lst_magnitude_first:
        lst_magnitude_second.append(i)
    return lst_magnitude_second

def get_sales_zacks(soup_zack):
    toRemove = soup_zack.findAll('table')
    tb = parse_html_table(toRemove[8])
    lst_revenue_zack  =  tb.iloc[:,1].to_list()
    return lst_revenue_zack

def get_revenue_estimate_yahoo(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"class": "W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    revenue = table[1]
    table1 = parse_html_table(revenue)
    lst_revenue_yahoo = table1.iloc[:,1].to_list()
    return lst_revenue_yahoo

def get_earning_history(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"class": "W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    history = table[2]
    table1 = parse_html_table(history)
    lst_history = table1.iloc[:,1].to_list()
    return lst_history

def get_earning_history2(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"class": "W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    history = table[2]
    table1 = parse_html_table(history)
    lst_history2 = table1.iloc[:,-1].to_list()
    return lst_history2

def get_eps_trend(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"class": "W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    eps = table[3]
    table1 = parse_html_table(eps)
    lst_eps = table1.iloc[:,1].to_list()
    return lst_eps

def get_revision(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"class": "W(100%) M(0) BdB Bdc($seperatorColor) Mb(25px)"})
    revision = table[4]
    table1 = parse_html_table(revision)
    lst_rev = table1.iloc[:,1].to_list()
    return lst_rev

def get_revision_zack(soup_zack):
    toRemove = soup_zack.findAll('table')
    tb = parse_html_table(toRemove[10])
    lst_revision_zack  =  tb.iloc[:,1].to_list()
    return lst_revision_zack

def get_growth_yahoo(soup_yahoo):
    table =  soup_yahoo.findAll("table", {"W(100%) M(0) BdB Bdc($c-fuji-grey-c) Mb(25px)"})
    revenue = table[0]
    table1 = parse_html_table(revenue)
    growth_yahoo  =  table1.iloc[:,1].to_list()
    return growth_yahoo


def _color_if_even(s):
    return ['font-weight: bold' if val == 'Earning Ectimate' or val == 'Revenue / Sales Estimate' or val == 'Earning history' or val == 'EPS Trend' or val == 'EPS Revision' or val == 'Growth Estimates' or val == 'Zacks Upside' else '' for val in s]

def write_data(all_value_yahoo, all_value_zack, name_stock, date_zack, date_yahoo):
    
    try:
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                    format(database_username, database_password, 
                                                        database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
        df = pd.read_sql('SELECT * FROM {}'.format(name_stock), con=database_connection)
        yahoo_current_week = []
        zacks_current_week = []
        zacks_current_week2 = []
        yahoo_current_week2 = []
        for i in df.columns:
            if 'Zacks' in i:
                zacks_current_week.append(df[i])
            
        for i, j in zip(zacks_current_week, range(1, len(zacks_current_week) + 1)):
            name = zacks_current_week[0].name
            new_sr_z = i.rename(name + ' ' + '-' + ' ' + str(j))
            zacks_current_week2.append(new_sr_z)
         
        for i in df.columns:
            if 'Yahoo' in i:
                yahoo_current_week.append(df[i])
            
        for i, j in zip(yahoo_current_week, range(1, len(yahoo_current_week) + 1)):
            name = yahoo_current_week[0].name
            new_sr_y = i.rename(name + ' ' + '-' + ' ' + str(j))
            yahoo_current_week2.append(new_sr_y)

        if len(yahoo_current_week2) < 13:
            s1 = pd.Series(title_first_column, name=name_stock)
            s2_yahoo = pd.Series(all_value_yahoo, name=date_yahoo)
            yahoo_current_week2.insert(0,s2_yahoo)
            yahoo_current_week2.insert(0, s1)
            result_yahoo = pd.concat(yahoo_current_week2, axis=1)
            s2_zacks = pd.Series(all_value_zack, name=date_zack)
            zacks_current_week2.insert(0, s2_zacks)
            result_zacks = pd.concat(zacks_current_week2, axis=1)
            result_new = pd.concat([result_yahoo, result_zacks], axis=1)   
            result_new.to_sql(con=database_connection, name='{}'.format(name_stock), if_exists='replace',chunksize=100, index=True)
        else:
            s1 = pd.Series(title_first_column, name=name_stock)
            s2_zacks = pd.Series(all_value_zack, name=date_zack)
            s2_yahoo = pd.Series(all_value_yahoo, name=date_yahoo)
            y_more_twelve = []
            z_more_twelve = []
            z_more_twelve2 = []
            y_more_twelve2 = []
            
            for i in df.columns:
                if 'Zacks' in i:
                    z_more_twelve.append(df[i]) 
                    
            for i in df.columns:
                if 'Yahoo' in i:
                    y_more_twelve.append(df[i]) 
                    
            z_more_twelve = z_more_twelve[:-1]
            y_more_twelve = y_more_twelve[:-1]
            
            for i, j in zip(z_more_twelve, range(1, len(z_more_twelve) + 1)):
                name = z_more_twelve[0].name
                new_sr_z = i.rename(name + ' ' + '-' + ' ' + str(j))
                z_more_twelve2.append(new_sr_z)
                
            for i, j in zip(y_more_twelve, range(1, len(y_more_twelve) + 1)):
                name = y_more_twelve[0].name
                new_sr_y = i.rename(name + ' ' + '-' + ' ' + str(j))
                y_more_twelve2.append(new_sr_y)
            
            z_more_twelve2.insert(0, s2_zacks)
            z_more_twelve2.insert(0, s1)
            res_zacks = pd.concat(z_more_twelve2, axis=1)
            y_more_twelve2.insert(0, s2_yahoo)
            y_more_twelve2.insert(0, s1)
            res_yahoo = pd.concat(y_more_twelve2, axis=1)
            result_new = pd.concat([res_yahoo, res_zacks], axis=1) 
            result_new.to_sql(con=database_connection, name='{}'.format(name_stock), if_exists='replace',chunksize=100, index=True)

    except sqlalchemy.exc.ProgrammingError: 
        s1 = pd.Series(title_first_column, name=name_stock)
        s2 = pd.Series(all_value_yahoo, name=date_yahoo)
        s3 = pd.Series(all_value_zack, name=date_zack)
        frames = [s1, s2, s3]
        result = pd.concat(frames, axis=1)
        create_database(result, name_stock)
   
   
if __name__ == '__main__':
    config = load_config('config.yaml')
    database_username = config['database_username']   #type your username
    database_password =  config['password']   #type your password
    database_ip       =  config['database_ip']
    database_name     = config['name_database']  #type name your database
    name_file_with_stock = config['name_file_with_stocks'] #name of file where lie all stocks
    list_symbol = read_stocks(name_file_with_stock)
    for item in list_symbol:
        print(item)
        soup_zack, df, date_zack, df_mst_acc_est_zack  = get_earning_estimate (item)
        soup_yahoo, No_analyst_estimates, date_yahoo = get_info_for_yahoo(item)
        if len(soup_yahoo) == 0 and len(soup_zack) != 0:
            all_value_zack = main_info_zacks()
            all_value_yahoo = []
            all_value_yahoo.extend([' ' for i in range(len(all_value_zack))]) 
            name_stock = item
            write_data(all_value_yahoo, all_value_zack, name_stock, date_zack, date_yahoo)
            
        if len(soup_zack) == 0 and len(soup_yahoo) != 0:
            all_value_yahoo = main_info_yahoo()
            all_value_zack = []
            all_value_zack.extend([' ' for i in range(len(all_value_yahoo))]) 
            name_stock = item
            write_data(all_value_yahoo, all_value_zack, name_stock, date_zack, date_yahoo)
           
        if len(soup_zack) != 0 and len(soup_yahoo) != 0:
            all_value_zack = main_info_zacks()
            all_value_yahoo = main_info_yahoo()
            name_stock = item
            write_data(all_value_yahoo, all_value_zack, name_stock, date_zack, date_yahoo)
 