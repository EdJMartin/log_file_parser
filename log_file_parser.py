import sqlite3
from collections import namedtuple
import glob

first_parse_list = []
secondary_parse_list = []
lines_to_execute = []

ips = ['64.233','66.102','66.249','72.14','74.125','209.85','216.239','66.184']

def create_database():
    try:
        conn = sqlite3.connect('Logs.db')
        conn.execute(('''CREATE TABLE GOOGLELOGS
       (ID INTEGER PRIMARY KEY,
       IP   TEXT,
       DATE TEXT,
       URL TEXT,
       STATUS TEXT,
       NUMBYTES TEXT,
       USERAGENT TEXT
       );'''))
        conn.commit()
        conn.close()
    except:
        pass

def open_db_connect():
    conn = sqlite3.connect('Logs.db')
    return conn

def primary_parse(filename):
    with open(filename,'r') as file:
        for line in file:
            split_line = line.split(' ')
            userAgent = ''.join(split_line[11:])
            if 'Googlebot' in userAgent:
                first_parse_list.append(line)

def secondary_parse():
    while len(first_parse_list) > 0:
        line = first_parse_list.pop()
        split_line = line.split(' ')
        ip_address = split_line[0]
        for ip in ips:
            if ip_address.startswith(ip):
                secondary_parse_list.append(line)
            else:
                continue

def split_line_database():
    data_line = namedtuple('data_line', 'ipadd date url status numbytes useragent')
    while len(secondary_parse_list) > 0:
        line = secondary_parse_list.pop()
        split_line = line.split(' ')
        ipAddress = split_line[0]
        date = split_line[3].replace('[', '')
        url = split_line[6]
        status = split_line[8]
        Numbytes = split_line[9]
        UserAgent = ''.join(split_line[11:])
        final_line = data_line(ipAddress,date,url,status,Numbytes,UserAgent)
        lines_to_execute.append(final_line)


def main():
    for file in glob.glob('*.proxy004'):
        create_database()
        primary_parse(file)
        secondary_parse()
        database = open_db_connect()
        split_line_database()
        database.executemany('''INSERT INTO GOOGLELOGS(ID,IP,DATE,URL,STATUS,NUMBYTES,USERAGENT)
                                        VALUES(NULL, ?,?,?,?,?,?)''', (lines_to_execute))
        database.commit()
        database.close()

main()
