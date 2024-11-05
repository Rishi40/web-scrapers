import configparser
import pymysql

config = configparser.ConfigParser()
config.read('credentials.ini')

def connect():
    return pymysql.connect(host = config['client']['host'],
                           user = config['client']['user'],
                           passwd = config['client']['pass'],
                           db = config['client']['db'])

if __name__ == '__main__':
    connect()
