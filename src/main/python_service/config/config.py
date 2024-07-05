import os

REDIS_HOST_NAME = os.getenv('my-redis') or 'localhost'
REDIS_PORT = 6379
ASSETS_DIR = './assets/'

DB_HOST_NAME = os.getenv('my-mysql') or 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'mysqlR0cks!'
DB_NAME = 'findb'
