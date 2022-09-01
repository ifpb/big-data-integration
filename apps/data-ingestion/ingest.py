# import libraries
import os
import sys
import argparse
import warnings
from dotenv import load_dotenv
from datastore.mssql.mssql import MSSQL
from datastore.postgres.postgres import Postgres
from datastore.mysql.mysql import MySQL

from objects.agent import Agent
from objects.credit_card import CreditCard
from objects.users import Users
from objects.musics import Musics
from objects.movies import Movies
from objects.rides import Rides
from datastore.storage.stg_minio import MinioStorage

# warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# get env
load_dotenv()

# load variables
get_dt_rows = os.getenv("EVENTS")

# main
if __name__ == '__main__':

    # instantiate arg parse
    parser = argparse.ArgumentParser(description='python application for ingesting data & events into a data store')

    # add parameters to arg parse
    parser.add_argument('entity', type=str, choices=[
        'mssql',
        'sqldb',
        'postgres',
        'mysql',
        'minio',
        'minio-movies',
    ], help='entities')

    # invoke help if null
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    # init variables
    users_object_name = Users().get_multiple_rows(get_dt_rows)
    agent_object_name = Agent().get_multiple_rows(get_dt_rows)
    credit_card_object_name = CreditCard().get_multiple_rows(get_dt_rows)
    musics_object_name = Musics().get_multiple_rows(get_dt_rows)
    movies_titles_object_name = Movies().get_movies(get_dt_rows)
    movies_keywords_object_name = Movies().get_keywords(get_dt_rows)
    movies_ratings_object_name = Movies().get_ratings(get_dt_rows)
    rides_object_name = Rides().get_multiple_rows(get_dt_rows)
    # relational databases
    if sys.argv[1] == 'mssql':
        MSSQL().insert_rows()

    if sys.argv[1] == 'postgres':
        Postgres().insert_rows()

    if sys.argv[1] == 'mysql':
        MySQL().insert_rows()

    # object stores
    if sys.argv[1] == 'minio':
        MinioStorage().write_all()

    if sys.argv[1] == 'minio-movies':
        MinioStorage().write_movies_json()