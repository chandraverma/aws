import os
import psycopg2
def redshiftServerConnect():
    #Try to connect to database
    try:
        connection=psycopg2.connect(host=os.environ["URL"],dbname='common', user=os.environ["USERNAME"], password=os.environ["PASSWORD"],port= '5439')
        print("DEBUG >>", "connection successful")
        return connection
    #If connection fails return None
    except Exception as e:
        print("DEBUG >>", "Unable to connect to redshift sever",e)
        return None
