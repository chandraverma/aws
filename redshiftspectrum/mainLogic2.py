from Connection import redshiftServerConnect
from json import load
from logger import Logger
import os
def main():
    """
    This is the main driver function which reads config file and decides accordingly what to do
    """
    with open("config.json") as inputFile:
        data=load(inputFile)
    Db=data["Dbconfig"]
    logger.log("DB",Db)
    Options=data["Options"]
    logger.log("OPTIONS",Options)
    logger.log("PROCESS STARTED", "***********************************")

    con = redshiftServerConnect()
    if not con is None:
        cur = con.cursor()
        cur.execute("create external schema {0} from data catalog database 'spectrumdb' iam_role '{1}' create external database if not exists".format(Db['Schema'],Options['arn1']))
        con.close()
    else:
        logger.log("ERROR","Failed to create connection")
        exit(1)

    spectrumtable(Db,Options)
    logger.log("PROCESS EXECUTED", "***********************************")


def spectrumtable(Db,Options):
    from sqlToString import getQueries
    con = redshiftServerConnect()
    if not con is None:
        cur = con.cursor()

        inputFile = {0} + "_spectrum.sql".fromat(Db['Tables'])
        queriesList = getQueries(inputFile)

        for query in queriesList:
            if query != "":
                query += ";"
                logger.log("EXECUTE", query)
                cur.execute(query)

        con.close()
    else:
        logger.log("ERROR","Failed to create connection")
        exit(1)
