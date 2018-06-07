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
    con = redshiftServerConnect()
    if not con is None:
        cur = con.cursor()
        cur.execute("create external table {0}.{1}( \
               companyname                         varchar(256),\
               companynumber                       varchar(256),\
               regaddress_careof                   varchar(256),\
               regaddress_pobox                    varchar(256),\
               regaddress_addressline1             varchar(256),\
               regaddress_addressline2             varchar(256),\
               regaddress_posttown                 varchar(256),\
               regaddress_county                   varchar(256),\
               regaddress_country                  varchar(256),\
               regaddress_postcode                 varchar(256),\
               companycategory                     varchar(256),\
               companystatus                       varchar(256),\
               countryoforigin                     varchar(256),\
               dissolutiondate                     varchar(256),\
               incorporationdate                   varchar(256),\
               accounts_accountrefday              varchar(256),\
               accounts_accountrefmonth            varchar(256),\
               accounts_nextduedate                varchar(256),\
               accounts_lastmadeupdate             varchar(256),\
               accounts_accountcategory            varchar(256),\
               returns_nextduedate                 varchar(256),\
               returns_lastmadeupdate              varchar(256),\
               mortgages_nummortcharges            varchar(256),\
               mortgages_nummortoutstanding        varchar(256),\
               mortgages_nummortpartsatisfied      varchar(256),\
               mortgages_nummortsatisfied          varchar(256),\
               siccode_sictext_1                   varchar(256),\
               siccode_sictext_2                   varchar(256),\
               siccode_sictext_3                   varchar(256),\
               siccode_sictext_4                   varchar(256),\
               limitedpartnerships_numgenpartners  varchar(256),\
               limitedpartnerships_numlimpartners  varchar(256),\
               uri                                 varchar(65535),\
               previousname_1_condate              varchar(256),\
               previousname_1_companyname          varchar(256),\
               previousname_2_condate              varchar(256),\
               previousname_2_companyname          varchar(256),\
               previousname_3_condate              varchar(256),\
               previousname_3_companyname          varchar(256),\
               previousname_4_condate              varchar(256),\
               previousname_4_companyname          varchar(256),\
               previousname_5_condate              varchar(256),\
               previousname_5_companyname          varchar(256),\
               previousname_6_condate              varchar(256),\
               previousname_6_companyname          varchar(256),\
               previousname_7_condate              varchar(256),\
               previousname_7_companyname          varchar(256),\
               previousname_8_condate              varchar(256),\
               previousname_8_companyname          varchar(256),\
               previousname_9_condate              varchar(256),\
               previousname_9_companyname          varchar(256),\
               previousname_10_condate             varchar(256),\
               previousname_10_companyname         varchar(256),\
               confstmtnextduedate                 varchar(256),\
               confstmtlastmadeupdate              varchar(256)\
            ) \
            row format delimited\
            fields terminated by '{2}'\
            stored as textfile\
            location '{3}'\
            table properties(skip.header.line.count'='1')".format(Db['Schema'],Db['Tables'],Options['Delimiter'],Options['S3PathPrefix']+"/"+Options['filename'])\
            )
            con.close()
    else:
        logger.log("ERROR","Failed to create connection")
        exit(1)    
