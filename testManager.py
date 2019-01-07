############ REQUIREMENTS ####################
# brew install python-pip
# brew install libpq-dev
# pip install psycopg2
# install sqlalchemy
# install sqlalchemy-redshift
##############################################
from datetime import date

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import pandas as pd


def dict_to_df(query_result, False):
    items = {
        val: dict(query_result[val])
        for val in range(len(query_result))
    }
    # df = pd.DataFrame.from_dict(items, orient="index").drop(["attributes"], axis=1)
    df = pd.DataFrame.from_dict(items, orient="index")

    if date:  # date indicates if the df contains datetime column
        df["start_date"] = pd.to_datetime(df["start_date"], format="%Y-%m-%d")  # convert to datetime
        df["start_date"] = df["start_date"].dt.strftime('%Y-%m-%d')  # reset string
    return df


class testManager:
        #>>>>>>>> Redshift DB configuration<<<<<<<<<<<<<
        DATABASE = "DB_KEY"
        USER = "USER_KEY"
        PASSWORD = "PASSWORD_KEY"
        HOST = "HOST_KEY"
        PORT = "PORT_KEY"
        SCHEMA = "SCHEMA_KEY"      #default is "public"

        ####### connection and session creation ##############
        connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (USER,PASSWORD,HOST,str(PORT),DATABASE)
        engine = sa.create_engine(connection_string)
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        SetPath = "SET search_path TO %s" % SCHEMA
        s.execute(SetPath)


        ################ write queries from here ######################

        def get_Shipment(self):
            query = "select id,start_date,lane_start,lane_end,total_receivable_amount,miles from dwprodsrc.src_shipment_data order by id desc limit 50;"
            rr = self.s.execute(query)
            all_results = rr.fetchall()
            print all_results
            shipments = dict_to_df(all_results, False)
            print shipments
            return shipments



        ########## close session in the end ###############
        s.close()


