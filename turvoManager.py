from simple_salesforce.exceptions import SalesforceExpiredSession
import pandas as pd

# import mysql.connector
import MySQLdb


class TurvoManager():
    def __init__(self):
        self.db = MySQLdb.connect(
            host="localhost",
            user="root",
            passwd="turvo",
            database="turvo"
        )

    def login(self):
        self.db = MySQLdb.connect(
            host="localhost",
            user="root",
            passwd="turvo",
            database="turvo"
        )
        return 0

    def dict_to_df(self, query_result, date=True):
        items = {
            val: dict(query_result[val])
            for val in range(len(query_result))
        }
        # df = pd.DataFrame.from_dict(items, orient="index").drop(["attributes"], axis=1)
        df = pd.DataFrame.from_dict(items, orient="index")

        if date:  # date indicates if the df contains datetime column
            df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], format="%Y-%m-%d")  # convert to datetime
            df["CreatedDate"] = df["CreatedDate"].dt.strftime('%Y-%m-%d')  # reset string
        return df

    def get_leads(self):
        try:
            desc = self.db.Lead.describe()
        except SalesforceExpiredSession as e:
            self.login()
            desc = self.db.Lead.describe()

        field_names = [field['name'] for field in desc['fields']]
        soql = "SELECT {} FROM Lead".format(','.join(field_names))
        query_result = self.db.query_all(soql)
        leads = self.dict_to_df(query_result)
        return leads

    def get_opportunities(self):
        query_text = "SELECT CreatedDate, Name, StageName, ExpectedRevenue, Amount, LeadSource, IsWon, IsClosed, Type, Probability FROM Opportunity"
        try:
            query_result = self.db.query(query_text)
        except SalesforceExpiredSession as e:
            self.login()
            query_result = self.db.query(query_text)
        opportunities = self.dict_to_df(query_result)
        return opportunities

    def get_cases(self):
        query_text = "SELECT CreatedDate, Type, Reason, Status, Origin, Subject, Priority, IsClosed, OwnerId, IsDeleted, AccountId FROM Case"
        try:
            query_result = self.db.query(query_text)
        except SalesforceExpiredSession as e:
            self.login()
            query_result = self.db.query(query_text)

        cases = self.dict_to_df(query_result)
        return cases

    def get_contacts(self):
        query_text = "SELECT Id, Salutation, FirstName, LastName FROM Contact"
        try:
            query_result = self.db.query(query_text)
        except SalesforceExpiredSession as e:
            self.login()
            query_result = self.db.query(query_text)

        contacts = self.dict_to_df(query_result, False)
        return contacts

    def get_users(self):
        query_text = "SELECT Id,FirstName, LastName FROM User"
        try:
            query_result = self.db.query(query_text)
        except SalesforceExpiredSession as e:
            self.login()
            query_result = self.db.query(query_text)

        users = self.dict_to_df(query_result, False)
        return users

    def get_shipments(self):
        query_text = "SELECT id, po_number FROM shipment limit 10"
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)

        # cursor = self.db.cursor()
        cursor.execute(query_text)
        query_result = cursor.fetchall()

        shipments = self.dict_to_df(query_result, False)
        return shipments

    def add_lead(self, query):
        try:
            self.db.Lead.create(query)
        except SalesforceExpiredSession as e:
            self.login()
            self.db.Lead.create(query)
        return 0

    def add_opportunity(self, query):
        try:
            self.db.Opportunity.create(query)
        except SalesforceExpiredSession as e:
            self.login()
            self.db.Opportunity.create(query)
        return 0

    def add_case(self, query):
        try:
            self.db.Case.create(query)
        except SalesforceExpiredSession as e:
            self.login()
            self.db.Case.create(query)
        return 0


# tm = TurvoManager()
# tm.get_shipments()
