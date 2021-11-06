from google.oauth2 import service_account
import pandas_gbq


filename =  './code/may-eleventh-e207a3fd73f9.json'
credentials = service_account.Credentials.from_service_account_file(filename)

query = """

SELECT * FROM may-eleventh.anhsandbox.fao_food_supply

"""


df = pandas_gbq.read_gbq(query, project_id='may-eleventh',credentials=credentials)