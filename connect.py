from simple_salesforce import Salesforce, SalesforceLogin
from dotenv import load_dotenv

def connect_to_salesforce():
    session_id, instance = SalesforceLogin(domain=load_dotenv("DOMAIN"), username=load_dotenv("USERNAME"), password=load_dotenv("PASSWORD"), security_token=load_dotenv("SECURITY_TOKEN"))
    sf = Salesforce(domain=load_dotenv("DOMAIN"), username=load_dotenv("USERNAME"), password=load_dotenv("PASSWORD"), security_token=load_dotenv("SECURITY_TOKEN"))
    # result = sf.query("SELECT Id FROM case LIMIT 1")
    # print(result)
    return session_id, instance, sf

connect_to_salesforce()