import pandas as pd
import requests
import json

# Autorisasjon API (Enin AS)
def print_obj(obj):
    print(json.dumps(obj, indent=4, ensure_ascii=False))

# Printing system status to see if the server is running
system_status = requests.get("https://api.enin.ai/datasets/v1/system-status").json()
print_obj(system_status)

# Authenticating
# For not sharing my key on GitHub, I have saved my key in an 
# unavailable file
client_data = pd.read_csv('../enin.csv',sep=';',index_col=None)
auth = (client_data['client_id'].iloc[0], client_data['client_secret'].iloc[0])

# Printing status to let you know if you are authenticated
auth_status = requests.get(
    "https://api.enin.ai/datasets/v1/auth-status",
    auth=auth
).json()
print_obj(auth_status)

# Extracting accounting data from the financial statements of 
# 2019 and 2020 for companies in Norway with organization
# number starting with 9175406
accounting = requests.get(
"https://api.enin.ai/datasets/v1/dataset/accounts-composite",
params={
"response_file_type": "csv",
"accounts.accounting_year": "IN:2019, 2020",
"company.org_nr": "LIKE:9175406%",
"keep_only_fields": ','.join(
[
    "company.org_nr",
    "accounts.accounting_year",
    "company_details.employees",
    "accounts.accounting_to_date",
    "accounts.accounting_from_date",
    "company_details.business_address_municipality_number",
    "accounts_balance_sheet.total_assets",
    "accounts_income_statement.total_operating_income",
    "organization_type.organization_type_code",
])},
auth=auth,
).content.decode()

# Converting to data frame
from io import StringIO
accounting = StringIO(accounting)
accounting = pd.read_csv(accounting, sep =",")

# Saving data
accounting.to_csv('accounting.csv', index=False, sep=';')


# Extracting information about the CEO of companies in Norway 
# with organization number starting with 9175406
ceo = requests.get(
    "https://api.enin.ai/datasets/v1/dataset/person-company-role-composite",
    params={
        "company_role.company_role_key": "EQ:ceo",
        "company.org_nr": "LIKE:9175406%",
        "response_file_type": "csv",
        "keep_only_fields": ','.join(
            [
                "person.municipality_code",
                "person.postal_code",
                "person.birth_year",
                "company.org_nr",
                "company_role.company_role_key",
                "person.gender_uuid", 
                'person_company_role.from_date',
                'person_company_role.to_date', 
                'person_company_role.person_uuid',      
             ]
    )
},
auth=auth,
).content.decode()

# Converting to data frame
from io import StringIO
ceo = StringIO(ceo)
ceo = pd.read_csv(ceo, sep =",")

# Saving data
ceo.to_csv('ceo.csv', index=False, sep=';')