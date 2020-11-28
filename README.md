# What's New in Databricks

Looking at new features including

* AutoLoader
* Service Principals and the SCIM API
* Cluster Policies
* Single Node Notebooks

The scripts are provided as is and are there for you to modify yourself.

The scripts depend on 2 files which have not been included in this project (because they contain credentials)

`/config.ini`

```ini
[default]
connection_string = <Shared Access Connection String>
```

`/dbx-scripts/credentials.json`

```json
{
    "clientId": "<Azure AD Application id>",
    "clientSecret": "<Azure AD Application client secret>",
    "clientName": "<Azure AD Application client name>"
}
```

Python code (outside of the `dbx` folder) has been written using Python 3.7.

Powershell code has been written against Powershell 7.1.0

Land Registry notebook has been written against the [UK Land Registry Price Paid](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) data, the example used is the single file option.
