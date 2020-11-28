# Databricks notebook source
storage_acc_name = dbutils.secrets.get(scope="platform", key="StorageName")
client_id = dbutils.secrets.get(scope="platform", key="DatabricksSPClientId")
client_secret = dbutils.secrets.get(scope="platform", key="DatabricksSPClientSecret")
tenant_id = dbutils.secrets.get(scope="platform", key="TenantId")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

mounts = [m.mountPoint for m in dbutils.fs.mounts()]

# COMMAND ----------

if "/mnt/landing" not in mounts:
  dbutils.fs.mount(
    source = f"abfss://landing@{storage_acc_name}.dfs.core.windows.net/",
    mount_point = "/mnt/landing",
    extra_configs = configs
  )

if "/mnt/staging" not in mounts:
  dbutils.fs.mount(
    source = f"abfss://staging@{storage_acc_name}.dfs.core.windows.net/",
    mount_point = "/mnt/staging",
    extra_configs = configs
  )

if "/mnt/curated" not in mounts:
  dbutils.fs.mount(
    source = f"abfss://curated@{storage_acc_name}.dfs.core.windows.net/",
    mount_point = "/mnt/curated",
    extra_configs = configs
  )