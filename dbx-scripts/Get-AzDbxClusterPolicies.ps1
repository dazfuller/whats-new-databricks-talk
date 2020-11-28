<#
 .SYNOPSIS
    Gets the cluster policies for the workspace deployed to a given resource group

 .DESCRIPTION
    Queries the Databricks workspace deployed to a given resource group and returns
    the cluster policies deployed.

 .LINK
    https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/policies

 .LINK
    https://www.elastacloud.com

 .PARAMETER ResourceGroupName
    The name of the resource group where the databricks workspace is deployed to

 .EXAMPLE
    Get-AzDbxClusterPolicies -ResourceGroupName my-demo-rg
    
    Lists the cluster policies for the Databricks instance deployed to my-demo-rg
#>

param (
    [Parameter(Mandatory=$false)] $ResourceGroupName = "talks01"
)

# This is Azure Databricks resource id
$DatabricksResourceId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

# Get the workspace deployed in this resource group
$DatabricksWorkspace = az resource list -g $ResourceGroupName --query "[?type == 'Microsoft.Databricks/workspaces']" | ConvertFrom-Json

# Generate the access tokens
$ManagementToken = az account get-access-token --resource "https://management.core.windows.net/" --query "accessToken" --output tsv
$DirectoryToken = az account get-access-token --resource $DatabricksResourceId --query "accessToken" --output tsv

# Set the headers
$Headers = @{
    "Authorization" = "Bearer $DirectoryToken"
    "X-Databricks-Azure-SP-Management-Token" = $ManagementToken
    "X-Databricks-Azure-Workspace-Resource-Id" = $DatabricksWorkspace.id
}

# Request the current access policies
$Uri = "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/policies/clusters/list"
$Response = Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers

$Response.policies | Format-List
