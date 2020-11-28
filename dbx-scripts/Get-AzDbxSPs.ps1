<#
 .SYNOPSIS
    Gets the service principals assigned to the Databricks instance in resource group

 .DESCRIPTION
    Connects to the databricks instance within the provided resource group and retrieves
    the service principals registered with the instance.

 .LINK
    https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/scim/

 .LINK
    https://www.elastacloud.com

 .PARAMETER ResourceGroupName
    The name of the resource group where the databricks workspace is deployed to

 .EXAMPLE
    Get-AzDbxSPs -ResourceGroupName my-demo-rg
    
    Gets the service principals registered with the Databricks instance registered in the
    my-demo-rg resource group
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

# Call the Service Principal SCIM API to list the service principals added to this workspace
$Uri = "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/preview/scim/v2/ServicePrincipals"
$Response = Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers

$Response.Resources | Format-Table
