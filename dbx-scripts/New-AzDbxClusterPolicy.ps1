<#
 .SYNOPSIS
    Create an Azure Databricks Cluster Policy

 .DESCRIPTION
    Connects to the Azure Databricks instance for a given resource group and creates a new
    cluster policy with the following configuration:

    - Databricks Runtime 7.3 LTS or 7.4
    - Min number of workers range 1-3
    - Max number of workers range 3-5
    - Fixed auto-termination of 30 minutes

 .LINK
    https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/policies

 .LINK
    https://www.elastacloud.com

 .PARAMETER ResourceGroupName
    The name of the resource group where the databricks workspace is deployed to

 .EXAMPLE
    New-AzDbxClusterPolicy -ResourceGroupName my-demo-rg
    
    Creates a new cluster policy for the Databricks instance deployed to my-demo-rg
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
Write-Host "Checking for existing policies" -ForegroundColor Yellow
$Uri = "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/policies/clusters/list"
$Response = Invoke-RestMethod -Method Get -Uri $Uri -Headers $Headers

$Policies = $Response.policies

# Define the access policy information
$PolicyName = "Simple Demo Policy"
$PolicyDefinition = '{ "spark_version": { "type": "allowlist", "values": [ "7.3.x-scala2.12", "7.4.x-scala2.12" ] }, "autoscale.min_workers": { "type": "range", "minValue": 1, "maxValue": 3 }, "autoscale.max_workers": { "type": "range", "minValue": 3, "maxValue": 5 }, "autotermination_minutes": { "type": "fixed", "value": 30, "hidden": true } }'
$PolicyDefinitionJson = ConvertTo-Json $PolicyDefinition

# Check to see if there is an existing policy with the same name
$ExistingPolicy = $Policies | Where-Object -FilterScript { $_.name -eq $PolicyName }

# If there is an existing policy then update it, otherwise createa new policy
if ($null -ne $ExistingPolicy) {
    Write-Host "Updating existing policy $($ExistingPolicy.policy_id)" -ForegroundColor Yellow

    $UpdateBody = "{ `"policy_id`": `"$($ExistingPolicy.policy_id)`", `"name`": `"$PolicyName`", `"definition`": $($PolicyDefinitionJson) }"
    Invoke-RestMethod "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/policies/clusters/edit" -Method Post -Body $UpdateBody -Headers $Headers
} else {
    Write-Host "Creating a new cluster policty" -ForegroundColor Yellow

    $UpdateBody = "{ `"name`": `"$PolicyName`", `"definition`": $($PolicyDefinitionJson) }"
    Invoke-RestMethod "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/policies/clusters/create" -Method Post -Body $UpdateBody -Headers $Headers
}
