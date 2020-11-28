<#
 .SYNOPSIS
    Create an Azure Databricks Personal Access Token

 .DESCRIPTION
    Using the credentials stored in the credentials.json file in the same directory as the script
    this will connect to the Databricks instance in the specified resource group and generate
    a new token, for the given duration in weeks, with the specified name.

 .LINK
    https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/scim/

 .LINK
    https://www.elastacloud.com

 .PARAMETER ResourceGroupName
    The name of the resource group where the databricks workspace is deployed to

 .PARAMETER TokenDurationWeeks
    The duration in weeks the token should be valid for

 .PARAMETER TokenHome
    The name to give to the token

 .EXAMPLE
    New-AzDbxSPToken -ResourceGroupName my-demo-rg -TokenDurationWeeks 52 -TokenName "Example token"
    
    Create a new token which is valid for a single year (365 days)
#>

param (
    [Parameter(Mandatory=$false)] [string] $ResourceGroupName = "talks01",
    [Parameter(Mandatory=$false)] [int] $TokenDurationWeeks = 1,
    [Parameter(Mandatory=$false)] [string] $TokenName = "Dbx Talk Token"
)

# Define configuration for script
$ManagementResourceId = "https://management.core.windows.net/"
$DatabricksResourceId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
$CredentialFile = Join-Path $PSScriptRoot "credentials.json"
$Credentials = Get-Content $CredentialFile | ConvertFrom-Json

$TenantId = az account show --query "tenantId" --output tsv

$ClientId = $Credentials.clientId
$ClientSecret = $Credentials.clientSecret

$RequestAccessTokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/token"

# Get the deployed Databricks workspace for the resource group
$DatabricksWorkspace = az resource list -g $ResourceGroupName --query "[?type == 'Microsoft.Databricks/workspaces']" | ConvertFrom-Json

# Generate the message body for requesting access tokens as the service princpial
$DirectoryBody = "grant_type=client_credentials&client_id=$ClientId&resource=$DatabricksResourceId&client_secret=$ClientSecret"
$ManagementBody = "grant_type=client_credentials&client_id=$ClientId&resource=$ManagementResourceId&client_secret=$ClientSecret"

# Make the API calls to get the access tokens
$DirectoryToken = (Invoke-RestMethod -Method Post -Uri $RequestAccessTokenUri -Body $DirectoryBody -ContentType 'application/x-www-form-urlencoded').access_token
$ManagementToken = (Invoke-RestMethod -Method Post -Uri $RequestAccessTokenUri -Body $ManagementBody -ContentType 'application/x-www-form-urlencoded').access_token

# Set the headers
$Headers = @{
    "Authorization" = "Bearer $DirectoryToken"
    "X-Databricks-Azure-SP-Management-Token" = $ManagementToken
    "X-Databricks-Azure-Workspace-Resource-Id" = $DatabricksWorkspace.id
}

# Get the existing access tokens created by the service princpial, check to see if an existing token exists and remove it
Write-Host "Retrieving current tokens for Service Principal" -ForegroundColor Yellow
$Tokens = (Invoke-RestMethod "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/token/list" -Method Get -Headers $Headers).token_infos

foreach ($Token in $Tokens) {
    Write-Host "$($Token.comment): $($Token.token_id)" -ForegroundColor Green
}

$ExistingToken = $Tokens | Where-Object -FilterScript { $_.comment -eq $TokenName }

if ($null -ne $ExistingToken) {
    Write-Host "Revoking existing demo token: $($ExistingToken.token_id)" -ForegroundColor Yellow
    $RevokeBody = @{
        token_id = $ExistingToken.token_id
    }

    $RevokeBodyText = ConvertTo-Json $RevokeBody -Depth 10
    Invoke-RestMethod "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/token/delete" -Method Post -Body $RevokeBodyText -Headers $Headers
}

# Generate a brand new access token
Write-Host "Creating demo access token" -ForegroundColor Yellow
$AccessTokenBody = @{
    comment = $TokenName
}

$AccessTokenBody.lifetime_seconds = $TokenDurationWeeks * 7 * 24 * 60 * 60    # Set duration for 1 week

$AccessTokenBodyText = ConvertTo-Json $AccessTokenBody -Depth 10

$AccessToken = Invoke-RestMethod "https://$($DatabricksWorkspace.location).azuredatabricks.net/api/2.0/token/create" -Method Post -Body $AccessTokenBodyText -Headers $Headers

# Write out the access token for the user
Write-Host "###################################################" -ForegroundColor White
Write-Host "# Generated access token                           " -ForegroundColor White
Write-Host "# Save this token for use in the data platform     " -ForegroundColor White
Write-Host "#                                                  " -ForegroundColor White
Write-Host "# Token: $($AccessToken.token_value)" -ForegroundColor White
Write-Host "###################################################" -ForegroundColor White