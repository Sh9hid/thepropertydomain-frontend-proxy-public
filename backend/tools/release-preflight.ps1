[CmdletBinding()]
param(
    [switch]$SkipFrontendBuild = $false,
    [switch]$SkipDockerBuild = $false
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Assert-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

Write-Step "Release preflight started"

Assert-Command git
Assert-Command docker

Write-Step "Checking git status"
$status = git status --short
if (-not [string]::IsNullOrWhiteSpace($status)) {
    Write-Host $status
    throw "Working tree is not clean. Commit or stash changes before release."
}

if (-not $SkipFrontendBuild) {
    Assert-Command npm
    Write-Step "Building frontend"
    Push-Location frontend
    try {
        npm run build
    }
    finally {
        Pop-Location
    }
}
else {
    Write-Host "Skipping frontend build check by request."
}

if (-not $SkipDockerBuild) {
    Write-Step "Building backend production image"
    docker build -f backend/Dockerfile backend -t woonona-backend:preflight

    Write-Step "Import smoke test inside production image"
    docker run --rm `
        -e APP_ENV=development `
        -e DATABASE_URL=postgresql+asyncpg://placeholder:placeholder@127.0.0.1:5432/placeholder `
        -e REDIS_URL=memory://local `
        -e API_KEY=local_preflight_key `
        woonona-backend:preflight `
        python -c "import main; print('import-ok')"
}
else {
    Write-Host "Skipping Docker checks by request."
}

Write-Step "Release preflight passed"
Write-Host "Safe to push branch and promote to production." -ForegroundColor Green
