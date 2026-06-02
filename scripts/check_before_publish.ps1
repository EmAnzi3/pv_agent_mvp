param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Continue"
$failed = $false

function Fail($Message) {
    Write-Host "[FAIL] $Message" -ForegroundColor Red
    $script:failed = $true
}

function Ok($Message) {
    Write-Host "[OK]   $Message" -ForegroundColor Green
}

function Info($Message) {
    Write-Host "[INFO] $Message"
}

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

Info "Repository: $Root"

$requiredFiles = @(
    "README.md",
    "CURRENT_STATE.md",
    "AGENTS.md",
    "CHANGELOG.md"
)

foreach ($file in $requiredFiles) {
    if (Test-Path $file) { Ok "presente: $file" } else { Fail "manca: $file" }
}

if (Test-Path ".git") {
    if (Get-Command git -ErrorAction SilentlyContinue) {
        Info "git status --short"
        git status --short

        Info "git diff --check"
        git diff --check
        if ($LASTEXITCODE -ne 0) {
            Fail "git diff --check ha trovato problemi"
        } else {
            Ok "git diff --check senza problemi"
        }
    } else {
        Fail "git non trovato nel PATH"
    }
} else {
    Info "cartella .git non presente: controllo git saltato"
}

if (Test-Path "docs\data.json") {
    try {
        $null = Get-Content "docs\data.json" -Raw -Encoding UTF8 | ConvertFrom-Json
        Ok "docs/data.json Ã¨ JSON valido"
    } catch {
        Fail "docs/data.json non Ã¨ JSON valido"
    }
}

if (Test-Path "reports\site\data.json") {
    try {
        $null = Get-Content "reports\site\data.json" -Raw -Encoding UTF8 | ConvertFrom-Json
        Ok "reports/site/data.json Ã¨ JSON valido"
    } catch {
        Fail "reports/site/data.json non Ã¨ JSON valido"
    }
}

if (Test-Path "docs\index.html") {
    $sizeMb = [Math]::Round((Get-Item "docs\index.html").Length / 1MB, 2)
    Info "docs/index.html: $sizeMb MB"
    if ($sizeMb -gt 15) {
        Fail "docs/index.html supera 15 MB: rischio performance GitHub Pages/browser"
    }
}

if (Test-Path "index.html") {
    $sizeMb = [Math]::Round((Get-Item "index.html").Length / 1MB, 2)
    Info "index.html: $sizeMb MB"
    if ($sizeMb -gt 15) {
        Fail "index.html supera 15 MB: rischio performance GitHub Pages/browser"
    }
}

if (-not $SkipTests) {
    $hasPytestSignals = (Test-Path "tests") -or (Test-Path "pytest.ini")
    if ($hasPytestSignals) {
        $pythonCmd = $null
        if (Test-Path ".venv\Scripts\python.exe") {
            $pythonCmd = ".\.venv\Scripts\python.exe"
        } elseif (Get-Command python -ErrorAction SilentlyContinue) {
            $pythonCmd = "python"
        }

        if ($pythonCmd) {
            Info "eseguo pytest"
            & $pythonCmd -m pytest
            if ($LASTEXITCODE -ne 0) {
                Fail "pytest ha restituito errore"
            } else {
                Ok "pytest completato"
            }
        } else {
            Fail "python non trovato per eseguire pytest"
        }
    } else {
        Info "nessuna cartella tests/ o pytest.ini: test Python saltati"
    }

    if (Test-Path "package.json") {
        if (Get-Command npm -ErrorAction SilentlyContinue) {
            Info "npm test --if-present"
            npm test --if-present
            if ($LASTEXITCODE -ne 0) {
                Fail "npm test ha restituito errore"
            } else {
                Ok "npm test completato o non presente"
            }
        } else {
            Info "npm non trovato: test JS saltati"
        }
    }
}

if ($failed) {
    Write-Host ""
    Write-Host "Controllo completato con errori." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Controllo completato senza errori bloccanti." -ForegroundColor Green
exit 0
