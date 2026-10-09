#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Installa gs-agent come servizio Windows (tramite WinSW).

.DESCRIPTION
    Da lanciare in PowerShell come amministratore, dalla cartella del pacchetto gs-agent
    (package.json + dist/ compilati, oppure i sorgenti: lo script esegue npm ci + build).
    Richiede Node.js 22 o superiore.

    WinSW (https://github.com/winsw/winsw) trasforma un eseguibile qualsiasi in servizio Windows.
    Va scaricato dalla release ufficiale (WinSW-x64.exe) e indicato con -WinSWPath: lo script non
    scarica nulla da solo.

    Il servizio gira come "NT AUTHORITY\LocalService" (account con privilegi minimi): la cartella dati
    e' leggibile solo da SYSTEM, Administrators e LocalService.

.EXAMPLE
    .\install\windows\install.ps1 -WinSWPath C:\Downloads\WinSW-x64.exe -Hub https://hub.example -Token gsat_...
#>
param(
    [Parameter(Mandatory = $true)][string]$WinSWPath,
    [string]$Hub = "",
    [string]$Token = "",
    [string]$AppDir = "$env:ProgramFiles\GlueSuite\gs-agent",
    [string]$DataDir = "$env:ProgramData\GlueSuite\agent"
)

$ErrorActionPreference = "Stop"
$ServiceId = "gs-agent"

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Green }

if (-not (Test-Path "package.json")) { throw "Lancia lo script dalla cartella del pacchetto gs-agent" }
if (-not (Test-Path $WinSWPath)) { throw "WinSW non trovato: $WinSWPath" }
if ($Token -and -not $Hub) { throw "-Token richiede anche -Hub" }

$node = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $node) { throw "Node.js non trovato (serve la versione 22 o superiore)" }
$major = [int](& $node -p "process.versions.node.split('.')[0]")
if ($major -lt 22) { throw "Node.js $major troppo vecchio: serve la 22 o superiore" }

# servizio gia' presente: fermarlo prima di sostituire i file
$existing = Get-Service -Name $ServiceId -ErrorAction SilentlyContinue
if ($existing -and $existing.Status -ne "Stopped") {
    Step "Arresto del servizio esistente"
    Stop-Service -Name $ServiceId -Force
}

Step "Codice in $AppDir"
New-Item -ItemType Directory -Force -Path $AppDir | Out-Null
Copy-Item package.json, package-lock.json -Destination $AppDir -Force
if (Test-Path dist) { Copy-Item dist -Destination $AppDir -Recurse -Force }
if (Test-Path src) { Copy-Item src, tsconfig.json -Destination $AppDir -Recurse -Force }
Push-Location $AppDir
try {
    if (Test-Path src) {
        npm ci --no-audit --no-fund; if ($LASTEXITCODE) { throw "npm ci fallito" }
        npm run build; if ($LASTEXITCODE) { throw "build fallita" }
        npm prune --omit=dev --no-audit --no-fund; if ($LASTEXITCODE) { throw "npm prune fallito" }
    } else {
        npm ci --omit=dev --no-audit --no-fund; if ($LASTEXITCODE) { throw "npm ci fallito" }
    }
} finally { Pop-Location }

Step "Cartella dati $DataDir"
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

if ($Token) {
    Step "Attivazione dell'agente"
    & $node "$AppDir\dist\cli.js" enroll --hub $Hub --token $Token --data-dir $DataDir
    if ($LASTEXITCODE) { throw "attivazione fallita" }
}

# ACL: niente ereditarieta', solo SYSTEM, Administrators e LocalService (chiave privata e segreti)
& icacls $DataDir /inheritance:r /grant:r "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-19:(OI)(CI)M" | Out-Null
if ($LASTEXITCODE) { throw "impostazione dei permessi fallita" }

Step "Servizio Windows (WinSW)"
$wrapper = Join-Path $AppDir "$ServiceId.exe"
Copy-Item $WinSWPath $wrapper -Force
$xml = @"
<service>
  <id>$ServiceId</id>
  <name>GlueSuite agent</name>
  <description>Collega i sistemi locali all'integration hub di GlueSuite</description>
  <executable>$node</executable>
  <arguments>"$AppDir\dist\cli.js" run</arguments>
  <env name="GS_AGENT_DATA_DIR" value="$DataDir" />
  <env name="NODE_ENV" value="production" />
  <startmode>Automatic</startmode>
  <delayedAutoStart>true</delayedAutoStart>
  <onfailure action="restart" delay="5 sec" />
  <onfailure action="restart" delay="30 sec" />
  <resetfailure>1 hour</resetfailure>
  <serviceaccount>
    <username>NT AUTHORITY\LocalService</username>
  </serviceaccount>
  <log mode="roll-by-size">
    <sizeThreshold>10240</sizeThreshold>
    <keepFiles>5</keepFiles>
  </log>
</service>
"@
Set-Content -Path (Join-Path $AppDir "$ServiceId.xml") -Value $xml -Encoding UTF8

if (-not $existing) {
    & $wrapper install; if ($LASTEXITCODE) { throw "installazione del servizio fallita" }
}

if (Test-Path (Join-Path $DataDir "identity.json")) {
    Start-Service -Name $ServiceId
    Step "Servizio avviato"
    Get-Service -Name $ServiceId
} else {
    Write-Warning "Agente non ancora attivato: attivalo e poi avvia il servizio:"
    Write-Host "  & `"$node`" `"$AppDir\dist\cli.js`" enroll --hub https://<hub> --token gsat_... --data-dir `"$DataDir`""
    Write-Host "  Start-Service $ServiceId"
}

Write-Host @"

============================================================
gs-agent installato
  Codice:  $AppDir
  Dati:    $DataDir   (chiave privata e segreti: non copiarli altrove)
  Log:     $AppDir\$ServiceId.out.log / .err.log
  Stato:   & "$node" "$AppDir\dist\cli.js" status --data-dir "$DataDir"
============================================================
"@
