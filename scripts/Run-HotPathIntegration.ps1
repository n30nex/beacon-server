param(
  [string]$Dsn = "postgres://beacon:beacon@localhost:5432/beacon_hotpath_test?sslmode=disable",
  [string]$Output = "docs/query-hot-paths-explain.latest.md",
  [string]$TimingOutput = "",
  [ValidateRange(1, 100)]
  [int]$Scale = 1,
  [switch]$SkipCreateDatabase,
  [switch]$Benchmark,
  [switch]$Snapshot,
  [string]$RegionSlug = "western-canada",
  [string]$IATAs = "",
  [ValidateRange(1, 168)]
  [int]$WindowHours = 6,
  [ValidateRange(1, 20)]
  [int]$IATALimit = 4,
  [string]$Since = "",
  [string]$Until = "",
  [string]$StatementTimeout = "60s"
)

$ErrorActionPreference = "Stop"

function Get-DatabaseName {
  param([string]$ConnectionString)
  $uri = [Uri]$ConnectionString
  return $uri.AbsolutePath.TrimStart("/")
}

$dbName = Get-DatabaseName -ConnectionString $Dsn
if (-not $Snapshot) {
  if ($dbName -notmatch "test") {
    throw "Refusing to use database '$dbName'. The integration database name must contain 'test'. Use -Snapshot for a read-only scrubbed snapshot capture."
  }
  if ($dbName -notmatch "^[A-Za-z0-9_]+$") {
    throw "Database name '$dbName' contains characters this helper will not create automatically."
  }
}

if (-not $Snapshot -and -not $SkipCreateDatabase) {
  $podman = Get-Command podman.exe -ErrorAction Stop
  $exists = & $podman.Source exec beacon-postgres psql -U beacon -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$dbName'"
  if (($exists -join "").Trim() -ne "1") {
    & $podman.Source exec beacon-postgres createdb -U beacon $dbName
  }
}

if ($Snapshot -and $Output -eq "docs/query-hot-paths-explain.latest.md") {
  $Output = "docs/query-hot-paths-explain.snapshot.md"
}

$outputPath = if ([System.IO.Path]::IsPathRooted($Output)) {
  $Output
} else {
  Join-Path (Get-Location) $Output
}
$timingOutputPath = ""
if (-not [string]::IsNullOrWhiteSpace($TimingOutput)) {
  $timingOutputPath = if ([System.IO.Path]::IsPathRooted($TimingOutput)) {
    $TimingOutput
  } else {
    Join-Path (Get-Location) $TimingOutput
  }
}

$env:BEACON_TEST_DATABASE_URL = $Dsn
$env:BEACON_EXPLAIN_OUTPUT = $outputPath
$env:BEACON_TIMINGS_OUTPUT = $timingOutputPath
$env:BEACON_HOTPATH_SNAPSHOT_MODE = if ($Snapshot) { "1" } else { "0" }
$env:BEACON_HOTPATH_FIXTURE_SCALE = if ($Snapshot) { "" } else { [string]$Scale }
$env:BEACON_HOTPATH_REGION_SLUG = $RegionSlug
$env:BEACON_HOTPATH_IATAS = $IATAs
$env:BEACON_HOTPATH_WINDOW_HOURS = [string]$WindowHours
$env:BEACON_HOTPATH_IATA_LIMIT = [string]$IATALimit
$env:BEACON_HOTPATH_SINCE = $Since
$env:BEACON_HOTPATH_UNTIL = $Until
$env:BEACON_HOTPATH_STATEMENT_TIMEOUT = if ($Snapshot) { $StatementTimeout } else { "" }
go test ./db -run TestHotPathIntegrationFixture -count=1 -v
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

if ($Benchmark) {
  go test ./db -run '^$' -bench BenchmarkHotPathStoreMethods -benchmem -count=1
  exit $LASTEXITCODE
}
