<#
.SYNOPSIS
    Fast failure for jobs that start beside the Guard tests job (#5669).

.DESCRIPTION
    On a pull request the long jobs (build, the PostgreSQL shards, the Lite shards, the Linux build) start with the
    Guard tests job instead of after it. This script keeps the old fast failure: when Guard tests fails, the job that
    is still working stops and ends as FAILURE, never as cancelled.

    GitHub has no API that cancels one job; cancelling the run turns every running required check into "cancelled".
    So each long job watches the Guard tests job itself and fails ITSELF:

      Start  Launches a detached copy of this script in the Watch mode and returns at once.
      Watch  Polls the run's jobs through the API. When the Guard tests job ends in anything but success, it ends the
             process trees that the runner's worker is running for the CURRENT step (shell, dotnet, docker, ...).
             It leaves the worker's console host and the runner's helpers running: ending those fails every later step (#5669).
             The step ends non-zero, the job's later default-condition steps are skipped, and the job fails.
      Check  The job's last step (if: always()). Stops the watcher, waits for the Guard tests job to finish if it has
             not (the `build` job: it is a required check and nothing else reads the guard for it), and fails the job
             with an error that names the guard when it did not succeed. -NoWait reports without waiting. This covers a guard
             that fails after the work ended, and a kill that landed between two steps.

    The token needs `actions: read` (the workflow's default; a job with its own permissions block must add it).
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet('Start', 'Watch', 'Check')][string] $Mode,
    [string] $GuardJobName = 'Guard tests',
    [int] $PollSeconds = 15,
    # Check waits for a guard that has not finished; its own job-level limit is 20 minutes.
    [int] $CheckTimeoutMinutes = 25,
    # Check, for the jobs whose result job reads the guard's result anyway: report a failure, but do not wait for a
    # guard that has not finished (a short job should not idle on a runner).
    [switch] $NoWait
)

$ErrorActionPreference = 'Stop'
$pidFile = Join-Path $env:RUNNER_TEMP 'guard-watch.pid'

function Get-GuardJob {
    # $null when the API cannot be read or the job is not listed yet: neither is a verdict.
    try {
        $uri = "https://api.github.com/repos/$($env:GITHUB_REPOSITORY)/actions/runs/$($env:GITHUB_RUN_ID)/jobs?filter=latest&per_page=100"
        $headers = @{ Authorization = "Bearer $($env:GH_TOKEN)"; Accept = 'application/vnd.github+json'; 'X-GitHub-Api-Version' = '2022-11-28' }
        $response = Invoke-RestMethod -Uri $uri -Headers $headers -TimeoutSec 20
        return @($response.jobs | Where-Object { $_.name -eq $GuardJobName }) | Select-Object -First 1
    }
    catch {
        Write-Host "guard-watch: could not read the jobs ($($_.Exception.Message)); will retry."
        return $null
    }
}

function Test-GuardFailed($job) {
    return ($null -ne $job) -and ($job.status -eq 'completed') -and ($job.conclusion -ne 'success')
}

function Get-ProcessTable {
    # pid -> parent pid, for every process.
    $table = @{}
    if ($IsWindows) {
        foreach ($p in Get-CimInstance Win32_Process -Property ProcessId, ParentProcessId) { $table[[int] $p.ProcessId] = [int] $p.ParentProcessId }
    }
    else {
        foreach ($line in (& ps -e -o 'pid=,ppid=')) {
            $parts = @($line.Trim() -split '\s+')
            if ($parts.Count -ge 2) { $table[[int] $parts[0]] = [int] $parts[1] }
        }
    }
    return $table
}

function Get-Descendants([hashtable] $table, [int] $root) {
    $found = [System.Collections.Generic.List[int]]::new()
    $queue = [System.Collections.Generic.Queue[int]]::new()
    $queue.Enqueue($root)
    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        foreach ($entry in $table.GetEnumerator()) {
            if ($entry.Value -eq $current -and $entry.Key -ne $current -and -not $found.Contains([int] $entry.Key)) {
                $found.Add([int] $entry.Key)
                $queue.Enqueue([int] $entry.Key)
            }
        }
    }
    return $found
}

function Get-ProcessNames {
    # pid -> process name (no extension), for every process.
    $names = @{}
    foreach ($p in Get-Process -ErrorAction SilentlyContinue) { $names[[int] $p.Id] = $p.ProcessName }
    return $names
}

function Test-KeepRunning([string] $name) {
    # Not part of the step: the console host the worker keeps for every step it starts (ending it makes every later
    # step fail to start with 0xC0000142, STATUS_DLL_INIT_FAILED, measured on this PR's own CI) and the runner's own
    # helper processes. Only the step's process tree is ended.
    return ($name -ieq 'conhost') -or ($name -like 'Runner.*') -or ($name -like 'Agent.*')
}

function Stop-CurrentStep {
    # The runner's worker starts one process per step. Everything under the worker except this watcher's own tree and
    # the processes the worker keeps (see Test-KeepRunning) is the step that is running now. Returns how many
    # top-level processes were ended.
    $workers = @(Get-Process -Name 'Runner.Worker' -ErrorAction SilentlyContinue)
    Write-Host "guard-watch: $($workers.Count) Runner.Worker process(es): $(($workers | ForEach-Object { $_.Id }) -join ',')."
    if ($workers.Count -eq 0) {
        Write-Host 'guard-watch: no Runner.Worker process found, so nothing can be stopped from here; the Check step still fails the job.'
        return 0
    }
    $table = Get-ProcessTable
    $names = Get-ProcessNames
    $ended = 0
    foreach ($worker in $workers) {
        $children = @($table.GetEnumerator() | Where-Object { $_.Value -eq $worker.Id } | ForEach-Object { [int] $_.Key })
        Write-Host "guard-watch: worker $($worker.Id) has child processes: $(($children | ForEach-Object { "$_ ($($names[$_]))" }) -join ', ')."
        foreach ($child in $children) {
            if (Test-KeepRunning $names[$child]) { Write-Host "guard-watch: left pid $child ($($names[$child])) running; it is not the step."; continue }
            $tree = @($child) + @(Get-Descendants $table $child)
            if ($tree -contains $PID) { continue }
            # A console host inside the step's tree is left alone too; the step's own processes end around it.
            $tree = @($tree | Where-Object { -not (Test-KeepRunning $names[$_]) })
            # Deepest first, so a parent is not left to respawn a child.
            [array]::Reverse($tree)
            foreach ($id in $tree) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue }
            $ended++
            Write-Host "guard-watch: ended the step process tree rooted at pid $child ($($names[$child]); $($tree.Count) processes)."
        }
    }
    return $ended
}

switch ($Mode) {
    'Start' {
        $self = $MyInvocation.MyCommand.Path
        $log = Join-Path $env:RUNNER_TEMP 'guard-watch.log'
        # The streams go to files: a detached child that inherits the step's pipes would hold the step open. The arguments are one
        # string with quotes around the two values that can hold a space (Start-Process does not quote an array's elements).
        $err = Join-Path $env:RUNNER_TEMP 'guard-watch.err'
        $watchArgs = "-NoProfile -NonInteractive -File `"$self`" -Mode Watch -GuardJobName `"$GuardJobName`" -PollSeconds $PollSeconds"
        if ($IsWindows) {
            # A process the step starts itself dies with the step on a hosted Windows runner (measured on this PR's own CI:
            # the watcher logged one poll and was gone). A process the WMI service creates is outside the step's job
            # object, so it lives until the job ends or the Check step stops it.
            $pwsh = (Get-Command pwsh).Source
            # The WMI service gives the process ITS environment, not ours, so the four values the watcher reads are set here.
            $setEnv = ('RUNNER_TEMP', 'GITHUB_REPOSITORY', 'GITHUB_RUN_ID', 'GH_TOKEN' | ForEach-Object { "set `"$_=$([Environment]::GetEnvironmentVariable($_))`"" }) -join ' && '
            $commandLine = "cmd.exe /c `"$setEnv && `"$pwsh`" $watchArgs > `"$log`" 2> `"$err`"`""
            $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $commandLine }
            if ($created.ReturnValue -ne 0) { throw "could not start the watcher: Win32_Process.Create returned $($created.ReturnValue)" }
            $watcherPid = $created.ProcessId
        }
        else {
            $started = Start-Process -FilePath 'setsid' -PassThru -ArgumentList "pwsh $watchArgs" `
                -RedirectStandardOutput $log -RedirectStandardError $err
            $watcherPid = $started.Id
        }
        Set-Content -Path $pidFile -Value $watcherPid
        Write-Host "::notice title=Guard watcher started::Watching the '$GuardJobName' job of this run every $PollSeconds seconds (watcher pid $watcherPid). If it fails, this job stops and fails too."
    }
    'Watch' {
        $ended = 0
        try {
        while ($true) {
            $job = Get-GuardJob
            Write-Host "$(Get-Date -Format o) guard-watch: poll, '$GuardJobName' status '$(if ($job) { $job.status } else { 'unknown' })' conclusion '$(if ($job) { $job.conclusion } else { '' })'."
            if (Test-GuardFailed $job) {
                Write-Host "guard-watch: '$GuardJobName' ended '$($job.conclusion)'. Stopping the running step."
                # A kill can land between two steps and find no process; try again until one step has been ended.
                for ($i = 0; $i -lt 60 -and $ended -eq 0; $i++) {
                    $ended = Stop-CurrentStep
                    if ($ended -eq 0) { Start-Sleep -Seconds 1 }
                }
                break
            }
            if ($null -ne $job -and $job.status -eq 'completed') { Write-Host "guard-watch: '$GuardJobName' succeeded; nothing to do."; break }
            Start-Sleep -Seconds $PollSeconds
        }
        }
        catch { Write-Host "guard-watch: the watcher stopped on an error: $($_ | Out-String)" }
    }
    'Check' {
        # The watcher's own log, so a job log shows what it saw and did (its output goes to files).
        foreach ($name in 'guard-watch.log', 'guard-watch.err') {
            $file = Join-Path $env:RUNNER_TEMP $name
            if (Test-Path $file) { Write-Host "--- $name (last 15 lines)"; Get-Content $file -Tail 15 }
        }
        if (Test-Path $pidFile) {
            $watcher = [int] (Get-Content $pidFile)
            # The watcher is a command shell around pwsh on Windows: end the whole tree, deepest first.
            $tree = @($watcher) + @(Get-Descendants (Get-ProcessTable) $watcher)
            [array]::Reverse($tree)
            foreach ($id in $tree) { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue }
            Remove-Item $pidFile -ErrorAction SilentlyContinue
        }
        $deadline = (Get-Date).AddMinutes($CheckTimeoutMinutes)
        $job = Get-GuardJob
        if ($NoWait -and -not (Test-GuardFailed $job)) {
            Write-Host "guard-watch: '$GuardJobName' has not failed (status '$(if ($job) { $job.status } else { 'unknown' })'); the result job reads its final result."
            return
        }
        while ((Get-Date) -lt $deadline -and -not ($null -ne $job -and $job.status -eq 'completed')) {
            Write-Host "guard-watch: waiting for '$GuardJobName' to finish (status '$(if ($job) { $job.status } else { 'unknown' })')."
            Start-Sleep -Seconds $PollSeconds
            $job = Get-GuardJob
        }
        if ($null -eq $job -or $job.status -ne 'completed') {
            Write-Host "::error title=Guard tests did not finish::The '$GuardJobName' job of this run had no final result after $CheckTimeoutMinutes minutes, so this job cannot report the guards as passed. Read that job's log."
            exit 1
        }
        if ($job.conclusion -ne 'success') {
            Write-Host "::error title=Guard tests failed::The '$GuardJobName' job of this run ended '$($job.conclusion)', so this job was stopped and fails with it. Read that job's log for the failing class: $($job.html_url)"
            exit 1
        }
        Write-Host "::notice title=Guard tests passed::The '$GuardJobName' job of this run succeeded."
    }
}
