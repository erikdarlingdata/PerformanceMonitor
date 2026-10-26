/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6470: after starting the service, install-darling.ps1 waits until the service's own log says it is ready (or has
/// failed, stopped, or run out of time) instead of printing "give it ~2 minutes". The script's functions run under
/// Windows PowerShell 5.1 against a temp log folder and stand-ins for the service state and the clock.
/// </summary>
public sealed class DarlingInstallReadyWaitTests
{
    private static string Script => ReadRepoFile("Darling", "tools", "install-darling.ps1").Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string Functions() =>
        string.Join(
            "\n",
            new[]
            {
                "Get-DarlingServiceLogEntries", "Get-DarlingStartupVerdict", "Get-DarlingStartupProblemLines",
                "Wait-DarlingServiceReady", "Get-DarlingWebAddress", "Get-DarlingConfiguredServerCount", "Get-DarlingStartupRemedy",
            }.Select(name => DarlingInstallerConfigStepTests.ExtractFunction(Script, name)));

    private const string Prelude = """
        function E([string]$level, [string]$text) { [pscustomobject]@{ Time = [datetime]'2026-10-22 10:00:00'; Level = $level; Text = $text } }
        $loaded = E 'INFO' 'Loaded configuration from C:\x\darling.json: 2 server(s)'
        $store = E 'INFO' 'Postgres store ready (schema v79, 79 migration(s) applied)'
        $loop = E 'INFO' 'PerformanceMonitor Darling collection loop started'
        $noSleep = { param($seconds) }
        $running = { param($name) 'Running' }
        """;

    /// <summary>The log is read by timestamp: an entry from before the start is the previous run's and is left out, a
    /// continuation line joins its entry, and a log the service still has open can be read.</summary>
    [Fact]
    public void TheLogReader_KeepsOnlyEntriesFromThisStart_AndJoinsContinuationLines()
    {
        var folder = Path.Combine(Path.GetTempPath(), "darling-ready-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            var log = Path.Combine(folder, "darling-service_20261022.log");
            File.WriteAllText(
                log,
                "2026-10-22 09:00:00.100 [INFO ] [Worker] Postgres store ready (last run)\r\n" +
                "2026-10-22 10:00:01.200 [INFO ] [Worker] Loaded configuration from C:\\x\\darling.json: 3 server(s)\r\n" +
                "2026-10-22 10:00:02.300 [CRIT ] [Worker] Cannot reach or migrate the Postgres store: boom\r\n" +
                "    at Npgsql.Thing()\r\n" +
                "2026-10-22 10:00:03.400 [INFO ] [Web] Starting web dashboard on https://0.0.0.0:5153 (LAN-exposed)\r\n");

            var lines = Run(Functions() + $$"""

                $since = [datetime]'2026-10-22 10:00:00'
                $stream = [System.IO.File]::Open('{{log}}', 'Open', 'Read', 'ReadWrite')
                try {
                    $entries = @(Get-DarlingServiceLogEntries '{{folder}}' $since)
                    'count=' + $entries.Count
                    'first=' + $entries[0].Text
                    'level=' + $entries[1].Level
                    'joined=' + ($entries[1].Text -replace "`n", '|')
                    'servers=' + (Get-DarlingConfiguredServerCount $entries)
                    'web=' + (Get-DarlingWebAddress $entries 'HOST1')
                } finally { $stream.Dispose() }
                """);

            Assert.Equal(
                new[]
                {
                    "count=3",
                    "first=Loaded configuration from C:\\x\\darling.json: 3 server(s)",
                    "level=CRIT",
                    "joined=Cannot reach or migrate the Postgres store: boom|    at Npgsql.Thing()",
                    "servers=3",
                    "web=https://HOST1:5153",
                },
                lines);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>Ready when the collection loop starts, after the earlier steps are shown; the wait counts the sleeps and
    /// reports each step the service logged once.</summary>
    [Fact]
    public void TheWait_ReturnsReadyWhenTheCollectionLoopStarts_AndShowsEachStepOnce()
    {
        var lines = Run(Functions() + "\n" + Prelude + """

            $script:calls = 0
            $feed = {
                param($dir, $from)
                $script:calls++
                if ($script:calls -eq 1) { return @() }
                if ($script:calls -eq 2) { return @($loaded) }
                if ($script:calls -eq 3) { return @($loaded, $store) }
                return @($loaded, $store, $loop)
            }
            $r = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 420 3 $feed $running $noSleep
            'state=' + $r.State
            'seconds=' + $r.Seconds
            """);

        Assert.Contains("state=Ready", lines);
        Assert.Contains("seconds=9", lines);
        Assert.Single(lines, line => line.Contains("darling.json read", StringComparison.Ordinal));
        Assert.Single(lines, line => line.Contains("store created and migrated", StringComparison.Ordinal));
    }

    /// <summary>A start that never gets there reports a timeout after exactly the allowed time and prints a progress line
    /// with the latest log line on the way, so a long initdb does not look like a hang.</summary>
    [Fact]
    public void TheWait_ReportsATimeout_AndShowsItIsStillWorking()
    {
        var lines = Run(Functions() + "\n" + Prelude + """

            $feed = { param($dir, $from) @($loaded) }
            $r = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 60 3 $feed $running $noSleep
            'state=' + $r.State
            'seconds=' + $r.Seconds
            """);

        Assert.Contains("state=TimedOut", lines);
        Assert.Contains("seconds=60", lines);
        Assert.Contains(lines, line => line.Contains("still starting - latest log line: Loaded configuration", StringComparison.Ordinal));
    }

    /// <summary>A startup failure the service logs ends the wait at once with its lines; a CRITICAL line about file
    /// permissions is not a failed start; a stopped service ends the wait; the loop starting wins over an earlier failure.</summary>
    [Fact]
    public void TheWait_StopsOnAStartupFailure_ButNotOnAPermissionWarning_AndNotifiesAStoppedService()
    {
        var lines = Run(Functions() + "\n" + Prelude + """

            $fail = E 'CRIT' 'Cannot reach or migrate the Postgres store: no pg_hba entry'
            $acl = E 'CRIT' 'C:\x\darling.json is READABLE by Users/Authenticated Users/Everyone/INTERACTIVE.'
            $stopped = { param($name) 'Stopped' }

            $r1 = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 420 3 { param($d, $f) @($loaded, $fail) } $running $noSleep
            'failed=' + $r1.State + '@' + $r1.Seconds
            $r2 = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 30 3 { param($d, $f) @($loaded, $acl) } $running $noSleep
            'acl=' + $r2.State
            $r3 = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 420 3 { param($d, $f) @($loaded) } $stopped $noSleep
            'stopped=' + $r3.State
            $r4 = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 420 3 { param($d, $f) @($loaded, $fail, $loop) } $running $noSleep
            'recovered=' + $r4.State
            # The sustained-retry criticals (#4508) are logged at ~120 s while the service keeps retrying; the loop line comes minutes later.
            $sustained = @(
                (E 'CRIT' 'Cannot reach or migrate the Postgres store within the 120s retry budget (57P03); this is still a recoverable failure, so it keeps retrying every 60s instead of standing down.'),
                (E 'CRIT' 'Managed Postgres bootstrap has not succeeded within the 120s retry budget (boom); this is still a recoverable failure, so it keeps retrying every 60s instead of standing down.'),
                (E 'CRIT' 'Cannot load configuration within the 120s retry budget (locked); this is still a recoverable failure, so it keeps retrying every 60s instead of standing down.'))
            $script:polls = 0
            $sequential = { param($d, $f) $script:polls++; if ($script:polls -le 40) { @($loaded) + $sustained } else { @($loaded) + $sustained + @($loop) } }
            $r5 = Wait-DarlingServiceReady 'svc' 'C:\none' ([datetime]::Now) 420 3 $sequential $running $noSleep
            'sustained=' + $r5.State + '@' + $r5.Seconds
            (Get-DarlingStartupProblemLines @($loaded, $acl, $fail)) -join ' || '
            """);

        Assert.Contains("failed=Failed@0", lines);
        Assert.Contains("acl=TimedOut", lines);
        Assert.Contains("stopped=Stopped", lines);
        Assert.Contains("recovered=Ready", lines);
        Assert.Contains("sustained=Ready@120", lines);
        Assert.Contains(
            "10:00:00 [CRIT] C:\\x\\darling.json is READABLE by Users/Authenticated Users/Everyone/INTERACTIVE. || 10:00:00 [CRIT] Cannot reach or migrate the Postgres store: no pg_hba entry",
            lines);
    }

    /// <summary>The closing remedy is true for the state the wait ended in: a logged failure and a stopped service are
    /// not "retrying a store", and a quiet timeout does not claim a failure.</summary>
    [Fact]
    public void TheRemedy_SaysWhatIsTrueForEachEndState()
    {
        var lines = Run(Functions() + """

            'failed=' + (Get-DarlingStartupRemedy 'Failed' 1 'svc')
            'stopped=' + (Get-DarlingStartupRemedy 'Stopped' 0 'svc')
            'retrying=' + (Get-DarlingStartupRemedy 'TimedOut' 2 'svc')
            'quiet=' + (Get-DarlingStartupRemedy 'TimedOut' 0 'svc')
            """);

        Assert.Equal(4, lines.Count);
        Assert.StartsWith("failed=The service logged a startup failure it will not get past without help.", lines[0], StringComparison.Ordinal);
        Assert.Contains("Restart-Service -Name \"svc\"", lines[0], StringComparison.Ordinal);
        Assert.StartsWith("stopped=The service is not running.", lines[1], StringComparison.Ordinal);
        Assert.Contains("Start-Service -Name \"svc\"", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("retrying", lines[1] + lines[0], StringComparison.Ordinal);
        Assert.StartsWith("retrying=The service may still be retrying a store it cannot reach.", lines[2], StringComparison.Ordinal);
        Assert.StartsWith("quiet=The service may only be slow.", lines[3], StringComparison.Ordinal);
        Assert.DoesNotContain("cannot reach", lines[3], StringComparison.Ordinal);
    }

    /// <summary>A dashboard listening on a wildcard address is printed under the machine's name, since 0.0.0.0 and :: are
    /// not addresses a browser can open; a loopback or specific address is printed as the service logged it.</summary>
    [Fact]
    public void TheWebAddress_ReplacesAWildcardListenWithTheMachineName()
    {
        var lines = Run(Functions() + "\n" + Prelude + """

            function W([string]$text) { Get-DarlingWebAddress @((E 'INFO' $text)) 'HOST1' }
            'v4=' + (W 'Starting web dashboard on https://0.0.0.0:5153 (LAN-exposed to 10.0.0.0/8)')
            'v6=' + (W 'Starting web dashboard on https://[::]:5153 (LAN-exposed to 10.0.0.0/8)')
            'v6bare=' + (W 'Starting web dashboard on https://:::5153 (LAN-exposed to 10.0.0.0/8)')
            'specific=' + (W 'Starting web dashboard on https://10.1.2.3:5153 (LAN-exposed to 10.0.0.0/8)')
            'loopback=' + (W 'Starting web dashboard on http://localhost:5153 (loopback only)')
            """);

        Assert.Equal(
            new[]
            {
                "v4=https://HOST1:5153", "v6=https://HOST1:5153", "v6bare=https://HOST1:5153",
                "specific=https://10.1.2.3:5153", "loopback=http://localhost:5153",
            },
            lines);
    }

    private static List<string> Run(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-ready-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(WindowsPowerShellStartInfo.ForScript(path))!;
            var stdoutTask = process.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();
            var exited = process.WaitForExit(120_000);
            if (!exited)
            {
                process.Kill(entireProcessTree: true);
            }

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();
            Assert.True(exited, $"powershell.exe did not exit within 120 seconds. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error:\n{stderr}");
            return stdout.Split('\n').Select(line => line.TrimEnd('\r')).Where(line => line.Trim().Length > 0).ToList();
        }
        finally
        {
            File.Delete(path);
        }
    }
}
