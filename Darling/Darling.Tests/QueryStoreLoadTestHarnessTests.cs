/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the load-test harness of #5794 to the store's migrations: <c>Darling/loadtest/inflate-query-store.sql</c> must
/// clone every Query Store interval table that a migration creates. A new interval table that the script leaves out
/// would make the load test measure a store with fewer rows than the inflated one it claims to be.
/// </summary>
public sealed partial class QueryStoreLoadTestHarnessTests
{
    private const string ScriptPath = "Darling/loadtest/inflate-query-store.sql";

    /* A table a migration creates. V171 and V172 rename the old table to <name>_legacy and attach it as a partition of
       the new parent, so a _legacy name is a partition of a table already on the list, not a table of its own. The schema
       is optional: a migration that runs with collect on its search_path can create the table unqualified (#5794
       review round 1). */
    [GeneratedRegex(@"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:collect\.)?(query_store_interval_[a-z_]+)", RegexOptions.IgnoreCase)]
    private static partial Regex CreatedIntervalTable();

    [Theory]
    [InlineData("CREATE TABLE collect.query_store_interval_wide (", "query_store_interval_wide")]
    [InlineData("CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest_coverage", "query_store_interval_latest_coverage")]
    [InlineData("CREATE TABLE query_store_interval_wide_pending (", "query_store_interval_wide_pending")]
    [InlineData("create table if not exists query_store_interval_new\n(", "query_store_interval_new")]
    public void CreatedIntervalTable_MatchesQualifiedAndUnqualifiedNames(string sql, string expected)
    {
        var match = CreatedIntervalTable().Match(sql);

        Assert.True(match.Success, $"The census regex does not see the table in: {sql}");
        Assert.Equal(expected, match.Groups[1].Value.ToLowerInvariant());
    }

    [GeneratedRegex(@"'collect\.(query_store_interval_[a-z_]+)'")]
    private static partial Regex ScriptTableLiteral();

    private static string[] MigrationIntervalTables() =>
        PgMigrations.Scripts
            .SelectMany(m => CreatedIntervalTable().Matches(m.Sql).Select(x => x.Groups[1].Value.ToLowerInvariant()))
            .Where(name => !name.EndsWith("_legacy", StringComparison.Ordinal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void Migrations_CreateTheSixKnownIntervalTables()
    {
        /* A floor for the census below: if the regex stops matching the migrations, this fails instead of the census
           passing on an empty list. */
        Assert.Equal(
            new[]
            {
                "query_store_interval_latest",
                "query_store_interval_latest_coverage",
                "query_store_interval_latest_pending",
                "query_store_interval_wide",
                "query_store_interval_wide_coverage",
                "query_store_interval_wide_pending",
            },
            MigrationIntervalTables());
    }

    [Fact]
    public void InflateScript_NamesEveryIntervalTableTheMigrationsCreate()
    {
        var script = File.ReadAllText(Path.Combine(RepoRoot(), ScriptPath));
        var listed = ScriptTableLiteral().Matches(script).Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);

        var missing = MigrationIntervalTables().Where(name => !listed.Contains(name)).ToArray();
        Assert.True(missing.Length == 0,
            $"{ScriptPath} does not clone {string.Join(", ", missing)}. Add each to loadtest.query_store_tables().");

        var unknown = listed.Except(MigrationIntervalTables(), StringComparer.Ordinal).ToArray();
        Assert.True(unknown.Length == 0,
            $"{ScriptPath} names {string.Join(", ", unknown)}, which no migration creates.");
    }

    /* measure.ps1's Jobs step reads the hourly builders' summary lines from the service log with one regex. A template
       word that changes would make the step print "no builder lines" for that job and leave it out of hourly_total_ms
       (#5794 review round 2). PowerShell's regex is .NET's, so the test runs the script's own pattern. */
    [Fact]
    public void MeasureScript_JobsPattern_MatchesEveryBuilderLogTemplate()
    {
        var script = File.ReadAllText(Path.Combine(RepoRoot(), "Darling/loadtest/measure.ps1"));
        var pieces = Regex.Match(script, @"\$pattern = '(?<a>[^']*)' \+\s*'(?<b>[^']*)' \+\s*'(?<c>[^']*)'");
        Assert.True(pieces.Success, "measure.ps1 no longer builds $pattern from three single-quoted pieces.");
        var pattern = new Regex(pieces.Groups["a"].Value + pieces.Groups["b"].Value + pieces.Groups["c"].Value);

        (string File, string Job)[] sources =
        [
            ("Darling/PerformanceMonitor.Darling.Storage/PlanRegressionDaily.cs", "PLAN_REGRESSION daily totals"),
            ("Darling/PerformanceMonitor.Darling.Storage/QueryStoreTopDaily.cs", "Query Store top daily summary"),
            ("Darling/PerformanceMonitor.Darling.Storage/QueryStoreComposeStamp.cs", "Query Store compose rollup"),
            ("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "Store object convergence"),
        ];
        /* The line each template becomes is the one the service's own file logger writes, read back from the log file.
           A hand-built line had a "[INF]" level the logger never writes: the real one is padded to five characters
           ("[INFO ]"), which a \[\w+\] pattern refuses. */
        var logDirectory = Path.Combine(Path.GetTempPath(), "darling-5794-jobs-" + Guid.NewGuid().ToString("N"));
        try
        {
            string logFile;
            using (var provider = new DarlingFileLoggerProvider(logDirectory))
            {
                var logger = provider.CreateLogger("PerformanceMonitor.Darling.Test");
                foreach (var (file, job) in sources)
                {
                    var source = RepoFile.ReadRepoFile(Path.Combine(RepoRoot(), file));
                    var templates = Regex.Matches(source, @"LogInformation\(\s*""(?<t>" + Regex.Escape(job) + @": [^""]*\{ElapsedMs\} ms)""");
                    Assert.True(templates.Count == 1, $"{file}: expected one LogInformation(\"{job}: ... {{ElapsedMs}} ms\"), found {templates.Count}.");
                    logger.Log(LogLevel.Information, "{Line}", Regex.Replace(templates[0].Groups["t"].Value, @"\{\w+\}", "1"));
                }

                provider.Flush();
                logFile = provider.CurrentLogFile();
            }

            var written = File.ReadAllLines(logFile);
            foreach (var (_, job) in sources)
            {
                var line = Assert.Single(written, l => l.Contains(job, StringComparison.Ordinal));
                Assert.Contains("[INFO ]", line, StringComparison.Ordinal);
                var match = pattern.Match(line);
                Assert.True(match.Success, $"measure.ps1's Jobs pattern does not match the {job} line the service's logger wrote: {line}");
                Assert.Equal(job, match.Groups["job"].Value);
                Assert.Equal("1", match.Groups["ms"].Value);
            }
        }
        finally
        {
            try { Directory.Delete(logDirectory, recursive: true); } catch (IOException) { /* best-effort */ } catch (UnauthorizedAccessException) { /* best-effort */ }
        }
    }

    /* #5794: under `pwsh -File`, `-Steps Jobs,Compression` reaches the script as the one string "Jobs,Compression", which the
       old ValidateSet refused. The script now splits and checks the list itself, before it does anything else. This runs the
       real script with a -Psql path that does not exist, so a run that gets past step parsing prints the first step's
       header and then stops at the missing program, and a bad step name stops before any header. */
    [Fact]
    public void MeasureScript_Steps_TakesACommaListUnderFile_AndRefusesAnUnknownStep()
    {
        var accepted = RunMeasureScript("Jobs,Compression");
        if (accepted is null) { Assert.Skip("PowerShell 7 (pwsh) is not installed on this machine."); return; }

        Assert.DoesNotContain("ValidateSet", accepted.Output, StringComparison.Ordinal);
        Assert.DoesNotContain("Unknown step", accepted.Output, StringComparison.Ordinal);
        Assert.Contains("## Jobs", accepted.Output, StringComparison.Ordinal);
        Assert.NotEqual(0, accepted.ExitCode);

        var refused = RunMeasureScript("Jobs,Bogus");
        Assert.NotNull(refused);
        Assert.NotEqual(0, refused!.ExitCode);
        Assert.Contains("Unknown step 'Bogus'", refused.Output, StringComparison.Ordinal);
        foreach (var name in new[] { "Reads", "ReadLatency", "Jobs", "Writes", "Compression" })
        {
            Assert.Contains(name, refused.Output, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("## Jobs", refused.Output, StringComparison.Ordinal);
    }

    private sealed record ScriptRun(int ExitCode, string Output);

    /* Runs measure.ps1 under pwsh with -Steps as one argument and a -Psql that does not exist. Null when pwsh is missing. */
    private static ScriptRun? RunMeasureScript(string steps)
    {
        var startInfo = new System.Diagnostics.ProcessStartInfo("pwsh")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var argument in new[]
        {
            "-NoProfile", "-File", Path.Combine(RepoRoot(), "Darling/loadtest/measure.ps1"),
            "-Psql", Path.Combine(Path.GetTempPath(), "darling-5794-no-such-psql-" + Guid.NewGuid().ToString("N")),
            "-Store", "host=127.0.0.1 dbname=none",
            "-Steps", steps,
        })
        {
            startInfo.ArgumentList.Add(argument);
        }

        System.Diagnostics.Process? process;
        try { process = System.Diagnostics.Process.Start(startInfo); }
        catch (System.ComponentModel.Win32Exception) { return null; }
        Assert.NotNull(process);
        using (process)
        {
            var stderr = process!.StandardError.ReadToEndAsync();
            var stdout = process.StandardOutput.ReadToEnd();
            Assert.True(process.WaitForExit(120_000), "measure.ps1 did not stop.");
            return new ScriptRun(process.ExitCode, stdout + stderr.GetAwaiter().GetResult());
        }
    }

    /* #5809: measure.ps1's purge-edge warning once held its own AddDays(-9) while the service's horizon moved to 10,
       so it warned on whole windows and told the operator to restore a newer copy. The script now reads the
       constant from source: this runs the script's own pattern against that file, and holds no day literal there. */
    [Fact]
    public void MeasureScript_PurgeEdge_ReadsTheWideHorizonConstant()
    {
        var root = RepoRoot();
        var script = File.ReadAllText(Path.Combine(root, "Darling/loadtest/measure.ps1"));
        Assert.DoesNotMatch(@"\$purgeEdge\s*=[^\r\n]*AddDays\(-\d", script);

        var path = Regex.Match(script, @"Join-Path \$here '(?<rel>[^']*QueryStoreIntervalPartitions\.cs)'");
        Assert.True(path.Success, "measure.ps1 no longer reads QueryStoreIntervalPartitions.cs for the purge edge.");
        var pattern = Regex.Match(script, @"\[regex\]::Match\(\(Get-Content -LiteralPath \$partitionsSource -Raw\), '(?<p>[^']*)'\)");
        Assert.True(pattern.Success, "measure.ps1 no longer matches WideHorizonDays with a single-quoted pattern.");

        var source = File.ReadAllText(Path.GetFullPath(Path.Combine(root, "Darling/loadtest", path.Groups["rel"].Value.Replace('\\', '/'))));
        var horizon = Regex.Match(source, pattern.Groups["p"].Value);
        Assert.True(horizon.Success, "measure.ps1's WideHorizonDays pattern does not match QueryStoreIntervalPartitions.cs.");
        Assert.Equal(QueryStoreIntervalPartitions.WideHorizonDays.ToString(System.Globalization.CultureInfo.InvariantCulture), horizon.Groups[1].Value);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
