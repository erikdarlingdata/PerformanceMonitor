/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// THE MISSING ASSERTION. The collector runner must get its connection from the target's own provider, for
/// every collector, not only the ones that fan out per database.
///
/// <para><b>What this exists to catch.</b> The runner had two paths: the per-database branch resolved
/// <c>TargetProviders.For(target)</c> correctly, and the branch serving everything else did
/// <c>new SqlConnection(server.ConnectionString)</c> literally. Six of the seven PostgreSQL collectors then
/// shipping took the second path — only <c>pg_autovacuum_stats</c> fanned out — so they were handed a SQL Server connection.
/// SqlClient rejects Npgsql's keywords while PARSING the connection string, before any query runs
/// ("Keyword not supported: 'host'"), and the resulting <c>ArgumentException</c> is neither
/// <c>SqlException</c> nor <c>PostgresException</c>, so it missed both fault-classification arms and recorded
/// a raw ERROR every sweep, forever, including for all three Tier 0 outage predictors.</para>
///
/// <para>Both providers were correct and both were individually tested. Every test on both sides passed.
/// Nothing asserted that the RUNNER asked the provider — the seam itself was untested, and a passing suite
/// plus green CI reported a feature that could not collect a single row from a PostgreSQL target. It took
/// pointing the service at a live target to find it.</para>
/// </summary>
public sealed class CollectorRunnerConnectionEngineTests
{
    private static ServerRuntime Runtime(CollectorTargetEngine engine, string connectionString) => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h" },
        ConnectionString = connectionString,
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = "h",
        ServerId = 1,
    };

    /// <summary>
    /// The pin. Nothing is opened — the TYPE is the whole assertion, and it is enough: a connection of the
    /// wrong type cannot even parse the other engine's connection string.
    /// </summary>
    [Fact]
    public void PostgresTarget_GetsAnNpgsqlConnection()
    {
        using var connection = DarlingCollectorRunner.CreateTargetConnection(
            Runtime(CollectorTargetEngine.PostgreSql, "Host=pg1;Database=postgres;Username=monitor"));

        Assert.IsType<NpgsqlConnection>(connection);
    }

    [Fact]
    public void SqlServerTarget_StillGetsASqlConnection()
    {
        using var connection = DarlingCollectorRunner.CreateTargetConnection(
            Runtime(CollectorTargetEngine.SqlServer, "Server=sql1;Integrated Security=true"));

        Assert.IsType<SqlConnection>(connection);
    }

    /// <summary>
    /// The failure mode itself, pinned: handing a PostgreSQL connection string to SqlClient throws while
    /// PARSING it. Stated as a test so the reason the type matters is not just prose — and so nobody
    /// "simplifies" the provider indirection away believing the driver would cope.
    /// </summary>
    [Fact]
    public void ASqlConnectionCannotEvenParseAPostgresConnectionString()
    {
        var ex = Assert.ThrowsAny<System.Exception>(
            () => new SqlConnection("Host=pg1;Database=postgres;Username=monitor"));

        Assert.Contains("Keyword not supported", ex.Message, System.StringComparison.Ordinal);

        /* And this is why it evaded classification: not a SqlException, so DarlingWorker's SqlException arm
           never saw it, and not a PostgresException either. */
        Assert.IsNotType<SqlException>(ex);
        Assert.IsNotType<PostgresException>(ex);
    }

    /// <summary>
    /// Belt and braces over the helper: the runner must not construct an engine-specific connection directly
    /// anywhere in its collector paths. A future edit that bypasses
    /// <see cref="DarlingCollectorRunner.CreateTargetConnection"/> would pass the type tests above while
    /// reintroducing the bug, so the source is scanned too — the same idiom the viewer-coverage and
    /// alert-wiring pins already use in this suite.
    /// </summary>
    [Fact]
    public void NoServiceFileConstructsAnEngineSpecificConnectionDirectly()
    {
        /* The precise hazard is not "constructs a connection" — it is "constructs a connection from
           server.ConnectionString", the engine-AMBIGUOUS value, which is exactly what the bug did. A
           connection built from a string an explicitly SQL-Server-only plan produced is fine and must stay
           allowed: the Azure SQL master hop calls SqlServerTargetProvider.Instance.BuildDatabaseListPlan and
           then opens its own SqlConnection, because per-database enumeration on Azure SQL DB is a SQL Server
           feature by definition. A blunter "no constructions at all" rule flags that and teaches the next
           person to suppress the test rather than read it.

           DIRECTORY-scoped, not runner-scoped, because the single-file form missed the round-2 live catch:
           the identical construction sat in DarlingXeSessions.cs, out of scan reach, and failed once a
           minute on every PostgreSQL target. Files listed below are ALLOWED to carry the construction
           because every path into them is engine-gated, and each entry names the gate that makes it true —
           an allowlisted file whose gate is later removed is a live bug this test can no longer see, so the
           entry must name something a reviewer can check. */
        var allowed = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            /* BOTH public entry points self-gate on Engine == SqlServer at their first line
               (EnsureAllAsync and ReconcileLongQueryCompletionsAsync) — XE is SQL Server only, and
               every construction site in the file sits behind one of those two gates. */
            ["DarlingXeSessions.cs"] = "self-gates at EnsureAllAsync and ReconcileLongQueryCompletionsAsync entries",

            /* Backfill dispatch runs behind CollectorCatalog.AppliesTo (the composed engine gate) at
               QueryStoreBackfill's work-selection, so a PostgreSQL target never reaches the SQL branch. */
            ["QueryStoreBackfill.cs"] = "composed AppliesTo gate at backfill dispatch",
        };

        var offenders = new List<string>();
        /* Recursive: Mcp/, Targets/ and friends are exactly one directory down, and "the single-file
           scan is how this one hid" applies verbatim to a single-directory scan. */
        foreach (var path in Directory.EnumerateFiles(ServiceSourceDirectory(), "*.cs", SearchOption.AllDirectories))
        {
            var name = Path.GetFileName(path);
            if (allowed.ContainsKey(name))
            {
                continue;
            }

            foreach (Match match in Regex.Matches(
                File.ReadAllText(path),
                @"new\s+(?:SqlConnection|NpgsqlConnection)\s*\(\s*server\.ConnectionString\s*\)"))
            {
                offenders.Add(name + ": " + match.Value);
            }
        }

        Assert.True(
            offenders.Count == 0,
            "Service code constructs an engine-specific connection from server.ConnectionString: "
            + string.Join(", ", offenders)
            + ". That value's engine is whatever the target is — route it through "
            + "CreateTargetConnection/TargetProviders.For(server.Target), or gate every path into the file "
            + "on engine and add an allowlist entry NAMING the gate. A hardcoded SqlConnection here is what "
            + "made six of seven PostgreSQL collectors fail in the connection-string parser every sweep — "
            + "and the seventh occurrence hid in a file the old single-file scan never read.");
    }

    private static string ServiceSourceDirectory([CallerFilePath] string thisFile = "")
    {
        var testsDir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(testsDir, "..", "PerformanceMonitor.Darling.Service"));
    }

    /// <summary>
    /// The scheduled ANALYSIS pass is NOT gated by engine at the call site any more (#3542) — and the history
    /// of why it used to be is the argument for what replaced it. <c>RunAnalysisPassAsync</c> takes a serverId
    /// and a storage name, not the target, so the pass could not gate itself; ungated, a PostgreSQL target got
    /// a full SQL-Server-shaped pass reading tables that would never have rows for its server_id, hit the
    /// 24-hour data-span gate and persisted <c>insufficient_data = true</c> forever — "still collecting" for
    /// the life of the deployment, the one state <c>analysis_state</c> exists to tell apart from an all-clear.
    /// So #2213 gated the call site and wrote an engine tombstone instead.
    ///
    /// <para>The PostgreSQL-target analysis engine makes that gate wrong in the other direction: a gated call
    /// site would skip the pass that now exists. The invariant moved one layer DOWN, into the service, which
    /// resolves the engine set from the registry's <c>engine_kind</c> on every call — the only place that can
    /// decide it, because two of the service's three construction sites are process-wide singletons that
    /// never see a target. This pin holds both halves: the worker no longer tests the engine or names the
    /// tombstone anywhere, and the service resolves the engine at every one of its four entry points.</para>
    /// </summary>
    [Fact]
    public void TheScheduledAnalysisPassRoutesByRegistryEngine()
    {
        var source = File.ReadAllText(WorkerSourcePath());

        /* The tombstone and its latch are gone from the worker entirely, not merely unused. */
        Assert.DoesNotContain("PostgresAnalysisNotApplicable", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PostgresAnalysisStateWritten", source, StringComparison.Ordinal);
        Assert.DoesNotContain("does not apply to a PostgreSQL target", source, StringComparison.Ordinal);

        /* No engine test between the analysis tick's due check and the call: the call is unconditional.
           Anchored on the tick's own comment so the window is the tick, not the whole file — the pileup gate a
           few lines above legitimately tests the engine, and a whole-file DoesNotContain would forbid it. */
        var tickAt = source.IndexOf("Every engine takes the pass (#3542)", StringComparison.Ordinal);
        var callAt = source.IndexOf("await RunScheduledAnalysisAsync(", StringComparison.Ordinal);
        Assert.True(tickAt > 0, "the analysis tick's rationale comment has moved or been rewritten");
        Assert.True(callAt > tickAt, "the scheduled analysis call must follow the tick's rationale");
        Assert.DoesNotContain("CollectorTargetEngine.PostgreSql", source[tickAt..callAt], StringComparison.Ordinal);

        /* And the service resolves the engine at every entry point — the pass, the facts read, the period
           comparison and audit_config's own narrow read (#4192) — so no consumer of the singleton can reach
           a collector without the registry's answer. Exactly four call sites: one per entry point, none
           cached in a field. */
        var service = File.ReadAllText(AnalysisServiceSourcePath());
        Assert.Equal(4, Regex.Matches(service, @"await ResolveEngineAsync\(").Count);
        Assert.DoesNotMatch(new Regex(@"private\s+(?:readonly\s+)?AnalysisEngineSet\??\s+_resolved"), service);
    }

    private static string AnalysisServiceSourcePath([CallerFilePath] string thisFile = "")
    {
        var testsDir = Path.GetDirectoryName(thisFile)!;
        return Path.Combine(testsDir, "..", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs");
    }

    private static string WorkerSourcePath([CallerFilePath] string thisFile = "")
    {
        var testsDir = Path.GetDirectoryName(thisFile)!;
        return Path.Combine(testsDir, "..", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
    }
}
