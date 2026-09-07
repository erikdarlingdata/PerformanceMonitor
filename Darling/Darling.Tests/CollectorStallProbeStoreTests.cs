/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V112 (#2880) — the out-of-band stall probe's STORE side: the rung, the fact that its table is deliberately
/// not a collector table, the write path's agreement with the DDL, and the read's
/// wiring, and the retention that bounds it.
///
/// <para>Split the way V111's suite is, and for the same practical reason: the viewer's connect-time gate is
/// the only part of this rung's suite that needs the WPF Viewer assembly, which is <c>net10.0-windows</c>.
/// Keeping it here would make the whole file Windows-only, and everything else in it runs against
/// <c>net10.0</c> projects — see <see cref="CollectorStallProbeViewerGateTests"/>.</para>
///
/// <para>The probe's firing condition and its bounds are pinned separately in
/// <c>Lite.Tests/StallWaitProbePolicyTests</c>, where they need no store, no target and no host.</para>
/// </summary>
public class CollectorStallProbeStoreTests
{
    internal const int RungVersion = 112;

    /// <summary>The table the rung creates.</summary>
    private const string TableName = "collector_stall_probes";

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collector-stall-wait-probes",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung creates the table and its lookup index, schema-qualified.
    ///
    /// <para>Schema qualification is not cosmetic: the migrate session's <c>search_path</c> puts
    /// <c>collect</c> first, so a bare <c>CREATE TABLE</c> lands wherever that path points rather than where
    /// the rung meant, with that schema's ACL.</para>
    /// </summary>
    [Fact]
    public void TheRungCreatesTheProbeTableAndItsIndex()
    {
        var sql = RungSql();

        Assert.Contains($"CREATE TABLE IF NOT EXISTS collect.{TableName}", sql, StringComparison.Ordinal);
        Assert.Contains($"ON collect.{TableName}(server_id, probe_time)", sql, StringComparison.Ordinal);

        /* NOT NULL only where a row is meaningless without it: who, when, which collector, what happened,
           and the two figures that make the trigger readable. Everything the SAMPLE carries is nullable,
           because a probe that never got an answer must still be storable — that is the whole point of the
           CONNECT_FAILED outcome. */
        foreach (var column in new[]
                 {
                     "probe_time timestamp NOT NULL",
                     "server_id integer NOT NULL",
                     "server_name text NOT NULL",
                     "collector_name text NOT NULL",
                     "outcome text NOT NULL",
                     "budget_ms integer NOT NULL",
                     "trigger_elapsed_ms integer NOT NULL",
                 })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        /* And nothing on the sample side is NOT NULL, asserted over the whole DDL rather than column by
           column: a NOT NULL there would make a failed probe unstorable and the failure would be silent,
           because the write is failure-isolated. */
        foreach (var column in DdlColumns(sql).Where(c => !IsTriggerSideColumn(c.Name)))
        {
            Assert.False(column.NotNull, $"collect.{TableName}.{column.Name} must be nullable — a probe "
                + "that could not connect still has to be storable");
        }
    }

    /// <summary>
    /// <b>The INSERT names exactly the columns the rung creates.</b>
    ///
    /// <para>Load-bearing rather than tidy. The probe write is failure-isolated — it logs at Debug and never
    /// throws, because a diagnostic must not be able to break a collection sweep — so a column name that does
    /// not exist would raise 42703 on every probe forever and produce, from the outside, a feature that simply
    /// never stores anything. Nothing else in this suite would notice. Derived as a set comparison on both
    /// sides, not a hand-written list, so a column added to one and not the other fails whichever way round
    /// the omission happens.</para>
    /// </summary>
    [Fact]
    public void TheWritePathNamesExactlyTheColumnsTheRungCreates()
    {
        var ddl = DdlColumns(RungSql()).Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
        var inserted = InsertColumns(StallWaitProbeRunner.InsertSql).ToHashSet(StringComparer.Ordinal);

        /* Non-empty separately, because two empty sets are equal and would pass this vacuously — the exact
           trap a set-equality assertion invites. */
        Assert.Equal(24, ddl.Count);
        Assert.Equal(ddl.Count, inserted.Count);
        Assert.Equal(ddl.OrderBy(c => c, StringComparer.Ordinal), inserted.OrderBy(c => c, StringComparer.Ordinal));

        /* And the placeholders are dense and match the column count, so a column added without its $N (or
           the reverse) is caught here rather than as a runtime bind error inside a swallowed catch. */
        var placeholders = Regex.Matches(StallWaitProbeRunner.InsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture))
            .ToList();

        Assert.Equal(inserted.Count, placeholders.Count);
        Assert.Equal(Enumerable.Range(1, inserted.Count), placeholders);
    }

    /// <summary>
    /// The read selects from the table the rung creates, and its outcome census is NOT filtered to successful
    /// samples.
    ///
    /// <para>The filter question is the substantive one. #2880 lists "whether a new connection succeeds
    /// mid-stall" as untested, so the probes that could not connect are the finding rather than noise, and a
    /// read that quietly dropped them would remove the only evidence anyone will have about it.</para>
    /// </summary>
    [Fact]
    public void TheReadSelectsFromTheTableAndHidesNoOutcome()
    {
        foreach (var sql in new[] { DarlingStallProbeReader.ProbesSql, DarlingStallProbeReader.OutcomeCensusSql })
        {
            Assert.Contains($"FROM collect.{TableName}", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("outcome =", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("outcome IN", sql, StringComparison.Ordinal);
            Assert.DoesNotContain(StallWaitProbePolicy.OutcomeSampled, sql, StringComparison.Ordinal);
        }

        /* The census groups by outcome, so every value is reported rather than bucketed by a list that a
           later outcome would silently miss. */
        Assert.Contains("GROUP BY p.outcome", DarlingStallProbeReader.OutcomeCensusSql, StringComparison.Ordinal);

        /* Both reads take the same optional server scope, so the census and the detail always describe the
           same population — a census over the fleet beside per-server detail would read as a much higher
           failure rate than the server actually has. */
        foreach (var sql in new[] { DarlingStallProbeReader.ProbesSql, DarlingStallProbeReader.OutcomeCensusSql })
        {
            Assert.Contains("$2::integer IS NULL OR p.server_id = $2", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The read is wired everywhere it has to be: the MCP host, the web dashboard's read catalog, and the
    /// dispatch that serves it. A tool present in one and absent from another is reachable by some callers
    /// and not others.
    /// </summary>
    [Fact]
    public void TheReadIsWiredEverywhereItHasToBe()
    {
        Assert.Contains("get_collector_stall_probes", DarlingWebEndpoints.BuildReadDispatch().Keys);
        Assert.Contains("get_collector_stall_probes", DarlingWebEndpoints.CatalogDescriptors.Keys);

        var host = ReadSource("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs");
        Assert.Contains("DarlingMcpStallProbeTools", host, StringComparison.Ordinal);
    }

    /// <summary>
    /// The probe table is deliberately NOT a collector table.
    ///
    /// <para>Which is what keeps it out of the generator-parity pins, the catalog-driven hypertable
    /// conversion and the catalog retention purge — so hand-written DDL here is correct rather than a parity
    /// miss, and the retention below is the only thing bounding it. The V53 / V105 / V111 shape.</para>
    /// </summary>
    [Fact]
    public void TheProbeTableIsNotACollectorTable()
    {
        Assert.DoesNotContain(TableName, CollectorCatalog.All.Select(c => c.TargetTable));
        Assert.DoesNotContain(TableName, CollectorCatalog.All.Select(c => c.Name));

        /* And the population it is being excluded from is non-empty, so this is not passing because the
           catalog failed to load. */
        Assert.NotEmpty(CollectorCatalog.All);
    }

    /// <summary>
    /// The table is bounded, and its horizon matches the <c>collection_log</c> rows a probe is read beside.
    ///
    /// <para>A bounded ARRIVAL RATE over unbounded time is unbounded — the <c>plan_force_actions</c>
    /// reasoning — and this table is outside the catalog purge, so its own DELETE is the only bound. Matching
    /// <c>collection_log</c>'s effective 60 days is not arbitrary: a probe row's only use is beside the run
    /// that triggered it, so outliving that row would leave a sample with nothing to explain, and expiring
    /// first would erase the explanation while the symptom was still queryable.</para>
    /// </summary>
    [Fact]
    public void TheProbeRowsAreBoundedOnTheCollectionLogHorizon()
    {
        Assert.Equal(DarlingRetention.CollectionLogRetentionDays, StallWaitProbeRunner.RetentionDays);

        Assert.Contains($"DELETE FROM collect.{TableName}", StallWaitProbeRunner.PurgeSql, StringComparison.Ordinal);
        Assert.Contains("probe_time < $1", StallWaitProbeRunner.PurgeSql, StringComparison.Ordinal);

        /* The read cannot ask beyond what the store keeps, or an empty answer would look like an absence of
           stalls rather than an absence of retained rows. */
        Assert.Equal(StallWaitProbeRunner.RetentionDays, DarlingMcpStallProbeTools.MaxDaysBack);
    }

    /// <summary>
    /// <b>The wiring pin</b>: the arm is installed on the server-scoped read path, it is handed the two facts
    /// its gates key on, and the probe it fires is NOT awaited by the run.
    ///
    /// <para>This is the #2213 shape of check — both engines' code individually correct, nothing asserting
    /// that the RUNNER asked them — applied to the two claims this design rests on. The engine and the
    /// declared budget are what make the arm inert for ~60 collectors and every PostgreSQL target; an
    /// <c>await</c> on the probe would put a ten-second watchdog on the collector's critical path, carry it
    /// into the run's own duration, and delay the next collector on a server whose sweep is already blown.
    /// Neither is visible in any behaviour a unit test can reach without a live target, so both are asserted
    /// against the shipped source.</para>
    /// </summary>
    [Fact]
    public void TheArmIsWiredOnTheServerScopedPath_AndItsProbeIsNotAwaited()
    {
        var runner = ReadSource("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");

        /* Exactly one arm in the runner: a second call site would be a second concurrent probe per server,
           which is the pool bound this design promises not to exceed. */
        Assert.Single(Regex.Matches(runner, @"StallProbeArm\.Start\("));

        /* Both gates reach the policy from the call site. */
        Assert.Contains("StallProbeArm.Start(\n                    server.Target.Engine,\n                    definition.PerItemWallClockBudget,",
            runner.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* The live counters are handed to the arm, or every observation would read -1 forever and the
           throughput half of the firing condition would be inert. */
        Assert.Contains("countingForProbe = counting;", runner, StringComparison.Ordinal);

        /* And the probe is dispatched, not awaited. */
        Assert.Contains("fire: observation => StallWaitProbeRunner.RunAsync(", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("await StallWaitProbeRunner", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("await StallProbeArm", runner, StringComparison.Ordinal);

        /* The arm is disarmed EXPLICITLY when the read ends, not left to the `using` at the end of the
           branch: that would keep it armed through the storage and watermark phases, long enough for a probe
           to fire against a target the sweep has already stopped reading from. The `using` stays for the
           exception paths, which is why Dispose has to be idempotent. */
        Assert.Contains("stallProbeArm.Dispose();", runner, StringComparison.Ordinal);
        Assert.Contains("using var stallProbeArm = StallProbeArm.Start(", runner, StringComparison.Ordinal);
    }

    private static string RungSql() => PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

    private static bool IsTriggerSideColumn(string name) =>
        name is "probe_time" or "server_id" or "server_name" or "collector_name" or "outcome"
            or "budget_ms" or "trigger_elapsed_ms";

    /// <summary>
    /// The rung's own <c>CREATE TABLE</c> body, parsed rather than transcribed, so this suite compares the
    /// shipped DDL against the shipped INSERT instead of comparing two copies of a list somebody typed.
    /// </summary>
    private static List<(string Name, bool NotNull)> DdlColumns(string rungSql)
    {
        var start = rungSql.IndexOf($"CREATE TABLE IF NOT EXISTS collect.{TableName}", StringComparison.Ordinal);
        Assert.True(start >= 0, "the rung does not create the probe table");

        var open = rungSql.IndexOf('(', start);
        var close = rungSql.IndexOf(");", open, StringComparison.Ordinal);
        Assert.True(open > 0 && close > open, "the probe table's column list could not be located");

        var columns = new List<(string, bool)>();

        foreach (var raw in rungSql[(open + 1)..close].Split(','))
        {
            var line = raw.Trim();
            if (line.Length == 0)
            {
                continue;
            }

            var name = line.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0];
            columns.Add((name, line.Contains("NOT NULL", StringComparison.Ordinal)));
        }

        return columns;
    }

    /// <summary>The INSERT's column list, parsed from the shipped statement for the same reason.</summary>
    private static List<string> InsertColumns(string insertSql)
    {
        var open = insertSql.IndexOf('(', StringComparison.Ordinal);
        var close = insertSql.IndexOf(')', open);
        Assert.True(open > 0 && close > open, "the INSERT's column list could not be located");

        return insertSql[(open + 1)..close]
            .Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }

    private static string ReadSource(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Directory.GetParent(dir)?.FullName;
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }
}
