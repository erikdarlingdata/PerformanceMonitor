/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1875, Darling half: the payload probe-failure contract on the PER-DATABASE path.
///
/// <para>
/// #1851 gave a payload collector an optional trailing (item_name, error_text) result set and both runners
/// read it on the plain path. The per-database path — Azure SQL DB, where the XE collectors run once per
/// monitored database (#1535) — never advanced its reader to that set, so when #1865 made
/// <c>blocked_process_report</c> the second declaring collector AND the first declaring collector that runs
/// per database, its batch built those rows every cycle and threw them away.
/// </para>
///
/// <para>
/// What could not simply be copied from the plain path is the REPORTING, and that is what these pin. The
/// plain path reads once, so it assigns the read's own note straight onto the run. This path reads N times:
/// N single-shot assignments keep only the last database's note, and N calls to the host's capped logger
/// give a 200-database server 200 five-line bursts. <see cref="CycleProbeFailures"/> is where the
/// per-READ answer becomes a per-CYCLE one.
/// </para>
///
/// <para>Lite.Tests' <c>PerDatabaseProbeFailureTests</c> pins the identical expectations, so editing one app's copy alone fails a build.</para>
/// </summary>
public sealed class DarlingPerDatabaseProbeFailureTests
{
    /* ── the accumulation seam ── */

    [Fact]
    public async Task Two_Databases_Failures_Become_One_Note_Listing_Both()
    {
        /* The seam #1875 exists for, run against the real reader and the real shared read: each database
           is a separate reader, and the cycle has to end up with ONE note carrying the TOTAL. Before this,
           the second assignment would have overwritten the first — and in fact neither read happened. */
        var cycle = new CycleProbeFailures();

        using (var first = MakeReader(("StackOverflow2013", "PAGE/RID lock resolution (page reallocated): a")))
        {
            await DrainPayloadAsync(first);
            cycle.Add(await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(first, CancellationToken.None));
        }

        using (var second = MakeReader(("Crap", "PAGE/RID lock resolution (page reallocated): b")))
        {
            await DrainPayloadAsync(second);
            cycle.Add(await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(second, CancellationToken.None));
        }

        Assert.Equal(2, cycle.Failures.Count);
        Assert.Equal("StackOverflow2013", cycle.Failures[0].Item);
        Assert.Equal("Crap", cycle.Failures[1].Item);
        Assert.Contains("2 item(s) failed their enumeration probe", cycle.Note);
    }

    [Fact]
    public async Task The_Per_Read_Note_Is_Discarded_In_Favour_Of_The_Cycles()
    {
        /* Each read composes a note for ITS OWN count, which is right on the plain path and wrong here.
           Add() takes the whole outcome precisely so discarding that note is one documented step rather
           than an omission repeated at each call site. */
        var cycle = new CycleProbeFailures();

        using var reader = MakeReader(("db1", "boom"));
        await DrainPayloadAsync(reader);
        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);

        cycle.Add(outcome);
        using var second = MakeReader(("db2", "boom"));
        await DrainPayloadAsync(second);
        cycle.Add(await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(second, CancellationToken.None));

        Assert.Contains("2 item(s) failed their enumeration probe", cycle.Note);
    }

    [Fact]
    public void A_Cycle_With_Nothing_To_Report_Composes_No_Note()
    {
        /* The healthy case, and the back-compat guarantee: this path set no note at all before #1875, and a
           contract that annotated every clean Azure cycle would be worse than the gap it closed. */
        Assert.Null(new CycleProbeFailures().Note);
        Assert.Empty(new CycleProbeFailures().Failures);
    }

    [Fact]
    public async Task An_Absent_Trailing_Set_Contributes_Nothing()
    {
        /* A declaring collector need not produce the set on every target or every run — the contract reads
           an absent one as zero failures. Across a cycle that stays true per database, so a server where
           only one database has anything to report still gets a note naming exactly one. */
        var cycle = new CycleProbeFailures();

        using (var quiet = MakeReader())
        {
            await DrainPayloadAsync(quiet);
            cycle.Add(await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(quiet, CancellationToken.None));
        }

        Assert.Null(cycle.Note);

        using (var noisy = MakeReader(("db2", "boom")))
        {
            await DrainPayloadAsync(noisy);
            cycle.Add(await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(noisy, CancellationToken.None));
        }

        Assert.Contains("1 item(s) failed their enumeration probe", cycle.Note);
    }

    [Fact]
    public void The_Cycle_Keeps_Every_Failure_So_The_Log_Cap_Can_Be_Applied_Once()
    {
        /* The cap belongs to the host's logger and is applied to whatever list it is handed. Handing it one
           list per database would cap per database — the burst this had to avoid — so the accumulator keeps
           them ALL and the host logs once. Twelve failures across three databases is one 5-line burst plus
           one overflow line, not three bursts. */
        var cycle = new CycleProbeFailures();
        for (var database = 0; database < 3; database++)
        {
            cycle.Add(new ProbeFailureOutcome(
                Enumerable.Range(0, 4).Select(i => new EnumerationProbeFailure($"db{database}_{i}", "denied")).ToArray(),
                Note: "ignored"));
        }

        Assert.Equal(12, cycle.Failures.Count);
        Assert.Contains("12 item(s) failed their enumeration probe", cycle.Note);
        Assert.True(cycle.Failures.Count > EnumeratedCollectorDriver.MaxLoggedProbeFailures,
            "the fixture must exceed the cap or it proves nothing about it");
    }

    [Fact]
    public void The_Note_Comes_From_The_Same_Composer_Both_Other_Channels_Use()
    {
        /* Three channels, one wording. A second formatter here would give an operator a third string to
           grep for the same condition, which is the drift #1837 and #1851 each closed in turn. */
        var cycle = new CycleProbeFailures();
        cycle.Add(new ProbeFailureOutcome(new[] { new EnumerationProbeFailure("db", "denied") }, Note: null));

        Assert.Equal(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 1), cycle.Note);
    }

    /* ── the host wiring: read per database, report per cycle ── */

    [Fact]
    public void The_Per_Database_Loop_Reads_The_Trailing_Set_And_Reports_It_After_The_Loop()
    {
        /* The ordering IS the fix. The read must sit inside the loop (once per database, while that
           database's reader is still open) and BOTH the note and the capped log must sit outside it (once
           per cycle). Getting either side wrong reintroduces exactly what #1875 was filed for: a read
           outside the loop reads nothing, and a report inside it overwrites the note N times and bursts the
           log N times. */
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));
        var perDatabase = source[source.IndexOf("definition.RunsPerDatabase(context.Target)", StringComparison.Ordinal)..];

        var declaration = perDatabase.IndexOf("definition.EmitsProbeFailures", StringComparison.Ordinal);
        var read = perDatabase.IndexOf("ReadPayloadProbeFailuresAsync", StringComparison.Ordinal);
        var loopExit = perDatabase.IndexOf("context.CurrentDatabaseName = null;", StringComparison.Ordinal);
        var note = perDatabase.IndexOf("cycleProbeFailures.Note", StringComparison.Ordinal);
        var log = perDatabase.IndexOf("LogEnumerationProbeFailures", StringComparison.Ordinal);

        Assert.True(declaration >= 0, "the per-database path must consult the declaration");
        Assert.True(read >= 0 && loopExit >= 0 && note >= 0 && log >= 0, "read, loop exit, note and log must all be present");
        Assert.True(declaration < loopExit, "the declaration must be checked inside the loop");
        Assert.True(read < loopExit, "the trailing set must be read inside the per-database loop");
        Assert.True(loopExit < note, "the note must be composed once, after the loop");
        Assert.True(loopExit < log, "the capped log burst must happen once, after the loop");
    }

    [Fact]
    public void Blocked_Process_Report_Is_The_Collector_This_Reaches()
    {
        /* The gap was invisible while database_size_stats was the only declaring collector, because it
           never runs per database. This pins the pairing that made it reachable, so a future collector
           cannot quietly become the third case without someone reading this. */
        Assert.True(BlockedProcessReportCollector.Instance.EmitsProbeFailures);
        Assert.True(BlockedProcessReportCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));

        /* #5498: database_size_stats now runs per database on Azure too. Its Azure text emits no trailing set, and
           the contract reads an absent set as zero failures, so declaring the flag there changes nothing. */
        Assert.True(DatabaseSizeStatsCollector.Instance.EmitsProbeFailures);
        Assert.True(DatabaseSizeStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(DatabaseSizeStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
    }

    /* ── helpers ── */

    private static async Task DrainPayloadAsync(DbDataReader reader)
    {
        while (await reader.ReadAsync(CancellationToken.None))
        {
        }
    }

    /// <summary>One database's reader: a one-row payload, then the trailing failure set when given one.</summary>
    private static DataTableReader MakeReader(params (string Item, string Error)[] failures)
    {
        var dataSet = new DataSet();

        var payload = new DataTable("payload");
        payload.Columns.Add("event_time", typeof(DateTime));
        payload.Columns.Add("blocked_process_report_xml", typeof(string));
        payload.Columns.Add("object_id", typeof(int));
        payload.Columns.Add("database_id", typeof(int));
        payload.Columns.Add("contentious_object", typeof(string));
        payload.Rows.Add(DateTime.UtcNow, "<blocked-process-report/>", DBNull.Value, 6, "dbo.Users");
        dataSet.Tables.Add(payload);

        if (failures.Length > 0)
        {
            var table = new DataTable("probe_failures");
            table.Columns.Add("name", typeof(string));
            table.Columns.Add("error_text", typeof(string));
            foreach (var (item, error) in failures)
            {
                table.Rows.Add(item, error);
            }
            dataSet.Tables.Add(table);
        }

        return dataSet.CreateDataReader();
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
