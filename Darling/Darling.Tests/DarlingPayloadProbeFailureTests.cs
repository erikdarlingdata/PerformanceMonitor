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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1851's probe-failure contract for the PAYLOAD path, Darling half. #1837 gave the ENUMERATING collectors
/// a channel for items they could not probe; a collector whose one result set IS the payload had none, so
/// <c>database_size_stats</c>' server-side cursor threw every inaccessible database into an empty CATCH and
/// reported SUCCESS with that database's file space simply missing. Such a collector may now return an
/// OPTIONAL trailing (item_name, error_text) result set AFTER its payload, which both runners read through
/// the same shared machinery as the enumeration channel.
///
/// <para>
/// The contract itself is shared with Lite
/// (<see cref="EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync"/>), and these are the same
/// expectations Lite.Tests' <c>PayloadProbeFailureTests</c> asserts — pinned independently in both suites so
/// editing one app's copy alone fails a build. A <see cref="DataSet"/> supplies a real multi-result-set
/// reader; only the host wiring stays a source pin, because that branch is the tail of a live payload read.
/// </para>
/// </summary>
public sealed class DarlingPayloadProbeFailureTests
{
    /* ── the back-compat guarantee: a payload-only reader behaves exactly as before ── */

    [Fact]
    public async Task A_Payload_With_No_Trailing_Result_Set_Reads_As_Clean()
    {
        using var reader = MakeReader(payloadRows: 2);
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Empty(outcome.ProbeFailures);
        Assert.Null(outcome.Note);
    }

    [Fact]
    public async Task An_Empty_Trailing_Result_Set_Is_The_Same_As_None()
    {
        /* The shape database_size_stats actually returns on a healthy server: the failure table is declared
           and selected unconditionally, so the trailing set is present but empty. */
        using var reader = MakeReader(payloadRows: 1, probeFailures: Array.Empty<(string, string)>());
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Empty(outcome.ProbeFailures);
        Assert.Null(outcome.Note);
    }

    /* ── the failures themselves ── */

    [Fact]
    public async Task Trailing_Failures_Are_Read_With_Their_Item_And_Error_Text()
    {
        using var reader = MakeReader(
            payloadRows: 1,
            probeFailures: new[]
            {
                ("Restoring", "The database Restoring is not currently available."),
                ("NoAccess", "The server principal is not able to access the database."),
            });
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Equal(2, outcome.ProbeFailures.Count);
        Assert.Equal("Restoring", outcome.ProbeFailures[0].Item);
        Assert.Contains("not currently available", outcome.ProbeFailures[0].Error);
        Assert.Equal("NoAccess", outcome.ProbeFailures[1].Item);
        Assert.Contains("2 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task The_Payload_Rows_Survive_Their_Own_Collectors_Read_And_The_Trailing_One()
    {
        /* The seam this whole change lives on, run end to end against the REAL definition: the collector's
           ReadAsync must stop at its payload (never swallowing the failure set), and the runner's follow-up
           read must then find it. Either half alone tests green and the pair is what breaks — the payload
           row seeded here is exactly the one the defect produces, a database whose probe failed and whose
           files therefore carry used_size_mb NULL. */
        using var reader = MakeReader(
            payloadRows: 2,
            probeFailures: new[] { ("Restoring", "The database Restoring is not currently available.") });

        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(reader, Context(), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal("db0", rows[0].DatabaseName);
        Assert.Null(rows[0].UsedSizeMb);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal("Restoring", failure.Item);
        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task The_Note_Is_The_Enumeration_Channels_Wording_Verbatim()
    {
        /* Two channels, ONE wording. The note is what an operator greps for and what Collection Health
           renders, so a payload collector's summary must be byte-identical to an enumeration's for the same
           count — which it is because both compose it through the same BuildNote. */
        using var reader = MakeReader(payloadRows: 1, probeFailures: ManyFailures(7));
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Equal(
            string.Format(CultureInfo.InvariantCulture, EnumeratedCollectorDriver.ProbeFailureNoteFormat, 7),
            outcome.Note);
        Assert.Equal(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 7), outcome.Note);
    }

    [Fact]
    public async Task The_Empty_Enumeration_Breadcrumb_Never_Rides_A_Payload_Note()
    {
        /* A payload collector has no enumeration whose emptiness could be reported, and its own empty result
           set is already visible as rows_collected = 0. */
        using var reader = MakeReader(payloadRows: 0, probeFailures: new[] { ("A", "denied") });
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.NotNull(outcome.Note);
        Assert.DoesNotContain(EnumeratedCollectorDriver.EmptyEnumerationMessage, outcome.Note);
    }

    /* ── misuse surfaces through the contract instead of killing the cycle ── */

    [Fact]
    public async Task A_Malformed_Trailing_Result_Set_Is_Reported_Not_Thrown()
    {
        /* A one-column trailing set is a first-party SQL defect. Same reasoning as #1837's: throwing would
           trade an invisible problem for a loud unrelated one, and here the cost is higher still — the
           payload rows are already materialized and about to be written. */
        var dataSet = new DataSet();
        dataSet.Tables.Add(PayloadTable(rowCount: 1));
        var malformed = new DataTable("probe_failures");
        malformed.Columns.Add("name", typeof(string));
        malformed.Rows.Add("B");
        dataSet.Tables.Add(malformed);

        using var reader = dataSet.CreateDataReader();
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal(EnumeratedCollectorDriver.ContractViolationItem, failure.Item);
        Assert.Equal(EnumeratedCollectorDriver.ContractViolationError, failure.Error);
        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task A_Reader_That_Faults_While_Advancing_Is_Reported_Not_Thrown()
    {
        /* The failure mode the payload path has and the enumeration path does not: advancing PAST the
           payload can throw, because a batch may raise an error after emitting its rows and the provider
           surfaces that here. Discarding a good collection to announce a diagnostics fault is the one
           outcome this contract must never produce, so the throw becomes a reported failure.

           A closed reader stands in for the faulting provider — DataTableReader rejects NextResult once
           closed, which is the same shape of exception out of the same call. */
        using var reader = MakeReader(payloadRows: 1, probeFailures: Array.Empty<(string, string)>());
        await DrainPayloadAsync(reader);
        reader.Close();

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal(EnumeratedCollectorDriver.ContractViolationItem, failure.Item);
        Assert.Contains("could not be read", failure.Error);
        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task Null_Item_And_Error_Text_Do_Not_Drop_The_Failure()
    {
        /* ERROR_MESSAGE() should never be NULL, but a probe failure is the LAST thing that may vanish for
           want of its own text — the count is the part the operator acts on. */
        var dataSet = new DataSet();
        dataSet.Tables.Add(PayloadTable(rowCount: 0));
        var failureTable = new DataTable("probe_failures");
        failureTable.Columns.Add("name", typeof(string));
        failureTable.Columns.Add("error_text", typeof(string));
        failureTable.Rows.Add(DBNull.Value, DBNull.Value);
        dataSet.Tables.Add(failureTable);

        using var reader = dataSet.CreateDataReader();
        await DrainPayloadAsync(reader);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal(EnumeratedCollectorDriver.UnnamedItem, failure.Item);
        Assert.NotEqual(EnumeratedCollectorDriver.ContractViolationItem, failure.Item);
        Assert.False(string.IsNullOrWhiteSpace(failure.Error));
    }

    /* ── the declaration, and who holds it ── */

    [Fact]
    public void Exactly_Two_Collectors_Declare_The_Contract()
    {
        /* The opt-in is the back-compat guarantee: a collector that does not declare it never has its reader
           advanced past the payload, which is the pre-#1851 behavior exactly. Asserted over the whole
           catalog rather than a sample, so a collector adopting the channel cannot arrive without its own
           SQL and tests — tempdb_stats in particular reads TWO result sets of its own, and a runner that
           advanced unconditionally would read one of them as failures.

           blocked_process_report joined in #1865, and is the reason this list is worth keeping exact rather
           than loosening to a Contains: it adopted the channel for ONE of its two failure causes and screens
           the other out entirely, so a future collector that declares the flag without making that same
           decision is exactly what should fail here. */
        var declaring = CollectorCatalog.All
            .Where(c => c.GetType().GetProperty(nameof(ICollectorDefinition<int>.EmitsProbeFailures))
                ?.GetValue(c) is true)
            .Select(c => c.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "blocked_process_report", "database_size_stats" }, declaring);
        Assert.False(TempDbStatsCollector.Instance.EmitsProbeFailures);
    }

    [Fact]
    public void DatabaseSizeStats_OnPrem_Records_What_Its_Cursor_Could_Not_Probe()
    {
        /* #1851's defect, closed: the per-database CATCH was empty, so a database that was mid-restore or
           inaccessible contributed no used_size_mb under a SUCCESS row that said nothing had happened. */
        var text = DatabaseSizeStatsCollector.Instance.BuildQuery(Context()).Text;

        Assert.Contains("@probe_failures TABLE", text, StringComparison.Ordinal);
        Assert.Contains("INSERT @probe_failures (name, error_text)", text, StringComparison.Ordinal);
        Assert.Contains("VALUES (@db_name, ERROR_MESSAGE());", text, StringComparison.Ordinal);
        Assert.True(DatabaseSizeStatsCollector.Instance.EmitsProbeFailures);

        /* The failures must come AFTER the payload, never merged into it: the payload set is one row per
           FILE and is written to database_size_stats verbatim, so a failure row inside it would be stored
           as a file. */
        var payload = text.IndexOf("FROM sys.master_files AS mf", StringComparison.Ordinal);
        var failures = text.IndexOf("FROM @probe_failures", StringComparison.Ordinal);
        Assert.True(payload >= 0 && failures >= 0, "both result sets must be selected");
        Assert.True(payload < failures, "the payload must be the first result set");

        /* And the CATCH must no longer be empty. */
        Assert.DoesNotContain("BEGIN CATCH\r\n    END CATCH", text.ReplaceLineEndings("\r\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseSizeStats_Azure_Still_Emits_Only_Its_Payload()
    {
        /* Azure SQL DB takes an entirely database-scoped query with no cursor and nothing to probe, so it
           returns no trailing set. The declaration is deliberately NOT branched on the target: the contract
           reads an absent set as zero failures, which keeps one flag honest for both shapes and leaves the
           Azure path byte-for-byte unchanged. */
        var text = DatabaseSizeStatsCollector.Instance.BuildQuery(Context(azure: true)).Text;

        Assert.DoesNotContain("@probe_failures", text, StringComparison.Ordinal);
        Assert.DoesNotContain("CURSOR", text, StringComparison.Ordinal);
        Assert.True(DatabaseSizeStatsCollector.Instance.EmitsProbeFailures);
    }

    /* ── the host wiring, pinned at source (the branch is the tail of a live payload read) ── */

    [Fact]
    public void Host_Reads_The_Trailing_Set_Through_The_Shared_Contract()
    {
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));

        Assert.Contains("definition.EmitsProbeFailures", source, StringComparison.Ordinal);
        Assert.Contains("EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync", source, StringComparison.Ordinal);
        Assert.Contains("EnumeratedCollectorDriver.MaxLoggedProbeFailures", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Host_Reads_The_Trailing_Set_After_The_Payload_And_Before_The_Write()
    {
        /* Ordering is the whole correctness argument for putting this on the plain path: read it before the
           payload and the definition loses its rows; read it after the write and the note misses the row it
           belongs on. It also must not disturb the delta/EndPayload ordering, which it cannot, because
           everything between ReadAsync and the write here touches only the note. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")));
        var plainPath = source[source.IndexOf("Plain single-query path", StringComparison.Ordinal)..];

        /* #2864: the payload is read through the counting decorator rather than the provider reader
           directly, so the anchor is the wrapper. The ORDERING this test exists to pin is unchanged -
           only the argument name moved. */
        var read = plainPath.IndexOf("definition.ReadAsync(counting", StringComparison.Ordinal);
        var probes = plainPath.IndexOf("ReadPayloadProbeFailuresAsync", StringComparison.Ordinal);
        var write = plainPath.IndexOf("WriteBatchAsync(pgConnection", StringComparison.Ordinal);

        Assert.True(read >= 0 && probes >= 0 && write >= 0, "the plain path must read, probe, then write");
        Assert.True(read < probes, "the payload must be read before the trailing set");
        Assert.True(probes < write, "the trailing set must be read before the storage phase");
    }

    /* ── helpers ── */

    private static CollectorContext Context(bool azure = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
    };

    private static (string, string)[] ManyFailures(int count) =>
        Enumerable.Range(0, count).Select(i => ($"db{i}", "denied")).ToArray();

    /// <summary>Reads the payload set to its end, as the definition's own read does, without advancing.</summary>
    private static async Task DrainPayloadAsync(DbDataReader reader)
    {
        while (await reader.ReadAsync(CancellationToken.None))
        {
        }
    }

    /// <summary>
    /// A real multi-result-set <c>DbDataReader</c> over in-memory tables: one table = the pre-#1851 shape,
    /// two = the payload plus its trailing probe-failure set. The payload carries
    /// <c>database_size_stats</c>' real 19 columns so the definition's own read can run against it.
    /// </summary>
    private static DataTableReader MakeReader(int payloadRows, (string Item, string Error)[]? probeFailures = null)
    {
        var dataSet = new DataSet();
        dataSet.Tables.Add(PayloadTable(payloadRows));

        if (probeFailures is not null)
        {
            var failureTable = new DataTable("probe_failures");
            failureTable.Columns.Add("name", typeof(string));
            failureTable.Columns.Add("error_text", typeof(string));
            foreach (var (item, error) in probeFailures)
            {
                failureTable.Rows.Add(item, error);
            }
            dataSet.Tables.Add(failureTable);
        }

        return dataSet.CreateDataReader();
    }

    /// <summary>
    /// One row per file in <c>database_size_stats</c>' payload order, with <c>used_size_mb</c> NULL — the
    /// shape a database whose probe failed actually produces, since the outer SELECT left-joins #file_space.
    /// </summary>
    private static DataTable PayloadTable(int rowCount)
    {
        var table = new DataTable("payload");
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("database_id", typeof(int));
        table.Columns.Add("file_id", typeof(int));
        table.Columns.Add("file_type_desc", typeof(string));
        table.Columns.Add("file_name", typeof(string));
        table.Columns.Add("physical_name", typeof(string));
        table.Columns.Add("total_size_mb", typeof(decimal));
        table.Columns.Add("used_size_mb", typeof(decimal));
        table.Columns.Add("auto_growth_mb", typeof(decimal));
        table.Columns.Add("max_size_mb", typeof(decimal));
        table.Columns.Add("recovery_model_desc", typeof(string));
        table.Columns.Add("compatibility_level", typeof(int));
        table.Columns.Add("state_desc", typeof(string));
        table.Columns.Add("volume_mount_point", typeof(string));
        table.Columns.Add("volume_total_mb", typeof(decimal));
        table.Columns.Add("volume_free_mb", typeof(decimal));
        table.Columns.Add("is_percent_growth", typeof(int));
        table.Columns.Add("growth_pct", typeof(int));
        table.Columns.Add("vlf_count", typeof(int));

        for (var i = 0; i < rowCount; i++)
        {
            table.Rows.Add(
                $"db{i}", i + 5, 1, "ROWS", $"db{i}_data", $"C:\\{i}.mdf",
                1024m, DBNull.Value, 64m, -1m,
                "SIMPLE", 160, "ONLINE", "C:\\", 500000m, 250000m, 0, DBNull.Value, DBNull.Value);
        }

        return table;
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 10 && dir is not null; i++)
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
