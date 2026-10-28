/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1837's probe-failure contract, Lite half. An enumerating collector's SQL can now hand the runner an
/// OPTIONAL SECOND RESULT SET of (item_name, error_text) rows for items it could not probe. It has to be a
/// second result set: the FIRST one is the item list both runners consume as database names, so anything
/// added to it would be collected FROM. Before this, the on-prem query_store enumeration threw every
/// per-database probe failure away in an empty CATCH — a login that could enter no database produced zero
/// items, one SUCCESS row, and no evidence anywhere.
///
/// <para>
/// Unlike the note wiring (pinned at source, because the branch is the tail of a live enumeration read),
/// the contract itself is fully reachable: <see cref="EnumeratedCollectorDriver.ReadEnumerationAsync"/>
/// takes a DbDataReader, and a <see cref="DataSet"/> gives a real multi-result-set one. Darling.Tests
/// pins the identical behavior against the identical shared method.
/// </para>
/// </summary>
public class EnumerationProbeFailureTests
{
    /* ── the back-compat guarantee: one result set behaves exactly as before ── */

    [Fact]
    public async Task One_Result_Set_Enumeration_Is_Unchanged()
    {
        /* Every enumeration that shipped before #1837 returns one result set. It must produce items, no
           probe failures, and NO note — otherwise this contract would annotate every healthy cycle in
           the product. */
        using var reader = MakeReader(items: new[] { "AdventureWorks", "StackOverflow" });

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { "AdventureWorks", "StackOverflow" }, outcome.Items);
        Assert.Empty(outcome.ProbeFailures);
        Assert.Null(outcome.Note);
    }

    [Fact]
    public async Task An_Empty_Second_Result_Set_Is_The_Same_As_None()
    {
        /* The shape query_store's enumeration actually returns on a healthy server: the failure table is
           declared and selected unconditionally, so the second result set is present but empty. That must
           be indistinguishable from an enumeration that never had one. */
        using var reader = MakeReader(items: new[] { "AdventureWorks" }, probeFailures: Array.Empty<(string, string)>());

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { "AdventureWorks" }, outcome.Items);
        Assert.Empty(outcome.ProbeFailures);
        Assert.Null(outcome.Note);
    }

    /* ── the failures themselves ── */

    [Fact]
    public async Task Probe_Failures_Are_Read_With_Their_Item_And_Error_Text()
    {
        using var reader = MakeReader(
            items: new[] { "Healthy" },
            probeFailures: new[]
            {
                ("Restoring", "The database Restoring is not currently available."),
                ("NoAccess", "The server principal is not able to access the database."),
            });

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { "Healthy" }, outcome.Items);
        Assert.Equal(2, outcome.ProbeFailures.Count);
        Assert.Equal("Restoring", outcome.ProbeFailures[0].Item);
        Assert.Contains("not currently available", outcome.ProbeFailures[0].Error);
        Assert.Equal("NoAccess", outcome.ProbeFailures[1].Item);
    }

    [Fact]
    public async Task Items_Found_Plus_Probe_Failures_Notes_Only_The_Failures()
    {
        /* The partial case: most databases enumerated fine, two did not. The cycle collects normally, so
           the empty-enumeration breadcrumb would be a lie — only the probe summary belongs on the row. */
        using var reader = MakeReader(
            items: new[] { "A", "B" },
            probeFailures: new[] { ("C", "boom"), ("D", "boom") });

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.NotNull(outcome.Note);
        Assert.DoesNotContain(EnumeratedCollectorDriver.EmptyEnumerationMessage, outcome.Note);
        Assert.Contains("2 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task Every_Probe_Failing_Notes_Both_The_Emptiness_And_The_Cause()
    {
        /* The defect that started #1837 and #1836 both: a login that cannot enter ANY database probes
           every one of them into the CATCH and enumerates nothing. "0 items" alone would leave the
           operator guessing between "no Query Store databases" and "no access"; the row now says which. */
        using var reader = MakeReader(
            items: Array.Empty<string>(),
            probeFailures: new[] { ("A", "denied"), ("B", "denied"), ("C", "denied") });

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Empty(outcome.Items);
        Assert.NotNull(outcome.Note);
        Assert.StartsWith(EnumeratedCollectorDriver.EmptyEnumerationMessage, outcome.Note);
        Assert.Contains("3 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task Zero_Items_And_Zero_Failures_Is_Still_The_Plain_Empty_Message()
    {
        /* A server with no Query-Store-enabled database and nothing that failed: legitimately empty.
           Unchanged from #1843, and the case the health design must NOT turn into an alarm. */
        using var reader = MakeReader(items: Array.Empty<string>());

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(EnumeratedCollectorDriver.EmptyEnumerationMessage, outcome.Note);
    }

    /* ── the note is a summary, not a dump ── */

    [Fact]
    public void The_Note_Counts_Failures_Rather_Than_Listing_Them()
    {
        /* One unlucky server can fail to probe hundreds of databases. The collection_log note is read at
           a glance in Collection Health, so it carries the count and points at the app log; the per-item
           text goes to the log, capped. */
        var note = EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 250);

        Assert.Equal("250 item(s) failed their enumeration probe - see the app log for the per-item errors", note);
    }

    [Fact]
    public void The_Log_Cap_Is_Shared_And_Small()
    {
        /* Both hosts log at most this many per-item lines per cycle, then one line for the remainder.
           Shared so a burst does not flood one app's log and not the other's. */
        Assert.Equal(5, EnumeratedCollectorDriver.MaxLoggedProbeFailures);
        Assert.Contains("{Item}", EnumeratedCollectorDriver.ProbeFailureLogTemplate);
        Assert.Contains("{Error}", EnumeratedCollectorDriver.ProbeFailureLogTemplate);
        Assert.Contains("{Suppressed}", EnumeratedCollectorDriver.ProbeFailureOverflowLogTemplate);
    }

    [Fact]
    public void Host_Logs_The_Capped_Failures_Through_The_Shared_Templates()
    {
        /* The logging call takes a live ILogger and a live server, so it is pinned at source: both the
           cap and the overflow line must come from the shared constants, never from host-local text. */
        var source = File.ReadAllText(FindRepoFile(
            Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs")));

        Assert.Contains("EnumeratedCollectorDriver.MaxLoggedProbeFailures", source);
        Assert.Contains("EnumeratedCollectorDriver.ProbeFailureLogTemplate", source);
        Assert.Contains("EnumeratedCollectorDriver.ProbeFailureOverflowLogTemplate", source);
    }

    /* ── misuse surfaces through the contract instead of killing the cycle ── */

    [Fact]
    public async Task A_Malformed_Second_Result_Set_Is_Reported_Not_Thrown()
    {
        /* A one-column second result set is a first-party SQL defect. Throwing would trade an invisible
           problem for a loud unrelated one (a whole collector's cycle recorded ERROR); reporting it as a
           probe failure puts it in the note and the log, which is exactly what this contract is for. */
        var dataSet = new DataSet();
        var itemTable = new DataTable("items");
        itemTable.Columns.Add("name", typeof(string));
        itemTable.Rows.Add("A");
        var malformed = new DataTable("probe_failures");
        malformed.Columns.Add("name", typeof(string));
        malformed.Rows.Add("B");
        dataSet.Tables.Add(itemTable);
        dataSet.Tables.Add(malformed);

        using var reader = dataSet.CreateDataReader();
        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { "A" }, outcome.Items);
        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal(EnumeratedCollectorDriver.ContractViolationItem, failure.Item);
        Assert.Equal(EnumeratedCollectorDriver.ContractViolationError, failure.Error);
        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task Null_Error_Text_Does_Not_Drop_The_Failure()
    {
        /* ERROR_MESSAGE() should never be NULL, but a probe failure is the LAST thing that may vanish for
           want of its own text — the count is the part the operator acts on. */
        var dataSet = new DataSet();
        var itemTable = new DataTable("items");
        itemTable.Columns.Add("name", typeof(string));
        var failureTable = new DataTable("probe_failures");
        failureTable.Columns.Add("name", typeof(string));
        failureTable.Columns.Add("error_text", typeof(string));
        failureTable.Rows.Add("A", DBNull.Value);
        dataSet.Tables.Add(itemTable);
        dataSet.Tables.Add(failureTable);

        using var reader = dataSet.CreateDataReader();
        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal("A", failure.Item);
        Assert.False(string.IsNullOrWhiteSpace(failure.Error));
    }

    [Fact]
    public async Task A_Failure_With_No_Item_Name_Is_Not_Mistaken_For_A_Malformed_Result_Set()
    {
        /* Two different things that both leave the item unnamed: a GENUINE failure whose name column came
           back NULL, and a result set with the wrong SHAPE. Sharing one sentinel would send an operator
           hunting a SQL defect that is not there, so they read differently. */
        var dataSet = new DataSet();
        var itemTable = new DataTable("items");
        itemTable.Columns.Add("name", typeof(string));
        var failureTable = new DataTable("probe_failures");
        failureTable.Columns.Add("name", typeof(string));
        failureTable.Columns.Add("error_text", typeof(string));
        failureTable.Rows.Add(DBNull.Value, "denied");
        dataSet.Tables.Add(itemTable);
        dataSet.Tables.Add(failureTable);

        using var reader = dataSet.CreateDataReader();
        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal(EnumeratedCollectorDriver.UnnamedItem, failure.Item);
        Assert.NotEqual(EnumeratedCollectorDriver.ContractViolationItem, failure.Item);
        Assert.Equal("denied", failure.Error);
    }

    /* ── the collector that needed it ── */

    [Fact]
    public void QueryStore_OnPrem_Enumeration_Records_Its_Probe_Failures()
    {
        /* #1836's finding, closed: the on-prem cursor's CATCH was empty, so every per-database probe
           failure vanished. It now records (db, ERROR_MESSAGE()) and returns them as the contract's
           second result set. The SQL is one long const, so this is a text pin — the live behavior needs
           a server that denies a database. */
        var plan = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = new RecordingCollectorDeltaCalculator(),
        });
        Assert.NotNull(plan);

        var text = plan!.Text;
        Assert.Contains("@probe_failures TABLE", text);
        Assert.Contains("INSERT @probe_failures (name, error_text)", text);
        Assert.Contains("VALUES (@db, ERROR_MESSAGE());", text);

        /* The failures must be the SECOND result set — after the item list, never merged into it, or the
           runner would try to collect from a database it could not even probe. */
        var items = text.IndexOf("FROM @result", StringComparison.Ordinal);
        var failures = text.IndexOf("FROM @probe_failures", StringComparison.Ordinal);
        Assert.True(items >= 0 && failures >= 0, "both result sets must be selected");
        Assert.True(items < failures, "the item list must be the first result set");

        /* And the CATCH must no longer be empty. */
        Assert.DoesNotContain("BEGIN CATCH\r\n    END CATCH", text.ReplaceLineEndings("\r\n"));
    }

    [Fact]
    public void QueryStore_OnPrem_Enumeration_Screens_Databases_The_Login_Cannot_Enter()
    {
        /* #1823's screen, which this collector never got — the sibling enumerations
           (database_scoped_config, index_object_stats, database_size_stats) all carry it. Without it a
           least-privilege login probes every database it cannot enter and takes a 916 per database; those
           failures were harmless while the CATCH swallowed them, but recording them turns a permission
           posture that is not changing into a probe-failure note and a warning burst EVERY cycle. The
           screen keeps the probe-failure channel for the failures worth reading. */
        var plan = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = new RecordingCollectorDeltaCalculator(),
        });

        Assert.Contains("IF HAS_DBACCESS(@db) = 0", plan!.Text, StringComparison.Ordinal);
    }

    /* ── helpers ── */

    /// <summary>
    /// A real multi-result-set <c>DbDataReader</c> over in-memory tables: one table = the pre-#1837
    /// shape, two = the item list plus the probe-failure set.
    /// </summary>
    private static DataTableReader MakeReader(string[] items, (string Item, string Error)[]? probeFailures = null)
    {
        var dataSet = new DataSet();

        var itemTable = new DataTable("items");
        itemTable.Columns.Add("name", typeof(string));
        foreach (var item in items)
        {
            itemTable.Rows.Add(item);
        }
        dataSet.Tables.Add(itemTable);

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

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (int i = 0; i < 8 && dir is not null; i++)
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
