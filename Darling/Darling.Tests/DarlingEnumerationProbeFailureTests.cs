/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #1837's probe-failure contract, Darling half. An enumerating collector's SQL can now hand the runner an
/// OPTIONAL SECOND RESULT SET of (item_name, error_text) rows for items it could not probe. It has to be a
/// second result set: the FIRST one is the item list both runners consume as database names, so anything
/// added to it would be collected FROM.
///
/// <para>
/// The contract itself is shared with Lite (<see cref="EnumeratedCollectorDriver.ReadEnumerationAsync"/>),
/// and these are the same expectations Lite.Tests' <c>EnumerationProbeFailureTests</c> asserts — pinned
/// independently in both suites so editing one app's copy alone fails a build. A <see cref="DataSet"/>
/// supplies a real multi-result-set reader; only the host wiring stays a source pin, because that branch
/// is the tail of a live enumeration read.
/// </para>
/// </summary>
public sealed class DarlingEnumerationProbeFailureTests
{
    /* ── the back-compat guarantee: one result set behaves exactly as before ── */

    [Fact]
    public async Task One_Result_Set_Enumeration_Is_Unchanged()
    {
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
           declared and selected unconditionally, so the second result set is present but empty. */
        using var reader = MakeReader(items: new[] { "AdventureWorks" }, probeFailures: Array.Empty<(string, string)>());

        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

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
        var note = EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 250);

        Assert.Equal("250 item(s) failed their enumeration probe - see the app log for the per-item errors", note);
    }

    [Fact]
    public void The_Log_Cap_Is_Shared_And_Small()
    {
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
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

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

    /* ── the collector that needed it ── */

    [Fact]
    public void QueryStore_OnPrem_Enumeration_Records_Its_Probe_Failures()
    {
        /* #1836's finding, closed: the on-prem cursor's CATCH was empty, so every per-database probe
           failure vanished. It now records (db, ERROR_MESSAGE()) and returns them as the contract's
           second result set. Asserted here as well as in Lite.Tests because the collector is shared and
           either app regressing it is the same field defect. */
        var plan = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
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
    }

    [Fact]
    public void QueryStore_OnPrem_Enumeration_Screens_Databases_The_Login_Cannot_Enter()
    {
        /* #1823's screen, which this collector never got — the sibling enumerations
           (database_scoped_config, index_object_stats, database_size_stats) all carry it. Without it a
           least-privilege login probes every database it cannot enter and takes a 916 per database; those
           failures were harmless while the CATCH swallowed them, but recording them turns a permission
           posture that is not changing into a probe-failure note and a warning burst EVERY cycle. */
        var plan = QueryStoreCollector.Instance.BuildEnumerationQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        });

        Assert.Contains("HAS_DBACCESS(d.name) = 1", plan!.Text, StringComparison.Ordinal);
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
}
