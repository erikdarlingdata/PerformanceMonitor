/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1865, Darling half: a <c>blocked_process_report</c> row the object resolution could not name now says
/// WHY, and the two reasons it can say are routed to deliberately different places.
///
/// <para>
/// The design is the point of the issue, not the plumbing. Both of this collector's resolution CATCHes
/// covered two failures at once: a login with no metadata access to the contended database (a PERMISSION
/// POSTURE — the same answer next cycle and the one after) and a page reallocated between the event firing
/// and the read (transient, and nobody's to fix). The cursors run per contended database on EVERY
/// blocked-process cycle, so adopting #1851's probe-failure channel unscreened would have put a permanent
/// note plus a five-minute warning burst on a posture that is not changing — precisely what #1854 had to add
/// <c>HAS_DBACCESS</c> to <c>query_store</c>'s enumeration to stop. So the posture is SCREENED before it is
/// attempted and answers on the row's own label; only the transient cause rides the channel.
/// </para>
///
/// <para>
/// Lite.Tests' <c>BlockedProcessReportUnresolvedReasonTests</c> pins the identical expectations against the identical shared definition, so editing one
/// app's copy alone fails a build.
/// </para>
/// </summary>
public sealed class DarlingBlockedProcessReportUnresolvedReasonTests
{
    /* ── the screen: what is asked, and that it is asked BEFORE the attempt ── */

    [Fact]
    public void Both_Resolution_Sites_Screen_With_HasDbAccess()
    {
        /* Verified live on SQL 2022 against a login with no user in the target database: DB_NAME() still
           returns the name and DATABASEPROPERTYEX still reports ONLINE, so the guard that was already here
           was never a permission screen — the cross-database lookup was attempted and raised 916 on every
           cycle, forever. HAS_DBACCESS is the one of the three that answers, and both sites need it: with
           VIEW SERVER STATE but no user in the database, sys.dm_db_page_info raises the same 916 the KEY
           lookup does, not a permission-class denial. */
        var text = Sql();

        Assert.Equal(2, Regex.Matches(text, Regex.Escape("HAS_DBACCESS(DB_NAME(@resolve_database_id)) = 1")).Count);
        Assert.Contains("b.lock_type = 'KEY'", text, StringComparison.Ordinal);
        Assert.Contains("b.lock_type IN ('PAGE', 'RID')", text, StringComparison.Ordinal);
    }

    [Fact]
    public void The_Screen_Stamps_Its_Reason_Without_Running_A_Lookup()
    {
        /* The screen has to do two things or it is not a fix: skip the doomed attempt (no burst) AND leave
           the row able to explain itself (the reported gap). A screen that only skipped would trade a noisy
           unknown for a silent one. Two sites x two screened outcomes (no access | database gone), plus the
           two CATCH stamps that share the posture constant. */
        var text = Sql();

        Assert.Equal(2, Regex.Matches(text, Regex.Escape("b.unresolved_reason = @unavailable_reason")).Count);
        Assert.Equal(2, Regex.Matches(text, Regex.Escape("b.unresolved_reason = @posture_reason")).Count);
        Assert.Equal(2, Regex.Matches(text, Regex.Escape("b.unresolved_reason = @resolve_reason")).Count);
    }

    [Fact]
    public void There_Is_Deliberately_No_Server_Level_Permission_Screen()
    {
        /* sys.dm_db_page_info IS gated by VIEW SERVER STATE (VIEW SERVER PERFORMANCE STATE on 2022+), which
           makes a server-scoped screen look obviously right. It is not: the sys.dm_xe_session_targets read
           this same batch opens with needs that permission too, so a login without it fails the FIRST
           statement with 297 and never reaches the page loop. Verified on SQL 2022. A screen there could
           only ever answer yes. This pin exists so the appealing dead branch does not get added back. */
        var text = Sql();

        Assert.DoesNotContain("HAS_PERMS_BY_NAME", text, StringComparison.Ordinal);
    }

    /* ── the routing: which cause is news and which is a posture ── */

    [Fact]
    public void The_Permission_Posture_Is_Never_Written_To_The_Probe_Failure_Channel()
    {
        /* The whole reason #1865 needed a design instead of a mechanical adoption of #1851's channel. Every
           write to the channel must sit behind the posture comparison; a third resolution site added later
           without that guard is exactly the regression this counts. */
        var text = Sql();

        var inserts = Regex.Matches(text, @"INSERT\s+@probe_failures").Count;
        var guards = Regex.Matches(text, Regex.Escape("IF @resolve_reason <> @posture_reason")).Count;

        Assert.Equal(2, inserts);
        Assert.Equal(inserts, guards);
    }

    [Fact]
    public void The_Posture_Reason_Has_Exactly_One_Definition()
    {
        /* Screens and CATCHes both stamp it and each CATCH compares against it. Spelled more than once, a
           later edit to one copy would silently split "screened" rows from "caught" rows into two labels
           and, worse, let a caught posture slip past the comparison into the note channel. */
        var text = Sql();

        var spellings = Regex.Matches(text, Regex.Escape("N'no metadata access'")).Count;

        Assert.Equal(1, spellings);
        Assert.Contains("@posture_reason nvarchar(64) = N'no metadata access'", text, StringComparison.Ordinal);
        Assert.Contains("@unavailable_reason nvarchar(64) = N'database unavailable'", text, StringComparison.Ordinal);
    }

    [Fact]
    public void A_Reallocated_Page_Is_Named_As_Such_And_Does_Ride_The_Channel()
    {
        /* 2561 is what the engine actually raises for a page past the end of the file, an unallocated page,
           or one handed to another object between the event and the read — verified live for all three. It
           is the cause that IS worth reporting: unpredictable, so a note is information rather than noise. */
        var text = Sql();

        Assert.Contains("WHEN @resolve_error = 2561", text, StringComparison.Ordinal);
        Assert.Contains("THEN N'page reallocated'", text, StringComparison.Ordinal);
        Assert.Contains("PAGE/RID lock resolution (", text, StringComparison.Ordinal);
        Assert.Contains("KEY lock resolution (", text, StringComparison.Ordinal);
        /* An unclassified failure still says something specific rather than falling back to silence. */
        Assert.Contains("N'lookup error ' + CONVERT(nvarchar(11), @resolve_error)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Every_Handler_Wraps_The_Execute_Rather_Than_Living_Inside_It()
    {
        /* Moved out of the dynamic batch by #1865. A TRY/CATCH written INSIDE a dynamic batch cannot catch
           that batch's own compile-time failure, and compile time is exactly when both of these fail — a
           cross-database reference the login cannot bind, and (pre-2019) an Invalid-object-name on
           sys.dm_db_page_info. Verified live: 916 raised inside sp_executesql is caught out here with
           ERROR_NUMBER and ERROR_MESSAGE intact. */
        var text = Sql();

        Assert.Equal(2, Regex.Matches(text, "BEGIN TRY").Count);
        Assert.Equal(2, Regex.Matches(text, @"BEGIN TRY\s+EXECUTE sys\.sp_executesql").Count);
        Assert.Equal(2, Regex.Matches(text, "BEGIN CATCH").Count);
    }

    [Fact]
    public void The_Page_Lookup_Stays_In_Dynamic_Sql_Even_Though_Nothing_Is_Spliced_Into_It()
    {
        /* It looks like pointless indirection — no name is concatenated in — but it is what keeps this
           collector runnable on SQL 2016 and 2017 at all: sys.dm_db_page_info does not exist there, and an
           outer-batch reference to it would fail to compile the WHOLE collection query, so the runtime
           version gate would never get the chance to skip it. */
        var text = Sql();

        var pageLookup = text.IndexOf("sys.dm_db_page_info(b.resource_database_id", StringComparison.Ordinal);
        Assert.True(pageLookup > 0, "the page lookup must be present");

        var dynamicAssignment = text.LastIndexOf("SET @resolve_sql = N'", pageLookup, StringComparison.Ordinal);
        Assert.True(dynamicAssignment > 0, "the page lookup must sit inside a dynamic-SQL assignment");
        Assert.Contains("''LIMITED''", text, StringComparison.Ordinal);
    }

    /* ── the label: the part an operator actually reads ── */

    [Fact]
    public void The_Unresolved_Label_Appends_The_Reason_And_Keeps_Its_Prefix()
    {
        /* The reported gap. The prefix is unchanged on purpose: it is what every store, chart groupBy and
           alert fingerprint has always seen, and only the suffix is new. */
        var text = Sql();

        Assert.Contains("N'Unresolved: ' +", text, StringComparison.Ordinal);
        Assert.Contains("ISNULL(N' (' + b.unresolved_reason + N')', N'')", text, StringComparison.Ordinal);

        /* The suffix must be INSIDE the Unresolved arm of the COALESCE, never on a resolved name. */
        var unresolved = text.IndexOf("N'Unresolved: ' +", StringComparison.Ordinal);
        var suffix = text.IndexOf("ISNULL(N' (' + b.unresolved_reason", StringComparison.Ordinal);
        Assert.True(unresolved < suffix, "the reason must be appended to the Unresolved sentinel, not the object name");
    }

    [Fact]
    public void A_Row_Nobody_Tried_To_Resolve_Is_Labeled_Exactly_As_Before()
    {
        /* An OBJECT lock, or a wait resource with no lookup path at all, never reaches a screen or a CATCH,
           so its reason stays NULL. The ISNULL around the WHOLE fragment — not just the column — is what
           keeps those labels byte-identical to the pre-#1865 text instead of trailing an empty " ()". */
        var text = Sql();

        Assert.Contains("unresolved_reason = CONVERT(nvarchar(64), NULL)", text, StringComparison.Ordinal);
        Assert.DoesNotContain("N' (' + ISNULL(b.unresolved_reason", text, StringComparison.Ordinal);
    }

    /* ── shapes this collector takes on other targets ── */

    [Fact]
    public void Pre2019_OnPrem_Says_The_Engine_Cannot_Answer()
    {
        /* On 2016/2017 EVERY page and RID row is unresolvable, for a reason that is neither a permission
           problem nor this server's health — there is simply no sys.dm_db_page_info to call. Before #1865
           an operator there saw a bare Unresolved forever with no way to learn that. */
        var text = Sql();

        Assert.Contains("N'page lookup needs sql server 2019'", text, StringComparison.Ordinal);

        var gate = text.IndexOf("IF @product_version >= 15", StringComparison.Ordinal);
        var fallback = text.IndexOf("N'page lookup needs sql server 2019'", StringComparison.Ordinal);
        Assert.True(gate > 0 && gate < fallback, "the version reason must be the ELSE of the version gate");
    }

    [Fact]
    public void Azure_Takes_The_Same_Resolution_Batch_And_The_Same_Trailing_Set()
    {
        /* Only the ring-buffer source differs by target; the resolution batch and its trailing set are
           shared, so Azure gets the labels too. The trailing set goes UNREAD there because Azure runs this
           collector per database and that path reads through its own contract — the labels, which are the
           part #1865 exists for, are unaffected. */
        var azure = Sql(azure: true);

        Assert.Contains("sys.dm_xe_database_session_targets AS xet", azure, StringComparison.Ordinal);
        Assert.Contains("FROM @probe_failures", azure, StringComparison.Ordinal);
        Assert.Contains("ISNULL(N' (' + b.unresolved_reason + N')', N'')", azure, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(azure, Regex.Escape("HAS_DBACCESS(DB_NAME(@resolve_database_id)) = 1")).Count);
    }

    /* ── the contract wiring ── */

    [Fact]
    public void Declares_The_Contract_And_Puts_Its_Failures_After_The_Payload()
    {
        /* A failure row inside the payload set would be read as a blocked-process report and stored as one. */
        var text = Sql();

        Assert.True(BlockedProcessReportCollector.Instance.EmitsProbeFailures);
        Assert.Contains("@probe_failures TABLE", text, StringComparison.Ordinal);

        var payload = text.IndexOf("b.contentious_object\nFROM #bpr AS b", StringComparison.Ordinal);
        var failures = text.IndexOf("FROM @probe_failures", StringComparison.Ordinal);
        Assert.True(payload >= 0 && failures >= 0, "both result sets must be selected");
        Assert.True(payload < failures, "the payload must be the first result set");
    }

    [Fact]
    public async Task ReadAsync_Stops_At_Its_Payload_So_The_Trailing_Set_Survives_For_The_Host()
    {
        /* The seam, run end to end against the REAL definition: this collector's own read must stop at the
           payload rather than swallowing the trailing set, and the runner's follow-up read must then find
           it. Either half alone tests green; the pair is what breaks. The seeded payload row is the one
           #1865 is about — a report whose object could not be named, carrying its reason in the label. */
        using var reader = MakeReader(
            unresolvedLabel: "Unresolved: page lock, database: StackOverflow2013 (page reallocated)",
            probeFailures: new[] { ("StackOverflow2013", "PAGE/RID lock resolution (page reallocated): Parameter 3 is incorrect for this statement.") });

        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, Context(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("Unresolved: page lock, database: StackOverflow2013 (page reallocated)", row.ContentiousObject);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        var failure = Assert.Single(outcome.ProbeFailures);
        Assert.Equal("StackOverflow2013", failure.Item);
        Assert.Contains("page reallocated", failure.Error, StringComparison.Ordinal);
        Assert.Contains("1 item(s) failed their enumeration probe", outcome.Note);
    }

    [Fact]
    public async Task A_Screened_Cycle_Produces_Labels_And_No_Note_At_All()
    {
        /* The posture case as the host sees it: rows explain themselves, and because the screen kept the
           attempt from happening there is nothing in the trailing set — so no note, no capped log burst, and
           nothing on the collection_log row. This is the outcome the burst constraint demanded, and it is
           the shape the live run on SQL 2022 produced against a login with no user in the database. */
        using var reader = MakeReader(
            unresolvedLabel: "Unresolved: key lock, database: StackOverflow2013 (no metadata access)",
            probeFailures: Array.Empty<(string, string)>());

        var rows = await BlockedProcessReportCollector.Instance.ReadAsync(reader, Context(), CancellationToken.None);

        Assert.Contains("(no metadata access)", Assert.Single(rows).ContentiousObject, StringComparison.Ordinal);

        var outcome = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, CancellationToken.None);

        Assert.Empty(outcome.ProbeFailures);
        Assert.Null(outcome.Note);
    }

    /* ── helpers ── */

    private static string Sql(bool azure = false) =>
        BlockedProcessReportCollector.Instance.BuildQuery(Context(azure)).Text.ReplaceLineEndings("\n");

    private static CollectorContext Context(bool azure = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 30, 12, 0, 0, DateTimeKind.Utc),
        Deltas = null!,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
    };

    private const string ReportXml =
        "<blocked-process-report monitorLoop=\"7\">" +
        "<blocked-process><process spid=\"55\" ecid=\"0\" waittime=\"9000\" " +
        "waitresource=\"PAGE: 6:5:2093167\" currentdbname=\"StackOverflow2013\">" +
        "<inputbuf>UPDATE dbo.Vetos SET x = 1;</inputbuf></process></blocked-process>" +
        "<blocking-process><process spid=\"66\" ecid=\"0\"><inputbuf>BEGIN TRAN;</inputbuf></process></blocking-process>" +
        "</blocked-process-report>";

    /// <summary>
    /// A real multi-result-set reader carrying this collector's actual 5-column payload projection plus the
    /// trailing (name, error_text) set, so the definition's own read and the contract's read run against
    /// each other rather than against a description of each other.
    /// </summary>
    private static DataTableReader MakeReader(string unresolvedLabel, (string Item, string Error)[]? probeFailures)
    {
        var dataSet = new DataSet();

        var payload = new DataTable("payload");
        payload.Columns.Add("event_time", typeof(DateTime));
        payload.Columns.Add("blocked_process_report_xml", typeof(string));
        payload.Columns.Add("object_id", typeof(int));
        payload.Columns.Add("database_id", typeof(int));
        payload.Columns.Add("contentious_object", typeof(string));
        payload.Rows.Add(new DateTime(2026, 7, 30, 11, 59, 0, DateTimeKind.Utc), ReportXml, DBNull.Value, 6, unresolvedLabel);
        dataSet.Tables.Add(payload);

        /* #4200: the gate's own trailing result set, between the payload and the probe-failure set in
           the real batch. ReadAsync consumes exactly this one itself (NextResultAsync) before
           returning, which is what leaves the reader positioned on probe_failures below for
           EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync -- the seam this test is about. */
        var gate = new DataTable("gate");
        gate.Columns.Add("execution_count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Rows.Add(1L, false);
        dataSet.Tables.Add(gate);

        if (probeFailures is not null)
        {
            var failures = new DataTable("probe_failures");
            failures.Columns.Add("name", typeof(string));
            failures.Columns.Add("error_text", typeof(string));
            foreach (var (item, error) in probeFailures)
            {
                failures.Rows.Add(item, error);
            }
            dataSet.Tables.Add(failures);
        }

        return dataSet.CreateDataReader();
    }
}
