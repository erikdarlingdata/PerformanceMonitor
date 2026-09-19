/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity + correctness contract of the long_query_completions collector (#1496): identity, the
/// server- vs database-scoped ring-buffer read + event filter, the event_time watermark (10-minute
/// fallback), the 20-column payload and its WritePayload ordinals, the attention handling (its duration is
/// dropped to NULL because the attention event's own duration is cancellation-handling time, not query
/// runtime), and the shared session DDL builders (the duration predicate on the COMPLETED events only,
/// attention unfiltered, the SET collect_* customizable columns, the 9 actions server-scoped, and the
/// idempotent guarded DROP).
///
/// <para><b>The Azure action list is pinned against a CLOSED list of rejects (#3753).</b> Azure SQL DB
/// refuses the whole CREATE when a database-scoped session names an action it does not offer (error
/// 25744), so one bad action means the session never exists and the collector is dead on every Azure
/// database. #1496 stripped <c>nt_username</c> by hand and left <c>server_principal_name</c> in; the
/// reporter's Azure server rejected that one on every sweep. The pins here assert the Azure DDL carries
/// NONE of <see cref="LongQueryCompletionsCollector.AzureSqlDbUnavailableActions"/>, that the list
/// itself is exactly the two known rejects (so a third is a conscious edit here, not a drive-by), that
/// the on-prem DDL still emits all nine, and that the stand-in <c>sqlserver.username</c> is emitted on
/// Azure only and read by the shared shred into the <c>server_principal_name</c> column.</para>
/// </summary>
public sealed class LongQueryCompletionsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false,
        DateTime? watermark = null,
        DateTime? collectionTime = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? new DateTime(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            Watermark = watermark,
        };

    [Fact]
    public void Identity_And_ThresholdDefault()
    {
        Assert.Equal("long_query_completions", LongQueryCompletionsCollector.Instance.Name);
        Assert.Equal("long_query_completions", LongQueryCompletionsCollector.Instance.TargetTable);
        Assert.Equal("event_time", LongQueryCompletionsCollector.Instance.WatermarkColumn);
        Assert.Equal("long_query_completion_id", LongQueryCompletionsCollector.Instance.PrefixIdColumnName);
        Assert.Equal("PerformanceMonitor_LongQueryCompletions", LongQueryCompletionsCollector.XeSessionName);
        /* The Dashboard's SQL Trace default (install/30_collect_trace_management.sql): 2 seconds in µs. */
        Assert.Equal(2_000_000, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds);
    }

    [Fact]
    public void RunsPerDatabase_OnAzureOnly_WithPerDatabaseWatermark()
    {
        Assert.True(LongQueryCompletionsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(LongQueryCompletionsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));
        Assert.False(LongQueryCompletionsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.Equal("database_name", LongQueryCompletionsCollector.Instance.PerDatabaseWatermarkColumn);
    }

    [Fact]
    public void AppliesTo_EverywhereIncludingAzure()
    {
        /* rpc_completed/sql_batch_completed/attention are all available in an Azure database-scoped session
           (verified against MS Learn), so the collector applies on every platform. */
        Assert.True(LongQueryCompletionsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
        Assert.True(LongQueryCompletionsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(LongQueryCompletionsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAwsRds = true }));
    }

    [Fact]
    public void BuildQuery_ServerScoped_UsesServerDmvs_AndEventFilter()
    {
        var plan = LongQueryCompletionsCollector.Instance.BuildQuery(MakeContext());

        Assert.Contains("sys.dm_xe_session_targets AS xet", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_xe_database_session_targets", plan.Text, StringComparison.Ordinal);
        Assert.Contains("N'PerformanceMonitor_LongQueryCompletions'", plan.Text, StringComparison.Ordinal);
        Assert.Contains("event[@name=\"rpc_completed\" or @name=\"sql_batch_completed\" or @name=\"attention\"]", plan.Text, StringComparison.Ordinal);
        Assert.Contains("> @cutoff_time", plan.Text, StringComparison.Ordinal);
        /* statement/batch_text are coalesced into one column; result is projected. */
        Assert.Contains("data[@name=\"statement\"]", plan.Text, StringComparison.Ordinal);
        Assert.Contains("data[@name=\"batch_text\"]", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_UsesDatabaseScopedDmvs()
    {
        var plan = LongQueryCompletionsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true));

        Assert.Contains("sys.dm_xe_database_session_targets AS xet", plan.Text, StringComparison.Ordinal);
        Assert.Contains("JOIN sys.dm_xe_database_sessions AS xes", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_PinsWatermarkCutoff_AndTenMinuteFallback()
    {
        var watermark = new DateTime(2026, 7, 18, 11, 45, 0, DateTimeKind.Utc);
        var withWatermark = LongQueryCompletionsCollector.Instance.BuildQuery(MakeContext(watermark: watermark));
        var parameter = Assert.Single(withWatermark.Parameters);
        Assert.Equal("@cutoff_time", parameter.Name);
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);

        var collectionTime = new DateTime(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc);
        var fallback = LongQueryCompletionsCollector.Instance.BuildQuery(MakeContext(collectionTime: collectionTime));
        Assert.Equal(collectionTime.AddMinutes(-10), Assert.Single(fallback.Parameters).Value);
    }

    [Fact]
    public void PayloadColumns_20Columns_InTableOrder()
    {
        var names = LongQueryCompletionsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(20, names.Length);
        Assert.Equal("event_time", names[0]);
        Assert.Equal("event_type", names[1]);
        Assert.Equal("database_name", names[3]);
        Assert.Equal("event_sequence", names[10]);
        Assert.Equal("duration_microseconds", names[11]);
        Assert.Equal("result", names[17]);
        Assert.Equal("statement_text", names[18]);
        Assert.Equal("object_name", names[19]);
    }

    /* Reader ordinals follow the ShredSelect projection (0-19); the object[] value types must match the
       reader's Get* calls (long for the bigint payloads, int for the int actions). */
    private static object[] CompletedRow() => new object[]
    {
        new DateTime(2026, 7, 18, 11, 59, 30, DateTimeKind.Utc), // 0 event_time
        "rpc_completed",                                          // 1 event_type
        5_000_000L,                                               // 2 duration (µs)
        3_000_000L,                                               // 3 cpu_time
        100L,                                                     // 4 physical_reads
        2_000L,                                                   // 5 logical_reads
        5L,                                                       // 6 writes
        42L,                                                      // 7 row_count
        "OK",                                                     // 8 result
        "EXEC dbo.myproc @a = 1;",                                // 9 statement_text
        "dbo.myproc",                                             // 10 object_name
        "SqlClient",                                              // 11 client_app_name
        1234,                                                     // 12 client_pid
        7,                                                        // 13 database_id
        "SO",                                                     // 14 database_name
        999L,                                                     // 15 event_sequence
        "CORP\\svc",                                              // 16 nt_username
        "0xABCD",                                                 // 17 query_hash
        "sa",                                                     // 18 server_principal_name
        55,                                                       // 19 session_id
    };

    private static object[] AttentionRow() => new object[]
    {
        new DateTime(2026, 7, 18, 11, 59, 40, DateTimeKind.Utc), // 0 event_time
        "attention",                                             // 1 event_type
        40_000L,                                                 // 2 duration = attention-HANDLING time (must be nulled)
        DBNull.Value,                                            // 3 cpu_time
        DBNull.Value,                                            // 4 physical_reads
        DBNull.Value,                                            // 5 logical_reads
        DBNull.Value,                                            // 6 writes
        DBNull.Value,                                            // 7 row_count
        DBNull.Value,                                            // 8 result
        DBNull.Value,                                            // 9 statement_text
        DBNull.Value,                                            // 10 object_name
        "SqlClient",                                             // 11 client_app_name
        1234,                                                    // 12 client_pid
        7,                                                       // 13 database_id
        "SO",                                                    // 14 database_name
        1000L,                                                   // 15 event_sequence
        DBNull.Value,                                            // 16 nt_username
        "0xABCD",                                                // 17 query_hash
        "sa",                                                    // 18 server_principal_name
        55,                                                      // 19 session_id
    };

    [Fact]
    public async Task ReadAsync_CompletedRow_MapsFields_AndWritePayloadPins20Ordinals()
    {
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(CompletedRow());
        var rows = await LongQueryCompletionsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(5_000_000L, row.DurationMicroseconds);
        Assert.Equal("OK", row.Result);
        Assert.Equal("dbo.myproc", row.ObjectName);
        Assert.Equal("EXEC dbo.myproc @a = 1;", row.StatementText);

        var writer = new RecordingCollectorRowWriter();
        LongQueryCompletionsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(20, writer.Values.Count);
        Assert.Equal(new DateTime(2026, 7, 18, 11, 59, 30, DateTimeKind.Utc), writer.Values[0]);
        Assert.Equal("rpc_completed", writer.Values[1]);
        Assert.Equal(7, writer.Values[2]);            /* database_id */
        Assert.Equal("SO", writer.Values[3]);          /* database_name */
        Assert.Equal(55, writer.Values[4]);            /* session_id */
        Assert.Equal(999L, writer.Values[10]);         /* event_sequence */
        Assert.Equal(5_000_000L, writer.Values[11]);   /* duration_microseconds */
        Assert.Equal(42L, writer.Values[16]);          /* row_count */
        Assert.Equal("OK", writer.Values[17]);         /* result */
        Assert.Equal("EXEC dbo.myproc @a = 1;", writer.Values[18]); /* statement_text */
        Assert.Equal("dbo.myproc", writer.Values[19]); /* object_name */
    }

    [Fact]
    public async Task ReadAsync_AttentionRow_DropsDurationToNull()
    {
        /* The attention event's own duration (40 ms here) is cancellation-handling time, not the query's
           runtime, so it must NOT populate the runtime duration column. */
        var context = MakeContext();
        using var reader = new FakeCollectorDataReader(AttentionRow());
        var rows = await LongQueryCompletionsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("attention", row.EventType);
        Assert.Null(row.DurationMicroseconds);
        Assert.Equal(55, row.SessionId);
        Assert.Equal("0xABCD", row.QueryHash);

        var writer = new RecordingCollectorRowWriter();
        LongQueryCompletionsCollector.Instance.WritePayload(row, writer, context);
        Assert.Equal(20, writer.Values.Count);
        Assert.Null(writer.Values[11]); /* duration_microseconds nulled for attention */
    }

    [Fact]
    public async Task ReadAsync_PerDatabasePath_CaptureDatabaseWinsOverParsedValue()
    {
        var context = MakeContext(isAzureSqlDb: true);
        context.CurrentDatabaseName = "ringdb";

        using var reader = new FakeCollectorDataReader(CompletedRow());
        var rows = await LongQueryCompletionsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal("ringdb", Assert.Single(rows).DatabaseName);
    }

    [Fact]
    public void BuildCreateSessionSql_ServerScoped_PredicateOnCompletedOnly_AttentionUnfiltered()
    {
        var sql = LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: false, 2_000_000);

        Assert.Contains("ON SERVER", sql, StringComparison.Ordinal);
        Assert.Contains("ADD EVENT sqlserver.rpc_completed", sql, StringComparison.Ordinal);
        Assert.Contains("ADD EVENT sqlserver.sql_batch_completed", sql, StringComparison.Ordinal);
        Assert.Contains("ADD EVENT sqlserver.attention", sql, StringComparison.Ordinal);

        /* The duration predicate fires on the two COMPLETED events (twice), never on attention. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "WHERE duration >= 2000000").Count);

        /* Customizable text columns must be turned on or they arrive NULL. */
        Assert.Contains("collect_statement = 1", sql, StringComparison.Ordinal);
        Assert.Contains("collect_batch_text = 1", sql, StringComparison.Ordinal);

        /* #2129 — the field failure this pin used to ENFORCE: rpc_completed has no customizable
           collect_object_name attribute (that one is sp_statement_completed's), so SETting it failed
           the CREATE on every server and the session never existed. object_name is one of
           rpc_completed's default data fields and arrives with no SET at all. */
        Assert.DoesNotContain("collect_object_name", sql, StringComparison.Ordinal);

        /* All 9 QuickSessionStandard actions, including nt_username server-scoped + package0.event_sequence. */
        Assert.Contains("sqlserver.nt_username", sql, StringComparison.Ordinal);
        Assert.Contains("package0.event_sequence", sql, StringComparison.Ordinal);
        Assert.Contains("sqlserver.query_hash", sql, StringComparison.Ordinal);

        Assert.Contains("MEMORY_PARTITION_MODE = NONE", sql, StringComparison.Ordinal);
        Assert.Contains("package0.ring_buffer", sql, StringComparison.Ordinal);
    }

    /* The nine actions the server-scoped DDL has emitted since #1496, spelled out here rather than read
       off the collector so a member silently dropping out of the source list reds this pin instead of
       shrinking both sides at once. Order is the emission order. */
    private static readonly string[] s_onPremActions =
    {
        "sqlserver.client_app_name",
        "sqlserver.client_pid",
        "sqlserver.database_id",
        "sqlserver.database_name",
        "package0.event_sequence",
        "sqlserver.nt_username",
        "sqlserver.query_hash",
        "sqlserver.server_principal_name",
        "sqlserver.session_id",
    };

    [Fact]
    public void BuildCreateSessionSql_ServerScoped_EmitsAllNineActions_InOrder_OnEveryEvent_AndNoAzureStandIn()
    {
        /* #3753 made the Azure branch generate from lists instead of splicing a string; this pins that the
           on-prem branch came through that refactor emitting exactly what it emitted before — all nine,
           in the same order, on each of the three events — and that the Azure stand-in did NOT leak into
           the on-prem DDL (on-prem keeps server_principal_name itself; sqlserver.username is Azure-only). */
        var sql = LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: false, 2_000_000);

        foreach (var action in s_onPremActions)
        {
            Assert.Equal(3, CountOf(sql, action));
        }

        var actionBlock = string.Join(",", s_onPremActions.Select(a => "\n        " + a));
        Assert.Equal(3, CountOf(sql, actionBlock));

        Assert.DoesNotContain(LongQueryCompletionsCollector.AzureSqlDbUsernameAction, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureSqlDbUnavailableActions_IsExactlyTheTwoKnownRejects_AndEveryMemberIsAnOnPremAction()
    {
        /* A CLOSED list: the two actions Azure SQL DB has actually refused in this session's DDL —
           nt_username (#1496, stripped ad hoc then) and server_principal_name (#3753, the one that killed
           the session on the reporter's Azure server). Pinned by literal so growing it is a deliberate
           edit here with the evidence attached, not a drive-by. */
        Assert.Equal(
            new[] { "sqlserver.nt_username", "sqlserver.server_principal_name" },
            LongQueryCompletionsCollector.AzureSqlDbUnavailableActions);

        /* Every member must be something the on-prem list actually emits — a misspelled member would
           strip nothing, the Azure DDL would still carry the real action, and the CREATE would still fail
           while this list claimed otherwise. */
        foreach (var action in LongQueryCompletionsCollector.AzureSqlDbUnavailableActions)
        {
            Assert.Contains(action, s_onPremActions);
        }

        /* The stand-in is not itself a reject, or the Azure branch would strip what it just added. */
        Assert.DoesNotContain(LongQueryCompletionsCollector.AzureSqlDbUsernameAction, LongQueryCompletionsCollector.AzureSqlDbUnavailableActions);
        Assert.Equal("sqlserver.username", LongQueryCompletionsCollector.AzureSqlDbUsernameAction);
    }

    [Fact]
    public void BuildCreateSessionSql_Azure_DatabaseScoped_CarriesNoRejectedAction_SubstitutesUsername_AndOmitsPartitionMode()
    {
        var sql = LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: true, 2_000_000);

        Assert.Contains("ON DATABASE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ON SERVER", sql, StringComparison.Ordinal);

        /* #3753 — the field failure this pin now ENFORCES: Azure SQL DB rejects the whole CREATE (error
           25744) for any action a database-scoped session may not carry, so the Azure DDL must name NONE
           of the closed reject list. Before this pin the DDL stripped nt_username and still emitted
           server_principal_name, and the session never existed on any Azure database. */
        Assert.NotEmpty(LongQueryCompletionsCollector.AzureSqlDbUnavailableActions);
        foreach (var rejected in LongQueryCompletionsCollector.AzureSqlDbUnavailableActions)
        {
            Assert.DoesNotContain(rejected, sql, StringComparison.Ordinal);
        }

        /* The two field failures by name, independent of the list above — so this pin still reds if the
           list itself is edited down, which is what the mutation check that wrote it confirmed. */
        Assert.DoesNotContain("sqlserver.server_principal_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sqlserver.nt_username", sql, StringComparison.Ordinal);

        /* The seven actions Azure allows still ride on all three events, in on-prem order, and the
           stand-in for the principal name is appended — eight per event. */
        var azureActions = s_onPremActions
            .Where(a => !LongQueryCompletionsCollector.AzureSqlDbUnavailableActions.Contains(a))
            .Append(LongQueryCompletionsCollector.AzureSqlDbUsernameAction)
            .ToArray();
        Assert.Equal(8, azureActions.Length);
        foreach (var action in azureActions)
        {
            Assert.Equal(3, CountOf(sql, action));
        }

        var actionBlock = string.Join(",", azureActions.Select(a => "\n        " + a));
        Assert.Equal(3, CountOf(sql, actionBlock));

        /* Azure database-scoped sessions do not accept MEMORY_PARTITION_MODE. */
        Assert.DoesNotContain("MEMORY_PARTITION_MODE", sql, StringComparison.Ordinal);
        /* The predicate + the SET clauses still apply. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "WHERE duration >= 2000000").Count);
        Assert.Contains("collect_statement = 1", sql, StringComparison.Ordinal);
        Assert.Contains("collect_batch_text = 1", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildQuery_ShredReadsEitherPrincipalAction_IntoServerPrincipalName(bool isAzureSqlDb)
    {
        /* The shred is shared by both scopes, so the server_principal_name column must read whichever of
           the two principal actions the session actually carries: server_principal_name on-prem,
           sqlserver.username on Azure (#3753). An action[@name=...] path with no matching node yields
           NULL, so exactly one arm of the COALESCE is ever non-NULL for a given session's payload — the
           on-prem result is unchanged and Azure stops landing NULL. Pinned on both scopes because a
           scope-conditional shred would be the drift this collector centralizes its DDL to prevent. */
        var plan = LongQueryCompletionsCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: isAzureSqlDb));

        Assert.Contains(
            "server_principal_name =\n        COALESCE\n        (\n" +
            "            evt.value('(action[@name=\"server_principal_name\"]/value/text())[1]', 'nvarchar(256)'),\n" +
            "            evt.value('(action[@name=\"username\"]/value/text())[1]', 'nvarchar(256)')\n" +
            "        ),",
            Lf(plan.Text), StringComparison.Ordinal);

        /* nt_username keeps its plain single-action read — on Azure that action is absent and the column
           lands NULL by the same no-node rule; nothing stands in for it. */
        Assert.Contains("nt_username = evt.value('(action[@name=\"nt_username\"]/value/text())[1]', 'nvarchar(256)'),", plan.Text, StringComparison.Ordinal);

        /* The projection order the reader's ordinals depend on is untouched: server_principal_name is still
           the 19th column (ordinal 18), between query_hash and session_id. */
        var qh = plan.Text.IndexOf("query_hash = evt.value", StringComparison.Ordinal);
        var spn = plan.Text.IndexOf("server_principal_name =", StringComparison.Ordinal);
        var sid = plan.Text.IndexOf("session_id = evt.value", StringComparison.Ordinal);
        Assert.True(qh > 0 && qh < spn && spn < sid, "server_principal_name must stay between query_hash and session_id in the shred");
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }
        return count;
    }

    /* The collector source is CRLF (repo .gitattributes), so verbatim-literal lines carry \r\n while the
       generated action lines carry \n; multi-line asserts normalize to LF first, the same way
       QueryStoreCollectorDefinitionTests does. */
    private static string Lf(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void BuildStartSessionSql_ScopeSelected()
    {
        Assert.Equal("ALTER EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER STATE = START;",
            LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: false));
        Assert.Equal("ALTER EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE STATE = START;",
            LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: true));
    }

    [Fact]
    public void BuildDropSessionSql_IsIdempotentlyGuarded_PerScope()
    {
        var serverDrop = LongQueryCompletionsCollector.BuildDropSessionSql(databaseScoped: false);
        Assert.Contains("sys.server_event_sessions", serverDrop, StringComparison.Ordinal);
        Assert.Contains("DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;", serverDrop, StringComparison.Ordinal);
        Assert.Contains("IF EXISTS", serverDrop, StringComparison.Ordinal);

        var databaseDrop = LongQueryCompletionsCollector.BuildDropSessionSql(databaseScoped: true);
        Assert.Contains("sys.database_event_sessions", databaseDrop, StringComparison.Ordinal);
        Assert.Contains("DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;", databaseDrop, StringComparison.Ordinal);
        Assert.Contains("IF EXISTS", databaseDrop, StringComparison.Ordinal);
    }
}
