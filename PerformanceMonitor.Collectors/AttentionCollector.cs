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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Client attentions (#5640): a query a client cancelled or timed out, from the app-managed
/// <c>PerformanceMonitor_Attention</c> session. The session carries two targets, and the collector uses each for
/// what it is good at:
///
/// <para><b>event_counter is the count, and only the count.</b> It is exact however many events the session saw. Each run
/// reads it and writes a <c>count</c> row ONLY when the delta is above zero: <c>collection_time</c> is this run's time,
/// <c>attention_count</c> is the counter's delta from the prior count (staged in the collector state, so a restart does not
/// lose it, and landed only once the rows of the same run are written). A count BELOW the prior means the session
/// restarted, so the whole current count is the delta. A quiet run (delta 0) writes no count row: that is one row per run
/// per server (per database on Azure SQL Database) of nothing, which is not what a plain, low-volume table is for, and
/// freshness is judged from collection_log, not from zero rows. An unreadable counter writes no count row and keeps the
/// prior, so the next delta spans both runs; the first run after a start has no prior and only stages one. In every
/// case the counter read is staged for the next run. The alert and any rate read <c>count</c> rows and
/// nothing else, so the sum of <c>attention_count</c> equals what the session counted (#5640).</para>
///
/// <para><b>ring_buffer is the samples, and only the samples.</b> It keeps at most max_events_limit events (1,000 by
/// default) and receives them up to MAX_DISPATCH_LATENCY (5 s) after the counter counts them, so it can never be summed
/// against the counter: an event can sit in one but not yet the other, or fall between the two reads of a run. The rows
/// newer than the stored watermark become one <c>sample</c> row each (attention_count 0, event_time set), deduplicated by
/// that watermark. A sample never moves a count and a count never moves the watermark (a count row's event_time is
/// NULL), so no event is counted twice and a storm lost from the ring buffer is still counted.</para>
///
/// <para>The ring buffer's cast and shred run only when the target's execution_count moved (<see cref="XeShredGate"/>)
/// or the counter did, so a quiet server pays for two integer reads. A missing session THROWs 50000 with
/// "XE session ... is not running" in the batch, so the Darling runner records SESSION_MISSING instead of the zero rows
/// and SUCCESS that <see cref="DeadlocksCollector"/> records for the same fault.</para>
///
/// <para>SQL Server gives the event no query_hash and no runtime of the cancelled request (measured: query_hash and
/// plan_handle were 0 on every attention, and the only data fields are <c>duration</c> of the interrupt and
/// <c>request_id</c>), so a row carries the client application, host, database, session and statement text. The text
/// goes through the statement filter (#4348) and is truncated to <see cref="MaxStatementTextLength"/> characters; the
/// fingerprint is <see cref="QueryHashIdentity.StatementFingerprint"/> of the filtered text (before truncation), the one
/// definition the long-query pileup detector groups by, NULL when the filter withheld the text.</para>
/// </summary>
public sealed class AttentionCollector : CollectorDefinitionBase<AttentionCollector.Row>
{
    public static AttentionCollector Instance { get; } = new();

    private AttentionCollector()
    {
    }

    /// <summary>The app-managed session. The ensure (AlwaysOnXeSessions) creates it; this read names it.</summary>
    public const string XeSessionName = "PerformanceMonitor_Attention";

    /// <summary>A run's counter row, written only when the delta is above zero: collection_time is the run's time, attention_count the counter delta, event_time NULL.</summary>
    public const string KindCount = "count";

    /// <summary>One kept ring-buffer event: the detail of an attention, attention_count 0, never summed into a count.</summary>
    public const string KindSample = "sample";

    /// <summary>The stored statement text is cut here (after the statement filter).</summary>
    public const int MaxStatementTextLength = 4000;

    /// <summary>State key: the event_counter count the prior run saw. Per database on Azure SQL DB, via <see cref="CountKeyFor"/>.</summary>
    public const string CountStateKey = "attention_event_count";

    public static string CountKeyFor(string? databaseName) =>
        databaseName is null ? CountStateKey : CountStateKey + ":" + databaseName;

    /// <summary>1 when this cycle's gate skipped the cast+shred, else 0.</summary>
    public const string ShredGatedMeasurement = "shred_gated";

    public sealed class Row
    {
        public DateTime? EventTime { get; set; }
        public string EventKind { get; set; } = KindSample;
        public long AttentionCount { get; set; }
        public string? DatabaseName { get; set; }
        public string? ClientAppName { get; set; }
        public string? ClientHostName { get; set; }
        public int? SessionId { get; set; }
        public int? RequestId { get; set; }
        public long? InterruptDurationUs { get; set; }
        public string? StatementFingerprint { get; set; }
        public string? StatementText { get; set; }
    }

    public override string Name => "attention";

    public override string TargetTable => "attention_events";

    /// <summary>
    /// A plain table on every store (#5640). The 73rd hypertable is a grid decision (#3035, #3044, #3107: the compression
    /// band, the worker sizing and the refresh watch lines all derive from the hypertable count), and attention rows are
    /// low volume: the ring buffer caps the detail at about 1,800 events per read, and a healthy server reads near zero.
    /// The 30-day retention is the time-sliced DELETE the sweep runs for any table that is not a hypertable. The only
    /// holder of this opt-out; <c>AttentionPlainTableTests</c> pins it.
    /// </summary>
    public override bool StaysPlainTable => true;

    public override string PrefixIdColumnName => "attention_event_id";

    public override string? WatermarkColumn => "event_time";

    /// <summary>Azure SQL DB sessions are database-scoped, one per monitored database, like the deadlock and blocked-process ones.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    public override string? PerDatabaseWatermarkColumn => "database_name";

    public override IReadOnlyList<string> StateKeys { get; } = new[] { XeShredGate.StateKey, CountStateKey };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("event_kind", CollectorColumnType.Varchar),
        new CollectorColumn("attention_count", CollectorColumnType.BigInt),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("client_app_name", CollectorColumnType.Varchar),
        new CollectorColumn("client_hostname", CollectorColumnType.Varchar),
        new CollectorColumn("session_id", CollectorColumnType.Integer),
        new CollectorColumn("request_id", CollectorColumnType.Integer),
        new CollectorColumn("interrupt_duration_us", CollectorColumnType.BigInt),
        new CollectorColumn("statement_fingerprint", CollectorColumnType.Varchar),
        new CollectorColumn("statement_text", CollectorColumnType.Varchar),
    };

    /// <summary>
    /// The batch. <c>{0}</c> = the session name, <c>{1}</c> = the sessions DMV, <c>{2}</c> = the targets DMV. Server-scoped and
    /// database-scoped sessions differ only in those two DMVs.
    /// </summary>
    private const string QueryTemplate = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @execution_count bigint,
    @event_count bigint,
    @shred_needed bit,
    @target_data xml;

DECLARE
    @PerformanceMonitor_Attention TABLE
(
    ring_buffer xml NOT NULL
);

/* A missing session must not read as zero rows and SUCCESS: the host maps ""XE session"" to SESSION_MISSING. */
IF NOT EXISTS
(
    SELECT
        1
    FROM {1} AS xes
    WHERE xes.name = N'{0}'
)
BEGIN
    THROW 50000, N'XE session [{0}] is not running', 1;
END;

SELECT
    @execution_count = xet.execution_count
FROM {2} AS xet
JOIN {1} AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{0}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    @target_data = TRY_CAST(xet.target_data AS xml)
FROM {2} AS xet
JOIN {1} AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{0}'
AND   xet.target_name = N'event_counter'
OPTION(RECOMPILE);

SELECT
    @event_count = @target_data.value('sum(/CounterTarget/Packages/Package/Event[@name=""attention""]/@count)', 'bigint');

SET @shred_needed =
    CASE
        WHEN @execution_count IS NULL THEN 1
        WHEN @last_execution_count IS NULL THEN 1
        WHEN @execution_count <> @last_execution_count THEN 1
        WHEN @event_count IS NULL THEN 1
        WHEN @last_event_count IS NULL THEN 1
        WHEN @event_count <> @last_event_count THEN 1
        ELSE 0
    END;

IF @shred_needed = 1
BEGIN
    INSERT
        @PerformanceMonitor_Attention
    (
        ring_buffer
    )
    SELECT /* PerformanceMonitorLite */
        ring_xml = TRY_CAST(xet.target_data AS xml)
    FROM {2} AS xet
    JOIN {1} AS xes
      ON xes.address = xet.event_session_address
    WHERE xes.name = N'{0}'
    AND   xet.target_name = N'ring_buffer'
    OPTION(RECOMPILE);
END;

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    database_name = evt.value('(action[@name=""database_name""]/value)[1]', 'nvarchar(128)'),
    client_app_name = evt.value('(action[@name=""client_app_name""]/value)[1]', 'nvarchar(256)'),
    client_hostname = evt.value('(action[@name=""client_hostname""]/value)[1]', 'nvarchar(256)'),
    session_id = evt.value('(action[@name=""session_id""]/value)[1]', 'integer'),
    request_id = evt.value('(data[@name=""request_id""]/value)[1]', 'integer'),
    interrupt_duration_us = evt.value('(data[@name=""duration""]/value)[1]', 'bigint'),
    statement_text = evt.value('(action[@name=""sql_text""]/value)[1]', 'nvarchar(max)')
FROM @PerformanceMonitor_Attention AS pma
CROSS APPLY pma.ring_buffer.nodes('RingBufferTarget/event[@name=""attention""]') AS e(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
ORDER BY
    evt.value('(@timestamp)[1]', 'datetime2')
OPTION(RECOMPILE);

SELECT
    execution_count = @execution_count,
    event_count = @event_count,
    shred_gated = CASE WHEN @shred_needed = 1 THEN CONVERT(bit, 0) ELSE CONVERT(bit, 1) END;";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var sessionName = AlwaysOnXeSessions.ReadNameFor(AlwaysOnXeSessionKind.Attention, context.AlwaysOnSessionName);
        var text = BuildText(sessionName, context.Target.IsAzureSqlDb);

        /* The detail cutoff: the stored watermark, or ten minutes back on a first run (the sibling XE collectors' rule). */
        var cutoff = context.Watermark ?? context.CollectionTime.AddMinutes(-10);

        var parameters = new List<CollectorParameter>
        {
            new("@cutoff_time", cutoff, CollectorParameterType.DateTime2),
            new("@last_execution_count", XeShredGate.ReadLast(context.State, context.CurrentDatabaseName), CollectorParameterType.BigInt),
            new("@last_event_count", ReadLastCount(context.State, context.CurrentDatabaseName), CollectorParameterType.BigInt),
        };

        return new CollectorQuery(text, parameters);
    }

    /// <summary>The batch text for a session name, exposed so a test can pin it.</summary>
    public static string BuildText(string sessionName, bool isAzureSqlDb) =>
        QueryTemplate
            .Replace("{0}", sessionName.Replace("'", "''", StringComparison.Ordinal), StringComparison.Ordinal)
            .Replace("{1}", isAzureSqlDb ? "sys.dm_xe_database_sessions" : "sys.dm_xe_sessions", StringComparison.Ordinal)
            .Replace("{2}", isAzureSqlDb ? "sys.dm_xe_database_session_targets" : "sys.dm_xe_session_targets", StringComparison.Ordinal);

    /// <summary>The stored prior event_counter count, or null on a first run, a restarted host or an unreadable value.</summary>
    public static long? ReadLastCount(IReadOnlyDictionary<string, string> state, string? databaseName) =>
        state.TryGetValue(CountKeyFor(databaseName), out var raw)
        && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    /// <summary>
    /// The attentions the session counted since the prior run, or null when the counter was unreadable. No prior is the first
    /// run (null: nothing to subtract). A count below the prior is a restarted session, so the delta is the current count.
    /// </summary>
    public static long? CountDelta(long? current, long? prior)
    {
        if (current is null || prior is null)
        {
            return null;
        }

        return current.Value >= prior.Value ? current.Value - prior.Value : current.Value;
    }

    public static string? Truncate(string? text) =>
        text is not null && text.Length > MaxStatementTextLength ? text[..MaxStatementTextLength] : text;

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var scrub = context.BeginStatementScrub();

        while (await reader.ReadAsync(cancellationToken))
        {
            var filtered = scrub.Text(reader.IsDBNull(7) ? null : reader.GetString(7));
            rows.Add(new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventKind = KindSample,
                AttentionCount = 0,
                /* A database-scoped session captures its own database, which the action may also name. */
                DatabaseName = context.CurrentDatabaseName ?? (reader.IsDBNull(1) ? null : reader.GetString(1)),
                ClientAppName = reader.IsDBNull(2) ? null : reader.GetString(2),
                ClientHostName = reader.IsDBNull(3) ? null : reader.GetString(3),
                SessionId = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                RequestId = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                InterruptDurationUs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                StatementFingerprint = QueryHashIdentity.StatementFingerprint(filtered),
                StatementText = Truncate(filtered),
            });
        }

        /* The trailing result set is always one row: this run's counter reads and whether the gate skipped the shred. */
        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            var executionCount = reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0);
            var eventCount = reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1);

            /* A count row only when the delta is above zero: a quiet run, an unreadable counter and the first run (no prior) write
               none, and the prior is still staged below. event_time stays NULL so it never moves the samples' watermark;
               collection_time is stamped by the writer from the run's time, never from a watermark. */
            if (CountDelta(eventCount, ReadLastCount(context.State, context.CurrentDatabaseName)) is { } delta and > 0)
            {
                rows.Add(new Row
                {
                    EventKind = KindCount,
                    AttentionCount = delta,
                    DatabaseName = context.CurrentDatabaseName,
                });
            }

            /* Staged, not pending: a count saved ahead of a failed write would drop that run's events from every later delta. */
            if (executionCount is { } ec)
            {
                context.StagedItemState[XeShredGate.KeyFor(context.CurrentDatabaseName)] = XeShredGate.ToStateValue(ec);
            }

            if (eventCount is { } cc)
            {
                context.StagedItemState[CountKeyFor(context.CurrentDatabaseName)] = cc.ToString(CultureInfo.InvariantCulture);
            }

            context.Measure(ShredGatedMeasurement, !reader.IsDBNull(2) && reader.GetBoolean(2) ? 1 : 0);
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.EventTime)
            .Value(row.EventKind)
            .Value(row.AttentionCount)
            .Value(row.DatabaseName)
            .Value(row.ClientAppName)
            .Value(row.ClientHostName)
            .Value(row.SessionId)
            .Value(row.RequestId)
            .Value(row.InterruptDurationUs)
            .Value(row.StatementFingerprint)
            .Value(row.StatementText);
    }
}
