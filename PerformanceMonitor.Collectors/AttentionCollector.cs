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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Client attentions (#5640): a query a client cancelled or timed out, from the app-managed
/// <c>PerformanceMonitor_Attention</c> session. The session carries two targets, and the collector uses each for
/// what it is good at:
///
/// <para><b>event_counter is the count.</b> It is exact however many events the session saw. Each run reads it and
/// takes the delta from the prior count (staged in the collector state, so a restart does not lose it, and landed
/// only once the rows of the same run are written): the delta is the number of attentions since the last run. A
/// count BELOW the prior means the session restarted, so the whole current count is the delta.</para>
///
/// <para><b>ring_buffer is the detail.</b> It keeps at most max_events_limit events (1,000 by default), so a storm
/// can lose detail. The rows newer than the stored watermark become one <c>attention</c> row each (attention_count 1).
/// When the delta is larger than the rows kept, one <c>overflow</c> row carries the remainder, so the sum of
/// <c>attention_count</c> always equals what the session counted. The first run after a start has no prior count: it
/// keeps the rows newer than the watermark and writes no overflow row, because with nothing to subtract it cannot
/// tell how many events the rows already stored account for.</para>
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
/// fingerprint is a hash of the filtered text (before truncation), NULL when the filter withheld the text.</para>
/// </summary>
public sealed class AttentionCollector : CollectorDefinitionBase<AttentionCollector.Row>
{
    public static AttentionCollector Instance { get; } = new();

    private AttentionCollector()
    {
    }

    /// <summary>The app-managed session. The ensure (AlwaysOnXeSessions) creates it; this read names it.</summary>
    public const string XeSessionName = "PerformanceMonitor_Attention";

    public const string KindAttention = "attention";
    public const string KindOverflow = "overflow";

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
        public string EventKind { get; set; } = KindAttention;
        public long AttentionCount { get; set; } = 1;
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

    /// <summary>The overflow remainder: the counted events the kept rows do not cover, or 0 when nothing is missing or the delta is unknown.</summary>
    public static long OverflowCount(long? delta, long kept) =>
        delta is { } d && d > kept ? d - kept : 0;

    /// <summary>40 hex characters of a hash of the filtered statement text; null for no text or the filter's withheld marker.</summary>
    public static string? Fingerprint(string? filteredText)
    {
        if (string.IsNullOrEmpty(filteredText) || string.Equals(filteredText, SensitiveStatements.PlaceholderText, StringComparison.Ordinal))
        {
            return null;
        }

        /* SHA-256 cut to 160 bits: the width of the SHA-1 the plan named, without a weak-algorithm primitive in the tree. */
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(filteredText)), 0, 20).ToLowerInvariant();
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
                EventKind = KindAttention,
                AttentionCount = 1,
                /* A database-scoped session captures its own database, which the action may also name. */
                DatabaseName = context.CurrentDatabaseName ?? (reader.IsDBNull(1) ? null : reader.GetString(1)),
                ClientAppName = reader.IsDBNull(2) ? null : reader.GetString(2),
                ClientHostName = reader.IsDBNull(3) ? null : reader.GetString(3),
                SessionId = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                RequestId = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                InterruptDurationUs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                StatementFingerprint = Fingerprint(filtered),
                StatementText = Truncate(filtered),
            });
        }

        /* The trailing result set is always one row: this run's counter reads and whether the gate skipped the shred. */
        if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
        {
            var executionCount = reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0);
            var eventCount = reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1);

            var delta = CountDelta(eventCount, ReadLastCount(context.State, context.CurrentDatabaseName));
            var overflow = OverflowCount(delta, rows.Count);
            if (overflow > 0)
            {
                /* Sits at the newest detail row's time, else the stored watermark, so it never moves the watermark past
                   events the ring buffer has not dispatched yet; the counter still counts them in the next delta. */
                DateTime? newest = null;
                foreach (var row in rows)
                {
                    if (row.EventTime is { } t && (newest is null || t > newest))
                    {
                        newest = t;
                    }
                }

                rows.Add(new Row
                {
                    EventTime = newest ?? context.Watermark ?? context.CollectionTime,
                    EventKind = KindOverflow,
                    AttentionCount = overflow,
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
