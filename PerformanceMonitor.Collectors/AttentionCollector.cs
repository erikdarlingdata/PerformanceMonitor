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
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Client attentions (#5640): a query a client cancelled or timed out, from the app-managed
/// <c>PerformanceMonitor_{product}_{id}_Attention</c> session of the install (#9895). The session carries two targets, and the collector uses each for
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
/// <para><b>ring_buffer is the samples, and only the samples.</b> It keeps at most 1,000 events: the target's documented
/// MAX_EVENTS_LIMIT default (Microsoft Learn, "Ring Buffer Target"), which the session does not override. Its
/// <c>max_memory = 1024</c> KB is the second cap and binds first only when the events average over about 1 KB each (a long
/// sql_text), so the buffer holds 1,000 events or fewer, never more. It receives them up to MAX_DISPATCH_LATENCY (5 s) after the counter counts them, so it can
/// never be summed against the counter: an event can sit in one but not yet the other, or fall between the two reads of a run. The rows
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

    /// <summary>
    /// The shared name older builds make and every install used to read. Since #9895 a build makes and reads only its own session,
    /// <c>PerformanceMonitor_{product}_{id}_Attention</c> (<see cref="AlwaysOnXeSessions.OwnNameFor"/>), and never creates, reads,
    /// alters or drops this one, because an older install may still read it. It stays to cut the own name's suffix from, and for
    /// the operator's verb that drops sessions by name.
    /// </summary>
    public const string XeSessionName = "PerformanceMonitor_Attention";

    /// <summary>A run's counter row, written only when the delta is above zero: collection_time is the run's time, attention_count the counter delta, event_time NULL.</summary>
    public const string KindCount = AttentionWindowSql.KindCount;

    /// <summary>One kept ring-buffer event: the detail of an attention, attention_count 0, never summed into a count.</summary>
    public const string KindSample = AttentionWindowSql.KindSample;

    /// <summary>
    /// #9507: one sampled captured call, from an <c>rpc_completed</c> or <c>sql_batch_completed</c> event the session kept because
    /// it ended Abort, or Error after at least 0.9 s. A call the client cancelled while it ran has that shape (SQL Server 2016 to
    /// 2022 report Abort for it, 2025 reports Error), but so does a batch-aborting error after 0.9 s or more (a conversion error,
    /// a deadlock victim that waited), which ends with result Error on 2022 as well. Such a row has no attention partner, so every
    /// read leaves it out. <c>interrupt_duration_us</c> is the call's elapsed time (not the attention's). <c>statement_text</c> is
    /// the <c>sql_text</c> action, which carries no parameter values: the object name, a colon and that text for an RPC, the
    /// object name alone for a stored-procedure RPC with no text, or the text for a batch. The judge matches it to an attention
    /// sample by session and time. No count row: the attention count stays the exact total.
    /// </summary>
    public const string KindAborted = AttentionWindowSql.KindAborted;

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
    /// A hypertable on a TimescaleDB store that is outside the compression grid (#8967). It was a plain table from #5640 on, with a
    /// 30-day time-sliced DELETE, but the table holds about 0.7 GB a day on a busy store, so those deletes left dead space that only a
    /// vacuum reclaims. As a hypertable, retention drops a whole day's chunk at once. The 73rd compression slot is a grid decision
    /// (#3035, #3044, #3107: the compression band, the worker sizing and the refresh watch lines all derive from the hypertable
    /// count), so it takes no phase slot and <c>HypertableCount</c> does not move. Its compression settings and policy are added by
    /// <c>TimescaleSupport.EnsureAttentionEventsCompressionAsync</c> (#9216), without a phase. A store without TimescaleDB keeps a
    /// plain table and the DELETE.
    /// </summary>
    public override bool CompressionOffGrid => true;

    public override string PrefixIdColumnName => "attention_event_id";

    public override string? WatermarkColumn => "event_time";

    /// <summary>
    /// #8967: the newest <c>event_time</c> a written batch carried, so the Darling runner keeps this server's watermark in
    /// memory (<c>ServerWatermarkCache</c>) and reads <c>MAX(event_time)</c> from the store once per server per service
    /// start, not every run. A count row's event_time is NULL and moves nothing, exactly as the store's MAX skips it. The
    /// runner advances the value only after the batch's write returns, floors it to the microsecond the store keeps, and
    /// drops it on any fault, a reconnect or a re-add, so the next run reads the store again. On Azure SQL Database this
    /// collector runs per database, which takes the per-database read and never this cache. Lite ignores the accessor.
    /// </summary>
    public override Func<Row, DateTime?>? WatermarkValueAccessor => static row => row.EventTime;

    /// <summary>#6893: not dispatched on an Amazon RDS Web or Express instance, where RDS refuses every Extended Events session create.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => CollectorTargetInfo.XeSessionSupported(target);

    /// <summary>Azure SQL DB sessions are database-scoped, one per monitored database, like the deadlock and blocked-process ones.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /* #6312: the session is database-scoped on Azure SQL Database and master cannot host one, so the fan-out leaves
       master out instead of counting its "XE session is not running" as a failed database every sweep. */
    public override bool SkipsMasterDatabase => true;

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
    /// The event_counter read (#5754): the attention count as a bigint, from the <c>@target_data</c> xml of the batch. NULL when
    /// <c>@target_data</c> is NULL (no counter target). An XQuery <c>sum()</c> over the untyped <c>@count</c> attributes returns
    /// <c>xs:double</c>, and <c>value()</c> reaches <c>bigint</c> through that value's TEXT, so a <c>'bigint'</c> read fails with
    /// "Error converting data type nvarchar to bigint" the moment the text is <c>0E0</c> or <c>1.2E1</c>: on every run, because
    /// event_counter lists the event at count 0. So the sum is read as <c>float</c> and the
    /// CONVERT to bigint is explicit. Measured on SQL Server 2016 to 2025: the read is exact through 15 significant digits
    /// (999,999,999,999,999 reads back exactly; 9,007,199,254,740,991 reads back one low, because the engine hands the double
    /// over at 15 digits). The counter counts client interrupts, and 10^15 of them is one per millisecond for over 30,000 years,
    /// so the CONVERT never rounds a count that can occur.
    /// The batch embeds this text; a test runs it alone against hand-written counter xml.
    /// </summary>
    public const string EventCountExpression =
        "CONVERT(bigint, @target_data.value('sum(/CounterTarget/Packages/Package/Event[@name=\"attention\"]/@count)', 'float'))";

    /// <summary>
    /// The batch. <c>{0}</c> = the session name, <c>{1}</c> = <see cref="RingBufferXml.ResolveAddressSql"/>, <c>{2}</c> and
    /// <c>{8}</c> = <see cref="RingBufferXml.TargetFromSql"/> for the ring_buffer and event_counter targets, <c>{3}</c> =
    /// <see cref="EventCountExpression"/>. The session is found once (#9517) and all three target reads use its address.
    /// Server-scoped and database-scoped sessions differ only in the DMVs those fragments name.
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

{1}

/* A missing session must not read as zero rows and SUCCESS: the host maps ""XE session"" to SESSION_MISSING. */
IF @xe_address IS NULL
BEGIN
    THROW 50000, N'XE session [{0}] is not running', 1;
END;

SELECT
    @execution_count = xet.execution_count
{2}
OPTION(RECOMPILE);

SELECT
    @target_data = TRY_CAST(xet.target_data AS xml)
{8}
OPTION(RECOMPILE);

SELECT
    @event_count = {3};

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
    {4}

    SELECT /* PerformanceMonitorLite */
        @ring_text = CONVERT(nvarchar(max), xet.target_data),
        @ring_session = @xe_name
    {2}
    OPTION(RECOMPILE);

    {5}

    INSERT
        @PerformanceMonitor_Attention
    (
        ring_buffer
    )
    SELECT
        ring_xml = @ring_xml
    WHERE @ring_xml IS NOT NULL;
END;

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    database_name = evt.value('(action[@name=""database_name""]/value)[1]', 'nvarchar(128)'),
    client_app_name = evt.value('(action[@name=""client_app_name""]/value)[1]', 'nvarchar(256)'),
    client_hostname = evt.value('(action[@name=""client_hostname""]/value)[1]', 'nvarchar(256)'),
    session_id = evt.value('(action[@name=""session_id""]/value)[1]', 'integer'),
    request_id = evt.value('(data[@name=""request_id""]/value)[1]', 'integer'),
    interrupt_duration_us = evt.value('(data[@name=""duration""]/value)[1]', 'bigint'),
    statement_text = evt.value('(action[@name=""sql_text""]/value)[1]', 'nvarchar(max)'),
    event_kind = CONVERT(varchar(20), '{6}'),
    object_name = CONVERT(nvarchar(512), NULL)
FROM @PerformanceMonitor_Attention AS pma
CROSS APPLY pma.ring_buffer.nodes('RingBufferTarget/event[@name=""attention""]') AS e(evt)
WHERE evt.exist('@timestamp[. > sql:variable(""@cutoff_time"")]') = 1

UNION ALL

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    database_name = evt.value('(action[@name=""database_name""]/value)[1]', 'nvarchar(128)'),
    client_app_name = evt.value('(action[@name=""client_app_name""]/value)[1]', 'nvarchar(256)'),
    client_hostname = evt.value('(action[@name=""client_hostname""]/value)[1]', 'nvarchar(256)'),
    session_id = evt.value('(action[@name=""session_id""]/value)[1]', 'integer'),
    request_id = evt.value('(data[@name=""request_id""]/value)[1]', 'integer'),
    interrupt_duration_us = evt.value('(data[@name=""duration""]/value)[1]', 'bigint'),
    statement_text =
        COALESCE
        (
            NULLIF(evt.value('(action[@name=""sql_text""]/value)[1]', 'nvarchar(max)'), N''),
            NULLIF(evt.value('(data[@name=""statement""]/value)[1]', 'nvarchar(max)'), N''),
            NULLIF(evt.value('(data[@name=""batch_text""]/value)[1]', 'nvarchar(max)'), N'')
        ),
    event_kind = CONVERT(varchar(20), '{7}'),
    object_name = evt.value('(data[@name=""object_name""]/value)[1]', 'nvarchar(512)')
FROM @PerformanceMonitor_Attention AS pma
CROSS APPLY pma.ring_buffer.nodes('RingBufferTarget/event[@name=""rpc_completed"" or @name=""sql_batch_completed""]') AS e(evt)
WHERE evt.exist('@timestamp[. > sql:variable(""@cutoff_time"")]') = 1
ORDER BY
    event_time
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
            .Replace("{1}", RingBufferXml.ResolveAddressSql(isAzureSqlDb, sessionName.Replace("'", "''", StringComparison.Ordinal)), StringComparison.Ordinal)
            .Replace("{2}", RingBufferXml.TargetFromSql(isAzureSqlDb, "ring_buffer"), StringComparison.Ordinal)
            .Replace("{8}", RingBufferXml.TargetFromSql(isAzureSqlDb, "event_counter"), StringComparison.Ordinal)
            .Replace("{3}", EventCountExpression, StringComparison.Ordinal)
            .Replace("{4}", RingBufferXml.DeclareSql, StringComparison.Ordinal)
            .Replace("{5}", RingBufferXml.ParseSql, StringComparison.Ordinal)
            .Replace("{6}", KindSample, StringComparison.Ordinal)
            .Replace("{7}", KindAborted, StringComparison.Ordinal);

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
            /* An empty string is no text: the completed events still send their statement and batch_text nodes, empty, because the
               session turns those fields off (#9507), so the batch reads the sql_text action first and the C# side never sees them. */
            var rawText = reader.IsDBNull(7) ? null : reader.GetString(7);
            var filtered = scrub.Text(string.IsNullOrEmpty(rawText) ? null : rawText);
            var aborted = !reader.IsDBNull(8) && string.Equals(reader.GetString(8), KindAborted, StringComparison.Ordinal);
            var objectName = aborted && !reader.IsDBNull(9) ? reader.GetString(9) : null;
            if (!string.IsNullOrEmpty(objectName)
                && !string.Equals(filtered, SensitiveStatements.PlaceholderText, StringComparison.Ordinal))
            {
                /* An RPC names its object: "dbo.proc: <statement>", or the object name alone when the call has no sql_text. A batch has
                   no object name. A withheld statement stays the bare marker: prefixed, it would get a fingerprint and group every
                   withheld call of one object together (#4348). */
                filtered = filtered is null ? objectName : objectName + ": " + filtered;
            }

            rows.Add(new Row
            {
                EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                EventKind = aborted ? KindAborted : KindSample,
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

    /// <summary>
    /// #5980: one caller sighting for the row's client application, client host and database, with its statement
    /// text. The row carries no query_hash, and no login, so there is no hash sighting. Null when the row names
    /// no application or host.
    /// </summary>
    public override IReadOnlyList<QueryTagSighting>? DescribeQueryTagSightings(Row row)
        => QueryTagSightings.Collect(
            QueryTagSightings.Caller(row.DatabaseName, row.ClientAppName, null, row.ClientHostName, row.StatementText));
}
