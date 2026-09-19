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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// CPU utilization — ring buffer (on-prem, MI, RDS) or sys.dm_db_resource_stats (Azure SQL DB).
/// Extracted verbatim from Lite's RemoteCollectorService.Cpu.cs, including the platform rules
/// that are the parity contract: Linux stores NULL other-process CPU when SystemIdle reports
/// 0 there (#1048); incomplete SystemHealth ring-buffer records are skipped (#989);
/// Azure filters server-side on the watermark while the ring buffer dedups client-side
/// (its sample_time is computed and cannot be filtered in SQL).
///
/// <para><b>Also an identity-epoch carrier for SQL Server targets (#3653 A5) — the second one.</b> The
/// ring-buffer batch already reads <c>sys.dm_os_sys_info</c> on every run for <c>ms_ticks</c> and
/// <c>sqlserver_start_time</c> (the ring-buffer timestamps are converted off them), so the instance's start
/// time is on the target side of the wire for free; a second result set hands it back with <c>@@SERVERNAME</c>,
/// and <see cref="ReadAsync"/> gives the pair to <see cref="ServerEpoch.ObserveInstance"/>, which compares it
/// against the pair persisted in <c>collector_state</c> under this collector's name and, on a value →
/// different-value change, forgets every delta baseline for the server, marks the run's note, and persists
/// the new pair. No new DMV, no new permission (the DMV was already required), no new round trip. The Azure
/// SQL DB path carries no identity: its batch never touched the DMV (#1535's permission story) and a logical
/// server's failover keeps its name, so an Azure target simply never observes an epoch here. The declared
/// <see cref="StateKeys"/> are what makes both hosts load and persist the pair (#1962's generic wiring,
/// CollectorStateContractTests).</para>
///
/// <para>This was the ONLY carrier until the carrier-order residue #3694 named was closed: this collector is
/// tenth in both hosts' order, so the five delta families before it subtracted once from the dead instance
/// on every epoch pass. <see cref="WaitStatsCollector"/>, first in the order, now carries the same pair on
/// its own batch and normally does the forgetting; this carrier finds the calculator already on the new
/// identity and only catches its persisted pair up (<see cref="ServerEpoch"/>, "two carriers, one forget").
/// It is kept because <c>wait_stats</c> can be disabled by an operator on either host, and a carrier that
/// can be switched off must not be the only one.</para>
///
/// <para><b>Two clocks per row since Darling V134 / Lite v63 (#3653 item 13, Q7 — the "time honesty"
/// rung).</b> <c>sample_time</c> is, and stays, the MONITORED SERVER'S LOCAL wall clock: on the ring-buffer
/// arm it is <c>SYSDATETIME()</c> minus the entry's age, and every reader that windows or plots it in the
/// server's own frame (Lite's CPU chart, Lite's <c>GetTimeRangeServerLocal</c> window, the viewer's
/// Server-time mode) wants exactly that value; it is also this collector's WATERMARK, and a watermark that
/// changed frame mid-series would re-ingest or skip one UTC offset's worth of samples on the first poll
/// after the upgrade. <c>sample_time_utc</c> is the SAME instant in UTC, written beside it by the same
/// arithmetic off <c>SYSUTCDATETIME()</c> (the two clock functions are runtime constants folded once per
/// statement, so the pair differs by the server's offset to within the nanoseconds between two clock
/// reads — not by a minute-quantised DATEDIFF that could straddle a boundary). Readers that compare the
/// sample against a UTC window prefer it — <c>COALESCE(sample_time_utc, …)</c> with the pre-rung derivation
/// as the fallback — because the derivations were the lie this column retires: Darling's #1262 per-batch
/// de-skew recovers the offset from <c>MAX(sample_time) - collection_time</c> rounded to 15 minutes, and
/// Lite shifts the window by the one <c>utc_offset_minutes</c> the store holds NOW, so every sample on the
/// far side of a DST transition from the offset in force was placed an hour wrong, silently and in the
/// plausible direction. A stored UTC instant needs no offset at all. Pre-rung rows carry NULL and keep
/// the derivation; nothing is backfilled, because the offset a row's server HAD at its sample time is the
/// very thing the store never recorded (<c>PgMigrations</c> V134 says why in full).</para>
///
/// <para>On the Azure SQL DB arm the two columns hold the same value, and that is a fact rather than a
/// shortcut: <c>sys.dm_db_resource_stats.end_time</c> is documented UTC, and Azure SQL Database's server
/// clock IS UTC (<c>GETDATE()</c> there returns UTC, so <c>utc_offset_minutes</c> collects as 0), so
/// "server-local" and UTC coincide and neither column is wrong. The ring-buffer arm is where the frames
/// differ, and it is the arm every on-premises, Managed Instance and RDS target runs.</para>
/// </summary>
public sealed class CpuUtilizationCollector : CollectorDefinitionBase<CpuUtilizationCollector.Row>
{
    public static CpuUtilizationCollector Instance { get; } = new();

    private CpuUtilizationCollector()
    {
    }

    /// <summary>
    /// One CPU sample. <paramref name="SampleTime"/> is the monitored server's LOCAL wall clock (the
    /// watermark; unchanged since the collector was extracted). <paramref name="SampleTimeUtc"/> is the same
    /// instant in UTC (#3653 item 13, Q7) — non-null on every row this collector reads today, nullable
    /// because the STORED column is (pre-rung rows never recorded it) and because the writer's contract is
    /// "one value per declared column", NULL included. No default, so every constructor site states it.
    /// </summary>
    public readonly record struct Row(DateTime SampleTime, int SqlServerCpuUtilization, int? OtherProcessCpuUtilization, DateTime? SampleTimeUtc);

    /* Both Azure arms project sample_time_utc = drs.end_time, the same column sample_time already reads:
       end_time is documented UTC, and Azure SQL Database's own clock is UTC, so the local frame and the UTC
       frame are one value there. Projected LAST, after the three pre-rung columns, so the reader's ordinals
       and the positional writer's payload order (sample_time_utc appended last in PayloadColumns) agree. */
    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (60)
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0,
    sample_time_utc = drs.end_time
FROM sys.dm_db_resource_stats AS drs
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";

    private const string AzureSqlDbWatermarkedQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    sample_time = drs.end_time,
    sqlserver_cpu_utilization = CONVERT(integer, drs.avg_cpu_percent),
    other_process_cpu_utilization = 0,
    sample_time_utc = drs.end_time
FROM sys.dm_db_resource_stats AS drs
WHERE drs.end_time > @last_sample_time
ORDER BY
    drs.end_time DESC
OPTION(RECOMPILE);";

    private const string RingBufferQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @ms_ticks bigint,
    @start_time datetime2(7),
    @is_linux bit = 0;

SELECT
    @ms_ticks = dosi.ms_ticks,
    @start_time = dosi.sqlserver_start_time
FROM sys.dm_os_sys_info AS dosi;

/* Detect SQL Server on Linux. SystemIdle reports 0 in the SCHEDULER_MONITOR
   ring buffer on some Linux/SQL Server version combos, so 100 - SystemIdle - ProcessUtilization
   fabricates a host figure that pins total CPU at 100% (Issue #1048). The ring-buffer metrics
   fix shipped in SQL Server 2025 CU1 (KB5078298 fix 4796293), so real SystemIdle values start
   there, not at 2025 RTM. Prior to that no DMV exposes true host CPU when SystemIdle is 0, so
   other_process is stored as NULL. sys.dm_os_linux_cpu_stats (2025 CU1+) exposes real host CPU
   time but is a cumulative counter requiring a two-sample delta, not a point-in-time snapshot
   like SCHEDULER_MONITOR, so it isn't used here. sys.dm_os_host_info is 2017+; referenced via
   sp_executesql so SQL 2016 never binds it (@is_linux = 0). */
IF OBJECT_ID(N'sys.dm_os_host_info', N'V') IS NOT NULL
    EXEC sys.sp_executesql
        N'SELECT @linux = CASE WHEN hi.host_platform = N''Linux'' THEN 1 ELSE 0 END FROM sys.dm_os_host_info AS hi;',
        N'@linux bit OUTPUT', @linux = @is_linux OUTPUT;

SELECT TOP (60)
    /* MILLISECOND, not SECOND-truncated: dividing by 1000 before DATEADD(SECOND, ...) discards the
       sub-second remainder of the elapsed-ticks offset, and that remainder is different on every poll
       (SYSDATETIME() keeps moving while a given ring-buffer entry's own timestamp does not). Since this
       query re-reads the ring buffer's whole retained history every cycle and dedups client-side on this
       COMPUTED value (a physical entry has no stable key), a recomputed sample_time that drifts forward
       by a fraction of a second on a later poll can land just above the watermark and re-insert the same
       entry as a fresh row - a near-duplicate a moment after the original. Issue #2749: this duplicate
       pattern (two samples ~200ms-1s apart, then a real ~60s gap to the next) collapses
       TimeSeriesGaps.BreakAtGaps's median-derived threshold to sub-second, breaking the CPU chart's line
       at every genuine interval and leaving only the tiny intra-duplicate dash. Millisecond arithmetic
       keeps the full offset, so recomputing the same entry on a later poll reproduces the same instant.
       Issue #2755: passing the full elapsed-ticks offset straight to DATEADD(MILLISECOND, ...) overflows
       once it exceeds int range (~24.8 days of milliseconds) on server uptimes past that point - hit in
       production on a long-uptime box within minutes of #2749's fix shipping. Split into a SECOND-scale
       DATEADD (safely inside int range for realistic uptimes, ~68 years) plus a MILLISECOND-scale DATEADD
       for the 0-999 remainder: same result as the single-step version (verified bit-identical against a
       live SQL Server across the full delta range, including values past the int boundary), because no
       intermediate value ever leaves int range. */
    sample_time = DATEADD(
        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),
        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME())),
    sqlserver_cpu_utilization = x.process_utilization,
    other_process_cpu_utilization =
        CASE
            WHEN @is_linux = 1 AND x.system_idle = 0
            THEN NULL
            WHEN (100 - x.system_idle - x.process_utilization) < 0
            THEN 0
            ELSE 100 - x.system_idle - x.process_utilization
        END,
    /* #3653 item 13 (Q7): the SAME instant as sample_time, in UTC — the identical two-step DATEADD (the
       #2749 millisecond precision and the #2755 overflow split both apply, because the age arithmetic is
       the same) anchored on SYSUTCDATETIME() instead of SYSDATETIME(). Both clock functions are runtime
       constants, folded once per statement, so the two columns differ by exactly the server's UTC offset
       at the moment of the poll. sample_time itself is deliberately NOT changed to UTC: it is the
       watermark and the server-local display value (see the class remarks); readers that want the UTC
       frame take this column and fall back to their pre-rung derivation where it is NULL. */
    sample_time_utc = DATEADD(
        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),
        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSUTCDATETIME()))
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
) AS t
CROSS APPLY
(
    SELECT
        process_utilization = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
        system_idle = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
) AS x
/* Skip ring-buffer records lacking a complete SystemHealth block — their
   XML values extract as NULL and would store NULL samples (Issue #989). */
WHERE x.process_utilization IS NOT NULL
AND   x.system_idle IS NOT NULL
ORDER BY t.timestamp DESC
OPTION(RECOMPILE);

/* #3653 A5: the instance identity, as a second result set off the payload (the default_trace_events
   idiom for a per-run fact the rows cannot carry). @start_time is the sqlserver_start_time this batch
   read above; a login without VIEW SERVER STATE fails this batch at its first DMV and never reaches
   this set, so an unknown start time here means the column was NULL, not that the read was refused. */
SELECT
    server_start_time = @start_time,
    server_name = @@SERVERNAME;";

    public override string Name => "cpu_utilization";

    public override string TargetTable => "cpu_utilization_stats";

    public override string? WatermarkColumn => "sample_time";

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    /// <summary>
    /// The identity pair this collector persists per server (#3653 A5) — declared so both hosts read the
    /// prior pair before the run and write the observed one after it. One row per server per key, for the
    /// life of the server_id; the previous-pair key is written only on an epoch.
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[]
    {
        ServerEpoch.IdentityStateKey,
        ServerEpoch.IdentityPreviousStateKey,
    };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("sample_time", CollectorColumnType.Timestamp),
        new CollectorColumn("sqlserver_cpu_utilization", CollectorColumnType.Integer),
        new CollectorColumn("other_process_cpu_utilization", CollectorColumnType.Integer),
        /* Appended (never inserted) so a fresh generated store and an ALTER-migrated one (Darling V134 /
           Lite v63, #3653 item 13) keep an identical physical column order for the positional writers
           (Lite DuckDB appender / Darling binary COPY). The same instant as sample_time, in UTC; NULL on
           every row written before the rung. */
        new CollectorColumn("sample_time_utc", CollectorColumnType.Timestamp),
    };

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb && context.Watermark.HasValue)
        {
            /* Azure SQL DB: end_time is a real column, so the watermark filters server-side. */
            return new CollectorQuery(
                AzureSqlDbWatermarkedQueryText,
                new[] { new CollectorParameter("@last_sample_time", context.Watermark.Value, CollectorParameterType.DateTime2) });
        }

        return context.Target.IsAzureSqlDb
            ? new CollectorQuery(AzureSqlDbQueryText)
            : new CollectorQuery(RingBufferQueryText);
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var sampleTime = reader.GetDateTime(0);

            /* Client-side dedup for the ring buffer (computed sample_time can't be filtered in SQL). */
            if (!context.Target.IsAzureSqlDb && context.Watermark.HasValue && sampleTime <= context.Watermark.Value)
            {
                continue;
            }

            rows.Add(new Row(
                sampleTime,
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                /* NULL = host/other CPU not derivable (SystemIdle reported 0, Issue #1048) */
                reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                /* The UTC twin (#3653 item 13). Every arm projects it non-null; the guard is for the
                   store's nullable column and the writer's one-value-per-column contract, not for a case
                   the queries above produce. */
                reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3)));
        }

        /* #3653 A5: the batch's SECOND result set — the instance identity. Read off the payload and after
           it, because this collector subtracts nothing itself (CPU samples are gauges), so nothing here is
           ordered before a delta call. With wait_stats carrying the same pair first in the order, the forget
           has normally already happened by the time this runs and ObserveInstance only records the
           observation; when wait_stats is disabled, this is the forget, and the families that subtract
           after this collector are the ones it protects on the pass itself (see ServerEpoch's remarks).
           The Azure batch has no second set; the explicit engine gate says so rather than leaving it to a
           NextResult that happens to return false. A NULL start time (never on this path today, but the
           column is nullable) observes an unknown, which ServerEpoch treats as no evidence. */
        if (!context.Target.IsAzureSqlDb
            && await reader.NextResultAsync(cancellationToken)
            && await reader.ReadAsync(cancellationToken))
        {
            ServerEpoch.ObserveInstance(
                context,
                new ServerEpoch.Stamp(
                    reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.SampleTime)                  /* sample_time TIMESTAMP (server-local; the watermark) */
            .Value(row.SqlServerCpuUtilization)     /* sqlserver_cpu_utilization INTEGER */
            .Value(row.OtherProcessCpuUtilization)  /* other_process_cpu_utilization INTEGER (nullable) */
            .Value(row.SampleTimeUtc);              /* sample_time_utc TIMESTAMP (nullable; the same instant in UTC, #3653 item 13) */
    }
}
