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
/// Wait statistics from sys.dm_os_wait_stats. Extracted verbatim from Lite's
/// RemoteCollectorService.WaitStats.cs (query text, ignored-wait filtering, delta groups/keys/gap
/// policy, and payload order are the parity contract — WaitStatsCollectorDefinitionTests pins them).
///
/// <para><b>Also the FIRST identity-epoch carrier for SQL Server targets (#3653 A5).</b> Both hosts run a
/// server's due collectors in <c>CollectorScheduleDefaults.All</c>'s declared order and this collector is
/// first, so the instance identity (<c>sys.dm_os_sys_info.sqlserver_start_time</c>, <c>@@SERVERNAME</c>)
/// rides here as a second result set — the same shape <see cref="CpuUtilizationCollector"/> carries — and
/// <see cref="ReadAsync"/> hands it to <see cref="ServerEpoch.ObserveInstance"/> after the rows are read
/// and BEFORE <see cref="WritePayload"/> subtracts any of them. That order is the whole point: a restart,
/// failover or re-point is then forgotten before any SQL Server delta family — this one included — has
/// subtracted from the dead instance's baseline, where the CPU carrier, tenth in the order, let five
/// families fabricate one interval each first. The CPU carrier stays (an operator can disable this
/// collector; see <see cref="ServerEpoch"/>'s remarks for how two observers make one forget). The DMV was
/// already required — <c>sys.dm_os_wait_stats</c> and <c>sys.dm_os_sys_info</c> are both VIEW SERVER STATE
/// — so this is no new permission and no new round trip; a login the first SELECT refuses never reaches the
/// second. Not carried on Azure SQL DB, by #3694's ruling for the CPU carrier and so that the two carriers
/// observe under one rule: the Azure batch is byte-for-byte the pre-#3653 text.</para>
/// </summary>
public sealed class WaitStatsCollector : CollectorDefinitionBase<WaitStatsCollector.Row>
{
    public static WaitStatsCollector Instance { get; } = new();

    private WaitStatsCollector()
    {
    }

    public readonly record struct Row(string WaitType, long WaitingTasks, long WaitTimeMs, long SignalWaitTimeMs);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type = ws.wait_type,
    waiting_tasks_count = ws.waiting_tasks_count,
    wait_time_ms = ws.wait_time_ms,
    signal_wait_time_ms = ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats AS ws
WHERE ws.wait_time_ms > 0
OPTION(RECOMPILE);";

    /* #3653 A5: the instance identity, appended to the batch as a SECOND result set off the payload (the
       default_trace_events idiom for a per-run fact the rows cannot carry, and the CPU carrier's exact
       column names). Read straight from the DMV rather than through a variable because nothing else in this
       batch needs it; the payload SELECT above is unchanged, so every column pin on it holds. Appended only
       when the target is not Azure SQL DB (BuildQuery), so the Azure text stays the verbatim parity contract. */
    private const string IdentityResultSetText = @"

SELECT
    server_start_time = dosi.sqlserver_start_time,
    server_name = @@SERVERNAME
FROM sys.dm_os_sys_info AS dosi;";

    public override string Name => "wait_stats";

    public override string TargetTable => "wait_stats";

    public override string? WatermarkColumn => null;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(context.Target.IsAzureSqlDb ? QueryText : QueryText + IdentityResultSetText);

    /// <summary>
    /// The identity pair this collector persists per server (#3653 A5), under its own collector name — the
    /// same two keys <see cref="CpuUtilizationCollector"/> declares, each carrier with its own prior. Declared
    /// so both hosts read the prior before the run and write the observed one after it (#1962's wiring,
    /// CollectorStateContractTests). One row per server per key, for the life of the server_id; the
    /// previous-pair key is written only on an epoch.
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[]
    {
        ServerEpoch.IdentityStateKey,
        ServerEpoch.IdentityPreviousStateKey,
    };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("wait_type", CollectorColumnType.Varchar),
        new CollectorColumn("waiting_tasks_count", CollectorColumnType.BigInt),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("signal_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_waiting_tasks", CollectorColumnType.BigInt),
        new CollectorColumn("delta_wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("delta_signal_wait_time_ms", CollectorColumnType.BigInt),
        /* Appended (Darling V127 / Lite v60, #3540): the measured seconds the row's three deltas accrued
           over, or 0 when no delta was knowable. Appended at the END because both stores' writers are
           positional — the same rule GoldenCollectorSchema's header states for every column a numbered
           migration adds by ALTER TABLE. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
    };

    /// <summary>
    /// #4428: the delta group <see cref="DetectClear"/> reads baselines from to decide a clear —
    /// <c>wait_time_ms</c>, because it is the family the field evidence measured (22% zero-interval rows on
    /// the affected store) and the one <c>DBCC SQLPERF(..., CLEAR)</c> zeroes alongside the other two.
    /// </summary>
    internal const string ClearDetectionFamily = "wait_stats_time";

    /// <summary>The three families a detected clear rebases together (#4428) — the same three
    /// <see cref="WritePayload"/> keys every row's counters under.</summary>
    internal static readonly string[] RebasedFamilies = { "wait_stats_tasks", "wait_stats_time", "wait_stats_signal" };

    /// <summary>Minimum count of baselined wait types a pass must carry before a majority-lower reading can
    /// even be considered (#4428, rule a) — refuses a clear verdict on a nearly-empty baseline set, where a
    /// handful of naturally-shrinking types could clear the 50% bar by chance.</summary>
    internal const int MinBaselinedTypesForClear = 20;

    /// <summary>
    /// #4428: true when this pass looks like a server-wide <c>DBCC SQLPERF(..., CLEAR)</c> rather than
    /// ordinary accrual or a few types' independent resets. BOTH must hold, and the test fails toward "not a
    /// clear" on any ambiguity — a false positive here would rebase (and so, on the very next ordinary pass,
    /// silently discard) baselines for hundreds of wait types that never reset at all:
    ///
    /// <para>(a) among wait types this calculator has a cached <c>wait_time_ms</c> baseline &gt; 0 for (a
    /// peek — <see cref="ICollectorDeltaCalculator.PeekBaselines"/> — not an update), at least
    /// <see cref="MinBaselinedTypesForClear"/> of them AND at least half read LOWER now than that baseline;</para>
    ///
    /// <para>(b) this pass's SUM of <c>wait_time_ms</c> across every row is LOWER than the SUM of every
    /// cached baseline — a rise in the total (even with a majority of individual types reading lower, which
    /// happens whenever a few heavy waits absorb the interval's growth) means the server kept accruing and
    /// this is not a clear.</para>
    ///
    /// <para>On the field evidence this fires on: ~900 types with baselines, essentially all of them lower,
    /// total lower. On ordinary accrual it does not: most types grow, and even where some shrink (a workload
    /// shift moving load off one wait type onto another) the SUM keeps rising because idle time — the
    /// biggest wait of all on a healthy server — dwarfs everything else and rarely shrinks on its own.</para>
    /// </summary>
    internal static bool DetectClear(IReadOnlyList<Row> rows, IReadOnlyDictionary<string, long> baselines)
    {
        if (rows is null || rows.Count == 0 || baselines is null || baselines.Count == 0)
        {
            return false;
        }

        var baselinedCount = 0;
        var lowerCount = 0;
        var currentTotal = 0L;
        var baselineTotal = 0L;

        foreach (var baseline in baselines.Values)
        {
            baselineTotal += baseline;
        }

        foreach (var row in rows)
        {
            currentTotal += row.WaitTimeMs;

            if (!baselines.TryGetValue(row.WaitType, out var baseline) || baseline <= 0)
            {
                continue;
            }

            baselinedCount++;

            if (row.WaitTimeMs < baseline)
            {
                lowerCount++;
            }
        }

        if (baselinedCount < MinBaselinedTypesForClear)
        {
            return false;
        }

        var majorityLower = lowerCount * 2 >= baselinedCount;

        return majorityLower && currentTotal < baselineTotal;
    }

    /// <summary>
    /// #4428: peeks this pass's <see cref="ClearDetectionFamily"/> baselines, decides via <see
    /// cref="DetectClear"/>, and — on a clear — rebases <see cref="RebasedFamilies"/> to zero and records
    /// the event. Called from <see cref="ReadAsync"/> BEFORE any row's delta is calculated, for the reason
    /// documented there: once <see cref="WritePayload"/> starts subtracting per row, a verdict formed after
    /// even one row has written would compare a mix of pre-clear and already-rebased baselines. Returns
    /// whether a clear was detected and rebased, so a caller (or a test) can assert on it directly.
    /// </summary>
    internal static bool ObserveWaitStatsClear(IReadOnlyList<Row> rows, CollectorContext context)
    {
        var baselines = context.Deltas.PeekBaselines(context.ServerId, ClearDetectionFamily);

        if (!DetectClear(rows, baselines))
        {
            return false;
        }

        context.Deltas.RebaseFamiliesToZero(context.ServerId, RebasedFamilies);
        context.Deltas.NoteWaitStatsClear(context.ServerId, context.ServerName, context.CollectionTime);

        return true;
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var waitType = reader.GetString(0);

            /* Skip ignored wait types (Lite: ignored_wait_types.json — #1240) */
            if (context.IgnoredWaitTypes.Contains(waitType))
            {
                continue;
            }

            rows.Add(new Row(
                WaitType: waitType,
                WaitingTasks: reader.GetInt64(1),
                WaitTimeMs: reader.GetInt64(2),
                SignalWaitTimeMs: reader.GetInt64(3)));
        }

        ObserveWaitStatsClear(rows, context);

        /* #3653 A5: the batch's SECOND result set — the instance identity. Observed HERE, after the rows are
           read and before this method returns, because this collector's subtractions all happen in
           WritePayload, which both hosts call only once ReadAsync has handed back the whole list: so an
           epoch forgets the server's baselines before the first wait_stats delta of the pass, and this
           family is honest on the very pass that sees the change (ServerEpochTests pins the order on the
           recording double — zero delta calls before the forget). Gated on the engine flag rather than on a
           NextResult that happens to return false, exactly as the CPU carrier is: the Azure batch has no
           second set by construction. A NULL start time observes an unknown, which ServerEpoch treats as no
           evidence. */
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
        /* Delta groups, keys, and the shared gap policy are the parity contract — do not reorder.

           The interval is stored beside the deltas (#3540). The calculator reports (delta 0, interval 0)
           when no delta is knowable — first sighting, counter reset, a gap past the policy — and (0, n)
           when the interval was genuinely idle, and that pairing is the ONLY way a reader can tell the two
           apart. This collector used to discard the interval at the write, so the fabricated zero survived
           as a measured one and every per-second reader LAG-divided it into a confident 0.00 ms/sec at
           exactly the moments (restarts) it was unknowable.

           One interval per ROW, the minimum over the row's three groups. The groups share a key and a
           collection time, so first-sighting, gap-policy and seeding decisions are identical across them
           and the three intervals agree in every case but an independent single-counter reset — which for
           this DMV means DBCC SQLPERF CLEAR, and that resets all three together. Taking the minimum rather
           than one headline group's value makes the stored pair mean "every delta in this row is knowable",
           so a reader never divides a reset counter's 0 by a sibling's real interval and reads it as idle. */
        var deltaWaitingTasks = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_tasks", row.WaitType, row.WaitingTasks, out var tasksInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_time", row.WaitType, row.WaitTimeMs, out var timeInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSignalWaitTimeMs = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "wait_stats_signal", row.WaitType, row.SignalWaitTimeMs, out var signalInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var sampleIntervalSeconds = Math.Min(tasksInterval, Math.Min(timeInterval, signalInterval));

        writer
            .Value(row.WaitType)              /* wait_type VARCHAR */
            .Value(row.WaitingTasks)          /* waiting_tasks_count BIGINT */
            .Value(row.WaitTimeMs)            /* wait_time_ms BIGINT */
            .Value(row.SignalWaitTimeMs)      /* signal_wait_time_ms BIGINT */
            .Value(deltaWaitingTasks)         /* delta_waiting_tasks BIGINT */
            .Value(deltaWaitTimeMs)           /* delta_wait_time_ms BIGINT */
            .Value(deltaSignalWaitTimeMs)     /* delta_signal_wait_time_ms BIGINT */
            .Value(sampleIntervalSeconds);    /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }
}
