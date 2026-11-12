/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly summary of <c>collect.job_history</c>'s job categories (V265, #8549): one row per (server, run hour, category) holding the
/// smallest and largest <c>run_datetime</c> the category has in that hour, so the Job History tab's Category list reads a few thousand
/// summary rows instead of every run of the window.
///
/// <para><b>Why.</b> The list is <c>SELECT DISTINCT category_name</c> over the window's runs. On a busy field store that is 21 M rows for
/// 30 days (11 s with every page cached, 34 s cold) and, for 24 hours, five index scans of the newest uncompressed chunks that read their
/// pages from disk (32 s cold), to find two categories. The answer changes only when a run of a new category arrives, so a small table
/// the service keeps answers it from a handful of rows.</para>
///
/// <para><b>What a row holds.</b> <c>run_hour</c> is <c>date_trunc('hour', run_datetime)</c> on the server's own wall clock (the clock
/// <c>run_datetime</c> is stored on), and <c>first_run</c> and <c>last_run</c> are the smallest and largest <c>run_datetime</c> of the
/// category's runs in that hour. A row is only ever widened (<c>LEAST</c> and <c>GREATEST</c>), so folding the same run in twice changes
/// nothing. NULL and empty category names are not kept, as the read skips them.</para>
///
/// <para><b>How it is kept.</b> <see cref="RollUpAsync"/> runs on the store self-metrics sweep, after
/// <c>CollectionLogHourly.RollUpAsync</c>, in one transaction. It folds the rows <i>collected</i> (by <c>collection_time</c>, not by run
/// time: a first collection copies a server's whole msdb history in one batch, so a run's hour can be long before the collection that
/// stores it) in the last <see cref="RecomputeHours"/> closed hours, then walks the coverage back by at most
/// <see cref="CatchUpDays"/> days toward the retention edge (the backfill, newest first), then moves the coverage row and deletes run
/// hours older than the retention edge. The one-row <c>collect.job_history_category_coverage</c> says which collection times the table holds
/// whole: <c>covered_from</c> to <c>covered_to</c>. The migration creates the tables empty, so the upgrade itself stays fast and the passes
/// fill them.</para>
///
/// <para><b>How it is read.</b> <see cref="BuildRolledCategoriesSql"/>: the categories of the summary's rows whose runs reach into the
/// window, plus the old statement's answer over the rows collected after <c>covered_to</c> (the open hour or two the pass has not folded
/// yet). Read only where the coverage holds the window (<see cref="Covers"/>), else the old statement as it was.</para>
/// </summary>
public static class JobHistoryCategoryHourly
{
    /// <summary>The summary table.</summary>
    public const string Table = "collect.job_history_category_hourly";

    /// <summary>The one-row coverage table.</summary>
    public const string CoverageTable = "collect.job_history_category_coverage";

    /// <summary>Closed hours every pass folds again, to count rows written late into an hour that is already folded. Folding is idempotent.</summary>
    public const int RecomputeHours = 2;

    /// <summary>The most days one pass walks the coverage back toward the retention edge.</summary>
    public const int CatchUpDays = 1;

    /// <summary>Command timeout of each statement of the pass.</summary>
    public const int PassTimeoutSeconds = 120;

    /// <summary>The pass's own slice of the sweep budget, so a slow backfill cannot cancel what runs after it.</summary>
    public static readonly TimeSpan PassSliceBudget = TimeSpan.FromSeconds(PassTimeoutSeconds);

    /// <summary>
    /// The two tables as the V265 rung runs them. Idempotent: a re-run keeps the rows. The rung embeds this text, so editing it edits a
    /// shipped rung: add a new rung instead. Plain tables on every store (about one row per server per category per hour, kept as long as
    /// <c>job_history</c> is); the <c>run_hour</c> index serves the read's range and the retention DELETE. Needs no GRANT: the
    /// <c>collect</c> schema's blanket SELECT covers a table a migration introduces.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.job_history_category_hourly
(
    server_id integer NOT NULL,
    run_hour timestamp NOT NULL,
    category_name text NOT NULL,
    first_run timestamp NOT NULL,
    last_run timestamp NOT NULL,
    CONSTRAINT pk_job_history_category_hourly PRIMARY KEY (server_id, run_hour, category_name)
);

CREATE INDEX IF NOT EXISTS idx_job_history_category_hourly_hour ON collect.job_history_category_hourly (run_hour);

CREATE TABLE IF NOT EXISTS collect.job_history_category_coverage
(
    id integer NOT NULL,
    covered_from timestamp NOT NULL,
    covered_to timestamp NOT NULL,
    CONSTRAINT pk_job_history_category_coverage PRIMARY KEY (id),
    CONSTRAINT ck_job_history_category_coverage_single_row CHECK (id = 1)
);";

    /// <summary>
    /// The coverage row and <c>job_history</c>'s retention in days (the fleet override when set, else the default: the horizon the
    /// raw table's own purge uses). No coverage row (a store no pass has run on) gives NULLs for the first two columns.
    /// </summary>
    internal static readonly string StateSql = $@"
SELECT c.covered_from, c.covered_to, {DarlingRetentionHorizons.FleetRetentionDaysSql("job_history")}
FROM (SELECT 1) AS one
LEFT JOIN collect.job_history_category_coverage AS c ON c.id = 1;";

    /// <summary>
    /// Folds the runs collected in <c>[$1, $2)</c> (naive UTC <c>collection_time</c>) whose <c>run_datetime</c> is at or after <c>$3</c>
    /// (the retention edge: older runs would be deleted by the same pass) into the summary. A new (server, hour, category) is inserted;
    /// an existing one is widened, and left alone (no new row version) when the fold changes neither end.
    /// </summary>
    internal const string UpsertSql = @"
INSERT INTO collect.job_history_category_hourly AS t (server_id, run_hour, category_name, first_run, last_run)
SELECT
    jh.server_id,
    date_trunc('hour', jh.run_datetime),
    jh.category_name,
    min(jh.run_datetime),
    max(jh.run_datetime)
FROM collect.job_history AS jh
WHERE jh.collection_time >= $1
AND   jh.collection_time < $2
AND   jh.run_datetime >= $3
AND   jh.category_name IS NOT NULL
AND   jh.category_name <> ''
GROUP BY
    jh.server_id,
    date_trunc('hour', jh.run_datetime),
    jh.category_name
ON CONFLICT (server_id, run_hour, category_name) DO UPDATE
SET
    first_run = LEAST(t.first_run, EXCLUDED.first_run),
    last_run = GREATEST(t.last_run, EXCLUDED.last_run)
WHERE EXCLUDED.first_run < t.first_run
OR    EXCLUDED.last_run > t.last_run;";

    /// <summary>Moves the coverage row: <c>$1</c> covered_from, <c>$2</c> covered_to.</summary>
    internal const string CoverageUpsertSql = @"
INSERT INTO collect.job_history_category_coverage (id, covered_from, covered_to)
VALUES (1, $1, $2)
ON CONFLICT (id) DO UPDATE
SET covered_from = EXCLUDED.covered_from,
    covered_to = EXCLUDED.covered_to;";

    /// <summary>The bounded retention DELETE: run hours before <c>$1</c>, through the <c>run_hour</c> index.</summary>
    internal const string RetentionDeleteSql = @"
DELETE FROM collect.job_history_category_hourly
WHERE run_hour < $1::timestamp;";

    /// <summary>
    /// One pass (see the class remarks): fold, backfill, move the coverage, delete. <paramref name="nowUtc"/> is the service clock in UTC,
    /// the clock <c>collection_time</c> is written in. One transaction, so the coverage row never names rows the table does not hold.
    /// Returns the number of summary rows written.
    /// </summary>
    public static async Task<int> RollUpAsync(NpgsqlConnection connection, DateTime nowUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var currentHour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Unspecified);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        DateTime? coveredFrom;
        DateTime? coveredTo;
        int retentionDays;
        await using (var state = new NpgsqlCommand(StateSql, connection, transaction) { CommandTimeout = PassTimeoutSeconds })
        await using (var reader = await state.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            coveredFrom = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
            coveredTo = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
            retentionDays = reader.GetInt32(2);
        }

        var plan = Plan(coveredFrom, coveredTo, retentionDays, currentHour);
        var written = await UpsertRangeAsync(connection, transaction, plan.ForwardFrom, currentHour, plan.Cutoff, cancellationToken);
        if (plan.BackfillFrom is { } backfillFrom)
        {
            written += await UpsertRangeAsync(connection, transaction, backfillFrom, plan.BackfillTo, plan.Cutoff, cancellationToken);
        }

        await using (var coverage = new NpgsqlCommand(CoverageUpsertSql, connection, transaction) { CommandTimeout = PassTimeoutSeconds })
        {
            coverage.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = plan.CoveredFrom });
            coverage.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = currentHour });
            await coverage.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var retention = new NpgsqlCommand(RetentionDeleteSql, connection, transaction) { CommandTimeout = PassTimeoutSeconds })
        {
            retention.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = plan.Cutoff });
            await retention.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        logger?.LogDebug(
            "job_history_category_hourly pass wrote {Rows} summary rows, coverage now {From:u} to {To:u}", written, plan.CoveredFrom, currentHour);
        return written;
    }

    /// <summary>What one pass folds, as pure arithmetic over the coverage the store holds.</summary>
    /// <param name="ForwardFrom">The start of the forward range <c>[ForwardFrom, currentHour)</c>: the last <see cref="RecomputeHours"/> closed hours, or from there back to the old <c>covered_to</c> when that is older.</param>
    /// <param name="BackfillFrom">The start of the backfill range <c>[BackfillFrom, BackfillTo)</c>, or null when the coverage already reaches the retention edge.</param>
    /// <param name="BackfillTo">The end of the backfill range: where the coverage started before this pass.</param>
    /// <param name="CoveredFrom">The coverage's new start, never before <paramref name="Cutoff"/>.</param>
    /// <param name="Cutoff">The retention edge: runs older than this are neither kept nor folded.</param>
    internal readonly record struct PassPlan(DateTime ForwardFrom, DateTime? BackfillFrom, DateTime BackfillTo, DateTime CoveredFrom, DateTime Cutoff);

    /// <summary>
    /// The pass's ranges. The forward range always ends at <paramref name="currentHour"/> and starts <see cref="RecomputeHours"/> before
    /// the older of the old <c>covered_to</c> and <paramref name="currentHour"/>, so it joins the old coverage with no gap. The backfill
    /// takes at most <see cref="CatchUpDays"/> days before the (new) coverage start, not past the retention edge. The coverage start is
    /// then held to the retention edge: runs before it are deleted by the pass, so the table is not whole before it.
    /// </summary>
    internal static PassPlan Plan(DateTime? coveredFrom, DateTime? coveredTo, int retentionDays, DateTime currentHour)
    {
        var cutoff = currentHour.AddDays(-retentionDays);
        var forwardFrom = (coveredTo is { } to && to < currentHour ? to : currentHour).AddHours(-RecomputeHours);
        var from = coveredFrom is { } f && f < forwardFrom ? f : forwardFrom;

        DateTime? backfillFrom = null;
        var backfillTo = from;
        if (from > cutoff)
        {
            var next = from.AddDays(-CatchUpDays);
            backfillFrom = next > cutoff ? next : cutoff;
            from = backfillFrom.Value;
        }

        return new PassPlan(forwardFrom, backfillFrom, backfillTo, from < cutoff ? cutoff : from, cutoff);
    }

    private static async Task<int> UpsertRangeAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime from, DateTime to, DateTime cutoff, CancellationToken cancellationToken)
    {
        if (from >= to)
        {
            return 0;
        }

        await using var upsert = new NpgsqlCommand(UpsertSql, connection, transaction) { CommandTimeout = PassTimeoutSeconds };
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from });
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = to });
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = cutoff });
        return await upsert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The coverage row as the read takes it: the collection times the summary holds whole.</summary>
    public readonly record struct Coverage(DateTime From, DateTime To);

    /// <summary>The coverage read: one row, no scan. Fails with SQLSTATE 42P01 on a store the V265 rung has not reached.</summary>
    public const string CoverageReadSql = "SELECT covered_from, covered_to FROM collect.job_history_category_coverage WHERE id = 1";

    /// <summary>
    /// Whether the summary answers a category read. It holds the window when its coverage starts at or before the read's
    /// <c>collection_time</c> floor (every run the window keeps was collected at or after that floor: see
    /// <see cref="BuildRolledCategoriesSql"/>), and the window is not backwards (<paramref name="untilUtc"/> before
    /// <paramref name="sinceUtc"/>, which the read's edge rule does not handle: it keeps nothing, and so does the old statement).
    /// </summary>
    public static bool Covers(Coverage coverage, DateTime floorUtc, DateTime sinceUtc, DateTime? untilUtc) =>
        coverage.From <= floorUtc && (untilUtc is not { } until || until >= sinceUtc);

    /// <summary>
    /// The category read over the summary. Parameters, as <see cref="DarlingJobHistoryReader.BuildCategoriesSql"/> numbers them:
    /// <c>$1</c> the window start (naive UTC), <c>$2</c> the <c>collection_time</c> floor of the raw part (the coverage's <c>covered_to</c>,
    /// or the read's own floor when that is later), with <paramref name="bounded"/> <c>$3</c> the window end, and with
    /// <paramref name="forServer"/> the server's id after those.
    ///
    /// <para><b>Two parts, one answer.</b> The summary part is every summary row whose runs reach into the server's local window: the row's
    /// <c>last_run</c> is at or after the window's start and, when bounded, its <c>first_run</c> at or before the window's end, both moved
    /// onto the server's wall clock and widened by an hour exactly as the old statement widens them. The raw part is the old statement
    /// over the rows collected at or after <c>$2</c>, the rows the summary does not hold yet. The two are united and ordered as the old
    /// statement orders its result.</para>
    ///
    /// <para><b>Why the summary part is exact.</b> A row spans less than an hour (<c>last_run - first_run &lt; 1 h</c>), and the bounds are
    /// at least an hour apart (the window is not backwards, and each side is widened by an hour), and <c>first_run</c> and
    /// <c>last_run</c> are runs that exist. So when the two ends straddle a bound, the end on the inside of it is itself a run inside
    /// the window, and when they do not, they say which side every run is on. A row that reaches the window therefore always has a run in
    /// it: no raw check of an edge hour is ever needed.</para>
    ///
    /// <para><b>The old statement's <c>collection_time &gt;= $2</c> is a pruning margin, not a filter.</b> A run is collected after it
    /// starts, and the margin is a day before the window (<see cref="EventWindowFloor"/>), so it never removes a run the
    /// <c>run_datetime</c> bound keeps. The summary holds the runs collected before the floor as well (down to the coverage's start), and
    /// none of them is in the window, so adding them changes nothing. That is the only assumption, and it is the one the old statement
    /// already makes.</para>
    ///
    /// <para>The summary's own scan is bounded by <c>run_hour</c> a day either side of the window, a superset of any server's local
    /// window (UTC offsets are within 14 hours), so the <c>run_hour</c> index serves it.</para>
    /// </summary>
    public static string BuildRolledCategoriesSql(bool bounded, bool forServer)
    {
        var serverParam = bounded ? 4 : 3;
        var rolledServer = forServer ? $"\n    AND   h.server_id = ${serverParam}" : string.Empty;
        var rawServer = forServer ? $"\n    AND   jh.server_id = ${serverParam}" : string.Empty;
        var rolledUpper = bounded
            ? "\n    AND   h.run_hour <= $3 + interval '1 day'"
              + "\n    AND   h.first_run <= $3 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) + interval '1 hour'"
            : string.Empty;
        var rawUpper = bounded
            ? "\n    AND   jh.run_datetime <= $3 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) + interval '1 hour'"
            : string.Empty;

        return $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
rolled AS (
    SELECT h.category_name
    FROM collect.job_history_category_hourly AS h
    LEFT JOIN svr ON svr.server_id = h.server_id
    WHERE h.run_hour >= $1 - interval '1 day'
    AND   h.last_run >= $1 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) - interval '1 hour'{rolledUpper}{rolledServer}
),
raw AS (
    SELECT jh.category_name
    FROM job_history AS jh
    LEFT JOIN svr ON svr.server_id = jh.server_id
    WHERE jh.collection_time >= $2
    AND   jh.run_datetime >= $1 + make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) - interval '1 hour'{rawUpper}{rawServer}
    AND   jh.category_name IS NOT NULL
    AND   jh.category_name <> ''
)
SELECT u.category_name
FROM
(
    SELECT category_name FROM rolled
    UNION
    SELECT category_name FROM raw
) AS u
ORDER BY u.category_name";
    }
}
