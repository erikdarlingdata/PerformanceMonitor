/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The background steps behind the day-partitioned Query Store interval tables (#5571): arm, validate and promote the
/// legacy table, create the day partitions ahead of the clock (draining DEFAULT as each is created), drop the expired
/// ones, and ANALYZE the parent. The migration rungs (V171 wide, V172 latest) only RENAME each table to <c>X_legacy</c>,
/// create the partitioned parent <c>X</c> and attach the legacy table as <c>MINVALUE..MAXVALUE</c>: they copy no data and
/// scan nothing. Everything that needs a heap read or a wait is done here instead, in short transactions under a 5 s
/// <c>lock_timeout</c>, so no step can hold a collector's upsert for longer than that.
///
/// <para><b>Why the legacy table is bounded before it can be dropped.</b> A partition can only be dropped as a whole.
/// The legacy table holds up to 9 (wide) or 15 (latest) days of rows and rows keep landing in it until a day partition
/// takes over. So <see cref="ArmAsync"/> adds <c>CHECK (first_execution_time &lt; S) NOT VALID</c> (a brief lock; every
/// new row is checked from then on, so the table stops growing past S),
/// <see cref="ValidateAsync"/> proves the old rows (a heap read under <c>SHARE UPDATE EXCLUSIVE</c>, which blocks no
/// upsert), and <see cref="PromoteAsync"/> re-attaches legacy as <c>MINVALUE..S</c> in one millisecond-long
/// transaction. The valid CHECK plus the column's NOT NULL imply the new partition constraint, so the re-attach scans
/// nothing. A row at or after S that arrives before promotion is refused (23514); the collector's pending path holds it
/// and replays it after promotion (the pending horizon is the table's horizon, so nothing is lost).</para>
///
/// <para><b>Locks.</b> Arm: ACCESS EXCLUSIVE on legacy for the ADD CONSTRAINT, 5 s lock_timeout, 55P03 retried three
/// times 30 s apart. Validate: SHARE UPDATE EXCLUSIVE on legacy (upserts and reads continue), lock_timeout 5 s, a
/// 7200 s command deadline. Promote: ACCESS EXCLUSIVE on the parent for the DETACH, ATTACH and the CREATEs, held for
/// milliseconds plus a lock wait of 5 s at most. Create-ahead: SHARE UPDATE EXCLUSIVE on the parent and ACCESS
/// EXCLUSIVE on DEFAULT and on the new table, for the drain and the attach. Drop: ACCESS EXCLUSIVE on the parent for
/// the DROP. A lock timeout is never an error: the step returns <see cref="StepOutcome.RetryLater"/>.</para>
///
/// <para><b>Whole-table identity.</b> The upsert's identity includes <c>first_execution_time</c>, and no ON CONFLICT
/// update assigns it, so a row never changes partition. A day's rows are in exactly one partition, so the parent's
/// unique index is a real uniqueness proof. A <c>ctid</c> repeats across partitions, so any
/// <c>DELETE ... WHERE ctid IN (SELECT ctid ...)</c> must name <c>X_legacy</c>, never the parent.</para>
///
/// <para><b>Precision.</b> The key is a naive-UTC <c>timestamp</c>. FROM is inclusive and TO exclusive, so
/// 23:59:59.999999 is in day d and 00:00:00 is in day d+1, with no tie. Every bound written here is a whole day, so
/// no microsecond is ever lost to a literal.</para>
/// </summary>
public static class QueryStoreIntervalPartitions
{
    /// <summary>How far ahead of the clock the day partitions are created (today plus this many days).</summary>
    public const int DaysAhead = 3;

    /// <summary>The Phase A arm, S = this many whole days after today's UTC midnight.</summary>
    public const int ArmDays = 2;

    /// <summary>Re-arm a CHECK that is still not valid once S is closer than this.</summary>
    public static readonly TimeSpan ReArmWithin = TimeSpan.FromHours(12);

    /// <summary>The lock wait of every DDL step, in seconds.</summary>
    public const int LockTimeoutSeconds = 5;

    /// <summary>How many times an arm that hit a lock timeout is retried, and how far apart.</summary>
    public const int ArmRetries = 3;

    /// <summary>The delay between those retries.</summary>
    public static readonly TimeSpan ArmRetryDelay = TimeSpan.FromSeconds(30);

    /// <summary>How many times <see cref="RunPromotionAsync"/> retries a promotion that hit a lock timeout, and how far apart.</summary>
    public const int PromoteRetries = 5;

    /// <summary>The delay between those retries.</summary>
    public static readonly TimeSpan PromoteRetryDelay = TimeSpan.FromSeconds(60);

    /// <summary>The validate's command deadline (a heap read of the legacy table), in seconds.</summary>
    public const int ValidateTimeoutSeconds = 7200;

    /// <summary>Deadline for the catalog reads and the short DDL, in seconds.</summary>
    public const int ShortTimeoutSeconds = 60;

    /// <summary>Deadline for moving one day's rows out of DEFAULT, in seconds (normally no rows).</summary>
    public const int DrainTimeoutSeconds = 600;

    /// <summary>Deadline for an ANALYZE of the parent, in seconds.</summary>
    public const int AnalyzeTimeoutSeconds = 1800;

    /// <summary>How long a parent's statistics are kept before <see cref="AnalyzeIfDueAsync"/> refreshes them.</summary>
    public static readonly TimeSpan AnalyzeEvery = TimeSpan.FromHours(24);

    /// <summary>What a step did.</summary>
    public enum StepOutcome
    {
        /// <summary>The step ran and changed (or proved) something.</summary>
        Done,

        /// <summary>There was nothing to do: the step's goal already held.</summary>
        NothingToDo,

        /// <summary>A lock wait timed out (55P03); not an error, run the step again later.</summary>
        RetryLater,

        /// <summary>A precondition failed and the step rolled back and logged (a parent index without a legacy child).</summary>
        Refused,

        /// <summary>The store is not in a state the step applies to (no partitioned parent yet, or not promoted yet).</summary>
        NotReady,
    }

    /// <summary>A step's outcome, a line for the log or a test, and a count (days created, partitions dropped, rows drained).</summary>
    public readonly record struct StepResult(StepOutcome Outcome, string Detail, int Count = 0);

    /// <summary>One partitioned interval table.</summary>
    /// <param name="Name">The table's name inside <c>collect</c>.</param>
    /// <param name="HorizonDays">How many days of <c>first_execution_time</c> the table keeps (the retention horizon).</param>
    public sealed record IntervalTable(string Name, int HorizonDays)
    {
        /// <summary>The schema every interval table is in.</summary>
        public const string Schema = "collect";

        /// <summary>The parent, schema-qualified.</summary>
        public string Parent => $"{Schema}.{Name}";

        /// <summary>The renamed pre-partitioning table.</summary>
        public string Legacy => $"{Schema}.{Name}_legacy";

        /// <summary>The DEFAULT partition.</summary>
        public string Default => $"{Schema}.{Name}_default";

        /// <summary>The NOT VALID CHECK that bounds the legacy table.</summary>
        public string CheckName => $"ck_{Name}_legacy_before";

        /// <summary>The day partition's schema-qualified name for the day starting at <paramref name="day"/>.</summary>
        public string DayPartition(DateTime day) => $"{Schema}.{Name}_p{day:yyyyMMdd}";
    }

    /// <summary>The wide table (9 days).</summary>
    public static readonly IntervalTable Wide = new("query_store_interval_wide", 9);

    /// <summary>The latest-snapshot table (15 days).</summary>
    public static readonly IntervalTable Latest = new("query_store_interval_latest", 15);

    /// <summary>Both tables, in the order the background steps run them.</summary>
    public static readonly IReadOnlyList<IntervalTable> All = new[] { Wide, Latest };

    /// <summary>
    /// What the catalogs say about one table. <see cref="Promoted"/> means the parent is partitioned and the legacy table,
    /// if it still exists, is bounded.
    /// </summary>
    public sealed record PartitionState(
        char? ParentKind,
        bool LegacyExists,
        bool CheckPresent,
        bool CheckValid,
        DateTime? CheckBound,
        bool LegacyUnbounded,
        DateTime? LegacyUpper,
        bool DefaultExists)
    {
        /// <summary>True when the parent is a partitioned table (relkind <c>p</c>).</summary>
        public bool ParentIsPartitioned => ParentKind == 'p';

        /// <summary>True when the parent is partitioned and no unbounded legacy partition is left under it.</summary>
        public bool Promoted => ParentIsPartitioned && !LegacyUnbounded;
    }

    /// <summary>One partition read from the catalog.</summary>
    /// <param name="Name">The schema-qualified name.</param>
    /// <param name="IsDefault">True for the DEFAULT partition.</param>
    /// <param name="Lower">The inclusive lower bound; null for MINVALUE and for DEFAULT.</param>
    /// <param name="Upper">The exclusive upper bound; null for MAXVALUE and for DEFAULT.</param>
    /// <param name="UpperUnbounded">True when the upper bound is MAXVALUE.</param>
    public sealed record PartitionInfo(string Name, bool IsDefault, DateTime? Lower, DateTime? Upper, bool UpperUnbounded);

    // ---------------------------------------------------------------------------------------------------------------
    // Pure helpers (no connection): the arm bound, the bound parsers, the days to create, the partitions to drop.
    // ---------------------------------------------------------------------------------------------------------------

    private static readonly string[] TimestampFormats =
    {
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.f",
        "yyyy-MM-dd HH:mm:ss.ff",
        "yyyy-MM-dd HH:mm:ss.fff",
        "yyyy-MM-dd HH:mm:ss.ffff",
        "yyyy-MM-dd HH:mm:ss.fffff",
        "yyyy-MM-dd HH:mm:ss.ffffff",
    };

    private static readonly Regex CheckBoundPattern = new(
        @"first_execution_time\s*<\s*'(?<ts>[^']+)'",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex RangeBoundPattern = new(
        @"^FOR VALUES FROM \((?:MINVALUE|'(?<lo>[^']+)')\) TO \((?:MAXVALUE|'(?<hi>[^']+)')\)$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>A date at midnight with no kind, the form a naive-UTC <c>timestamp</c> has when Npgsql reads it.</summary>
    public static DateTime DayStart(DateTime value) => DateTime.SpecifyKind(value.Date, DateTimeKind.Unspecified);

    /// <summary>S: today's UTC midnight plus <see cref="ArmDays"/> days.</summary>
    public static DateTime ArmBound(DateTime utcNow) => DayStart(utcNow).AddDays(ArmDays);

    /// <summary>The expiry cutoff: a partition whose upper bound is at or before this holds only expired rows.</summary>
    public static DateTime Cutoff(DateTime utcNow, int horizonDays) =>
        DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified).AddDays(-horizonDays);

    /// <summary>The CHECK's S read back from <c>pg_get_constraintdef</c> text, or false when it has none.</summary>
    public static bool TryParseCheckBound(string? constraintDefinition, out DateTime bound)
    {
        bound = default;
        if (string.IsNullOrEmpty(constraintDefinition))
        {
            return false;
        }

        var match = CheckBoundPattern.Match(constraintDefinition);
        return match.Success && TryParseTimestamp(match.Groups["ts"].Value, out bound);
    }

    /// <summary>
    /// A partition bound read from <c>pg_get_expr(relpartbound, oid)</c>: <c>DEFAULT</c>, or
    /// <c>FOR VALUES FROM (MINVALUE|'ts') TO (MAXVALUE|'ts')</c>. False for text this code did not write.
    /// </summary>
    public static bool TryParsePartitionBound(
        string? boundExpression, out bool isDefault, out DateTime? lower, out DateTime? upper, out bool upperUnbounded)
    {
        isDefault = false;
        lower = null;
        upper = null;
        upperUnbounded = false;
        if (string.IsNullOrEmpty(boundExpression))
        {
            return false;
        }

        if (string.Equals(boundExpression, "DEFAULT", StringComparison.Ordinal))
        {
            isDefault = true;
            return true;
        }

        var match = RangeBoundPattern.Match(boundExpression);
        if (!match.Success)
        {
            return false;
        }

        if (match.Groups["lo"].Success)
        {
            if (!TryParseTimestamp(match.Groups["lo"].Value, out var parsedLower))
            {
                return false;
            }

            lower = parsedLower;
        }

        if (match.Groups["hi"].Success)
        {
            if (!TryParseTimestamp(match.Groups["hi"].Value, out var parsedUpper))
            {
                return false;
            }

            upper = parsedUpper;
        }
        else
        {
            upperUnbounded = true;
        }

        return true;
    }

    private static bool TryParseTimestamp(string text, out DateTime value) =>
        DateTime.TryParseExact(text, TimestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out value);

    /// <summary>A timestamp literal. Every bound written by this class is a whole day, so seconds are enough.</summary>
    internal static string Literal(DateTime day) => day.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>
    /// Whether an armed, still-unvalidated CHECK should be dropped and added again with a new S: its S is closer than
    /// <see cref="ReArmWithin"/>, so a validate that has not finished would be racing the bound.
    /// </summary>
    public static bool ShouldReArm(bool checkValid, DateTime checkBound, DateTime utcNow) =>
        !checkValid && checkBound - DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified) < ReArmWithin;

    /// <summary>
    /// The day partitions the promotion creates: every day from S through today plus <see cref="DaysAhead"/>, with no
    /// holes. Empty when S is already past that.
    /// </summary>
    public static IReadOnlyList<DateTime> PromotionDays(DateTime s, DateTime utcNow) => DaysBetween(s, DayStart(utcNow).AddDays(DaysAhead));

    /// <summary>
    /// The days the create-ahead step adds: from the newest partition's upper bound through today plus
    /// <see cref="DaysAhead"/>, with no holes, so a catch-up row after an outage never falls into DEFAULT. Days older than
    /// the horizon are not created (the next drop would remove them at once).
    /// </summary>
    public static IReadOnlyList<DateTime> CreateAheadDays(DateTime? newestUpper, DateTime utcNow, int horizonDays)
    {
        var today = DayStart(utcNow);
        var floor = DayStart(Cutoff(utcNow, horizonDays));
        var start = newestUpper.HasValue ? DayStart(newestUpper.Value) : today;
        if (start < floor)
        {
            start = floor;
        }

        return DaysBetween(start, today.AddDays(DaysAhead));
    }

    private static List<DateTime> DaysBetween(DateTime first, DateTime lastInclusive)
    {
        var days = new List<DateTime>();
        for (var d = first; d <= lastInclusive; d = d.AddDays(1))
        {
            days.Add(d);
        }

        return days;
    }

    /// <summary>
    /// The partitions to drop, oldest first: a bounded partition whose upper bound is at or before <paramref name="cutoff"/>.
    /// That is a whole expired day, and the legacy table once S is at or before the cutoff. DEFAULT and an unbounded
    /// legacy table are never returned, and a day that straddles the cutoff is kept.
    /// </summary>
    public static IReadOnlyList<PartitionInfo> ExpiredPartitions(IEnumerable<PartitionInfo> partitions, DateTime cutoff) =>
        partitions
            .Where(p => !p.IsDefault && !p.UpperUnbounded && p.Upper.HasValue && p.Upper.Value <= cutoff)
            .OrderBy(p => p.Upper)
            .ToList();

    /// <summary>The newest finite upper bound among the partitions, or null when there is none.</summary>
    public static DateTime? NewestUpper(IEnumerable<PartitionInfo> partitions)
    {
        DateTime? newest = null;
        foreach (var partition in partitions)
        {
            if (!partition.IsDefault && !partition.UpperUnbounded && partition.Upper.HasValue
                && (newest is null || partition.Upper.Value > newest.Value))
            {
                newest = partition.Upper.Value;
            }
        }

        return newest;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // SQL
    // ---------------------------------------------------------------------------------------------------------------

    /* One read of the whole decision: $1 parent, $2 legacy, $3 the CHECK's name, $4 DEFAULT. Every name is bound. */
    internal const string StateSql = @"
SELECT
    (SELECT c.relkind::text FROM pg_class AS c WHERE c.oid = to_regclass($1)) AS parent_kind,
    to_regclass($2) IS NOT NULL AS legacy_exists,
    (SELECT pg_get_constraintdef(k.oid) FROM pg_constraint AS k WHERE k.conrelid = to_regclass($2) AND k.conname = $3) AS check_def,
    (SELECT k.convalidated FROM pg_constraint AS k WHERE k.conrelid = to_regclass($2) AND k.conname = $3) AS check_valid,
    (SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class AS c WHERE c.oid = to_regclass($2)) AS legacy_bound,
    to_regclass($4) IS NOT NULL AS default_exists;";

    /* The partitions directly under $1, with their bound text. */
    internal const string PartitionsSql = @"
SELECT format('%I.%I', n.nspname, c.relname), pg_get_expr(c.relpartbound, c.oid)
FROM pg_inherits AS i
JOIN pg_class AS c ON c.oid = i.inhrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE i.inhparent = to_regclass($1)
ORDER BY c.relname;";

    /* Every index on the parent ($1) that has no VALID child on the legacy table ($2): the re-attach would build each of
       these on the whole legacy heap. */
    internal const string MissingLegacyChildSql = @"
SELECT pi.indexrelid::regclass::text
FROM pg_index AS pi
WHERE pi.indrelid = to_regclass($1)
AND   NOT EXISTS
(
    SELECT 1
    FROM pg_inherits AS ih
    JOIN pg_index AS ci ON ci.indexrelid = ih.inhrelid
    WHERE ih.inhparent = pi.indexrelid
    AND   ci.indrelid = to_regclass($2)
    AND   ci.indisvalid
)
ORDER BY 1;";

    private static string LockTimeoutSql => $"SET LOCAL lock_timeout = '{LockTimeoutSeconds}s';";

    internal static string AddCheckSql(IntervalTable table, DateTime s) =>
        $"ALTER TABLE {table.Legacy} ADD CONSTRAINT {table.CheckName} CHECK (first_execution_time < '{Literal(s)}'::timestamp) NOT VALID;";

    internal static string DropCheckSql(IntervalTable table) =>
        $"ALTER TABLE {table.Legacy} DROP CONSTRAINT IF EXISTS {table.CheckName};";

    internal static string ValidateSql(IntervalTable table) =>
        $"ALTER TABLE {table.Legacy} VALIDATE CONSTRAINT {table.CheckName};";

    internal static string CreateDaySql(IntervalTable table, DateTime day) =>
        $"CREATE TABLE {table.DayPartition(day)} PARTITION OF {table.Parent} "
        + $"FOR VALUES FROM ('{Literal(day)}') TO ('{Literal(day.AddDays(1))}') WITH (fillfactor = 50);";

    internal static string CreateDefaultSql(IntervalTable table) =>
        $"CREATE TABLE {table.Default} PARTITION OF {table.Parent} DEFAULT WITH (fillfactor = 50);";

    // ---------------------------------------------------------------------------------------------------------------
    // Connection steps
    // ---------------------------------------------------------------------------------------------------------------

    private static bool IsLockTimeout(Exception ex) => ex is PostgresException pg && pg.SqlState == PostgresErrorCodes.LockNotAvailable;

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, int timeoutSeconds, NpgsqlTransaction? transaction = null) =>
        new(sql, connection, transaction) { CommandTimeout = timeoutSeconds };

    private static void AddText(NpgsqlCommand command, string value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = value });

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, string sql, int timeoutSeconds, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, sql, timeoutSeconds, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the catalogs for one table. <paramref name="connection"/> must be open.</summary>
    public static async Task<PartitionState> ReadStateAsync(
        NpgsqlConnection connection, IntervalTable table, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, StateSql, ShortTimeoutSeconds);
        AddText(command, table.Parent);
        AddText(command, table.Legacy);
        AddText(command, table.CheckName);
        AddText(command, table.Default);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

        char? kind = reader.IsDBNull(0) ? null : reader.GetString(0)[0];
        var legacyExists = reader.GetBoolean(1);
        var checkPresent = !reader.IsDBNull(2);
        var checkValid = !reader.IsDBNull(3) && reader.GetBoolean(3);
        DateTime? checkBound = checkPresent && TryParseCheckBound(reader.GetString(2), out var parsedBound) ? parsedBound : null;

        var legacyUnbounded = false;
        DateTime? legacyUpper = null;
        if (!reader.IsDBNull(4)
            && TryParsePartitionBound(reader.GetString(4), out _, out _, out var upper, out var upperUnbounded))
        {
            legacyUnbounded = upperUnbounded;
            legacyUpper = upper;
        }

        return new PartitionState(kind, legacyExists, checkPresent, checkValid, checkBound, legacyUnbounded, legacyUpper, reader.GetBoolean(5));
    }

    /// <summary>Every partition directly under the parent.</summary>
    public static async Task<IReadOnlyList<PartitionInfo>> ReadPartitionsAsync(
        NpgsqlConnection connection, IntervalTable table, CancellationToken cancellationToken)
    {
        var partitions = new List<PartitionInfo>();
        await using var command = Command(connection, PartitionsSql, ShortTimeoutSeconds);
        AddText(command, table.Parent);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!reader.IsDBNull(1)
                && TryParsePartitionBound(reader.GetString(1), out var isDefault, out var lower, out var upper, out var upperUnbounded))
            {
                partitions.Add(new PartitionInfo(reader.GetString(0), isDefault, lower, upper, upperUnbounded));
            }
        }

        return partitions;
    }

    /// <summary>
    /// Phase A step 1. Adds <c>CHECK (first_execution_time &lt; S) NOT VALID</c> to the legacy table, or re-arms a CHECK
    /// that is still not valid with S closer than <see cref="ReArmWithin"/>. A lock timeout is retried
    /// <see cref="ArmRetries"/> times, <see cref="ArmRetryDelay"/> apart, then returns
    /// <see cref="StepOutcome.RetryLater"/>.
    /// </summary>
    public static Task<StepResult> ArmAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken) =>
        ArmAsync(connection, table, utcNow, logger, ArmRetryDelay, cancellationToken);

    internal static async Task<StepResult> ArmAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, TimeSpan retryDelay, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
            if (!state.ParentIsPartitioned || !state.LegacyExists)
            {
                return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table with a legacy table yet");
            }

            if (state.Promoted)
            {
                return new StepResult(StepOutcome.NothingToDo, "already promoted");
            }

            var reArm = state.CheckPresent && state.CheckBound.HasValue
                && ShouldReArm(state.CheckValid, state.CheckBound.Value, utcNow);
            if (state.CheckPresent && !reArm)
            {
                return new StepResult(StepOutcome.NothingToDo, state.CheckValid ? "already armed and valid" : "already armed");
            }

            var s = ArmBound(utcNow);
            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                if (reArm)
                {
                    await ExecuteAsync(connection, DropCheckSql(table), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                }

                await ExecuteAsync(connection, AddCheckSql(table, s), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                logger.LogInformation(
                    "Query Store interval table {Table}: armed the legacy bound S = {Bound:O} ({Action}).",
                    table.Parent, s, reArm ? "re-armed" : "armed");
                return new StepResult(StepOutcome.Done, $"armed at {Literal(s)}");
            }
            catch (PostgresException ex) when (IsLockTimeout(ex))
            {
                if (attempt >= ArmRetries)
                {
                    logger.LogWarning(
                        "Query Store interval table {Table}: arming the legacy bound hit a lock timeout {Attempts} times; the next start retries.",
                        table.Parent, attempt + 1);
                    return new StepResult(StepOutcome.RetryLater, "lock timeout");
                }

                logger.LogInformation(
                    "Query Store interval table {Table}: arming the legacy bound hit a lock timeout; retrying in {Seconds:F0}s.",
                    table.Parent, retryDelay.TotalSeconds);
                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Phase A step 2. Validates the legacy CHECK: a heap read of the legacy table under SHARE UPDATE EXCLUSIVE, so
    /// upserts and reads continue. A lock timeout (autovacuum, a concurrent index build) returns
    /// <see cref="StepOutcome.RetryLater"/>.
    /// </summary>
    public static async Task<StepResult> ValidateAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.ParentIsPartitioned || !state.LegacyExists || !state.CheckPresent)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Legacy} has no armed bound yet");
        }

        if (state.Promoted || state.CheckValid)
        {
            return new StepResult(StepOutcome.NothingToDo, "already validated");
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, ValidateSql(table), ValidateTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (IsLockTimeout(ex))
        {
            logger.LogWarning(
                "Query Store interval table {Table}: validating the legacy bound hit a lock timeout; the next start retries.", table.Parent);
            return new StepResult(StepOutcome.RetryLater, "lock timeout");
        }

        logger.LogInformation(
            "Query Store interval table {Table}: validated the legacy bound in {Seconds:F1}s.", table.Parent, started.Elapsed.TotalSeconds);
        return new StepResult(StepOutcome.Done, "validated");
    }

    /// <summary>
    /// Phase A step 3. In one transaction under a 5 s lock timeout: LOCK the parent, refuse (ROLLBACK, log) when a parent
    /// index has no valid child on the legacy table (the re-attach would build it on the whole legacy heap), DETACH
    /// legacy, ATTACH it <c>MINVALUE..S</c>, create the day partitions S through today plus
    /// <see cref="DaysAhead"/>, and create DEFAULT last. A lock timeout returns <see cref="StepOutcome.RetryLater"/>.
    /// Needs a valid CHECK; otherwise <see cref="StepOutcome.NotReady"/>.
    /// </summary>
    public static async Task<StepResult> PromoteAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.ParentIsPartitioned || !state.LegacyExists)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table with a legacy table yet");
        }

        if (state.Promoted)
        {
            return new StepResult(StepOutcome.NothingToDo, "already promoted");
        }

        if (!state.CheckPresent || !state.CheckValid || !state.CheckBound.HasValue)
        {
            return new StepResult(StepOutcome.NotReady, "the legacy bound is not armed and valid yet");
        }

        var s = state.CheckBound.Value;
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(
                connection, $"LOCK TABLE {table.Parent} IN ACCESS EXCLUSIVE MODE;", ShortTimeoutSeconds, transaction, cancellationToken)
                .ConfigureAwait(false);

            var missing = new List<string>();
            await using (var check = Command(connection, MissingLegacyChildSql, ShortTimeoutSeconds, transaction))
            {
                AddText(check, table.Parent);
                AddText(check, table.Legacy);
                await using var reader = await check.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    missing.Add(reader.GetString(0));
                }
            }

            if (missing.Count > 0)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                logger.LogWarning(
                    "Query Store interval table {Table}: promotion refused and rolled back, because the parent index(es) {Indexes} have no valid "
                    + "child on {Legacy}, and the re-attach would build them on its whole heap. The background index step attaches them; "
                    + "promotion is tried again.",
                    table.Parent, string.Join(", ", missing), table.Legacy);
                return new StepResult(StepOutcome.Refused, "parent index without a legacy child: " + string.Join(", ", missing));
            }

            await ExecuteAsync(
                connection, $"ALTER TABLE {table.Parent} DETACH PARTITION {table.Legacy};", ShortTimeoutSeconds, transaction, cancellationToken)
                .ConfigureAwait(false);
            await ExecuteAsync(
                connection,
                $"ALTER TABLE {table.Parent} ATTACH PARTITION {table.Legacy} FOR VALUES FROM (MINVALUE) TO ('{Literal(s)}');",
                ShortTimeoutSeconds,
                transaction,
                cancellationToken).ConfigureAwait(false);

            var days = PromotionDays(s, utcNow);
            foreach (var day in days)
            {
                await ExecuteAsync(connection, CreateDaySql(table, day), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            }

            await ExecuteAsync(connection, CreateDefaultSql(table), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation(
                "Query Store interval table {Table}: promoted. {Legacy} is bounded below {Bound:O}; {Days} day partition(s) and DEFAULT created.",
                table.Parent, table.Legacy, s, days.Count);
            return new StepResult(StepOutcome.Done, $"promoted at {Literal(s)}", days.Count);
        }
        catch (PostgresException ex) when (IsLockTimeout(ex))
        {
            logger.LogWarning(
                "Query Store interval table {Table}: promotion hit a lock timeout and rolled back; it is tried again.", table.Parent);
            return new StepResult(StepOutcome.RetryLater, "lock timeout");
        }
    }

    /// <summary>ANALYZE of the parent. Autovacuum never analyzes a partitioned table, so the parent's statistics only exist when this runs.</summary>
    public static async Task<StepResult> AnalyzeAsync(
        NpgsqlConnection connection, IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.ParentIsPartitioned)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table");
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        await ExecuteAsync(connection, $"ANALYZE {table.Parent};", AnalyzeTimeoutSeconds, null, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Query Store interval table {Table}: analyzed the parent in {Seconds:F1}s.", table.Parent, started.Elapsed.TotalSeconds);
        return new StepResult(StepOutcome.Done, "analyzed");
    }

    /// <summary>The once-a-day ANALYZE: runs <see cref="AnalyzeAsync"/> when the parent was last analyzed more than <see cref="AnalyzeEvery"/> ago, or never.</summary>
    public static async Task<StepResult> AnalyzeIfDueAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        bool due;
        await using (var command = Command(
            connection,
            "SELECT COALESCE((SELECT s.last_analyze AT TIME ZONE 'UTC' FROM pg_stat_user_tables AS s WHERE s.relid = to_regclass($1)) < $2, true);",
            ShortTimeoutSeconds))
        {
            AddText(command, table.Parent);
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp,
                Value = DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified) - AnalyzeEvery,
            });
            due = (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        return due
            ? await AnalyzeAsync(connection, table, logger, cancellationToken).ConfigureAwait(false)
            : new StepResult(StepOutcome.NothingToDo, "analyzed within the last day");
    }

    /// <summary>
    /// Phase A as one call, for the once-per-start task: arm, validate, promote (a lock timeout is retried
    /// <see cref="PromoteRetries"/> times, <see cref="PromoteRetryDelay"/> apart), then ANALYZE the parent. Stops at the
    /// first step that is not done and returns it; the next start continues from the catalogs' state.
    /// </summary>
    public static Task<StepResult> RunPromotionAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken) =>
        RunPromotionAsync(connection, table, utcNow, logger, ArmRetryDelay, PromoteRetryDelay, cancellationToken);

    internal static async Task<StepResult> RunPromotionAsync(
        NpgsqlConnection connection,
        IntervalTable table,
        DateTime utcNow,
        ILogger logger,
        TimeSpan armRetryDelay,
        TimeSpan promoteRetryDelay,
        CancellationToken cancellationToken)
    {
        var arm = await ArmAsync(connection, table, utcNow, logger, armRetryDelay, cancellationToken).ConfigureAwait(false);
        if (arm.Outcome is StepOutcome.RetryLater or StepOutcome.NotReady)
        {
            return arm;
        }

        var validate = await ValidateAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
        if (validate.Outcome is StepOutcome.RetryLater or StepOutcome.NotReady)
        {
            return validate;
        }

        StepResult promote = default;
        for (var attempt = 0; attempt <= PromoteRetries; attempt++)
        {
            promote = await PromoteAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
            if (promote.Outcome != StepOutcome.RetryLater)
            {
                break;
            }

            if (attempt < PromoteRetries)
            {
                await Task.Delay(promoteRetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }

        if (promote.Outcome is not (StepOutcome.Done or StepOutcome.NothingToDo))
        {
            return promote;
        }

        await AnalyzeAsync(connection, table, logger, cancellationToken).ConfigureAwait(false);
        return promote;
    }

    /// <summary>
    /// Phase B, create-ahead. Creates every missing day from the newest partition's upper bound through today plus
    /// <see cref="DaysAhead"/>. Each day is one transaction under a 5 s lock timeout: <c>CREATE TABLE ... (LIKE parent)</c>
    /// with fillfactor 50, LOCK DEFAULT, move the day's rows out of DEFAULT into the new table (so the ATTACH's DEFAULT
    /// scan finds none and the attach cannot fail), then ATTACH. Any DEFAULT row is logged at Warning. Only after
    /// promotion; before it the result is <see cref="StepOutcome.NotReady"/>. Count is the days created.
    /// </summary>
    public static async Task<StepResult> CreateAheadAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.Promoted)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not promoted yet");
        }

        var partitions = await ReadPartitionsAsync(connection, table, cancellationToken).ConfigureAwait(false);
        var existing = new HashSet<string>(partitions.Select(p => p.Name), StringComparer.Ordinal);
        var created = 0;
        var drainedTotal = 0;
        foreach (var day in CreateAheadDays(NewestUpper(partitions), utcNow, table.HorizonDays))
        {
            var name = table.DayPartition(day);
            if (existing.Contains(name))
            {
                continue;
            }

            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(
                    connection,
                    $"CREATE TABLE {name} (LIKE {table.Parent} INCLUDING DEFAULTS) WITH (fillfactor = 50);",
                    ShortTimeoutSeconds,
                    transaction,
                    cancellationToken).ConfigureAwait(false);

                var drained = 0;
                if (state.DefaultExists)
                {
                    await ExecuteAsync(
                        connection, $"LOCK TABLE {table.Default} IN ACCESS EXCLUSIVE MODE;", ShortTimeoutSeconds, transaction, cancellationToken)
                        .ConfigureAwait(false);
                    await using var drain = Command(
                        connection,
                        $"WITH moved AS (DELETE FROM {table.Default} WHERE first_execution_time >= '{Literal(day)}'::timestamp "
                        + $"AND first_execution_time < '{Literal(day.AddDays(1))}'::timestamp RETURNING *) "
                        + $"INSERT INTO {name} SELECT * FROM moved;",
                        DrainTimeoutSeconds,
                        transaction);
                    drained = await drain.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await ExecuteAsync(
                    connection,
                    $"ALTER TABLE {table.Parent} ATTACH PARTITION {name} FOR VALUES FROM ('{Literal(day)}') TO ('{Literal(day.AddDays(1))}');",
                    ShortTimeoutSeconds,
                    transaction,
                    cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                created++;
                drainedTotal += drained;
                if (drained > 0)
                {
                    logger.LogWarning(
                        "Query Store interval table {Table}: moved {Rows} row(s) for {Day:yyyy-MM-dd} out of DEFAULT into {Partition}. "
                        + "Rows only reach DEFAULT when the clock is more than {DaysAhead} days ahead of the store or this step stalled.",
                        table.Parent, drained, day, name, DaysAhead);
                }
            }
            catch (PostgresException ex) when (IsLockTimeout(ex))
            {
                logger.LogWarning(
                    "Query Store interval table {Table}: creating {Partition} hit a lock timeout and rolled back; the next pass retries.",
                    table.Parent, name);
                return new StepResult(StepOutcome.RetryLater, "lock timeout", created);
            }
        }

        if (state.DefaultExists)
        {
            await ReportDefaultRowsAsync(connection, table, logger, cancellationToken).ConfigureAwait(false);
        }

        return created > 0
            ? new StepResult(StepOutcome.Done, $"created {created} day partition(s), drained {drainedTotal} row(s)", created)
            : new StepResult(StepOutcome.NothingToDo, "the days ahead already exist", 0);
    }

    private static async Task ReportDefaultRowsAsync(
        NpgsqlConnection connection, IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        await using var command = Command(
            connection, $"SELECT count(*) FROM (SELECT 1 FROM {table.Default} LIMIT 1000) AS d;", ShortTimeoutSeconds);
        var rows = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (rows > 0)
        {
            logger.LogWarning(
                "Query Store interval table {Table}: DEFAULT holds {Rows}{More} row(s). They are outside the day partitions "
                + "(a monitored clock far from the store's, or a stalled maintenance step); retention deletes them once they expire.",
                table.Parent, rows, rows >= 1000 ? "+" : string.Empty);
        }
    }

    /// <summary>
    /// Retention for the partitions: drops every bounded partition whose upper bound is at or before
    /// <c>utcNow - HorizonDays</c> (a whole expired day, and the legacy table once S is at or before the cutoff), oldest
    /// first, each in its own transaction under a 5 s lock timeout. A day that straddles the cutoff is kept until the whole
    /// day has expired. A lock timeout stops the pass with <see cref="StepOutcome.RetryLater"/> (not an error); the rest
    /// waits for the next pass. Only after promotion. Count is the partitions dropped.
    /// </summary>
    public static async Task<StepResult> DropExpiredAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.Promoted)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not promoted yet");
        }

        var partitions = await ReadPartitionsAsync(connection, table, cancellationToken).ConfigureAwait(false);
        var dropped = 0;
        foreach (var partition in ExpiredPartitions(partitions, Cutoff(utcNow, table.HorizonDays)))
        {
            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(connection, $"DROP TABLE {partition.Name};", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                dropped++;
                logger.LogInformation(
                    "Query Store interval table {Table}: dropped expired partition {Partition} (upper bound {Upper:O}).",
                    table.Parent, partition.Name, partition.Upper);
            }
            catch (PostgresException ex) when (IsLockTimeout(ex))
            {
                logger.LogInformation(
                    "Query Store interval table {Table}: dropping {Partition} hit a lock timeout; the next pass retries.",
                    table.Parent, partition.Name);
                return new StepResult(StepOutcome.RetryLater, "lock timeout", dropped);
            }
        }

        return dropped > 0
            ? new StepResult(StepOutcome.Done, $"dropped {dropped} partition(s)", dropped)
            : new StepResult(StepOutcome.NothingToDo, "no partition has fully expired", 0);
    }

    /// <summary>
    /// Phase B as one call, for the start step and the hourly pass: create-ahead, then drop-expired. Returns the create-ahead
    /// result unless it was not done, then the drop's.
    /// </summary>
    public static async Task<StepResult> RunMaintenanceAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var ahead = await CreateAheadAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
        if (ahead.Outcome == StepOutcome.NotReady)
        {
            return ahead;
        }

        var drop = await DropExpiredAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);

        /* A pass that created days AND dropped days reports both counts, so the hourly "changed" tally is not short by
           the drops (#5571). */
        return ahead.Outcome == StepOutcome.Done ? ahead with { Count = ahead.Count + drop.Count } : drop;
    }

    /// <summary>What one <see cref="RunMaintenancePassAsync"/> did: partitions created or dropped, and tables whose step failed.</summary>
    public readonly record struct PassResult(int Changed, int Failed);

    /// <summary>
    /// The hourly step (#5571): for each table, in order, <see cref="RunMaintenanceAsync"/> (create ahead with the DEFAULT
    /// drain, then drop expired days and the legacy table once it is bounded below the cutoff), then
    /// <see cref="AnalyzeIfDueAsync"/>. It does nothing for a table that is not promoted yet, so it is safe before and
    /// during the Phase A task. One table's failure is logged and counted and never stops the other table; a lock
    /// timeout (55P03) is <see cref="StepOutcome.RetryLater"/>, already logged once by the step, and is not a failure.
    /// If a failure closed the connection it is reopened for the next table. Shutdown (cancellation) propagates.
    /// </summary>
    public static Task<PassResult> RunMaintenancePassAsync(
        NpgsqlConnection connection, DateTime utcNow, ILogger logger, CancellationToken cancellationToken) =>
        RunMaintenancePassAsync(connection, All, utcNow, logger, cancellationToken);

    internal static async Task<PassResult> RunMaintenancePassAsync(
        NpgsqlConnection connection,
        IReadOnlyList<IntervalTable> tables,
        DateTime utcNow,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var changed = 0;
        var failed = 0;
        foreach (var table in tables)
        {
            try
            {
                var result = await RunMaintenanceAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
                if (result.Outcome == StepOutcome.Done)
                {
                    changed += result.Count;
                }

                if (result.Outcome != StepOutcome.NotReady)
                {
                    await AnalyzeIfDueAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(
                    "Query Store interval table {Table}: partition maintenance failed ({Message}); the other table still runs and the next pass retries.",
                    table.Parent, ex.Message);

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    try
                    {
                        await connection.CloseAsync().ConfigureAwait(false);
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception reopen) when (reopen is not OperationCanceledException)
                    {
                        logger.LogWarning("Query Store interval partition maintenance could not reopen its connection: {Message}", reopen.Message);
                    }
                }
            }
        }

        return new PassResult(changed, failed);
    }

    /// <summary>
    /// The once-per-start background task (#5571), never awaited on the startup path and never throwing: wait
    /// <paramref name="delay"/>, then Phase A for each table in order (arm, validate, promote, ANALYZE; each table on its
    /// own connection, one table's failure never stops the other), then the Query Store index ensures
    /// (<see cref="QueryStoreBackgroundIndexes.RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{QueryStoreBackgroundIndexes.IndexSpec}, CancellationToken)"/>),
    /// then promotion again for any table whose promotion was <see cref="StepOutcome.Refused"/> (a parent index had no
    /// legacy child, which the index ensure has just built). The order is the plan's: the VALIDATE and a concurrent
    /// <c>CREATE INDEX CONCURRENTLY</c> on the legacy table conflict, so the indexes wait for Phase A.
    /// </summary>
    public static Task RunDelayedAsync(
        NpgsqlDataSource postgres,
        ILogger logger,
        TimeSpan delay,
        IReadOnlyList<QueryStoreBackgroundIndexes.IndexSpec> specs,
        CancellationToken cancellationToken) =>
        RunDelayedAsync(
            logger,
            delay,
            All,
            async (table, token) =>
            {
                await using var connection = await postgres.OpenConnectionAsync(token).ConfigureAwait(false);
                return await RunPromotionAsync(connection, table, DateTime.UtcNow, logger, token).ConfigureAwait(false);
            },
            token => QueryStoreBackgroundIndexes.RunDelayedAsync(postgres, logger, TimeSpan.Zero, specs, token),
            cancellationToken);

    /// <summary><see cref="RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{QueryStoreBackgroundIndexes.IndexSpec}, CancellationToken)"/> with the two actions injected, so the order and the isolation run without a store.</summary>
    internal static async Task RunDelayedAsync(
        ILogger logger,
        TimeSpan delay,
        IReadOnlyList<IntervalTable> tables,
        Func<IntervalTable, CancellationToken, Task<StepResult>> promote,
        Func<CancellationToken, Task> ensureIndexes,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

            var refused = new List<IntervalTable>();
            foreach (var table in tables)
            {
                if (await TryPromoteAsync(logger, table, promote, cancellationToken).ConfigureAwait(false) == StepOutcome.Refused)
                {
                    refused.Add(table);
                }
            }

            try
            {
                await ensureIndexes(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("Query Store index ensure failed after the partition steps: {Message}", ex.Message);
            }

            foreach (var table in refused)
            {
                await TryPromoteAsync(logger, table, promote, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Query Store interval partition promotion was cancelled at shutdown; the next start continues from the catalogs' state.");
        }
        catch (Exception ex)
        {
            logger.LogInformation(
                "Query Store interval partition promotion stopped at shutdown: {ExceptionType}: {Message}; the next start continues.",
                ex.GetType().Name, ex.Message);
        }
    }

    private static async Task<StepOutcome?> TryPromoteAsync(
        ILogger logger,
        IntervalTable table,
        Func<IntervalTable, CancellationToken, Task<StepResult>> promote,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await promote(table, cancellationToken).ConfigureAwait(false);
            logger.LogInformation(
                "Query Store interval table {Table}: promotion step ended {Outcome} ({Detail}).", table.Parent, result.Outcome, result.Detail);
            return result.Outcome;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "Query Store interval table {Table}: promotion failed ({Message}); the other table still runs and the next start retries.",
                table.Parent, ex.Message);
            return null;
        }
    }
}
