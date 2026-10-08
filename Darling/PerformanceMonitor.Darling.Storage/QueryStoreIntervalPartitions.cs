/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
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
/// <para><b>Why a CHECK never refuses a current row (#5571 review H1).</b> Until the table is promoted the CHECK is a
/// wall at S, and S is 24 to 48 h away when it is armed. So two loops keep S ahead of the clock. The hourly convergence
/// step (<see cref="ConvergeUnpromotedAsync"/>, on the sweep loop, bounded by 5 s lock waits) arms when there is no CHECK,
/// tries one promote of a valid CHECK, and re-arms (valid or not, a valid one costs a new VALIDATE) when the table is
/// still not promoted and S is closer than <see cref="ReArmWithin"/>. The background loop
/// (<see cref="RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{QueryStoreBackgroundIndexes.IndexSpec}, CancellationToken)"/>,
/// off the sweep loop, every hour until shutdown) does the long work: a VALIDATE, which never starts when S is closer than
/// <see cref="ValidateGuard"/> (it re-arms first), and the promotion retries. S is the later of <see cref="ArmBound"/> and
/// the day after the legacy table's newest first_execution_time, read under the arm's own lock through the legacy
/// first-execution index; a maximum more than one retention horizon past the normal S is not armed over (see
/// <see cref="IsLegacyMaxBeyondHorizon"/>: the table stays unpartitioned, with one warning, until that row is gone). A
/// VALIDATE that fails with 23514 drops its CHECK and logs that newest time; when the time could be read the same
/// background pass arms again with a later S, and when it could not the next pass does. No day partition is created below S (<see cref="CreateAheadDays"/> starts at the newest upper bound,
/// which is S while only the legacy table bounds it), and the legacy table is dropped only when S is at or below the cutoff.</para>
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

    /// <summary>
    /// A VALIDATE never starts when S is closer than this: its deadline plus <see cref="ReArmWithin"/> (14 h). A VALIDATE
    /// that starts is then over before the re-arm window opens, so no hourly convergence pass ever queues its ACCESS
    /// EXCLUSIVE lock behind a running VALIDATE (which stalls the upserts for the whole lock wait), and the re-arm that has
    /// to follow a failed one is not racing S (#5571 review H1, round 2 L1).
    /// </summary>
    public static readonly TimeSpan ValidateGuard = TimeSpan.FromSeconds(ValidateTimeoutSeconds) + ReArmWithin;

    /// <summary>How often the background loop runs Phase A again (and the daily ANALYZE check) until shutdown.</summary>
    public static readonly TimeSpan BackgroundInterval = TimeSpan.FromHours(1);

    /// <summary><c>statement_timeout</c>, in seconds, of the read of the legacy table's newest first_execution_time.</summary>
    public const int LegacyMaxTimeoutSeconds = 5;

    /// <summary>The first PostgreSQL major version whose <c>ANALYZE ONLY</c> leaves the partitions of a partitioned table alone.</summary>
    public const int AnalyzeOnlyMajorVersion = 18;

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

        /// <summary>A VALIDATE was not started because S is too close for it to finish safely; arm again with a later S first.</summary>
        ReArmFirst,
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

        /// <summary>The legacy table's btree on <c>(first_execution_time)</c>, which answers its newest row in one index probe.</summary>
        public string LegacyFirstExecIndex => $"{Schema}.idx_{Name}_first_exec_legacy";

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
    /// Whether an armed CHECK should be dropped and added again with a new S: its S is closer than
    /// <see cref="ReArmWithin"/>. It applies to a valid CHECK as well, once the table could not be promoted in time: until
    /// the table is promoted the CHECK is the only thing between a current row and a refusal (#5571 review H1).
    /// </summary>
    public static bool ShouldReArm(DateTime checkBound, DateTime utcNow) =>
        checkBound - DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified) < ReArmWithin;

    /// <summary>Whether a VALIDATE may start: S is at least <see cref="ValidateGuard"/> away.</summary>
    public static bool CanStartValidate(DateTime checkBound, DateTime utcNow) =>
        checkBound - DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified) >= ValidateGuard;

    /// <summary>
    /// The bound S to arm: <see cref="ArmBound"/>, or the day after the legacy table's newest first_execution_time when
    /// that is later (a monitored clock ahead of the store's put a row there; with the normal S the VALIDATE would fail
    /// on it). No day partition is ever created below S, so a later S only lets the legacy table keep the rows up to it.
    /// A maximum too close to the end of <see cref="DateTime"/> to add a day to is ignored.
    /// </summary>
    public static DateTime ArmBoundFor(DateTime utcNow, DateTime? legacyMax)
    {
        var normal = ArmBound(utcNow);
        if (!legacyMax.HasValue || legacyMax.Value > DateTime.MaxValue.AddDays(-ArmDays - 1))
        {
            return normal;
        }

        var afterMax = DayStart(legacyMax.Value).AddDays(1);
        return afterMax > normal ? afterMax : normal;
    }

    /// <summary>
    /// Whether the legacy table's newest first_execution_time is too far ahead to arm over: S would land more than one
    /// retention horizon (<paramref name="horizonDays"/>) past the normal bound, or the maximum is so close to the end of
    /// <see cref="DateTime"/> that <see cref="ArmBoundFor"/> ignores it (the CHECK could then never validate). A
    /// promoted table is never re-armed, so one such row would keep every new row in the legacy table for as long as it
    /// is dated ahead, and deleting it afterwards would not help. The arm waits until the row is gone instead
    /// (#5571 review round 2 M1). A maximum inside the horizon is armed over, with S the day after it.
    /// </summary>
    public static bool IsLegacyMaxBeyondHorizon(DateTime utcNow, DateTime? legacyMax, int horizonDays)
    {
        if (!legacyMax.HasValue)
        {
            return false;
        }

        return legacyMax.Value > DateTime.MaxValue.AddDays(-ArmDays - 1)
            || ArmBoundFor(utcNow, legacyMax) > ArmBound(utcNow).AddDays(horizonDays);
    }

    /// <summary>The table name and maximum last warned about by <see cref="ArmCoreAsync"/> (a row dated too far ahead), so the hourly pass logs it once per start and again only when the maximum changes.</summary>
    private static readonly ConcurrentDictionary<string, DateTime> FarFutureMaxWarned = new(StringComparer.Ordinal);

    /// <summary>Forget what was warned about; for tests only.</summary>
    internal static void ResetFarFutureMaxWarnings() => FarFutureMaxWarned.Clear();

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

    /* $1 the legacy first-execution index, $2 the legacy table: true when it is valid, ready, not partial, on that table
       and led by first_execution_time, which is what makes max(first_execution_time) one backward index probe. */
    internal const string LegacyFirstExecIndexUsableSql = @"
SELECT COALESCE
(
    (
        SELECT i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indrelid = to_regclass($2)
               AND i.indkey[0] = (SELECT a.attnum FROM pg_attribute AS a WHERE a.attrelid = i.indrelid AND a.attname = 'first_execution_time')
        FROM pg_index AS i
        WHERE i.indexrelid = to_regclass($1)
    ),
    false
);";

    /* ANALYZE of the parent; ONLY (PostgreSQL 18+) leaves the partitions to autovacuum. */
    internal static string AnalyzeSql(IntervalTable table, bool only) => only ? $"ANALYZE ONLY {table.Parent};" : $"ANALYZE {table.Parent};";

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
        NpgsqlConnection connection, IntervalTable table, CancellationToken cancellationToken, NpgsqlTransaction? transaction = null)
    {
        await using var command = Command(connection, StateSql, ShortTimeoutSeconds, transaction);
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
    /// What <see cref="ArmCoreAsync"/> does with the state it read: an early result (nothing to arm, or not ready), or
    /// whether to drop the present CHECK first (a re-arm).
    /// </summary>
    private static (StepResult? Early, bool ReArm) DecideArm(IntervalTable table, PartitionState state, DateTime utcNow, bool reArmValid)
    {
        /* Promoted before everything else: once the legacy table is dropped the parent is still promoted, and that is not
           "not ready" (#5571 review L2). */
        if (state.Promoted)
        {
            return (new StepResult(StepOutcome.NothingToDo, "already promoted"), false);
        }

        if (!state.ParentIsPartitioned || !state.LegacyExists)
        {
            return (new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table with a legacy table yet"), false);
        }

        var reArm = state.CheckPresent && state.CheckBound.HasValue
            && ShouldReArm(state.CheckBound.Value, utcNow) && (reArmValid || !state.CheckValid);
        if (state.CheckPresent && !reArm)
        {
            return (new StepResult(StepOutcome.NothingToDo, state.CheckValid ? "already armed and valid" : "already armed"), false);
        }

        return (null, reArm);
    }

    /// <summary>
    /// The legacy table's newest <c>first_execution_time</c>, read through the legacy first-execution index
    /// (<c>idx_X_first_exec_legacy</c>, which leads with the column, so the maximum is one backward index probe) under
    /// <c>SET LOCAL statement_timeout</c> of <see cref="LegacyMaxTimeoutSeconds"/> s. Null when the table is empty, when
    /// that index is missing, invalid, partial or not led by the column (the read would scan the heap, so it is not
    /// attempted), and when the read times out (rolled back to a savepoint, so the caller's transaction and its lock
    /// survive). <paramref name="transaction"/> is required: the timeout is transaction-local and is reset afterwards.
    /// </summary>
    internal static async Task<DateTime?> ReadLegacyMaxAsync(
        NpgsqlConnection connection, IntervalTable table, NpgsqlTransaction transaction, ILogger logger, CancellationToken cancellationToken)
    {
        bool usable;
        await using (var probe = Command(connection, LegacyFirstExecIndexUsableSql, ShortTimeoutSeconds, transaction))
        {
            AddText(probe, table.LegacyFirstExecIndex);
            AddText(probe, table.Legacy);
            usable = (bool)(await probe.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        if (!usable)
        {
            logger.LogWarning(
                "Query Store interval table {Table}: {Index} is missing or unusable, so the newest first_execution_time of {Legacy} was not read.",
                table.Parent, table.LegacyFirstExecIndex, table.Legacy);
            return null;
        }

        await ExecuteAsync(connection, "SAVEPOINT legacy_max;", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
        try
        {
            await ExecuteAsync(
                connection, $"SET LOCAL statement_timeout = '{LegacyMaxTimeoutSeconds}s';", ShortTimeoutSeconds, transaction, cancellationToken)
                .ConfigureAwait(false);
            object? value;
            await using (var read = Command(connection, $"SELECT max(first_execution_time) FROM {table.Legacy};", LegacyMaxTimeoutSeconds + 5, transaction))
            {
                value = await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }

            /* The timeout was for this read only: the DDL after it keeps the session's own. */
            await ExecuteAsync(connection, "RESET statement_timeout;", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, "RELEASE SAVEPOINT legacy_max;", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            return value is DateTime max ? DateTime.SpecifyKind(max, DateTimeKind.Unspecified) : null;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled)
        {
            await ExecuteAsync(connection, "ROLLBACK TO SAVEPOINT legacy_max;", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            logger.LogWarning(
                "Query Store interval table {Table}: reading the newest first_execution_time of {Legacy} took longer than {Seconds} s and was cancelled.",
                table.Parent, table.Legacy, LegacyMaxTimeoutSeconds);
            return null;
        }
    }

    /// <summary>
    /// Phase A step 1. Adds <c>CHECK (first_execution_time &lt; S) NOT VALID</c> to the legacy table, or re-arms a CHECK
    /// that is still not valid with S closer than <see cref="ReArmWithin"/>. A valid CHECK is left alone here
    /// (<see cref="ConvergeUnpromotedAsync"/> re-arms a valid one that cannot be promoted in time). S is
    /// <see cref="ArmBoundFor"/>: the normal bound, or the day after the legacy table's newest row when that is later.
    /// A lock timeout is retried <see cref="ArmRetries"/> times, <see cref="ArmRetryDelay"/> apart, then returns
    /// <see cref="StepOutcome.RetryLater"/>.
    /// </summary>
    public static Task<StepResult> ArmAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken) =>
        ArmAsync(connection, table, utcNow, logger, ArmRetryDelay, cancellationToken);

    internal static Task<StepResult> ArmAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, TimeSpan retryDelay, CancellationToken cancellationToken) =>
        ArmCoreAsync(connection, table, utcNow, logger, retryDelay, ArmRetries, reArmValid: false, cancellationToken);

    /// <summary>
    /// Arm or re-arm in one transaction: <c>lock_timeout</c>, <c>LOCK TABLE legacy IN ACCESS EXCLUSIVE MODE</c> (so no
    /// row can arrive between the next two reads and the CHECK), the state again (a concurrent arm, re-arm or promotion
    /// wins and this step does nothing), the legacy maximum, then the DROP of the old CHECK when re-arming and the ADD
    /// of the new one. <paramref name="retries"/> lock timeouts are retried <paramref name="retryDelay"/> apart; the
    /// hourly convergence step passes 0 so it never sleeps inside its budget. <paramref name="reArmValid"/> lets a
    /// valid CHECK be re-armed (it costs a new VALIDATE).
    /// </summary>
    internal static async Task<StepResult> ArmCoreAsync(
        NpgsqlConnection connection,
        IntervalTable table,
        DateTime utcNow,
        ILogger logger,
        TimeSpan retryDelay,
        int retries,
        bool reArmValid,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
            var (early, _) = DecideArm(table, state, utcNow, reArmValid);
            if (early is { } decided)
            {
                return decided;
            }

            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await ExecuteAsync(
                    connection, $"LOCK TABLE {table.Legacy} IN ACCESS EXCLUSIVE MODE;", ShortTimeoutSeconds, transaction, cancellationToken)
                    .ConfigureAwait(false);

                var locked = await ReadStateAsync(connection, table, cancellationToken, transaction).ConfigureAwait(false);
                var (raced, reArm) = DecideArm(table, locked, utcNow, reArmValid);
                if (raced is { } already)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return already;
                }

                var legacyMax = await ReadLegacyMaxAsync(connection, table, transaction, logger, cancellationToken).ConfigureAwait(false);
                if (IsLegacyMaxBeyondHorizon(utcNow, legacyMax, table.HorizonDays))
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    WarnFarFutureMax(logger, table, legacyMax!.Value, utcNow);
                    return new StepResult(
                        StepOutcome.NotReady, $"{table.Legacy} holds a row dated {Literal(legacyMax.Value)}, too far ahead to arm over");
                }

                var normal = ArmBound(utcNow);
                var s = ArmBoundFor(utcNow, legacyMax);
                if (reArm)
                {
                    await ExecuteAsync(connection, DropCheckSql(table), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                }

                await ExecuteAsync(connection, AddCheckSql(table, s), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                if (s > normal)
                {
                    logger.LogWarning(
                        "Query Store interval table {Table}: the legacy table holds first_execution_time up to {Max:O} (a monitored clock ahead of the store's), "
                        + "so the legacy bound S is {Bound:O} instead of {Normal:O}. The day partitions start at S.",
                        table.Parent, legacyMax, s, normal);
                }

                logger.LogInformation(
                    "Query Store interval table {Table}: armed the legacy bound S = {Bound:O} ({Action}).",
                    table.Parent, s, reArm ? "re-armed" : "armed");
                return new StepResult(StepOutcome.Done, $"{(reArm ? "re-armed" : "armed")} at {Literal(s)}", 1);
            }
            catch (PostgresException ex) when (IsLockTimeout(ex))
            {
                if (attempt >= retries)
                {
                    logger.LogWarning(
                        "Query Store interval table {Table}: arming the legacy bound hit a lock timeout {Attempts} time(s); the next pass retries.",
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

    private static void WarnFarFutureMax(ILogger logger, IntervalTable table, DateTime legacyMax, DateTime utcNow)
    {
        var seen = FarFutureMaxWarned.TryGetValue(table.Parent, out var last) && last == legacyMax;
        FarFutureMaxWarned[table.Parent] = legacyMax;
        if (seen)
        {
            return;
        }

        logger.LogWarning(
            "Query Store interval table {Table}: {Legacy} holds a row with first_execution_time {Max:O}, more than {Horizon} days past the normal "
            + "legacy bound {Normal:O}. The table stays unpartitioned until that row is gone: arming over it would keep every new row in the legacy "
            + "table for that long, and a promoted table is never re-armed. Delete the row (or let the retention horizon reach it); the first "
            + "hourly pass after that arms the table.",
            table.Parent, table.Legacy, legacyMax, table.HorizonDays, ArmBound(utcNow));
    }

    /// <summary>
    /// The hourly convergence step for a table that is not promoted (#5571 review H1), cheap and bounded by 5 s lock
    /// waits, with no VALIDATE and no sleeping: no CHECK, arm; a valid CHECK, one promote; and when the table is still
    /// not promoted and S is closer than <see cref="ReArmWithin"/>, re-arm with a later S, valid or not (a valid one
    /// costs a new VALIDATE, which is accepted). That is what keeps the CHECK from ever refusing a current row while
    /// the table waits for its promotion. A promoted table returns <see cref="StepOutcome.NothingToDo"/>.
    /// </summary>
    public static async Task<StepResult> ConvergeUnpromotedAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.ParentIsPartitioned)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table with a legacy table yet");
        }

        if (state.Promoted)
        {
            return new StepResult(StepOutcome.NothingToDo, "already promoted");
        }

        if (!state.CheckPresent)
        {
            return await ArmCoreAsync(connection, table, utcNow, logger, TimeSpan.Zero, 0, reArmValid: false, cancellationToken).ConfigureAwait(false);
        }

        StepResult? promoteResult = null;
        if (state.CheckValid)
        {
            try
            {
                var promote = await PromoteAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
                if (promote.Outcome is StepOutcome.Done or StepOutcome.NothingToDo)
                {
                    return promote;
                }

                promoteResult = promote;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* A promote that fails for any reason but a lock wait must not skip the re-arm below: at S a valid CHECK refuses
                   every current row (#5571 review round 2 L3). */
                logger.LogWarning(
                    "Query Store interval table {Table}: the promotion failed ({Message}); the CHECK is re-armed if S is close, and the next pass tries again.",
                    table.Parent, ex.Message);
                promoteResult = new StepResult(StepOutcome.Refused, $"the promotion failed: {ex.Message}");
            }
        }

        if (state.CheckBound.HasValue && ShouldReArm(state.CheckBound.Value, utcNow))
        {
            return await ArmCoreAsync(connection, table, utcNow, logger, TimeSpan.Zero, 0, reArmValid: true, cancellationToken).ConfigureAwait(false);
        }

        return promoteResult ?? new StepResult(StepOutcome.NothingToDo, "armed; the background validate proves it");
    }

    /// <summary>
    /// Phase A step 2, run only by the background loop, never by a convergence step. Validates the legacy CHECK: a heap
    /// read of the legacy table under SHARE UPDATE EXCLUSIVE, so upserts and reads continue. It never starts when S is
    /// closer than <see cref="ValidateGuard"/> (the command deadline plus <see cref="ReArmWithin"/>): the result is
    /// <see cref="StepOutcome.ReArmFirst"/>. The transaction also sets <c>statement_timeout</c> to the deadline, so the
    /// server ends an orphaned VALIDATE itself. A lock timeout (autovacuum, a concurrent index build) returns
    /// <see cref="StepOutcome.RetryLater"/>. A 23514 means the legacy table holds a row at or after S: the CHECK is
    /// dropped, the table's newest <c>first_execution_time</c> is logged, and the result is
    /// <see cref="StepOutcome.Refused"/> when that time was read (the loop re-arms with a later S at once), or
    /// <see cref="StepOutcome.RetryLater"/> when it was not (a second VALIDATE would fail the same way).
    /// </summary>
    public static async Task<StepResult> ValidateAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (state.Promoted)
        {
            return new StepResult(StepOutcome.NothingToDo, "already promoted");
        }

        if (!state.ParentIsPartitioned || !state.LegacyExists || !state.CheckPresent || !state.CheckBound.HasValue)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Legacy} has no armed bound yet");
        }

        if (state.CheckValid)
        {
            return new StepResult(StepOutcome.NothingToDo, "already validated");
        }

        var s = state.CheckBound.Value;
        if (!CanStartValidate(s, utcNow))
        {
            return new StepResult(StepOutcome.ReArmFirst, $"the bound {Literal(s)} is too close for a validate to finish: re-arm first");
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            /* The server enforces the deadline too: a VALIDATE orphaned by a killed service or a lost cancel request would
               otherwise keep its SHARE UPDATE EXCLUSIVE lock, and every re-arm would hit a lock timeout (#5571 review round 2 L2). */
            await ExecuteAsync(
                connection, $"SET LOCAL statement_timeout = '{ValidateTimeoutSeconds}s';", ShortTimeoutSeconds, transaction, cancellationToken)
                .ConfigureAwait(false);
            await ExecuteAsync(connection, ValidateSql(table), ValidateTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (IsLockTimeout(ex))
        {
            logger.LogWarning(
                "Query Store interval table {Table}: validating the legacy bound hit a lock timeout; the background loop retries it.", table.Parent);
            return new StepResult(StepOutcome.RetryLater, "lock timeout");
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.CheckViolation)
        {
            return await DropViolatedCheckAsync(connection, table, s, logger, cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation(
            "Query Store interval table {Table}: validated the legacy bound in {Seconds:F1}s.", table.Parent, started.Elapsed.TotalSeconds);
        return new StepResult(StepOutcome.Done, "validated");
    }

    /// <summary>
    /// A VALIDATE failed with 23514: the legacy table holds a row at or after S (a monitored clock ahead of the store's).
    /// Do not leave a CHECK armed that can never validate. Read the table's newest first_execution_time for the log (it
    /// is only for the log, and it is the value the next arm uses for S), then drop the CHECK. If the drop hits a lock
    /// timeout the CHECK stays, and the next background pass finds the same failure and tries the drop again.
    /// </summary>
    private static async Task<StepResult> DropViolatedCheckAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime s, ILogger logger, CancellationToken cancellationToken)
    {
        DateTime? legacyMax = null;
        try
        {
            await using var read = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            legacyMax = await ReadLegacyMaxAsync(connection, table, read, logger, cancellationToken).ConfigureAwait(false);
            await read.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Query Store interval table {Table}: the legacy maximum could not be read: {Message}", table.Parent, ex.Message);
        }

        var dropped = false;
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, DropCheckSql(table), ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            dropped = true;
        }
        catch (PostgresException ex) when (IsLockTimeout(ex))
        {
            /* Left armed; the next pass drops it. */
        }

        logger.LogWarning(
            "Query Store interval table {Table}: the legacy bound S = {Bound:O} could not be validated, because {Legacy} holds a row at or "
            + "after it (newest first_execution_time {Max}). {Action}",
            table.Parent, s, table.Legacy,
            legacyMax.HasValue ? legacyMax.Value.ToString("O", CultureInfo.InvariantCulture) : "unknown",
            dropped
                ? "The CHECK was dropped; the next arm uses a later S when the newest row can be read."
                : "The CHECK could not be dropped (lock timeout); the next pass tries again.");

        /* With the maximum unknown the re-arm would use the normal S and the VALIDATE would fail the same way, so the background
           loop must not run Phase A a second time in this pass: one heap read per pass, not two (#5571 review round 2 L4). */
        if (!legacyMax.HasValue)
        {
            return new StepResult(StepOutcome.RetryLater, dropped ? "a legacy row is at or after S, newest unknown; the check was dropped" : "a legacy row is at or after S, newest unknown");
        }

        return new StepResult(StepOutcome.Refused, dropped ? "a legacy row is at or after S; the check was dropped" : "a legacy row is at or after S");
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
        if (state.Promoted)
        {
            return new StepResult(StepOutcome.NothingToDo, "already promoted");
        }

        if (!state.ParentIsPartitioned || !state.LegacyExists)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table with a legacy table yet");
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

            /* The parent's lock takes the legacy table's too, so nothing changes from here on. A convergence step or the
               background loop may have promoted, or re-armed, since the state above was read: the re-attach below is only
               a metadata operation while the VALID CHECK for exactly this S is on the table (a re-armed CHECK is NOT
               VALID, and the attach would then scan the whole legacy heap under this lock). */
            var locked = await ReadStateAsync(connection, table, cancellationToken, transaction).ConfigureAwait(false);
            if (locked.Promoted)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new StepResult(StepOutcome.NothingToDo, "already promoted");
            }

            if (!locked.CheckValid || locked.CheckBound != s)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return new StepResult(StepOutcome.NotReady, "the legacy bound changed under the lock; the next pass decides");
            }

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

    /// <summary>
    /// ANALYZE of the parent. Autovacuum never analyzes a partitioned table, so the parent's statistics only exist when
    /// this runs. In one transaction under a 5 s <c>lock_timeout</c> (a leaf held by an anti-wraparound vacuum or an index
    /// build gives <see cref="StepOutcome.RetryLater"/>, not a wait). On PostgreSQL 18 and later it is
    /// <c>ANALYZE ONLY parent</c>: the leaves are analyzed by autovacuum already, and without ONLY the statement
    /// samples every leaf again, the 91 GB legacy table included (#5571 review M1). Never called from a convergence
    /// step: it runs on the background loop and once after a promotion.
    /// </summary>
    public static async Task<StepResult> AnalyzeAsync(
        NpgsqlConnection connection, IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.ParentIsPartitioned)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not a partitioned table");
        }

        var started = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(connection, LockTimeoutSql, ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
            await ExecuteAsync(
                connection, AnalyzeSql(table, connection.PostgreSqlVersion.Major >= AnalyzeOnlyMajorVersion), AnalyzeTimeoutSeconds, transaction, cancellationToken)
                .ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (IsLockTimeout(ex))
        {
            logger.LogInformation(
                "Query Store interval table {Table}: analyzing the parent hit a lock timeout; the next background pass retries.", table.Parent);
            return new StepResult(StepOutcome.RetryLater, "lock timeout");
        }

        logger.LogInformation(
            "Query Store interval table {Table}: analyzed the parent in {Seconds:F1}s.", table.Parent, started.Elapsed.TotalSeconds);
        return new StepResult(StepOutcome.Done, "analyzed");
    }

    /// <summary>
    /// The once-a-day ANALYZE, for promoted tables: runs <see cref="AnalyzeAsync"/> when the parent was last analyzed more
    /// than <see cref="AnalyzeEvery"/> ago, or never. Not promoted is <see cref="StepOutcome.NotReady"/>.
    /// </summary>
    public static async Task<StepResult> AnalyzeIfDueAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var state = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
        if (!state.Promoted)
        {
            return new StepResult(StepOutcome.NotReady, $"{table.Parent} is not promoted yet");
        }

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
    /// Phase A as one call, one pass of the background loop: arm (or re-arm a not-yet-valid CHECK that is close to S),
    /// validate (never when S is closer than <see cref="ValidateGuard"/>), promote (a lock timeout is retried
    /// <see cref="PromoteRetries"/> times, <see cref="PromoteRetryDelay"/> apart), and ANALYZE the parent only when this
    /// call did the promotion (#5571 review L1). When the table is still not promoted and S is closer than
    /// <see cref="ReArmWithin"/>, the CHECK is re-armed, valid or not. Stops at the first step that is not done and
    /// returns it; the next pass of the loop continues from the catalogs' state.
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
        if (validate.Outcome is StepOutcome.RetryLater or StepOutcome.NotReady or StepOutcome.Refused or StepOutcome.ReArmFirst)
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

        if (promote.Outcome == StepOutcome.Done)
        {
            try
            {
                await AnalyzeAsync(connection, table, logger, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(
                    "Query Store interval table {Table}: analyzing the parent after the promotion failed ({Message}); the daily ANALYZE retries.",
                    table.Parent, ex.Message);
            }

            return promote;
        }

        if (promote.Outcome is StepOutcome.RetryLater or StepOutcome.Refused)
        {
            var current = await ReadStateAsync(connection, table, cancellationToken).ConfigureAwait(false);
            if (!current.Promoted && current.CheckBound.HasValue && ShouldReArm(current.CheckBound.Value, utcNow))
            {
                await ArmCoreAsync(connection, table, utcNow, logger, armRetryDelay, 0, reArmValid: true, cancellationToken).ConfigureAwait(false);
            }
        }

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
                await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {partition.Name};", ShortTimeoutSeconds, transaction, cancellationToken).ConfigureAwait(false);
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
    /// Phase B as one call, for the start step and the hourly pass: converge a table that is not promoted yet
    /// (<see cref="ConvergeUnpromotedAsync"/>), then create-ahead and drop-expired. Returns the create-ahead result unless
    /// it was not done, then the drop's; a convergence that changed something adds to the count.
    /// </summary>
    public static async Task<StepResult> RunMaintenanceAsync(
        NpgsqlConnection connection, IntervalTable table, DateTime utcNow, ILogger logger, CancellationToken cancellationToken)
    {
        var converge = await ConvergeUnpromotedAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
        var ahead = await CreateAheadAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);
        if (ahead.Outcome == StepOutcome.NotReady)
        {
            return converge.Outcome == StepOutcome.NotReady ? ahead : converge;
        }

        var drop = await DropExpiredAsync(connection, table, utcNow, logger, cancellationToken).ConfigureAwait(false);

        /* A pass that created days AND dropped days reports both counts, so the hourly "changed" tally is not short by
           the drops (#5571). */
        var result = ahead.Outcome == StepOutcome.Done ? ahead with { Count = ahead.Count + drop.Count } : drop;
        return converge.Outcome == StepOutcome.Done
            ? new StepResult(StepOutcome.Done, converge.Detail + "; " + result.Detail, Math.Max(converge.Count, 1) + (result.Outcome == StepOutcome.Done ? result.Count : 0))
            : result;
    }

    /// <summary>What one <see cref="RunMaintenancePassAsync"/> did: partitions created or dropped, and tables whose step failed.</summary>
    public readonly record struct PassResult(int Changed, int Failed);

    /// <summary>
    /// The hourly step (#5571): for each table, in order, <see cref="RunMaintenanceAsync"/> (converge a table that is not
    /// promoted: arm, one promote of a valid CHECK, re-arm before S; then create ahead with the DEFAULT drain, then drop
    /// expired days and the legacy table once it is bounded below the cutoff). It never validates and never ANALYZEs: both
    /// run on the background loop, off the sweep loop (#5571 review H1, M1). One table's failure is logged and counted and
    /// never stops the other table; a lock timeout (55P03) is <see cref="StepOutcome.RetryLater"/>, already logged once by
    /// the step, and is not a failure. If a failure closed the connection it is reopened for the next table. Shutdown
    /// (cancellation) propagates.
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
    /// The background task (#5571), launched at start, never awaited on the startup path and never throwing: wait
    /// <paramref name="delay"/>, then loop until shutdown, every <see cref="BackgroundInterval"/>: Phase A for each table
    /// that is not promoted (arm, validate, promote; each table on its own connection, one table's failure never stops the
    /// other), then, on the first pass and after any refusal, the Query Store index ensures
    /// (<see cref="QueryStoreBackgroundIndexes.RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{QueryStoreBackgroundIndexes.IndexSpec}, CancellationToken)"/>)
    /// and promotion again for any table whose promotion was <see cref="StepOutcome.Refused"/> (a parent index had no
    /// legacy child, which the index ensure has just built), then the once-a-day ANALYZE of each promoted parent. The
    /// VALIDATE and a concurrent <c>CREATE INDEX CONCURRENTLY</c> on the legacy table conflict, so the indexes wait for
    /// Phase A. The loop is serial, so one VALIDATE runs at a time per table, and none of it ever runs inside a
    /// convergence step's budget (#5571 review H1, M1).
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
            BackgroundInterval,
            All,
            async (table, token) =>
            {
                await using var connection = await postgres.OpenConnectionAsync(token).ConfigureAwait(false);
                return await RunPromotionAsync(connection, table, DateTime.UtcNow, logger, token).ConfigureAwait(false);
            },
            async (table, token) =>
            {
                await using var connection = await postgres.OpenConnectionAsync(token).ConfigureAwait(false);
                return await AnalyzeIfDueAsync(connection, table, DateTime.UtcNow, logger, token).ConfigureAwait(false);
            },
            token => QueryStoreBackgroundIndexes.RunDelayedAsync(postgres, logger, TimeSpan.Zero, specs, token),
            cancellationToken);

    /// <summary><see cref="RunDelayedAsync(NpgsqlDataSource, ILogger, TimeSpan, IReadOnlyList{QueryStoreBackgroundIndexes.IndexSpec}, CancellationToken)"/> with the three actions injected, so the order, the repetition and the isolation run without a store.</summary>
    internal static async Task RunDelayedAsync(
        ILogger logger,
        TimeSpan delay,
        TimeSpan interval,
        IReadOnlyList<IntervalTable> tables,
        Func<IntervalTable, CancellationToken, Task<StepResult>> promote,
        Func<IntervalTable, CancellationToken, Task<StepResult>> analyze,
        Func<CancellationToken, Task> ensureIndexes,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);

            var indexesEnsured = false;
            for (var pass = 0; ; pass++)
            {
                var quiet = pass > 0;
                var refused = new List<IntervalTable>();
                foreach (var table in tables)
                {
                    if (await TryStepAsync(logger, table, "promotion", promote, quiet, cancellationToken).ConfigureAwait(false) == StepOutcome.Refused)
                    {
                        refused.Add(table);
                    }
                }

                if (!indexesEnsured || refused.Count > 0)
                {
                    try
                    {
                        await ensureIndexes(cancellationToken).ConfigureAwait(false);
                        indexesEnsured = true;
                    }
                    catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                    {
                        logger.LogWarning("Query Store index ensure failed after the partition steps: {Message}", ex.Message);
                    }
                }

                foreach (var table in refused)
                {
                    await TryStepAsync(logger, table, "promotion", promote, quiet, cancellationToken).ConfigureAwait(false);
                }

                foreach (var table in tables)
                {
                    await TryStepAsync(logger, table, "analyze", analyze, quiet: true, cancellationToken).ConfigureAwait(false);
                }

                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Query Store interval partition background task was cancelled at shutdown; the next start continues from the catalogs' state.");
        }
        catch (Exception ex)
        {
            logger.LogInformation(
                "Query Store interval partition background task stopped at shutdown: {ExceptionType}: {Message}; the next start continues.",
                ex.GetType().Name, ex.Message);
        }
    }

    private static async Task<StepOutcome?> TryStepAsync(
        ILogger logger,
        IntervalTable table,
        string what,
        Func<IntervalTable, CancellationToken, Task<StepResult>> step,
        bool quiet,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await step(table, cancellationToken).ConfigureAwait(false);
            var level = quiet && result.Outcome is StepOutcome.NothingToDo or StepOutcome.NotReady ? LogLevel.Debug : LogLevel.Information;
            logger.Log(
                level, "Query Store interval table {Table}: {Step} step ended {Outcome} ({Detail}).", table.Parent, what, result.Outcome, result.Detail);
            return result.Outcome;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(
                "Query Store interval table {Table}: the {Step} step failed ({Message}); the other table still runs and the background task tries again in an hour.",
                table.Parent, what, ex.Message);
            return null;
        }
    }
}
