/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One service start, as <c>collect.service_starts</c> keeps it (#9415). Times are UTC instants.</summary>
/// <param name="StartId">This start's id, made when the process starts.</param>
/// <param name="ServiceHost">The machine the service runs on, as <c>config.service_version</c> names it.</param>
/// <param name="ProductVersion">The full build string the service reports, for example <c>3.14.0-nightly.20261124.730</c>, without the commit.</param>
/// <param name="CommitId">The commit the build was made from, or null for a build made outside a git checkout.</param>
/// <param name="ProcessStartedUtc">When the process started.</param>
/// <param name="SchemaVersionBefore">The highest rung the store had stamped when this start reached it.</param>
/// <param name="SchemaVersionAfter">The highest rung after this start's migrations. Null while the start has not got the store ready.</param>
/// <param name="StoreReadyUtc">When the migrations finished and the store was ready. Null while it is not.</param>
/// <param name="PreviousStopUtc">The last flush of the previous start row of this host. The service writes nothing on a clean stop except a last flush, so this is the last time the previous run was known to be alive. Null for the first start.</param>
/// <param name="LastFlushUtc">The last time this row's counts were written.</param>
/// <param name="ErrorCount">ERROR and CRITICAL lines the service logged since the process started.</param>
/// <param name="WarnCount">WARN lines since the process started.</param>
/// <param name="FirstErrors">The first <see cref="ServiceStartHistory.FirstLinesKept"/> ERROR lines, text masked and cut.</param>
/// <param name="FirstWarns">The first <see cref="ServiceStartHistory.FirstLinesKept"/> WARN lines, text masked and cut.</param>
/// <param name="Errors10Minutes">The ERROR count at start + 10 minutes. Null until then.</param>
/// <param name="Warns10Minutes">The WARN count at start + 10 minutes. Null until then.</param>
/// <param name="Errors30Minutes">The ERROR count at start + 30 minutes. Null until then.</param>
/// <param name="Warns30Minutes">The WARN count at start + 30 minutes. Null until then.</param>
public sealed record ServiceStartRow(
    Guid StartId,
    string ServiceHost,
    string ProductVersion,
    string? CommitId,
    DateTime ProcessStartedUtc,
    int? SchemaVersionBefore,
    int? SchemaVersionAfter,
    DateTime? StoreReadyUtc,
    DateTime? PreviousStopUtc,
    DateTime LastFlushUtc,
    long ErrorCount,
    long WarnCount,
    IReadOnlyList<string> FirstErrors,
    IReadOnlyList<string> FirstWarns,
    long? Errors10Minutes,
    long? Warns10Minutes,
    long? Errors30Minutes,
    long? Warns30Minutes);

/// <summary>
/// One migration rung a start applied, or failed (#9415). <paramref name="Attempt"/> counts the start's tries at the ladder: a rung that failed and then applied has two rows.
/// <paramref name="Objects"/> is how many object rows (#9438) the rung has in <c>collect.service_start_rung_objects</c>, and <paramref name="FreshInstall"/> is true when the
/// start's ladder began on an empty store, which writes no object rows for any rung.
/// </summary>
public sealed record ServiceStartRung(
    Guid StartId,
    int Version,
    string Name,
    int Attempt,
    DateTime StartedUtc,
    long ElapsedMilliseconds,
    string Outcome,
    string? ErrorText,
    int Objects = 0,
    bool FreshInstall = false);

/// <summary>A start row with its rungs, as <see cref="ServiceStartHistory.ReadAsync"/> returns it. <paramref name="Current"/> is true for the newest start of its host.</summary>
public sealed record ServiceStartRecord(ServiceStartRow Start, IReadOnlyList<ServiceStartRung> Rungs, bool Current);

/// <summary>A start whose build differs from the same host's previous start (#9415): the point on a trend where the build changed.</summary>
/// <param name="ServiceHost">The service host that restarted on another build.</param>
/// <param name="StartedUtc">When the process of the new build started.</param>
/// <param name="FromBuild">The build string of the host's previous start.</param>
/// <param name="ToBuild">The build string of this start.</param>
public sealed record ServiceBuildChange(string ServiceHost, DateTime StartedUtc, string FromBuild, string ToBuild);

/// <summary>
/// The store's record of each service start (V307, #9415): one row per start in <c>collect.service_starts</c>, one row per migration rung that
/// start tried in <c>collect.service_start_rungs</c>. The objects each rung created, changed or dropped are the child rows in <c>collect.service_start_rung_objects</c> (V311, #9438, <see cref="ServiceStartRungObjects"/>). <c>config.service_version</c> (V174) stays as it is: it holds the version each host last
/// reported, and this holds every start.
///
/// <para><b>Why the rows are written late.</b> The rung rows cannot be written by the runner as it goes, because the migration that creates
/// these tables is itself a rung. The service keeps what the runner reports in memory and writes it once the table exists.</para>
///
/// <para><b>Retention.</b> <see cref="RetentionDays"/> days, the same as the other store self-metrics tables, so the history covers the longest store
/// trend window. A start is removed when its last flush is older than that, so the start the service is running is never removed. Its rung rows and
/// object rows go with it.</para>
/// </summary>
public static class ServiceStartHistory
{
    /// <summary>The migration that creates the two tables. One constant, so a renumber is one edit.</summary>
    public const int Version = 307;

    /// <summary>Days a start is kept after its last flush. Equals <see cref="StoreSelfMetrics.RetentionDays"/>.</summary>
    public const int RetentionDays = StoreSelfMetrics.RetentionDays;

    /// <summary>How many ERROR lines and how many WARN lines a start row keeps.</summary>
    public const int FirstLinesKept = 10;

    /// <summary>Characters of one kept line.</summary>
    public const int LineLength = 500;

    private const int CommandTimeoutSeconds = 30;

    /// <summary>The tables, created empty. Idempotent, and no data moves. Times are naive UTC like the other <c>collect</c> self-metrics tables.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.service_starts
(
    start_id uuid NOT NULL,
    service_host text NOT NULL,
    product_version text NOT NULL,
    commit_id text,
    process_started_at timestamp NOT NULL,
    schema_version_before integer,
    schema_version_after integer,
    store_ready_at timestamp,
    previous_stop_at timestamp,
    last_flush_at timestamp NOT NULL,
    error_count bigint NOT NULL DEFAULT 0,
    warn_count bigint NOT NULL DEFAULT 0,
    first_errors text[] NOT NULL DEFAULT '{}',
    first_warns text[] NOT NULL DEFAULT '{}',
    errors_10m bigint,
    warns_10m bigint,
    errors_30m bigint,
    warns_30m bigint,
    CONSTRAINT pk_service_starts PRIMARY KEY (start_id)
);

CREATE INDEX IF NOT EXISTS idx_service_starts_started
    ON collect.service_starts(process_started_at);

CREATE TABLE IF NOT EXISTS collect.service_start_rungs
(
    start_id uuid NOT NULL,
    rung_version integer NOT NULL,
    attempt integer NOT NULL,
    rung_name text NOT NULL,
    started_at timestamp NOT NULL,
    elapsed_ms bigint NOT NULL,
    outcome text NOT NULL,
    error_text text,
    CONSTRAINT pk_service_start_rungs PRIMARY KEY (start_id, rung_version, attempt)
);";

    /// <summary>
    /// Writes a start row, or brings the one there up to date. The previous stop is read when the row is first written, from the host's other starts,
    /// and never changed after. The two snapshot pairs and the schema and ready columns are written once and kept: a later write that has none
    /// leaves them. $1 id, $2 host, $3 version, $4 commit, $5 process start, $6 before, $7 after, $8 ready, $9 flush, $10 errors, $11 warns,
    /// $12 first errors, $13 first warns, $14 errors 10m, $15 warns 10m, $16 errors 30m, $17 warns 30m (all times naive UTC).
    /// </summary>
    public const string UpsertStartSql = @"
INSERT INTO collect.service_starts AS s
    (start_id, service_host, product_version, commit_id, process_started_at, schema_version_before, schema_version_after, store_ready_at,
     previous_stop_at, last_flush_at, error_count, warn_count, first_errors, first_warns, errors_10m, warns_10m, errors_30m, warns_30m)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8,
     (SELECT max(p.last_flush_at) FROM collect.service_starts AS p WHERE p.service_host = $2 AND p.start_id <> $1),
     $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT (start_id) DO UPDATE
SET schema_version_before = COALESCE(s.schema_version_before, EXCLUDED.schema_version_before),
    schema_version_after = COALESCE(EXCLUDED.schema_version_after, s.schema_version_after),
    store_ready_at = COALESCE(s.store_ready_at, EXCLUDED.store_ready_at),
    last_flush_at = EXCLUDED.last_flush_at,
    error_count = EXCLUDED.error_count,
    warn_count = EXCLUDED.warn_count,
    first_errors = EXCLUDED.first_errors,
    first_warns = EXCLUDED.first_warns,
    errors_10m = COALESCE(s.errors_10m, EXCLUDED.errors_10m),
    warns_10m = COALESCE(s.warns_10m, EXCLUDED.warns_10m),
    errors_30m = COALESCE(s.errors_30m, EXCLUDED.errors_30m),
    warns_30m = COALESCE(s.warns_30m, EXCLUDED.warns_30m);";

    /// <summary>One rung row. A row written twice (a flush that failed after the insert) is kept as it was. $1 id, $2 version, $3 attempt, $4 name, $5 started, $6 elapsed ms, $7 outcome, $8 error text.</summary>
    public const string InsertRungSql = @"
INSERT INTO collect.service_start_rungs (start_id, rung_version, attempt, rung_name, started_at, elapsed_ms, outcome, error_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (start_id, rung_version, attempt) DO NOTHING;";

    /// <summary>
    /// The retention: removes the starts whose last flush is before $1 (naive UTC) and, in the same statement, their rungs and the objects of those rungs
    /// (#9438). The running start flushes every minute, so it is never removed.
    /// </summary>
    public const string RetentionSql = @"
WITH gone AS
(
    DELETE FROM collect.service_starts
    WHERE last_flush_at < $1
    RETURNING start_id
),
gone_rungs AS
(
    DELETE FROM collect.service_start_rungs AS r
    USING gone
    WHERE r.start_id = gone.start_id
    RETURNING r.start_id
)
DELETE FROM collect.service_start_rung_objects AS o
USING gone
WHERE o.start_id = gone.start_id;";

    /// <summary>The newest starts, newest first, with the current flag. $1 limit, $2 host filter (null for every host), $3 earliest process start (naive UTC, null for no bound).</summary>
    public const string SelectStartsSql = @"
SELECT s.start_id, s.service_host, s.product_version, s.commit_id, s.process_started_at, s.schema_version_before, s.schema_version_after,
       s.store_ready_at, s.previous_stop_at, s.last_flush_at, s.error_count, s.warn_count, s.first_errors, s.first_warns,
       s.errors_10m, s.warns_10m, s.errors_30m, s.warns_30m,
       NOT EXISTS (SELECT 1 FROM collect.service_starts AS n WHERE n.service_host = s.service_host AND n.process_started_at > s.process_started_at) AS is_current
FROM collect.service_starts AS s
WHERE ($2::text IS NULL OR s.service_host = $2::text)
AND   ($3::timestamp IS NULL OR s.process_started_at >= $3::timestamp)
ORDER BY s.process_started_at DESC, s.start_id
LIMIT $1;";

    /// <summary>
    /// The starts since $1 (naive UTC) whose build differs from the same host's previous start, oldest first. The previous start is found among all
    /// starts, so the first start inside the window is compared with the one before the window. A host's very first start has none and is not a change.
    /// </summary>
    public const string SelectBuildChangesSql = @"
SELECT service_host, process_started_at, previous_build, product_version
FROM
(
    SELECT s.service_host, s.process_started_at, s.product_version,
           lag(s.product_version) OVER (PARTITION BY s.service_host ORDER BY s.process_started_at, s.start_id) AS previous_build
    FROM collect.service_starts AS s
) AS x
WHERE x.previous_build IS NOT NULL
AND   x.previous_build <> x.product_version
AND   x.process_started_at >= $1
ORDER BY x.process_started_at, x.service_host;";

    /// <summary>
    /// The rungs of the given starts, in the order they ran, each with how many object rows it has (#9438) and whether its start wrote the fresh-install marker.
    /// The marker is the object row of rung 0 (<see cref="ServiceStartRungObjects.MarkerRung"/>). $1 start ids.
    /// </summary>
    public const string SelectRungsSql = @"
SELECT r.start_id, r.rung_version, r.rung_name, r.attempt, r.started_at, r.elapsed_ms, r.outcome, r.error_text,
       (SELECT count(*) FROM collect.service_start_rung_objects AS o
        WHERE o.start_id = r.start_id AND o.rung_version = r.rung_version AND o.attempt = r.attempt)::integer AS object_count,
       EXISTS (SELECT 1 FROM collect.service_start_rung_objects AS m WHERE m.start_id = r.start_id AND m.rung_version = 0) AS fresh_install
FROM collect.service_start_rungs AS r
WHERE r.start_id = ANY($1)
ORDER BY r.start_id, r.attempt, r.started_at, r.rung_version;";

    /// <summary>
    /// Writes the start row. A store without the tables (a store below V307 that has not migrated yet) throws
    /// <see cref="PostgresException"/> with <see cref="PostgresErrorCodes.UndefinedTable"/>, and the caller keeps what it holds and tries again.
    /// </summary>
    public static async Task UpsertStartAsync(NpgsqlDataSource dataSource, ServiceStartRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(row);

        await using var command = dataSource.CreateCommand(UpsertStartSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = row.StartId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.ServiceHost });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.ProductVersion });
        command.Parameters.Add(Nullable(NpgsqlDbType.Text, row.CommitId));
        command.Parameters.Add(Instant(row.ProcessStartedUtc));
        command.Parameters.Add(Nullable(NpgsqlDbType.Integer, row.SchemaVersionBefore));
        command.Parameters.Add(Nullable(NpgsqlDbType.Integer, row.SchemaVersionAfter));
        command.Parameters.Add(Nullable(NpgsqlDbType.Timestamp, row.StoreReadyUtc is { } ready ? Naive(ready) : null));
        command.Parameters.Add(Instant(row.LastFlushUtc));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = row.ErrorCount });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = row.WarnCount });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = row.FirstErrors.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = row.FirstWarns.ToArray() });
        command.Parameters.Add(Nullable(NpgsqlDbType.Bigint, row.Errors10Minutes));
        command.Parameters.Add(Nullable(NpgsqlDbType.Bigint, row.Warns10Minutes));
        command.Parameters.Add(Nullable(NpgsqlDbType.Bigint, row.Errors30Minutes));
        command.Parameters.Add(Nullable(NpgsqlDbType.Bigint, row.Warns30Minutes));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes rung rows. Call after <see cref="UpsertStartAsync"/>.</summary>
    public static async Task InsertRungsAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<ServiceStartRung> rungs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(rungs);

        foreach (var rung in rungs)
        {
            await using var command = dataSource.CreateCommand(InsertRungSql);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = rung.StartId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rung.Version });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rung.Attempt });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = rung.Name });
            command.Parameters.Add(Instant(rung.StartedUtc));
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = rung.ElapsedMilliseconds });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = rung.Outcome });
            command.Parameters.Add(Nullable(NpgsqlDbType.Text, rung.ErrorText));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Removes the starts older than <see cref="RetentionDays"/> as of <paramref name="utcNow"/>, with their rungs.</summary>
    public static async Task PurgeAsync(NpgsqlDataSource dataSource, DateTime utcNow, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        await using var command = dataSource.CreateCommand(RetentionSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(Instant(utcNow.AddDays(-RetentionDays)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The newest <paramref name="limit"/> starts, newest first, each with its rungs. A store without the tables throws
    /// <see cref="PostgresException"/> with <see cref="PostgresErrorCodes.UndefinedTable"/>, so the caller can say why.
    /// </summary>
    public static Task<IReadOnlyList<ServiceStartRecord>> ReadAsync(
        NpgsqlDataSource dataSource, int limit, string? serviceHost, CancellationToken cancellationToken = default)
        => ReadAsync(dataSource, limit, serviceHost, sinceUtc: null, cancellationToken);

    /// <summary>The starts that began at or after <paramref name="sinceUtc"/> (all of them when null), as <see cref="ReadAsync(NpgsqlDataSource, int, string?, CancellationToken)"/> returns them.</summary>
    public static async Task<IReadOnlyList<ServiceStartRecord>> ReadAsync(
        NpgsqlDataSource dataSource, int limit, string? serviceHost, DateTime? sinceUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var starts = new List<(ServiceStartRow Row, bool Current)>();
        await using (var command = dataSource.CreateCommand(SelectStartsSql))
        {
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit });
            command.Parameters.Add(Nullable(NpgsqlDbType.Text, serviceHost));
            command.Parameters.Add(Nullable(NpgsqlDbType.Timestamp, sinceUtc is { } since ? Naive(DateTime.SpecifyKind(since, DateTimeKind.Utc)) : null));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                starts.Add((new ServiceStartRow(
                    reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    Utc(reader.GetDateTime(4)),
                    reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    reader.IsDBNull(7) ? null : Utc(reader.GetDateTime(7)),
                    reader.IsDBNull(8) ? null : Utc(reader.GetDateTime(8)),
                    Utc(reader.GetDateTime(9)),
                    reader.GetInt64(10), reader.GetInt64(11),
                    reader.GetFieldValue<string[]>(12), reader.GetFieldValue<string[]>(13),
                    reader.IsDBNull(14) ? null : reader.GetInt64(14),
                    reader.IsDBNull(15) ? null : reader.GetInt64(15),
                    reader.IsDBNull(16) ? null : reader.GetInt64(16),
                    reader.IsDBNull(17) ? null : reader.GetInt64(17)),
                    reader.GetBoolean(18)));
            }
        }

        var rungsByStart = new Dictionary<Guid, List<ServiceStartRung>>();
        if (starts.Count > 0)
        {
            await using var command = dataSource.CreateCommand(SelectRungsSql);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid, Value = starts.Select(s => s.Row.StartId).ToArray() });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var rung = new ServiceStartRung(
                    reader.GetGuid(0), reader.GetInt32(1), reader.GetString(2), reader.GetInt32(3), Utc(reader.GetDateTime(4)),
                    reader.GetInt64(5), reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.GetInt32(8), reader.GetBoolean(9));
                if (!rungsByStart.TryGetValue(rung.StartId, out var list))
                {
                    rungsByStart[rung.StartId] = list = new List<ServiceStartRung>();
                }

                list.Add(rung);
            }
        }

        return starts
            .Select(s => new ServiceStartRecord(
                s.Row,
                rungsByStart.TryGetValue(s.Row.StartId, out var rungs) ? rungs : new List<ServiceStartRung>(),
                s.Current))
            .ToList();
    }

    /// <summary>
    /// The points since <paramref name="sinceUtc"/> where a host's build changed, oldest first (<see cref="SelectBuildChangesSql"/>). A store without
    /// the tables throws <see cref="PostgresException"/> with <see cref="PostgresErrorCodes.UndefinedTable"/>, so the caller can say why.
    /// </summary>
    public static async Task<IReadOnlyList<ServiceBuildChange>> ReadBuildChangesAsync(
        NpgsqlDataSource dataSource, DateTime sinceUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var changes = new List<ServiceBuildChange>();
        await using var command = dataSource.CreateCommand(SelectBuildChangesSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        /* Always UTC, whatever the kind: the trend reads hold their window start as an unspecified-kind UTC value, which Naive would read as local time. */
        command.Parameters.Add(Instant(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            changes.Add(new ServiceBuildChange(reader.GetString(0), Utc(reader.GetDateTime(1)), reader.GetString(2), reader.GetString(3)));
        }

        return changes;
    }

    /// <summary>The instant as the store keeps it: naive UTC, cut to whole microseconds so the value written is the value read back.</summary>
    public static DateTime Naive(DateTime instant)
    {
        var utc = instant.Kind == DateTimeKind.Utc ? instant : instant.ToUniversalTime();
        return new DateTime(utc.Ticks - (utc.Ticks % 10), DateTimeKind.Unspecified);
    }

    private static DateTime Utc(DateTime stored) => DateTime.SpecifyKind(stored, DateTimeKind.Utc);

    private static NpgsqlParameter Instant(DateTime instant) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(instant) };

    private static NpgsqlParameter Nullable(NpgsqlDbType type, object? value) =>
        new() { NpgsqlDbType = type, Value = value ?? DBNull.Value };
}
