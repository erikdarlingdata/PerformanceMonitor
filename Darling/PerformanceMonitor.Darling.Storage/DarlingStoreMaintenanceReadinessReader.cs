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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Is the monitoring store safe to do maintenance on right now (#8787 part D2)? The check an operator runs before every
/// install and before the #8570 table rewrite: a rewrite or an upgrade needs a strong lock on a product table, and it waits
/// (or takes the collectors down with it) behind whatever already holds one. This reads what would get in the way and says so
/// in one verdict sentence plus the detail the Viewer and the web page show as tables. It never writes, and it never returns
/// another session's query text, only application name, backend type, state and ages.
///
/// <para><b>What counts as a reason not to start</b> (<see cref="StoreMaintenanceReadiness.Reasons"/>, each a sentence):</para>
/// <list type="bullet">
/// <item><b>Strong locks held by another session</b> on a product table (<c>ShareRowExclusiveLock</c>, <c>ExclusiveLock</c>
/// or <c>AccessExclusiveLock</c> on a relation in the <c>collect</c> or <c>config</c> schemas or a TimescaleDB chunk
/// schema, in this database). A session WAITING for one is listed in the detail but is not by itself a reason: only a lock that is
/// granted blocks. The lock read works for every role.</item>
/// <item><b>Transactions open longer than <see cref="DefaultLongTransactionSeconds"/> seconds</b> in this database. They hold back
/// vacuum and make a table rewrite wait behind them. Autovacuum workers are left out here (they are the next item).</item>
/// <item><b>A vacuum running</b> in this database (<c>pg_stat_progress_vacuum</c>), listed with its phase and progress.</item>
/// <item><b>An inactive replication slot retaining at least <see cref="SlotRetainedReasonBytes"/> of WAL</b>: maintenance writes a lot
/// of WAL, and an abandoned slot keeps all of it. An active slot, or a small one, is listed and is not a reason.</item>
/// <item><b>A TimescaleDB background job crashing</b> (<see cref="DarlingStoreJobCrashReader"/>, <c>consecutive_crashes</c> above
/// zero): a crashed job backs off, and maintenance on top of it hides whether the cause was the maintenance.</item>
/// <item><b>A check that could not run</b> (a refused read), named, never counted as clear.</item>
/// </list>
///
/// <para><b>What a role cannot see.</b> PostgreSQL shows another role's transaction start and state only to a superuser or a member of
/// <c>pg_read_all_stats</c>. The <c>mcp</c> and <c>viewer</c> roles are neither, so for them every other role's session reads NULL and
/// the transaction-age check cannot see it. The result says so (<see cref="StoreMaintenanceReadiness.CanSeeAllSessions"/> false and
/// <see cref="StoreMaintenanceReadiness.HiddenSessionCount"/> sessions) and the safe verdict carries a sentence naming that gap. The
/// lock, vacuum, slot and job checks are not limited by it.</para>
///
/// <para>The job crash read can be missing on a TimescaleDB version or a role without it. That is not a reason: the verdict names the
/// check as not run.</para>
/// </summary>
public static class DarlingStoreMaintenanceReadinessReader
{
    /// <summary>A transaction open longer than this many seconds (two minutes) is a reason not to start.</summary>
    public const int DefaultLongTransactionSeconds = 120;

    /// <summary>An INACTIVE replication slot retaining at least this many bytes of WAL (1 GiB) is a reason not to start.</summary>
    public const long SlotRetainedReasonBytes = 1024L * 1024 * 1024;

    /// <summary>The strong lock modes: the ones a rewrite or an upgrade's DDL conflicts with.</summary>
    public static readonly IReadOnlyList<string> StrongLockModes = new[] { "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock" };

    /// <summary>Product tables for the lock read: the product schemas and TimescaleDB's chunk and catalog schemas. This database only.</summary>
    private const string StrongLocksSql = @"
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), l.mode, l.granted, a.application_name, a.backend_type, a.xact_start
FROM pg_catalog.pg_locks AS l
JOIN pg_catalog.pg_stat_activity AS a ON a.pid = l.pid
JOIN pg_catalog.pg_class AS c ON c.oid = l.relation
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE l.relation IS NOT NULL
AND   l.database = (SELECT d.oid FROM pg_catalog.pg_database AS d WHERE d.datname = current_database())
AND   l.mode = ANY ($1)
AND   l.pid <> pg_backend_pid()
AND   (n.nspname = ANY ($2) OR starts_with(n.nspname, '_timescaledb_'))
ORDER BY l.granted DESC, a.xact_start NULLS LAST, 1, l.mode
LIMIT 200";

    private const string LongTransactionsSql = @"
SELECT a.application_name, a.backend_type, a.state, extract(epoch FROM now() - a.xact_start)::bigint
FROM pg_catalog.pg_stat_activity AS a
WHERE a.xact_start < now() - make_interval(secs => $1)
AND   a.datname = current_database()
AND   a.pid <> pg_backend_pid()
AND   a.backend_type IS DISTINCT FROM 'autovacuum worker'
ORDER BY a.xact_start
LIMIT 200";

    private const string VacuumsSql = @"
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname), p.phase, p.heap_blks_scanned, p.heap_blks_total
FROM pg_catalog.pg_stat_progress_vacuum AS p
JOIN pg_catalog.pg_class AS c ON c.oid = p.relid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE p.datid = (SELECT d.oid FROM pg_catalog.pg_database AS d WHERE d.datname = current_database())
ORDER BY 1";

    private const string SlotsSql = @"
SELECT s.slot_name, s.active,
       CASE WHEN s.restart_lsn IS NULL OR pg_catalog.pg_is_in_recovery() THEN NULL
            ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), s.restart_lsn)::bigint END
FROM pg_catalog.pg_replication_slots AS s
ORDER BY 3 DESC NULLS LAST, 1";

    /// <summary>Whether the role sees every session's fields, and how many other roles' sessions are in this database.</summary>
    private const string VisibilitySql = @"
SELECT pg_catalog.pg_has_role(current_user, 'pg_read_all_stats', 'member'),
       (SELECT count(*) FROM pg_catalog.pg_stat_activity AS a
        WHERE a.datname = current_database() AND a.usename IS DISTINCT FROM current_user AND a.pid <> pg_backend_pid())::int";

    /// <summary>A strong lock on a product table held, or waited for, by another session.</summary>
    /// <param name="Table">The schema-qualified, quoted table name.</param>
    /// <param name="Mode">The lock mode, for example <c>AccessExclusiveLock</c>.</param>
    /// <param name="Granted">True when held, false when the session is waiting for it.</param>
    /// <param name="ApplicationName">The holder's application name, null when the role cannot see it.</param>
    /// <param name="BackendType">The holder's backend type, null when the role cannot see it.</param>
    /// <param name="TransactionStart">When the holder's transaction began (UTC), null when the role cannot see it.</param>
    public sealed record StoreStrongLock(string Table, string Mode, bool Granted, string? ApplicationName, string? BackendType, DateTime? TransactionStart);

    /// <summary>A transaction open past the threshold.</summary>
    /// <param name="ApplicationName">The session's application name.</param>
    /// <param name="BackendType">The session's backend type.</param>
    /// <param name="State">The session's state (<c>active</c>, <c>idle in transaction</c>).</param>
    /// <param name="AgeSeconds">How long the transaction has been open.</param>
    public sealed record StoreLongTransaction(string? ApplicationName, string? BackendType, string? State, long AgeSeconds);

    /// <summary>A vacuum in progress.</summary>
    /// <param name="Table">The schema-qualified, quoted table name.</param>
    /// <param name="Phase">The vacuum's phase, for example <c>scanning heap</c>.</param>
    /// <param name="HeapBlocksScanned">Heap blocks scanned so far.</param>
    /// <param name="HeapBlocksTotal">Heap blocks to scan.</param>
    public sealed record StoreRunningVacuum(string Table, string Phase, long HeapBlocksScanned, long HeapBlocksTotal);

    /// <summary>A replication slot.</summary>
    /// <param name="SlotName">The slot's name.</param>
    /// <param name="Active">True when a consumer is connected to it.</param>
    /// <param name="RetainedBytes">WAL bytes between the slot's restart point and the current WAL position, null when the slot holds no restart point or the server is a standby.</param>
    public sealed record StoreReplicationSlot(string SlotName, bool Active, long? RetainedBytes);

    /// <summary>
    /// The answer. <paramref name="Safe"/> is true when no reason was found. <paramref name="Verdict"/> is the one sentence to show first.
    /// The four lists are the detail behind it, whether or not each produced a reason.
    /// </summary>
    /// <param name="Safe">True when <paramref name="Reasons"/> is empty.</param>
    /// <param name="Verdict">One sentence: "Safe for maintenance now." (with a caveat sentence when a check was limited) or "Not safe for maintenance now: " and the reasons.</param>
    /// <param name="Reasons">One sentence per reason found. Empty when safe.</param>
    /// <param name="StrongLocks">Strong locks on product tables held or awaited by other sessions.</param>
    /// <param name="LongTransactions">Transactions open past the threshold, oldest first.</param>
    /// <param name="Vacuums">Vacuums running in this database.</param>
    /// <param name="Slots">Every replication slot, most WAL retained first.</param>
    /// <param name="JobCrashes">The TimescaleDB job crash read, which says when it is not available.</param>
    /// <param name="CanSeeAllSessions">True for a superuser or a member of <c>pg_read_all_stats</c>.</param>
    /// <param name="HiddenSessionCount">Sessions of other roles in this database whose transaction age this role cannot read, 0 when <paramref name="CanSeeAllSessions"/>.</param>
    /// <param name="CheckedAtUtc">When the check ran.</param>
    public sealed record StoreMaintenanceReadiness(
        bool Safe,
        string Verdict,
        IReadOnlyList<string> Reasons,
        IReadOnlyList<StoreStrongLock> StrongLocks,
        IReadOnlyList<StoreLongTransaction> LongTransactions,
        IReadOnlyList<StoreRunningVacuum> Vacuums,
        IReadOnlyList<StoreReplicationSlot> Slots,
        DarlingStoreJobCrashReader.StoreJobCrashRead JobCrashes,
        bool CanSeeAllSessions,
        int HiddenSessionCount,
        DateTime CheckedAtUtc);

    /// <summary>
    /// Runs the checks. A refused or failed read of one check becomes a reason (the check is never counted as clear) and the others
    /// still run. <paramref name="longTransactionSeconds"/> is the transaction-age threshold, <see cref="DefaultLongTransactionSeconds"/>
    /// unless a caller or test passes another.
    /// </summary>
    public static async Task<StoreMaintenanceReadiness> GetAsync(
        NpgsqlDataSource postgres, int longTransactionSeconds = DefaultLongTransactionSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var reasons = new List<string>();

        var locks = await CheckAsync(reasons, "strong locks", () => ReadLocksAsync(postgres, cancellationToken)) ?? new List<StoreStrongLock>();
        var transactions = await CheckAsync(reasons, "open transactions", () => ReadLongTransactionsAsync(postgres, longTransactionSeconds, cancellationToken))
            ?? new List<StoreLongTransaction>();
        var vacuums = await CheckAsync(reasons, "running vacuums", () => ReadVacuumsAsync(postgres, cancellationToken)) ?? new List<StoreRunningVacuum>();
        var slots = await CheckAsync(reasons, "replication slots", () => ReadSlotsAsync(postgres, cancellationToken)) ?? new List<StoreReplicationSlot>();
        var (canSee, hidden) = await CheckAsync(reasons, "session visibility", () => ReadVisibilityAsync(postgres, cancellationToken)) ?? (true, 0);
        var crashes = await DarlingStoreJobCrashReader.ReadAsync(postgres, cancellationToken);

        foreach (var held in locks.Where(l => l.Granted))
        {
            reasons.Add($"{held.Table} is locked in {Mode(held.Mode)} mode by another session{Holder(held.ApplicationName, held.BackendType)}.");
        }

        foreach (var transaction in transactions)
        {
            reasons.Add(string.Create(CultureInfo.InvariantCulture,
                $"A transaction has been open for {Age(transaction.AgeSeconds)}{Holder(transaction.ApplicationName, transaction.BackendType)}{State(transaction.State)}."));
        }

        foreach (var vacuum in vacuums)
        {
            reasons.Add(string.Create(CultureInfo.InvariantCulture,
                $"A vacuum is running on {vacuum.Table} ({vacuum.Phase}, {vacuum.HeapBlocksScanned} of {vacuum.HeapBlocksTotal} heap blocks)."));
        }

        foreach (var slot in slots.Where(s => !s.Active && s.RetainedBytes >= SlotRetainedReasonBytes))
        {
            reasons.Add(string.Create(CultureInfo.InvariantCulture,
                $"The inactive replication slot {slot.SlotName} is holding back {slot.RetainedBytes} bytes of WAL, and maintenance writes more."));
        }

        foreach (var job in crashes.Crashing)
        {
            reasons.Add(string.Create(CultureInfo.InvariantCulture,
                $"The background job {job.ProcName}{(job.HypertableName is null ? string.Empty : " on " + job.HypertableName)} (job {job.JobId}) has crashed {CountText.Plural(job.ConsecutiveCrashes, "time")} in a row."));
        }

        var safe = reasons.Count == 0;
        return new StoreMaintenanceReadiness(
            safe,
            Verdict(safe, reasons, canSee, hidden, crashes),
            reasons,
            locks,
            transactions,
            vacuums,
            slots,
            crashes,
            canSee,
            hidden,
            DateTime.UtcNow);
    }

    /// <summary>The verdict sentence. Public so the wording is testable without a store.</summary>
    public static string Verdict(
        bool safe, IReadOnlyList<string> reasons, bool canSeeAllSessions, int hiddenSessionCount, DarlingStoreJobCrashReader.StoreJobCrashRead crashes)
    {
        if (!safe)
        {
            return "Not safe for maintenance now: " + string.Join(" ", reasons.Select(r => r.TrimEnd('.') + ".")).TrimEnd();
        }

        var verdict = "Safe for maintenance now.";
        if (!canSeeAllSessions && hiddenSessionCount > 0)
        {
            verdict += string.Create(CultureInfo.InvariantCulture,
                $" Transaction age was checked only for sessions this role can see: {CountText.Plural(hiddenSessionCount, "session")} of other roles {(hiddenSessionCount == 1 ? "is" : "are")} hidden from it, so how long their transactions have been open is unknown. Locks, vacuums, slots and job crashes were checked in full.");
        }

        if (!crashes.Available)
        {
            verdict += " Background job crashes were not checked (" + crashes.UnavailableReason + ").";
        }

        return verdict;
    }

    private static string Mode(string mode) => mode switch
    {
        "ShareRowExclusiveLock" => "SHARE ROW EXCLUSIVE",
        "ExclusiveLock" => "EXCLUSIVE",
        "AccessExclusiveLock" => "ACCESS EXCLUSIVE",
        _ => mode,
    };

    private static string Holder(string? application, string? backend)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(application))
        {
            parts.Add($"application '{Clip(application)}'");
        }

        if (!string.IsNullOrWhiteSpace(backend))
        {
            parts.Add(backend);
        }

        return parts.Count == 0 ? string.Empty : " (" + string.Join(", ", parts) + ")";
    }

    private static string State(string? state) => string.IsNullOrWhiteSpace(state) ? string.Empty : $", {state}";

    private static string Clip(string text) => text.Length <= 60 ? text : text[..60] + "...";

    private static string Age(long seconds) => seconds < 120
        ? CountText.Plural(seconds, "second")
        : seconds < 7200
            ? string.Create(CultureInfo.InvariantCulture, $"{seconds / 60} minutes")
            : string.Create(CultureInfo.InvariantCulture, $"{seconds / 3600.0:0.#} hours");

    private static async Task<T?> CheckAsync<T>(List<string> reasons, string what, Func<Task<T>> read) where T : class
    {
        try
        {
            return await read();
        }
        catch (PostgresException ex)
        {
            reasons.Add($"Could not check {what} (SQLSTATE {ex.SqlState}), so it is not counted as clear.");
            return null;
        }
    }

    private static async Task<(bool, int)?> CheckAsync(List<string> reasons, string what, Func<Task<(bool, int)>> read)
    {
        try
        {
            return await read();
        }
        catch (PostgresException ex)
        {
            reasons.Add($"Could not check {what} (SQLSTATE {ex.SqlState}), so it is not counted as clear.");
            return null;
        }
    }

    private static async Task<List<StoreStrongLock>> ReadLocksAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(StrongLocksSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = StrongLockModes.ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = DarlingStoreTableMeasureReader.AllowedSchemas.ToArray() });
        var rows = new List<StoreStrongLock>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreStrongLock(
                reader.GetString(0), reader.GetString(1), reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc)));
        }

        return rows;
    }

    private static async Task<List<StoreLongTransaction>> ReadLongTransactionsAsync(NpgsqlDataSource postgres, int seconds, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(LongTransactionsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double, Value = (double)seconds });
        var rows = new List<StoreLongTransaction>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreLongTransaction(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3)));
        }

        return rows;
    }

    private static async Task<List<StoreRunningVacuum>> ReadVacuumsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(VacuumsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        var rows = new List<StoreRunningVacuum>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreRunningVacuum(reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3)));
        }

        return rows;
    }

    private static async Task<List<StoreReplicationSlot>> ReadSlotsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(SlotsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        var rows = new List<StoreReplicationSlot>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new StoreReplicationSlot(reader.GetString(0), reader.GetBoolean(1), reader.IsDBNull(2) ? null : reader.GetInt64(2)));
        }

        return rows;
    }

    private static async Task<(bool, int)> ReadVisibilityAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(VisibilitySql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var canSee = reader.GetBoolean(0);
        return (canSee, canSee ? 0 : reader.GetInt32(1));
    }
}
