/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_store_maintenance_readiness</c> (#8787 part D2): is the monitoring store safe to do maintenance on right now. The
/// checks and the verdict are <see cref="DarlingStoreMaintenanceReadinessReader"/>; this wraps them as text for MCP and the web.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreMaintenanceReadinessTools
{
    [McpServerTool(Name = "get_store_maintenance_readiness"), Description(
        "Says whether the monitoring STORE (not a monitored server) is safe for maintenance right now: an install, an upgrade or a table "
        + "rewrite. One verdict line, 'Safe for maintenance now.' or the reasons it is not, plus the detail. Run it before every install. "
        + "<<GUIDE>> No server_name. Reasons: a strong lock (SHARE ROW EXCLUSIVE, EXCLUSIVE or ACCESS EXCLUSIVE) granted to another session on "
        + "a product table, a transaction open longer than 2 minutes, a manual VACUUM or an autovacuum to prevent wraparound running (a plain "
        + "autovacuum is listed but is not a reason, because it stops by itself when a rewrite asks for a lock), an inactive replication slot "
        + "holding at least 1 GB of WAL, a TimescaleDB background job that has crashed (consecutive_crashes above zero), and any check that "
        + "could not run (never counted as clear). The lock, vacuum and job checks cover this database. The slot check covers the whole "
        + "server, because any slot holds WAL. Detail lists: strong_locks (held or waiting, with mode, application_name, backend_type, "
        + "transaction_start), long_transactions, vacuums_running (kind, phase, heap blocks scanned of total), replication_slots "
        + "(retained_bytes) and job_crashes (read: available or not available with the reason; per job next_start, which is null while the "
        + "job is in crash backoff, and crash_backoff, true then because the scheduler sets the next start itself). It never returns another "
        + "session's query text. Transaction age and another role's vacuums are visible only for a role that can read them: "
        + "sessions_hidden_from_role says how many other-role sessions it cannot see, and the safe verdict says so. Read-only and live.")]
    public static async Task<string> GetStoreMaintenanceReadiness(
        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var r = await DarlingStoreMaintenanceReadinessReader.GetAsync(postgres, cancellationToken: cancellationToken);
            return JsonSerializer.Serialize(new
            {
                verdict = r.Verdict,
                safe = r.Safe,
                reasons = r.Reasons,
                checked_at = DateTime.SpecifyKind(r.CheckedAtUtc, DateTimeKind.Unspecified),
                long_transaction_seconds = DarlingStoreMaintenanceReadinessReader.DefaultLongTransactionSeconds,
                can_see_all_sessions = r.CanSeeAllSessions,
                sessions_hidden_from_role = r.HiddenSessionCount,
                strong_locks = r.StrongLocks.Select(l => new
                {
                    table = l.Table,
                    mode = l.Mode,
                    granted = l.Granted,
                    application_name = l.ApplicationName,
                    backend_type = l.BackendType,
                    transaction_start = Iso(l.TransactionStart),
                }),
                long_transactions = r.LongTransactions.Select(t => new
                {
                    application_name = t.ApplicationName,
                    backend_type = t.BackendType,
                    state = t.State,
                    age_seconds = t.AgeSeconds,
                }),
                vacuums_running = r.Vacuums.Select(v => new
                {
                    table = v.Table,
                    kind = v.Kind switch
                    {
                        DarlingStoreMaintenanceReadinessReader.StoreVacuumKind.Manual => "manual",
                        DarlingStoreMaintenanceReadinessReader.StoreVacuumKind.AutovacuumToPreventWraparound => "autovacuum_to_prevent_wraparound",
                        DarlingStoreMaintenanceReadinessReader.StoreVacuumKind.Autovacuum => "autovacuum",
                        _ => "hidden",
                    },
                    phase = v.Phase,
                    heap_blocks_scanned = v.HeapBlocksScanned,
                    heap_blocks_total = v.HeapBlocksTotal,
                }),
                replication_slots = r.Slots.Select(s => new
                {
                    slot_name = s.SlotName,
                    active = s.Active,
                    retained_bytes = s.RetainedBytes,
                }),
                job_crashes = new
                {
                    read = r.JobCrashes.Available ? "available" : r.JobCrashes.UnavailableReason,
                    jobs = r.JobCrashes.Crashing.Select(j => new
                    {
                        job_id = j.JobId,
                        proc_name = j.ProcName,
                        hypertable_name = j.HypertableName,
                        consecutive_crashes = j.ConsecutiveCrashes,
                        total_crashes = j.TotalCrashes,
                        next_start = Iso(j.NextStart),
                        crash_backoff = j.CrashBackoff,
                    }),
                },
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_maintenance_readiness", ex);
        }
    }

    private static string? Iso(DateTime? value) => value?.ToString("O", CultureInfo.InvariantCulture);
}
