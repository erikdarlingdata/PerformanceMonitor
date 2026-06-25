/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Read-only classifier for an applied-but-unlogged audit failure (LOW-2). A
    /// force that succeeds but whose audit row will not write looks identical
    /// whether the cause is a one-off (deadlock / timeout) or a permanent setup
    /// problem (the login simply lacks INSERT on a present
    /// <c>config.remediation_action_log</c>). The permanent case would silently
    /// drop EVERY audit row on that server, so it must be surfaced loudly rather
    /// than buried as a per-target warning.
    ///
    /// <para>
    /// This is a PR-B-owned, purely read-only probe on the MONITORING connection
    /// (PerformanceMonitor DB) — it issues no mutation and is invoked only AFTER an
    /// applied-but-unlogged outcome, never on the apply path itself. It does not
    /// touch the reviewed PR-A execution/audit core.
    /// </para>
    /// </summary>
    public static class AuditWritabilityProbe
    {
        /// <summary>
        /// Classifies why a prior audit INSERT failed against a present table.
        /// Permanent when the login has no INSERT permission on
        /// <c>config.remediation_action_log</c>; Transient when it does (so the
        /// earlier failure was a one-off); Unknown if the probe itself fails.
        /// </summary>
        public static async Task<AuditWriteFailureKind> ClassifyAsync(string monitoringConnectionString, CancellationToken ct)
        {
            try
            {
                using var connection = new SqlConnection(monitoringConnectionString);
                await connection.OpenAsync(ct).ConfigureAwait(false);

                using var command = connection.CreateCommand();
                // OBJECT-scoped INSERT permission on the audit table. NULL (table
                // unexpectedly gone) is treated as not-insertable -> permanent.
                command.CommandText =
                    "SELECT can_insert = ISNULL(HAS_PERMS_BY_NAME(N'config.remediation_action_log', N'OBJECT', N'INSERT'), 0);";

                var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
                var canInsert = result is not null && result is not System.DBNull && System.Convert.ToInt32(result) == 1;
                return canInsert ? AuditWriteFailureKind.Transient : AuditWriteFailureKind.Permanent;
            }
            catch
            {
                // The classification probe is best-effort; a probe failure must not
                // escalate or mask the (successful, reversible) mutation.
                return AuditWriteFailureKind.Unknown;
            }
        }
    }
}
