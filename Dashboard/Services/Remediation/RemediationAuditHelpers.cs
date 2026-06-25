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

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Small shared helpers for the per-fact-type handlers (§5.1) so the audit-result
    /// taxonomy mapping and the never-throw audit-write wrapper are written once, not
    /// copy-pasted between ForcePlanHandler and DbConfigHandler (the two-bodies trap).
    /// The security-reviewed GATE logic lives in the executor, NOT here — these helpers
    /// only classify a status and shield a per-target error path from a logging throw.
    /// </summary>
    internal static class RemediationAuditHelpers
    {
        /// <summary>Maps a per-target status onto the audit <c>result</c> taxonomy.</summary>
        public static string AuditResult(RemediationStatus status) => status switch
        {
            RemediationStatus.Success => "success",
            RemediationStatus.Skipped => "skipped",
            RemediationStatus.PermissionDenied => "skipped",
            RemediationStatus.Blocked => "aborted",
            RemediationStatus.Error => "error",
            _ => "error"
        };

        /// <summary>
        /// True when the status's message should be persisted to the audit
        /// <c>error_message</c> column (the failure/blocked statuses).
        /// </summary>
        public static bool IsErrorish(RemediationStatus status) =>
            status is RemediationStatus.Error or RemediationStatus.PermissionDenied or RemediationStatus.Blocked;

        /// <summary>
        /// Writes the audit record, swallowing any throw so a logging failure while
        /// handling a per-target error never masks the original outcome. Returns false
        /// on any failure (caller surfaces applied-but-unlogged where relevant).
        /// </summary>
        public static async Task<bool> TryWriteAuditSafeAsync(IRemediationExecutor exec, RemediationAuditRecord record, CancellationToken ct)
        {
            try
            {
                return await exec.WriteAuditAsync(record, ct).ConfigureAwait(false);
            }
            catch
            {
                return false;
            }
        }
    }
}
