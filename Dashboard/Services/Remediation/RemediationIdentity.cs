/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// The executing context an audit row records — NOT a credential carrier.
    /// v1 has no in-app elevation: the force-plan runs under the existing
    /// per-server monitoring connection, so this type holds no secret, no
    /// password, and no <c>SqlCredential</c>. <see cref="OperatorIdentity"/> is
    /// the OS / Windows user running the Dashboard (recorded as
    /// <c>operator_identity</c>); the actual <c>executing_login</c> is re-probed
    /// server-side via <c>SUSER_SNAME()</c> on the same connection that issues the
    /// mutation. <see cref="SourceAlertRef"/> is an optional traceback handle
    /// (the alert's metric name / story hash) for the audit row.
    ///
    /// <para>
    /// Kept as a parameter on the handler/executor surface so the signatures stay
    /// stable if a Phase-2 elevation path is ever added — but in v1 it carries
    /// nothing privileged.
    /// </para>
    /// </summary>
    public sealed record RemediationIdentity(string OperatorIdentity, string? SourceAlertRef = null);
}
