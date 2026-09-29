/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The single answer to "which certificate file will Npgsql actually open" (#1970), and the anchor that
/// answer is measured from.
///
/// <para><b>The defect.</b> A bring-your-own <c>postgres.connectionString</c> carries the documented bare
/// <c>Root Certificate=server.crt</c>, and Npgsql hands that value to the file system as given — so a
/// relative one resolved against the viewer PROCESS's working directory. A desktop shortcut's "Start in", or
/// Explorer's last-used directory, therefore decided which file a <c>VerifyFull</c> pin trusted. That makes
/// the working directory the trust anchor: launched with a working directory an attacker can write
/// (Downloads, a shared folder), a planted <c>server.crt</c> becomes the pinned root, and with LAN position
/// that is interception of the store session and the credential inside it. The anchor is now the directory
/// of the darling.json the viewer actually loaded — the certificate the operator exported BESIDE their
/// config is the certificate that gets pinned, wherever the folder is copied and however the viewer is
/// launched.</para>
///
/// <para><b>Why it is one class with two entry points.</b> Two places have to agree about the resolved path:
/// the connection string that reaches Npgsql (<see cref="Anchor"/>, called from
/// <see cref="ViewerSettings"/>), and the startup diagnostics that promise "the absolute path Npgsql will
/// actually open" (#1954, <see cref="ViewerConfigDiagnostics"/>). A diagnostic that computes its own answer
/// is worse than none once the two drift, so <see cref="Anchor"/> is a thin wrapper over
/// <see cref="Resolve"/> and the diagnostics call <see cref="Resolve"/> directly — there is exactly one
/// implementation of the rule. <c>ViewerConfigDiagnosticsTests</c> pins both the routing and the agreement.</para>
///
/// <para>Pure and total: no process state is read and nothing here throws, because both callers run on the
/// viewer's startup path where an exception is a window that never opens.</para>
/// </summary>
public static class ViewerCertificateAnchor
{
    /// <summary>
    /// The absolute path a <c>Root Certificate</c> value names, or null when there is no value or it cannot
    /// be expressed as a path at all. An already-absolute value resolves to itself; a bare name or a
    /// relative subpath resolves against <paramref name="configDirectory"/>.
    /// </summary>
    /// <param name="rootCertificate">The connection string's <c>Root Certificate</c> value, as written.</param>
    /// <param name="configDirectory">
    /// The directory of the darling.json that was loaded (<see cref="ViewerConfigLocation.Directory"/>).
    /// Null means there is no anchor to measure from, so fall back to the process working directory — which
    /// is what Npgsql itself would have done, and is the honest report rather than an invented path.
    /// </param>
    public static string? Resolve(string? rootCertificate, string? configDirectory)
    {
        if (string.IsNullOrWhiteSpace(rootCertificate))
        {
            return null;
        }

        try
        {
            /* GetFullPath throws on a syntactically unusable value (illegal characters, a bare drive-relative
               form we cannot expand). Null rather than a throw: the diagnostics say so in a line, and the
               rewrite leaves the value alone so Npgsql reports the real problem in its own words. */
            return string.IsNullOrWhiteSpace(configDirectory)
                ? Path.GetFullPath(rootCertificate)
                : Path.GetFullPath(rootCertificate, Path.GetFullPath(configDirectory));
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// The connection string as Npgsql should see it: identical except that a relative <c>Root Certificate</c>
    /// is rewritten to the absolute path <see cref="Resolve"/> produces.
    ///
    /// <para>Untouched when there is no <c>Root Certificate</c> at all (this never ADDS the keyword — a
    /// connection that was not pinning is not made to pin), when the value is already fully qualified, when
    /// there is no <paramref name="configDirectory"/> to anchor to, or when the string will not parse. The
    /// rewrite deliberately does not gate on <c>SSL Mode</c>: <c>Root Certificate</c> is only consulted under
    /// <c>VerifyCA</c>/<c>VerifyFull</c>, so anchoring it under <c>Require</c> changes nothing Npgsql reads —
    /// while gating would leave a stale working-directory anchor behind for an operator who later tightens
    /// the mode, which is the one moment the pin has to be right.</para>
    /// </summary>
    public static string Anchor(string connectionString, string? configDirectory)
    {
        if (string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(configDirectory))
        {
            return connectionString;
        }

        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (Exception)
        {
            /* An unparseable string is rejected when the data service is built (ViewerDataService.TryCreate), with
               DarlingStoreConnection.DescribeParseFailure's text: Npgsql's own message can quote part of the string. */
            return connectionString;
        }

        /* Read through the Npgsql builder rather than matching a keyword ourselves: it accepts both
           "Root Certificate" and "rootcertificate" in any casing, and re-emits only the keys the operator
           actually set (no defaults are injected), so the rewritten string differs from theirs in the
           certificate value alone. */
        var configured = builder.RootCertificate;
        if (string.IsNullOrWhiteSpace(configured) || Path.IsPathFullyQualified(configured))
        {
            return connectionString;
        }

        var resolved = Resolve(configured, configDirectory);
        if (resolved is null)
        {
            return connectionString;
        }

        try
        {
            builder.RootCertificate = resolved;
            return builder.ConnectionString;
        }
        catch (Exception)
        {
            /* Same contract as the parse guard above — on any failure the operator's string reaches Npgsql
               unchanged. The setter and the re-serialization are realistically non-throwing for a
               GetFullPath-produced value, but the total-function claim is what both startup-path callers
               lean on, so the code keeps it rather than the comment promising it. */
            return connectionString;
        }
    }
}
