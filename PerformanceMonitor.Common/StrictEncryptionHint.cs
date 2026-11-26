/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The line a failed connection test adds when the entry used <c>encrypt_mode</c> Strict (#9674). A Strict connection that
/// the server or its certificate refuses fails with the driver's own text alone, for example "An existing connection was
/// forcibly closed by the remote host.", and nothing in it says Strict was in use or that Strict ignores
/// <c>trust_server_certificate</c>. Lite's connection tests (<c>--add-server</c>, the MCP server tools, the Add Server
/// window) and Darling's <c>add_servers</c> and <c>edit_server</c> all call this, so the wording lives here once.
/// Plain strings and numbers only, so this project needs no SqlClient reference.
/// </summary>
public static class StrictEncryptionHint
{
    /// <summary>The line itself, the same for every caller.</summary>
    public const string Text =
        "The connection used encrypt_mode Strict. Strict needs a server that supports TDS 8.0 and a certificate that "
        + "this machine trusts, and it ignores trust_server_certificate. For a self-signed certificate, trust it on this "
        + "machine, or use Mandatory with trust_server_certificate.";

    /// <summary>
    /// SQL Server error numbers the server sends after the connection is made over TLS, so Strict is not the cause:
    /// 18456 (login failed), 18452 (untrusted domain), 18470 (account disabled), 18486 (locked out),
    /// 18487 (password expired), 18488 (password must change), 4060 (cannot open the database the login asked for),
    /// 40532 (cannot open the server the login asked for, Azure), 40615 (the Azure SQL Database firewall refused
    /// the client address).
    /// </summary>
    private static readonly int[] ServerRefusalNumbers = [18456, 18452, 18470, 18486, 18487, 18488];

    /// <summary>
    /// The hint for a failed connection test, or null when none applies. It applies only when <paramref name="encryptMode"/>
    /// is <c>Strict</c> (any casing, trimmed, as Darling's connection builders read it) and the failure is not a
    /// server refusal (<c>ServerRefusalNumbers</c>). <paramref name="sqlErrorNumber"/> is the number of the <c>SqlException</c> the driver threw, or null
    /// when it threw something else. The caller does not ask for a hint on a cancellation.
    /// </summary>
    public static string? For(string? encryptMode, int? sqlErrorNumber)
    {
        /* Darling folds a stored mode with Trim and a case-insensitive match (hand-edited darling.json can say "strict"), so Strict
           here means what Darling connects with. Lite connects with Strict only for the exact word, and its caller passes the
           mode through <c>ServerEntryRules.EncryptOptionFor</c> first. */
        if (!string.Equals(encryptMode?.Trim(), "Strict", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (sqlErrorNumber is int number && Array.IndexOf(ServerRefusalNumbers, number) >= 0)
        {
            return null;
        }

        return Text;
    }

    /// <summary>
    /// <paramref name="failureText"/> with the hint on the line after it, or <paramref name="failureText"/> unchanged when
    /// <see cref="For"/> gives none.
    /// </summary>
    public static string Append(string failureText, string? encryptMode, int? sqlErrorNumber)
    {
        var hint = For(encryptMode, sqlErrorNumber);
        return hint is null ? failureText : failureText + Environment.NewLine + hint;
    }
}
