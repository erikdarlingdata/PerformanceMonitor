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
/// the server or its certificate refuses at the transport fails with the driver's own text alone, for example "An existing connection was
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
    /// The hint for a failed connection test, or null when none applies. It applies only when <paramref name="encryptMode"/>
    /// is <c>Strict</c> (any casing, trimmed, as Darling's connection builders read it) and <paramref name="transportFailure"/>
    /// is true. A transport failure is a <c>SqlException</c> none of whose errors the server sent. A server-sent error has a class
    /// below 20 and a positive number, such as a login failure, 4064 or 40613. A reset connection (10054), a certificate chain
    /// failure, a refused port, a timeout and any other client-side error are transport failures. An exception that is not a
    /// <c>SqlException</c>, such as a secret that cannot be read before any connection is made, is not. Each product computes
    /// the fact from the exception, because this project has no SqlClient reference. The caller does not ask for a hint on a
    /// cancellation.
    /// </summary>
    public static string? For(string? encryptMode, bool transportFailure) =>
        transportFailure && IsStrict(encryptMode) ? Text : null;

    /// <summary>
    /// True when <paramref name="encryptMode"/> is <c>Strict</c>, trimmed and in any casing. Darling folds a stored mode that way
    /// (hand-edited darling.json can say "strict"), so Strict here means what Darling connects with. Lite connects with Strict only
    /// for the exact word, so its callers pass the mode through <c>ServerEntryRules.EncryptOptionFor</c> first.
    /// </summary>
    public static bool IsStrict(string? encryptMode) =>
        string.Equals(encryptMode?.Trim(), "Strict", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// <paramref name="failureText"/> with the hint on the line after it, or <paramref name="failureText"/> unchanged when
    /// <see cref="For"/> gives none.
    /// </summary>
    public static string Append(string failureText, string? encryptMode, bool transportFailure)
    {
        var hint = For(encryptMode, transportFailure);
        return hint is null ? failureText : failureText + Environment.NewLine + hint;
    }
}
