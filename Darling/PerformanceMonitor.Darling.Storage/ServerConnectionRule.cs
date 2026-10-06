/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Every setting that decides where a server is reached and how it is trusted: what an edit, a
/// <c>test_connect</c>, the service's darling.json matching and the viewer compare against the stored row before a
/// stored password may be used.</summary>
public readonly record struct ServerConnectionSettings(
    string Host, int Port, string Engine, string? Database, bool ReadOnlyIntent, string Auth, string? Username,
    string EncryptMode, bool TrustServerCertificate, bool MultiSubnetFailover)
{
    /// <summary>The settings of a definition or store row, with the defaults a NULL column means: SQL Server, integrated
    /// authentication, mandatory encryption, port 0, an empty host.</summary>
    public static ServerConnectionSettings WithDefaults(
        string? host, int? port, string? engine, string? database, bool? readOnlyIntent, string? auth, string? username,
        string? encryptMode, bool? trustServerCertificate, bool? multiSubnetFailover) =>
        new(host ?? "", port ?? 0, engine ?? "sqlserver", database, readOnlyIntent ?? false, auth ?? "integrated", username,
            encryptMode ?? "Mandatory", trustServerCertificate ?? false, multiSubnetFailover ?? false);
}

/// <summary>The one rule for "this definition connects differently from that one", shared by the edit core,
/// <c>test_connect</c>, the service's darling.json matching and the viewer.</summary>
public static class ServerConnectionRule
{
    /// <summary>The sentence a refused change of how a server is reached gives when the row keeps its stored password. The
    /// web and MCP edit, the viewer and the store's own trigger and edit function (<c>provision-roles.sql</c>,
    /// <c>DarlingManagedRoles</c>, which keep the text literal) all answer with exactly this text.</summary>
    public const string PasswordNeededOnMoveText =
        "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";

    /// <summary>The sentence a refused password gives when it is a reference (env: or file:). The same text in the store's
    /// trigger and edit function.</summary>
    public const string ReferenceRefusedText =
        "Enter the password itself. References (env: or file:) can only be set in the configuration file.";

    /// <summary>The sentence a refused change of how a server is reached gives when the row holds a remediation login. The
    /// same text in the store's trigger and edit function.</summary>
    public const string RemediationKeptText =
        "This server has a remediation login stored. Change how it is reached on the service host, in the configuration file or with --add-server.";

    /// <summary>
    /// "The server is at the same address": the host text is equal ordinally (a request's host is trimmed when it is
    /// read; the text is compared exactly as stored, and case counts) and the port is equal.
    /// </summary>
    public static bool SameAddress(string host, int port, string otherHost, int otherPort) =>
        string.Equals(host, otherHost, StringComparison.Ordinal) && port == otherPort;

    /// <summary>
    /// Host, username and database compare ordinally (<see cref="SameAddress"/> for host and port); auth, encrypt mode and
    /// engine ignore case; the rest compare as stored. The store function in <c>provision-roles.sql</c>
    /// (<c>password_needed</c>) names the same set.
    /// </summary>
    public static bool ConnectionSettingsDiffer(ServerConnectionSettings a, ServerConnectionSettings b) =>
        !SameAddress(a.Host, a.Port, b.Host, b.Port)
        || !string.Equals(a.Engine, b.Engine, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Database, b.Database, StringComparison.Ordinal)
        || a.ReadOnlyIntent != b.ReadOnlyIntent
        || !string.Equals(a.Auth, b.Auth, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Username, b.Username, StringComparison.Ordinal)
        || !string.Equals(a.EncryptMode, b.EncryptMode, StringComparison.OrdinalIgnoreCase)
        || a.TrustServerCertificate != b.TrustServerCertificate
        || a.MultiSubnetFailover != b.MultiSubnetFailover;
}
