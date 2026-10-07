/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Every setting that decides where a monitored server is reached and how it is trusted (#5366): the fields a saved
/// password is bound to, and the set an edit or a connection test compares before a stored password may be reused.
/// It is built from the stored row by one mapping, <see cref="FromStoredColumns"/>, and compared by one rule,
/// <see cref="Differ"/>.
/// </summary>
public readonly record struct ServerConnectionIdentity(
    string Host, int Port, string Engine, string? Database, bool ReadOnlyIntent, string Auth, string? Username,
    string EncryptMode, bool TrustServerCertificate, bool MultiSubnetFailover)
{
    /// <summary>The columns every reader selects, in this order, to build an identity from a stored row.</summary>
    public const string StoredColumns = "host, port, engine, database, read_only_intent, auth, username, " +
        "encrypt_mode, trust_server_certificate, multi_subnet_failover";

    /// <summary>
    /// The one mapping from a raw stored row: null text becomes the empty string and a null port becomes 0. The text is
    /// not trimmed and its case is not changed, so the identity holds what the row holds.
    /// </summary>
    public static ServerConnectionIdentity FromStoredColumns(
        string? host, int? port, string? engine, string? database, bool readOnlyIntent, string? auth,
        string? username, string? encryptMode, bool trust, bool multiSubnet) =>
        new(host ?? "", port ?? 0, engine ?? "", database ?? "", readOnlyIntent, auth ?? "", username ?? "",
            encryptMode ?? "", trust, multiSubnet);

    /// <summary>
    /// True when <paramref name="b"/> connects differently from <paramref name="a"/>. Host, database and username
    /// compare ordinally, the port compares as a number, authentication, encrypt mode and engine ignore case, and the
    /// flags compare as stored.
    /// </summary>
    public static bool Differ(ServerConnectionIdentity a, ServerConnectionIdentity b) =>
        !string.Equals(a.Host, b.Host, StringComparison.Ordinal)
        || a.Port != b.Port
        || !string.Equals(a.Engine, b.Engine, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Database, b.Database, StringComparison.Ordinal)
        || a.ReadOnlyIntent != b.ReadOnlyIntent
        || !string.Equals(a.Auth, b.Auth, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Username, b.Username, StringComparison.Ordinal)
        || !string.Equals(a.EncryptMode, b.EncryptMode, StringComparison.OrdinalIgnoreCase)
        || a.TrustServerCertificate != b.TrustServerCertificate
        || a.MultiSubnetFailover != b.MultiSubnetFailover;
}
