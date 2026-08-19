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
/// Deterministic int server-id derivation used by the analysis pipeline.
///
/// <para>
/// <c>string.GetHashCode()</c> is randomized per process on .NET Core / .NET 10,
/// so persisted rows in <c>config.analysis_findings</c> / <c>config.analysis_muted</c>
/// (Dashboard) and their DuckDB equivalents (Lite) would not match the next launch's
/// value for the same server name. This shared helper produces a stable FNV-1a hash so
/// writes survive restart and are consistent across Dashboard, Lite, the MCP entry
/// points, and any scheduled-analysis path. Both apps MUST use this one implementation
/// so they derive the same id for the same server name.
/// </para>
/// </summary>
public static class ServerIdHelper
{
    /// <summary>
    /// Process-independent FNV-1a hash of a string.
    /// </summary>
    public static int GetDeterministicHashCode(string value)
    {
        unchecked
        {
            var hash = (int)2166136261;
            foreach (var c in value)
            {
                hash = (hash ^ c) * 16777619;
            }
            return hash;
        }
    }

    /// <summary>
    /// The canonical storage name a monitored server is identified by (hashed via
    /// <see cref="GetDeterministicHashCode"/> to derive server_id). Appends the database name
    /// for Azure SQL Database connections so different databases on one logical server get
    /// distinct server_ids, and ":RO" for ReadOnlyIntent connections so they differ from
    /// read-write connections to the same host. Extracted verbatim from Lite's
    /// RemoteCollectorService.GetServerNameForStorage; every SKU MUST build storage names
    /// through this one implementation so the same server derives the same id everywhere.
    ///
    /// <para><b>#2218 — engine and port, and why they are OPTIONAL rather than required.</b> The name carried
    /// neither, so a SQL Server and a PostgreSQL instance on one host collided into a single server_id and
    /// interleaved their histories, as did two instances distinguished only by port. Both are now discriminators
    /// — but only when they are actually present, and that is a correctness requirement, not tidiness.</para>
    ///
    /// <para>Lite derives server_id FRESH at runtime, everywhere, from this function, and has no stored-id
    /// concept to fall back on: <c>RemoteCollectorService.GetServerNameForStorage</c> hashes it on every read.
    /// So any change to what this returns for an EXISTING server re-keys that server in Lite and orphans all of
    /// its collected history, silently. Making the new parameters optional — and appending nothing at their
    /// defaults — is what keeps Lite's three-argument call byte-identical to what it produced before. The same
    /// protection covers Darling's SQL Server targets, which pass no port.</para>
    ///
    /// <para>Darling's already-registered PostgreSQL and explicit-port servers do not re-key either, for a
    /// different reason: their id comes from the store (<c>StoredServerId</c>), which is authoritative and is
    /// only ever DERIVED for an entry that has no row yet. So the new discriminators change what a FRESH
    /// registration derives, never what an existing one is called — which is the property that made #2158
    /// (identity assigned, not re-derived) a prerequisite for this rather than a sibling of it.</para>
    ///
    /// <para>Engine is folded to a short token rather than interpolated raw so an operator writing
    /// <c>"PostgreSQL"</c>, <c>"postgres"</c> or <c>"Postgres"</c> gets ONE identity instead of three. Only
    /// non-SQL-Server engines append anything, since SQL Server is the historical default and appending for it
    /// would re-key every server in both SKUs.</para>
    /// </summary>
    public static string BuildStorageName(
        string serverName,
        string? databaseName,
        bool readOnlyIntent,
        string? engine = null,
        int port = 0)
    {
        var name = string.IsNullOrWhiteSpace(databaseName)
            ? serverName
            : serverName + ":" + databaseName;

        /* Engine BEFORE port and both before :RO, so the suffix order is fixed regardless of which
           discriminators a caller supplies — two callers passing the same facts in a different order must
           not produce two identities. */
        var engineToken = EngineToken(engine);
        if (engineToken is not null)
        {
            name += ":" + engineToken;
        }

        if (port > 0)
        {
            name += ":" + port.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return readOnlyIntent ? name + ":RO" : name;
    }

    /// <summary>
    /// The identity token for an engine, or null when it contributes nothing — which is the case for SQL
    /// Server and for an unspecified engine (#2218).
    ///
    /// <para>Null for SQL Server is load-bearing: it is the historical default, so emitting a token for it
    /// would change every existing server's storage name in both SKUs and re-key the lot. Unrecognized values
    /// also return null rather than being interpolated raw — a typo must not mint a new identity for a server
    /// that already has one, and the engine gate elsewhere already rejects an unknown engine loudly.</para>
    /// </summary>
    private static string? EngineToken(string? engine)
    {
        if (string.IsNullOrWhiteSpace(engine))
        {
            return null;
        }

        var trimmed = engine.Trim();
        if (trimmed.StartsWith("postgres", StringComparison.OrdinalIgnoreCase)
            || trimmed.Equals("pg", StringComparison.OrdinalIgnoreCase))
        {
            return "pg";
        }

        return null;
    }
}
