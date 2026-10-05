/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The read behind the web Manage Servers grid (#5239): every CONFIGURED server, enabled or not, with the
/// facts the desktop's Manage Servers window shows - whether collection is on, the authentication mode, the
/// monthly cost and when the server was added - beside the engine, version and freshness the grid already had.
///
/// <para><b>Why not <c>list_servers</c>.</b> That tool filters <c>WHERE s.is_enabled</c> for every consumer (the
/// MCP clients, the peers block) and must keep doing so, so a disabled server can never appear in it. This read
/// starts from <c>config.config_monitored_servers</c>, the desired state, and left-joins the collected registry
/// row, so a server that is configured but has not connected yet still has a row. A server that was removed is
/// not configured and does not appear.</para>
///
/// <para><b>Credentials never leave the store.</b> The statement names the columns it reads, and none of them is
/// a credential: no <c>username</c>, no <c>encrypted_password</c>, no remediation login. <c>auth</c> is a mode
/// label ("sql" or "integrated"), not a secret. Every column below is in the <c>viewer</c> role's column grant
/// (<see cref="DarlingManagedRoles.ViewerRestrictedConfigTables"/>).</para>
///
/// <para><b>Display-ready.</b> The browser computes nothing: status, auth and cost arrive as the words and the
/// formatted string the grid paints, and the rows arrive in a total order.</para>
/// </summary>
internal static class DarlingAdminServersReader
{
    /// <summary>The route the Admin page's Servers tab reads.</summary>
    internal const string Route = "/api/admin/servers";

    /// <summary>
    /// Configured servers with their collected registry row (if any) and newest collection. $-free, so a
    /// test can pin the dialect ungated. The lateral probe is the same per-server ordered descent
    /// <see cref="DarlingDataReader.ServerListSql"/> uses.
    /// </summary>
    internal const string AdminServersSql = """
        SELECT
            c.server_id,
            c.name,
            c.host,
            c.database,
            c.read_only_intent,
            c.engine,
            c.port,
            c.auth,
            c.is_enabled,
            c.monthly_cost_usd,
            c.created_at,
            s.server_name,
            s.display_name,
            s.sql_major_version,
            s.postgres_major_version,
            s.sql_engine_edition,
            s.engine_kind,
            s.created_date,
            latest.collection_time AS last_collection
        FROM config.config_monitored_servers c
        LEFT JOIN servers s ON s.server_id = c.server_id
        LEFT JOIN LATERAL
        (
            SELECT cl.collection_time
            FROM v_collection_log cl
            WHERE cl.server_id = c.server_id
            ORDER BY cl.collection_time DESC
            LIMIT 1
        ) AS latest ON TRUE
        """;

    /// <summary>One configured server, as read: the config fields plus the collected registry row's, null when
    /// the server has not connected yet.</summary>
    internal sealed record Row(
        int ServerId,
        string Name,
        string Host,
        string? Database,
        bool ReadOnlyIntent,
        string? Engine,
        int Port,
        string? Auth,
        bool IsEnabled,
        decimal MonthlyCostUsd,
        DateTime CreatedAt,
        string? CollectedServerName,
        string? CollectedDisplayName,
        int? SqlMajorVersion,
        int? PostgresMajorVersion,
        int? SqlEngineEdition,
        string? EngineKind,
        DateTime? RegisteredAt,
        DateTime? LastCollection);

    internal static async Task<List<Row>> ReadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        await using var command = postgres.CreateCommand(AdminServersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetBoolean(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetBoolean(8),
                reader.GetDecimal(9),
                reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetInt32(13),
                reader.IsDBNull(14) ? null : reader.GetInt32(14),
                reader.IsDBNull(15) ? null : reader.GetInt32(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                reader.IsDBNull(18) ? null : reader.GetDateTime(18)));
        }

        return rows;
    }

    /// <summary>The route's JSON body: <c>{server_count, servers[]}</c>, rows in the total order.</summary>
    internal static string Render(IReadOnlyList<Row> rows, DateTime nowUtc) =>
        JsonSerializer.Serialize(new { server_count = rows.Count, servers = Build(rows, nowUtc) }, McpHelpers.JsonOptions);

    /// <summary>
    /// The display-ready rows, ordered by display name (ordinal, ignoring case) then server name (ordinal) -
    /// two servers that share a display name keep one order on every read.
    /// </summary>
    internal static List<AdminServerRow> Build(IReadOnlyList<Row> rows, DateTime nowUtc)
    {
        var built = rows.Select(r => ToRow(r, nowUtc)).ToList();
        built.Sort((a, b) =>
        {
            var byDisplay = StringComparer.OrdinalIgnoreCase.Compare(a.display_name, b.display_name);
            return byDisplay != 0 ? byDisplay : string.CompareOrdinal(a.server_name, b.server_name);
        });
        return built;
    }

    private static AdminServerRow ToRow(Row r, DateTime nowUtc)
    {
        /* The storage name the collectors stamp when the server has connected; the same name derived from the
           config fields when it has not, so a not-yet-collected server is still named the way it will be. */
        var serverName = string.IsNullOrEmpty(r.CollectedServerName)
            ? ServerIdHelper.BuildStorageName(r.Host, r.Database, r.ReadOnlyIntent, r.Engine, r.Port)
            : r.CollectedServerName;

        var displayName = !string.IsNullOrEmpty(r.CollectedDisplayName) ? r.CollectedDisplayName
            : !string.IsNullOrWhiteSpace(r.Name) ? r.Name
            : serverName;

        var engineKind = r.EngineKind
            ?? (MonitoredEngineKind.IsPostgres(r.Engine) ? MonitoredEngineKind.Postgres : MonitoredEngineKind.SqlServer);

        return new AdminServerRow
        {
            server_name = serverName,
            display_name = displayName,
            engine = engineKind,
            version = MonitoredEngineVersion.DescribeEngineVersion(
                engineKind, r.SqlMajorVersion, r.PostgresMajorVersion, r.SqlEngineEdition),
            freshness = DarlingMcpDataTools.FreshnessStatus(r.LastCollection, r.RegisteredAt, nowUtc),
            read_only = r.ReadOnlyIntent || serverName.EndsWith(":RO", StringComparison.Ordinal),
            last_collected = r.LastCollection?.ToString("o", CultureInfo.InvariantCulture),
            status = r.IsEnabled ? "Enabled" : "Disabled",
            auth = AuthLabel(r.Auth),
            monthly_cost = CostLabel(r.MonthlyCostUsd),
            monthly_cost_usd = r.MonthlyCostUsd,
            added = r.CreatedAt.ToString("o", CultureInfo.InvariantCulture),
        };
    }

    /// <summary>The desktop's words for the stored mode; a mode this build does not know is shown as stored.</summary>
    internal static string? AuthLabel(string? auth) => auth switch
    {
        null or "" => null,
        _ when string.Equals(auth, "sql", StringComparison.OrdinalIgnoreCase) => "SQL Server",
        _ when string.Equals(auth, "integrated", StringComparison.OrdinalIgnoreCase) => "Windows",
        _ => auth,
    };

    /// <summary>"$1,234" (invariant culture, halves away from zero), or null when no cost is set - a cost that
    /// rounds to $0 reads as none, not as "$0".</summary>
    internal static string? CostLabel(decimal monthlyCostUsd)
    {
        var rounded = Math.Round(monthlyCostUsd, 0, MidpointRounding.AwayFromZero);
        return rounded <= 0m ? null : rounded.ToString("$#,##0", CultureInfo.InvariantCulture);
    }

    /// <summary>One grid row. Field names are the grid's column keys.</summary>
    internal sealed class AdminServerRow
    {
        public string server_name { get; init; } = "";
        public string display_name { get; init; } = "";
        public string engine { get; init; } = "";
        public string version { get; init; } = "";
        public string freshness { get; init; } = "";
        public bool read_only { get; init; }
        public string? last_collected { get; init; }
        public string status { get; init; } = "";
        public string? auth { get; init; }
        public string? monthly_cost { get; init; }

        /// <summary>The figure behind <see cref="monthly_cost"/>, for the column's sort only.</summary>
        public decimal monthly_cost_usd { get; init; }
        public string added { get; init; } = "";
    }
}
