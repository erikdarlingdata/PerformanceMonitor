/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One service host's row in <c>config.service_version</c> (V174, #5655): the product version that host's service
/// reported when it started, when that process started, and when the row was last written. Both times are UTC instants
/// read from <c>timestamptz</c> columns, so they carry microseconds and no more.
/// </summary>
public sealed record ServiceVersionRow(string ServiceHost, string ProductVersion, DateTime ProcessStartedUtc, DateTime UpdatedUtc);

/// <summary>
/// The store's record of which version each service host runs (V174, #5655), its creating script, and the one write and
/// one read that the service and the viewers share. The Manage Servers window used to show the VIEWER's own version as
/// every server's "Installed Version"; this table is where the service says what it really is.
///
/// <para>A table of its own, keyed by the service host like <c>config.password_key_service</c>, because more than one
/// service host can write to one store. It is deliberately not a column of <c>config.config_service</c>: that row's
/// <c>trg_service_self_bump</c> trigger bumps <c>config_version</c> on every update, and a write at each service start
/// would make every service reload its configuration.</para>
/// </summary>
public static class ServiceVersionTable
{
    /// <summary>The store schema version of the rung that creates the table (V174). A store below it has no such table.</summary>
    public const int RungVersion = 174;

    /// <summary>
    /// The table, as the V174 rung runs it. Idempotent (<c>IF NOT EXISTS</c>), so a re-run changes nothing. No GRANT:
    /// the <c>config</c> schema's blanket SELECT and default privileges for the desktop viewer's roles cover it, as they
    /// cover every other plain <c>config</c> table. The rung embeds this text, so once V174 has shipped, editing it
    /// edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS config.service_version
(
    service_host text NOT NULL,
    product_version text NOT NULL,
    process_started_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_service_version PRIMARY KEY (service_host)
);
";

    /// <summary>The upsert: $1 host, $2 version, $3 process start (UTC). <c>updated_at</c> is the store's own clock.</summary>
    public const string UpsertSql = @"
INSERT INTO config.service_version (service_host, product_version, process_started_at, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (service_host) DO UPDATE
SET product_version = EXCLUDED.product_version,
    process_started_at = EXCLUDED.process_started_at,
    updated_at = EXCLUDED.updated_at;";

    /// <summary>The read, one row per service host, in host order.</summary>
    public const string SelectSql = @"
SELECT service_host, product_version, process_started_at, updated_at
FROM config.service_version
ORDER BY service_host;";

    /// <summary>
    /// Writes this host's row: inserts it, or updates the version and start time of the row already there, so a store
    /// holds one row per host however often that host's service starts.
    /// </summary>
    public static async Task UpsertAsync(
        NpgsqlDataSource dataSource, string serviceHost, string productVersion, DateTime processStartedUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceHost);
        ArgumentException.ThrowIfNullOrWhiteSpace(productVersion);

        await using var command = dataSource.CreateCommand(UpsertSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serviceHost });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = productVersion });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz, TypedValue = ToStoredInstant(processStartedUtc) });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Every service host's row. A store that lacks the table (a store whose service predates V174) reads as no rows:
    /// only the undefined-table error (SQLSTATE 42P01) is answered that way, so a permission or connection failure still
    /// surfaces to the caller.
    /// </summary>
    public static async Task<IReadOnlyList<ServiceVersionRow>> ReadAsync(
        NpgsqlDataSource dataSource, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var rows = new List<ServiceVersionRow>();
        try
        {
            await using var command = dataSource.CreateCommand(SelectSql);
            command.CommandTimeout = commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add(new ServiceVersionRow(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetFieldValue<DateTime>(2),
                    reader.GetFieldValue<DateTime>(3)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return Array.Empty<ServiceVersionRow>();
        }

        return rows;
    }

    /// <summary>
    /// The instant as the store keeps it: UTC, cut to whole microseconds (a .NET tick is 100 ns and a
    /// <c>timestamptz</c> keeps no finer than 1 us), so the value written is the value read back.
    /// </summary>
    public static DateTime ToStoredInstant(DateTime instant)
    {
        var utc = instant.Kind == DateTimeKind.Utc ? instant : instant.ToUniversalTime();
        return new DateTime(utc.Ticks - (utc.Ticks % 10), DateTimeKind.Utc);
    }
}
