/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database and every row it plants goes through DeleteRowsAsync. */

/// <summary>
/// #8914: the Overview extension panel counts "available and NOT installed" over the database the server entry connects to, not over
/// every database on the server. The collector stores one row per extension for each database, and the application database holds
/// the six extensions the monitoring uses while the <c>postgres</c> maintenance database holds none of them. The headline told the
/// operator to run CREATE EXTENSION for six extensions that were installed, and the grid led with the <c>postgres</c> rows.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPgExtensionPanelDatabaseLiveTests
{
    private const int ServerId = -8914001;
    private const int NoDatabaseServerId = -8914002;
    private const string ServerName = "viewer-pg-extension-panel";
    private const string NoDatabaseServerName = "viewer-pg-extension-panel-no-db";
    private static readonly string[] Extensions =
        { "hypopg", "pg_buffercache", "pg_cron", "pg_stat_statements", "pg_trgm", "pgstattuple" };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheExtensionPanel_CountsAndListsTheDatabaseTheServerEntryConnectsTo_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live extension panel test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;

            foreach (var (id, name, database) in new (int, string, string?)[]
            {
                (ServerId, ServerName, "appdb"), (NoDatabaseServerId, NoDatabaseServerName, null),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO config.config_monitored_servers (server_id, name, host, database) VALUES ($1, $2, $2, $3)",
                    id, name, (object?)database ?? DBNull.Value);

                /* appdb has all six created; postgres has them available and nothing created beyond plpgsql. */
                foreach (var (db, extension, state) in Extensions
                    .SelectMany(e => new[] { ("appdb", e, "installed"), ("postgres", e, "available") })
                    .Append(("postgres", "plpgsql", "installed")))
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name,
     state, installed_version, default_version, is_monitoring_relevant)
VALUES ($1::bigint, $2::timestamp, $3::integer, $4::text, $5::text, $6::text,
        $7::text, $8::text, $9::text, $10::boolean)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now), id, name,
                        db, extension, state, state == "installed" ? "1.0" : null, "1.0", extension != "plpgsql");
                }
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var start = now.AddDays(-1);
            var end = now.AddDays(1);

            /* The entry names appdb: the rows are appdb's, and the headline counts none as not installed. */
            var panel = await viewer.GetPgExtensionPanelAsync(ServerId, start, end, cancellationToken: ct);
            Assert.Equal("appdb", panel.EntryDatabase);
            Assert.Equal(Extensions.Length, panel.Rows.Count);
            Assert.All(panel.Rows, r => Assert.Equal("appdb", r.DatabaseName));
            var headline = PgExtensionPanelText.Headline(panel.Rows, panel.EntryDatabase);
            Assert.Contains("0 extensions", headline, StringComparison.Ordinal);
            Assert.Contains("NOT installed in database appdb", headline, StringComparison.Ordinal);

            /* An entry that names no database gets every database's rows, as before the fix. */
            var unscoped = await viewer.GetPgExtensionPanelAsync(NoDatabaseServerId, start, end, cancellationToken: ct);
            Assert.Null(unscoped.EntryDatabase);
            Assert.Equal(new[] { "appdb", "postgres" }, unscoped.Rows.Select(r => r.DatabaseName).Distinct().Order().ToArray());

            /* The census still counts every database. */
            var census = await viewer.GetPgExtensionInstallCensusAsync(ServerId, start, end, ct);
            Assert.Equal(2, census[0].DatabasesTotal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = new[] { ServerId, NoDatabaseServerId };
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_extension_availability WHERE server_id = ANY($1)", ids);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config.config_monitored_servers WHERE server_id = ANY($1)", ids);
    }
}
