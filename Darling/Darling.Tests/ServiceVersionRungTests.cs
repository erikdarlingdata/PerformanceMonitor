/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that adds <c>config.service_version</c> (V174, #5655): the rung's place in the ladder, the shape of its
/// script (one guarded CREATE, no GRANT, no data moved), the schema-version probe's sentinel and its mapping arm, and the
/// table's columns on a fresh store, a re-run, and a store that is one rung behind.
/// </summary>
public sealed class ServiceVersionRungTests
{
    public const string RungName = "service-version";

    private const int ProbeOrdinal = 149;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_InADenseLadder_AtTheVersionTheTableNamesItself()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(174, ServiceVersionTable.RungVersion);
        Assert.Equal(ServiceVersionTable.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.True(StorageVersion.SchemaVersion >= ServiceVersionTable.RungVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
        Assert.Equal(ServiceVersionTable.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheScript_IsOneGuardedCreate_OfOneSchemaQualifiedTable_WithNoGrantAndNoDataMoved()
    {
        var sql = Statements();

        Assert.Single(Regex.Matches(sql, @"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)\b", RegexOptions.IgnoreCase));
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.service_version", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT pk_service_version PRIMARY KEY (service_host)", sql, StringComparison.Ordinal);

        /* The times are timestamptz (a naive timestamp would drop the zone the viewer converts from). */
        Assert.Contains("process_started_at timestamptz NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("updated_at timestamptz NOT NULL DEFAULT now()", sql, StringComparison.Ordinal);

        /* The rung creates and moves nothing, and it never touches the row whose trigger would reload every service. */
        foreach (var verb in new[] { "UPDATE", "DELETE", "INSERT", "GRANT", "REVOKE", "TRUNCATE", "DROP", "ALTER" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }

        Assert.DoesNotContain("config_service", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeNamesTheTable_AndMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains("to_regclass('config.service_version')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);


        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasServiceVersion", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasQueryStoreComposeStamp", parameters[ProbeOrdinal - 1].Name);

        /* Every sentinel up to this rung's present, none of a newer rung's: maps to this rung; this sentinel absent: one rung back. */
        var atThisRung = Enumerable.Range(0, parameters.Length).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(ServiceVersionTable.RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(ServiceVersionTable.RungVersion - 1, (int)method.Invoke(null, behind)!);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesTheTable_AndRerunningChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the service version rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.service_version", ct));
            Assert.Equal(
                "service_host:text:NO,product_version:text:NO,process_started_at:timestamp with time zone:NO,updated_at:timestamp with time zone:NO",
                await ScalarAsync(connection,
                    "SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'service_version'", ct));
            Assert.Equal(true, await ScalarAsync(connection,
                "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'config.service_version'::regclass AND contype = 'p' AND conname = 'pk_service_version')", ct));

            /* The probe sees the table, so a fully migrated store maps to the newest rung. */
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* A row, then the rung applied again: the row stays exactly as it was. */
            await ExecAsync(connection,
                "INSERT INTO config.service_version (service_host, product_version, process_started_at, updated_at) VALUES ('HOST-A', '3.11.0', '2026-10-01 12:00:00.123456+00', '2026-10-01 12:00:01.654321+00')", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + ServiceVersionTable.RungVersion, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM config.service_version WHERE service_host = 'HOST-A' AND product_version = '3.11.0' AND process_started_at = '2026-10-01 12:00:00.123456+00' AND updated_at = '2026-10-01 12:00:01.654321+00'", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.service_version", ct));

            /* An upgraded store: one rung behind (no table, no version row), then migrated, gets the table. */
            await ExecAsync(connection, "DROP TABLE config.service_version", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + ServiceVersionTable.RungVersion, ct);
            /* V175's sentinel (#5631) is still on disk, so the probe maps the store past this rung; the rung is checked by its own table below. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.service_version", ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
