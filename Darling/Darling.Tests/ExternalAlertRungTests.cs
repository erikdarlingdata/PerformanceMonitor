/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// Pins the rung that adds the external alert tables and <c>config.config_mute_rules.external_source</c> (V180, #5654): the
/// rung's place in the ladder, the shape of its script (three guarded CREATE TABLEs, two guarded CREATE INDEXes, one guarded
/// ADD COLUMN, no GRANT, no data moved), the schema-version probe's sentinel and its mapping arm, and the tables' columns,
/// checks and cascades on a fresh store, a re-run, and a store that is one rung behind.
/// </summary>
public sealed class ExternalAlertRungTests
{
    public const string RungName = "external-alerts";

    private const int ProbeOrdinal = 155;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_AtTheTopOfTheLadder_AtTheVersionTheTablesNameThemselves()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(180, ExternalAlertTables.RungVersion);
        Assert.Equal(ExternalAlertTables.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(ExternalAlertTables.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheScript_IsGuardedDdlOnly_ForThreeSchemaQualifiedTables_WithNoGrantAndNoDataMoved()
    {
        var sql = Statements();

        Assert.Equal(3, Regex.Matches(sql, @"CREATE\s+TABLE\b", RegexOptions.IgnoreCase).Count);
        Assert.Equal(2, Regex.Matches(sql, @"CREATE\s+INDEX\b", RegexOptions.IgnoreCase).Count);

        /* Every CREATE and the one ADD COLUMN carry their own guard. */
        Assert.Equal(6, Regex.Matches(sql, @"IF NOT EXISTS", RegexOptions.IgnoreCase).Count);
        foreach (var table in new[] { "external_alert_source", "external_alert_pending", "external_alert_state" })
        {
            Assert.Contains("CREATE TABLE IF NOT EXISTS config." + table, sql, StringComparison.Ordinal);
        }

        /* The one ALTER is the nullable mute-rule column: catalog-only, and NULL names no source, so no stored rule changes. */
        Assert.Single(Regex.Matches(sql, @"(?m)^\s*ALTER\b"));
        Assert.Contains("ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS external_source text;", sql, StringComparison.Ordinal);

        /* The times are timestamptz, and the registry name is pinned to the shape the function and the CLI both rely on. */
        Assert.Contains("submitted_at timestamptz NOT NULL DEFAULT now()", sql, StringComparison.Ordinal);
        Assert.Contains("source_name ~ '^[a-z][a-z0-9_]{1,30}$'", sql, StringComparison.Ordinal);

        /* The rung moves nothing and grants nothing; its one privilege statement is the revoke of write privileges
           from PUBLIC and the three Darling roles (literal statements in one DO block, since the ladder holds no dynamic SQL). */
        var outsideTheRevokeBlock = Regex.Replace(sql, @"DO \$do\$.*?END \$do\$;", " ", RegexOptions.Singleline);
        Assert.Single(Regex.Matches(sql, @"DO \$do\$"));

        /* What is inside that one block: literal statements only (no EXECUTE, no format( call), and exactly the four REVOKEs. */
        var revokeBlock = Regex.Match(sql, @"DO \$do\$.*?END \$do\$;", RegexOptions.Singleline).Value;
        Assert.DoesNotContain("EXECUTE", revokeBlock, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("format(", revokeBlock, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, Regex.Matches(revokeBlock, @"\bREVOKE\b", RegexOptions.IgnoreCase).Count);
        Assert.Equal(4, Regex.Matches(revokeBlock, @"REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config\.external_alert_source, config\.external_alert_pending, config\.external_alert_state FROM (PUBLIC|admin|viewer|mcp);").Count);
        foreach (var verb in new[] { "UPDATE", "DELETE", "INSERT", "GRANT", "REVOKE", "TRUNCATE", "DROP" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", outsideTheRevokeBlock);
        }

        foreach (var grantee in new[] { "PUBLIC", "admin", "viewer", "mcp" })
        {
            Assert.Contains(
                "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON config.external_alert_source, config.external_alert_pending, config.external_alert_state FROM " + grantee + ";",
                sql, StringComparison.Ordinal);
        }
        Assert.DoesNotContain("GRANT", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesTheTablesAndTheColumn_AndRerunningChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the external alert rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT (SELECT count(*) FROM config.external_alert_source) + (SELECT count(*) FROM config.external_alert_pending) + (SELECT count(*) FROM config.external_alert_state)", ct));

            Assert.Equal(
                "source_id:integer:NO,source_name:text:NO,role_name:text:NO,enabled:boolean:NO,max_per_minute:integer:NO,max_pending:integer:NO,created_at:timestamp with time zone:NO",
                await ColumnsAsync(connection, "external_alert_source", ct));
            Assert.Equal(
                "pending_id:bigint:NO,source_id:integer:NO,kind:text:NO,alert_key:text:NO,severity:text:NO,title:text:YES,detail:text:YES,server_name:text:YES,value:double precision:YES,submitted_at:timestamp with time zone:NO,expires_at:timestamp with time zone:NO,status:text:NO,claimed_at:timestamp with time zone:YES,service_instance:text:YES,attempts:integer:NO,next_attempt_at:timestamp with time zone:YES,reason:text:YES,completed_at:timestamp with time zone:YES",
                await ColumnsAsync(connection, "external_alert_pending", ct));
            Assert.Equal(
                "source_id:integer:NO,alert_key:text:NO,server_label:text:NO,server_id:integer:NO,server_key:text:NO,server_name:text:NO,firing:boolean:NO,fired_severity:text:YES,last_fired_at:timestamp with time zone:YES,fired_severities:text:YES",
                await ColumnsAsync(connection, "external_alert_state", ct));
            Assert.Equal("external_source:text:YES", await ScalarAsync(connection,
                "SELECT column_name || ':' || data_type || ':' || is_nullable FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_mute_rules' AND column_name = 'external_source'", ct));

            /* The probe sees the registry, so a fully migrated store maps to the newest rung. */
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* The registry name shape and uniqueness, the queue's kind, severity and status checks, in the live store. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('Bad Name', 'r0')", ct));
            await ExecAsync(connection, "INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('billing_svc', 'r1')", ct);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('billing_svc', 'r2')", ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('other_svc', 'r1')", ct));
            Assert.Equal("60:500:true", await ScalarAsync(connection,
                "SELECT max_per_minute || ':' || max_pending || ':' || enabled FROM config.external_alert_source WHERE source_name = 'billing_svc'", ct));

            const string Insert = "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, expires_at) SELECT source_id, '{0}', 'k1', '{1}', now() + interval '15 minutes' FROM config.external_alert_source WHERE source_name = 'billing_svc'";
            await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, Insert, "fire", "critical"), ct);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, Insert, "page", "critical"), ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, Insert, "fire", "info"), ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, expires_at) VALUES (-1, 'fire', 'k1', 'warning', now())", ct));
            Assert.Equal("pending:0", await ScalarAsync(connection, "SELECT status || ':' || attempts FROM config.external_alert_pending", ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE config.external_alert_pending SET status = 'sent'", ct));

            await ExecAsync(connection,
                "INSERT INTO config.external_alert_state (source_id, alert_key, firing) SELECT source_id, 'k1', true FROM config.external_alert_source", ct);
            Assert.Equal(0, Convert.ToInt32(await ScalarAsync(connection, "SELECT server_id FROM config.external_alert_state", ct), CultureInfo.InvariantCulture));

            /* A mute rule that names a source keeps it, and a rule that names none reads NULL. */
            await ExecAsync(connection,
                "INSERT INTO config.config_mute_rules (id, created_at_utc, external_source) VALUES ('m1', now() at time zone 'utc', 'billing_svc'), ('m2', now() at time zone 'utc', NULL)", ct);

            /* The rung applied again: the rows stay exactly as they were. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + ExternalAlertTables.RungVersion, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct), CultureInfo.InvariantCulture));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.external_alert_source WHERE source_name = 'billing_svc'", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.external_alert_pending", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.external_alert_state", ct));
            Assert.Equal("billing_svc|", await ScalarAsync(connection,
                "SELECT string_agg(coalesce(external_source, '') , '|' ORDER BY id) FROM config.config_mute_rules", ct));

            /* Removing a source takes its queue and its firing state with it. */
            await ExecAsync(connection, "DELETE FROM config.external_alert_source WHERE source_name = 'billing_svc'", ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT (SELECT count(*) FROM config.external_alert_pending) + (SELECT count(*) FROM config.external_alert_state)", ct));

            /* An upgraded store: one rung behind (no tables, no column, no version row), then migrated, gets all of it. */
            await ExecAsync(connection, "DROP TABLE config.external_alert_state; DROP TABLE config.external_alert_pending; DROP TABLE config.external_alert_source; ALTER TABLE config.config_mute_rules DROP COLUMN external_source", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + ExternalAlertTables.RungVersion, ct);
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, ExternalAlertTables.RungVersion, ct);
            Assert.True(await ProbedVersionAsync(connection, ct) < ExternalAlertTables.RungVersion);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.external_alert_source", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_mute_rules' AND column_name = 'external_source'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static Task<object?> ColumnsAsync(NpgsqlConnection connection, string table, CancellationToken ct) =>
        ScalarAsync(connection,
            "SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = '" + table + "'", ct);

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
