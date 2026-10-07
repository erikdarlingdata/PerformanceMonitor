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
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the rung that records which old-format saved passwords were in the store at the upgrade (V167, #5456): its place
/// in a dense ladder, its owner-only table, the gate on its capture statement, and the schema-version probe's
/// sentinel. The live fact mints its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class LegacyPinCandidateRungTests
{
    private const int ProbeOrdinal = 142;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == "legacy-pin-candidates");

    [Fact]
    public void TheRungIsInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(LegacyPinCandidateTables.RungVersion, Rung.Version);
        /* Not `Rung.Version == StorageVersion.SchemaVersion`: this rung stopped being the newest when the per-server AWS
           role rung (V168) landed above it. The top and the declared version agreeing is the line below. */
        Assert.True(Rung.Version < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
        Assert.Equal(LegacyPinCandidateTables.CreateSql + "\n\n" + LegacyPinCandidateTables.CaptureSql, Rung.Sql);

        /* V165 does not change: a store already at 165 never runs it again, so this is a rung of its own. */
        Assert.DoesNotContain("legacy_secret_pin_candidate", PasswordKeyTables.CreateSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTableIsOwnerOnly_WithTheTriggersMadeBeforeTheCapture()
    {
        var create = LegacyPinCandidateTables.CreateSql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("CREATE TABLE IF NOT EXISTS config.legacy_secret_pin_candidate", create, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.legacy_secret_pin_candidate ENABLE ROW LEVEL SECURITY;", create, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE POLICY", create, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("value_sha256 bytea NOT NULL", create, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT ck_legacy_secret_pin_candidate_slot CHECK (slot IN ('server', 'remediation', 'smtp'))", create, StringComparison.Ordinal);

        Assert.Equal(2, LegacyPinCandidateTables.OwnerOnlyTriggers.Count);
        foreach (var (table, trigger) in LegacyPinCandidateTables.OwnerOnlyTriggers)
        {
            Assert.Equal("legacy_secret_pin_candidate", table);
            Assert.Contains($"CREATE TRIGGER {trigger}", create, StringComparison.Ordinal);
            Assert.Contains($"ALTER TABLE config.{table} ENABLE ALWAYS TRIGGER {trigger};", create, StringComparison.Ordinal);
        }

        Assert.Equal(2, Regex.Matches(create, "FOR EACH (ROW|STATEMENT) EXECUTE FUNCTION config.password_key_owner_only\\(\\);").Count);

        /* The capture comes after every trigger in the rung's text, so the table is never writable by another role. */
        var capture = Rung.Sql.IndexOf("INSERT INTO config.legacy_secret_pin_candidate", StringComparison.Ordinal);
        Assert.True(capture > Rung.Sql.LastIndexOf("ENABLE ALWAYS TRIGGER", StringComparison.Ordinal), "the capture runs after the triggers exist");

        /* The service checks the catalog for these two triggers, and both provisioning paths revoke and sweep the table. */
        var store = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingPasswordKeyStore.cs");
        Assert.Contains("LegacyPinCandidateTables.OwnerOnlyTriggers", store, StringComparison.Ordinal);
        Assert.Contains("'legacy_secret_pin_candidate'", store, StringComparison.Ordinal);
        foreach (var file in new[]
                 {
                     RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs"),
                     RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"),
                 })
        {
            Assert.Contains("legacy_secret_pin_candidate FROM PUBLIC", file, StringComparison.Ordinal);
            Assert.Contains("trg_legacy_secret_pin_candidate_owner_only_row", file, StringComparison.Ordinal);
            Assert.Contains("trg_legacy_secret_pin_candidate_owner_only_truncate", file, StringComparison.Ordinal);
            Assert.True(Regex.Matches(file, "to_regclass\\('(\\{config\\}|config)\\.legacy_secret_pin_candidate'\\)").Count >= 3, "the table is in every to_regclass list");
        }
    }

    [Fact]
    public void TheCaptureRunsOnlyWhileTheStepIsOpen_AndTakesOnlyOldFormatValues()
    {
        var capture = LegacyPinCandidateTables.CaptureSql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("m.state = 'pending'", capture, StringComparison.Ordinal);
        Assert.Contains("FROM config.legacy_secret_pin_marker AS m WHERE m.id = 1", capture, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, slot) DO NOTHING;", capture, StringComparison.Ordinal);

        /* The hash is of the stored text, as the pin step and the resolver hash it; the text itself is never stored. */
        Assert.Contains("pg_catalog.sha256(pg_catalog.convert_to(v.stored, 'UTF8'))", capture, StringComparison.Ordinal);

        /* Blank values, sealed values and references are not recorded. */
        Assert.Contains(@"v.stored !~ '^\s*$'", capture, StringComparison.Ordinal);
        foreach (var prefix in new[] { "sealed:", "env:", "file:" })
        {
            Assert.Contains($"NOT pg_catalog.starts_with(v.stored, '{prefix}')", capture, StringComparison.Ordinal);
        }

        /* The three slots, and the mail server's row is server 0. */
        Assert.Contains("'server'::text AS slot", capture, StringComparison.Ordinal);
        Assert.Contains("'remediation'::text", capture, StringComparison.Ordinal);
        Assert.Contains("SELECT 0, 'smtp'::text", capture, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesTheTable_AtItsOwnOrdinal()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "to_regclass('config.legacy_secret_pin_candidate') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(probe[probe.IndexOf(arm, StringComparison.Ordinal)..], "EXISTS"));

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        /* A newer rung's sentinel (V168's) follows this one. */
        Assert.True(ProbeOrdinal < parameters.Length - 1, "a newer rung's sentinel follows this one");
        Assert.Equal("hasLegacyPinCandidates", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasPagerDutyAutoResolve", parameters[ProbeOrdinal - 1].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(PgMigrations.Scripts[^1].Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        /* A store that stopped at this rung answers this rung; the same store without its sentinel answers the one before. */
        var atThisRung = Enumerable.Range(0, parameters.Length).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(Rung.Version, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(Rung.Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasLegacyPinCandidates)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPagerDutyAutoResolve)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && thisArm < previousArm, "this arm sits above the previous rung's");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        var armProseStart = viewer.LastIndexOf("/* V167 (#5456)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain("legacy_secret_pin_candidate", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStoreAt165_CapturesItsValues_ADoneStoreNone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the legacy pin candidate rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* A store at 165: the record is not there yet. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + LegacyPinCandidateTables.RungVersion, ct);
            await ExecAsync(connection, "DROP TABLE config.legacy_secret_pin_candidate", ct);
            await ExecAsync(connection, @"
INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, remediation_username, remediation_encrypted_password, port)
VALUES (1, 'alpha-example', 'alpha-example', '', 'sql', 'monitor_login', 'legacy-server-blob', 'fix_login', 'legacy-remediation-blob', 1433),
       (2, 'beta-example', 'beta-example', NULL, 'sql', 'monitor_login', 'sealed:v1:0123456789abcdef:AAAA', NULL, NULL, 1433),
       (3, 'gamma-example', 'gamma-example', NULL, 'sql', 'monitor_login', 'env:EXAMPLE_PASSWORD', NULL, NULL, 1433),
       (4, 'delta-example', 'delta-example', NULL, 'sql', 'monitor_login', '', NULL, NULL, 1433);
INSERT INTO config.config_notification (id, smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password)
VALUES (1, 'smtp.example.com', 587, TRUE, 'mail_login', 'legacy-smtp-blob')
ON CONFLICT (id) DO UPDATE SET smtp_host = EXCLUDED.smtp_host, smtp_port = EXCLUDED.smtp_port, smtp_use_ssl = EXCLUDED.smtp_use_ssl,
    smtp_username = EXCLUDED.smtp_username, smtp_encrypted_password = EXCLUDED.smtp_encrypted_password;", ct);
            Assert.Equal("pending", await ScalarAsync(connection, "SELECT state FROM config.legacy_secret_pin_marker", ct));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(3L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_candidate", ct));
            Assert.Equal(10L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid WHERE NOT t.tgisinternal AND p.proname = 'password_key_owner_only'", ct));

            /* The hash is the SHA-256 of the stored text, as C# takes it; the text is not in the table. */
            Assert.Equal(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-server-blob"))),
                Convert.ToHexString((byte[])(await ScalarAsync(connection, "SELECT value_sha256 FROM config.legacy_secret_pin_candidate WHERE server_id = 1 AND slot = 'server'", ct))!));
            Assert.Equal(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-remediation-blob"))),
                Convert.ToHexString((byte[])(await ScalarAsync(connection, "SELECT value_sha256 FROM config.legacy_secret_pin_candidate WHERE server_id = 1 AND slot = 'remediation'", ct))!));
            Assert.Equal(Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-smtp-blob"))),
                Convert.ToHexString((byte[])(await ScalarAsync(connection, "SELECT value_sha256 FROM config.legacy_secret_pin_candidate WHERE server_id = 0 AND slot = 'smtp'", ct))!));

            /* The connection columns are copied as the rows hold them. */
            Assert.Equal("alpha-example", await ScalarAsync(connection, "SELECT host FROM config.legacy_secret_pin_candidate WHERE server_id = 1 AND slot = 'server'", ct));
            Assert.Equal("fix_login", await ScalarAsync(connection, "SELECT remediation_username FROM config.legacy_secret_pin_candidate WHERE server_id = 1 AND slot = 'remediation'", ct));
            Assert.Equal("mail_login", await ScalarAsync(connection, "SELECT username FROM config.legacy_secret_pin_candidate WHERE server_id = 0 AND slot = 'smtp'", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_candidate WHERE server_id IN (2, 3, 4)", ct));

            /* A store whose step is done records nothing. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + LegacyPinCandidateTables.RungVersion, ct);
            await ExecAsync(connection, "DROP TABLE config.legacy_secret_pin_candidate", ct);
            await ExecAsync(connection, "UPDATE config.legacy_secret_pin_marker SET state = 'done'", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_candidate", ct));

            /* A store whose step was set aside (marker skipped) records nothing either. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + LegacyPinCandidateTables.RungVersion, ct);
            await ExecAsync(connection, "DROP TABLE config.legacy_secret_pin_candidate", ct);
            await ExecAsync(connection, "UPDATE config.legacy_secret_pin_marker SET state = 'skipped'", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_candidate", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
