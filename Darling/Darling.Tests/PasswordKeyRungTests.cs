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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that adds the tables for the service's published password key (V165, #5366): the rung's place
/// in the ladder (always the NEWEST rung, written as <c>Scripts[^1]</c> so the next rung needs no edit here), the shape of
/// its script, and the schema-version probe's newest sentinel. The live fact mints its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class PasswordKeyRungTests
{
    public const string RungName = "password-key";

    private const int ProbeOrdinal = 140;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_InADenseLadder_AtTheVersionTheTablesNameItself()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(PasswordKeyTables.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
        Assert.Equal(PasswordKeyTables.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheTablesAreSchemaQualified_AndEveryCreateIsGuarded()
    {
        var sql = Statements();

        foreach (var match in Regex.Matches(sql, @"(?:CREATE TABLE IF NOT EXISTS|INSERT INTO)\s+(\S+)").Cast<Match>())
        {
            Assert.Matches(@"^config\.", match.Groups[1].Value);
        }

        foreach (var create in Regex.Matches(sql, @"CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?!IF NOT EXISTS)", RegexOptions.IgnoreCase).Cast<Match>())
        {
            Assert.Fail("an unguarded CREATE: " + create.Value);
        }

        Assert.Equal(4, Regex.Matches(sql, @"CREATE TABLE IF NOT EXISTS config\.(password_key|password_key_service|legacy_secret_pin|legacy_secret_pin_marker)\b").Count);
        Assert.Contains("CREATE UNIQUE INDEX IF NOT EXISTS ux_password_key_current ON config.password_key ((true)) WHERE state = 'current';", sql, StringComparison.Ordinal);

        /* The marker row is the only data: one VALUES row into the table this rung creates, kept on a re-run. */
        Assert.Single(Regex.Matches(sql, @"INSERT\s+INTO", RegexOptions.IgnoreCase));
        Assert.Contains("INSERT INTO config.legacy_secret_pin_marker (id, state)\nVALUES (1, 'pending')\nON CONFLICT (id) DO NOTHING;", sql, StringComparison.Ordinal);

        /* No statement of the rung starts with one of these: the rung creates and seeds, and moves no data. */
        foreach (var verb in new[] { "UPDATE", "DELETE", "GRANT", "TRUNCATE" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }

        /* The only ALTER and DROP statements are the trigger ones (enable it always; drop it before it is created again) and
           the row-security switch on the two pin tables, which carries no policy. */
        foreach (var line in sql.Split('\n').Select(l => l.Trim()).Where(l => l.StartsWith("ALTER", StringComparison.Ordinal) || l.StartsWith("DROP", StringComparison.Ordinal)))
        {
            Assert.Matches(@"^(ALTER TABLE config\.\w+ ENABLE ALWAYS TRIGGER \w+|ALTER TABLE config\.legacy_secret_pin(_marker)? ENABLE ROW LEVEL SECURITY|DROP TRIGGER IF EXISTS \w+ ON config\.\w+);$", line);
        }
    }

    [Fact]
    public void TheOwnerOnlyFunction_QualifiesEveryCatalogName_KeepsPgTempLast_AndIsNotGrantedToPublic()
    {
        var sql = Statements();
        var start = sql.IndexOf("CREATE OR REPLACE FUNCTION config.password_key_owner_only()", StringComparison.Ordinal);
        var end = sql.IndexOf("$fn$;", start + 1, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the owner-only function is missing");
        var function = sql[start..end];

        Assert.Contains("SET search_path = pg_catalog, pg_temp", function, StringComparison.Ordinal);
        Assert.DoesNotContain("SECURITY DEFINER", function, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("pg_catalog.pg_has_role(", function, StringComparison.Ordinal);
        /* A NULL answer (a session user whose membership cannot be told) refuses too: the test is IS NOT TRUE, never NOT. */
        Assert.Contains("'USAGE') IS NOT TRUE THEN", function, StringComparison.Ordinal);
        Assert.DoesNotContain("IF NOT pg_catalog.pg_has_role", function, StringComparison.Ordinal);
        Assert.Contains("session_user", function, StringComparison.Ordinal);
        Assert.DoesNotContain("current_user", function, StringComparison.Ordinal);
        Assert.Contains("'USAGE'", function, StringComparison.Ordinal);
        Assert.Contains("ERRCODE = 'PW010'", function, StringComparison.Ordinal);
        Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, "PW010");
        Assert.Contains("Only the store owner can change the password key tables.", function, StringComparison.Ordinal);

        /* Every catalog relation the function names is schema-qualified: a bare pg_ name after FROM or JOIN would be found in
           the session's temporary schema first. */
        Assert.DoesNotMatch(@"(?:FROM|JOIN)\s+pg_(?!catalog\.)", function);
        Assert.Contains("FROM pg_catalog.pg_class AS c", function, StringComparison.Ordinal);

        Assert.Contains("REVOKE ALL ON FUNCTION config.password_key_owner_only() FROM PUBLIC;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EachOfTheFourTables_HasARowTriggerAndATruncateTrigger_OnTheOwnerOnlyFunction()
    {
        var sql = Statements();
        foreach (var table in new[] { "password_key", "password_key_service", "legacy_secret_pin", "legacy_secret_pin_marker" })
        {
            Assert.Contains($"DROP TRIGGER IF EXISTS trg_{table}_owner_only_row ON config.{table};\nCREATE TRIGGER trg_{table}_owner_only_row\n    BEFORE INSERT OR UPDATE OR DELETE ON config.{table}\n    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();", sql, StringComparison.Ordinal);
            Assert.Contains($"DROP TRIGGER IF EXISTS trg_{table}_owner_only_truncate ON config.{table};\nCREATE TRIGGER trg_{table}_owner_only_truncate\n    BEFORE TRUNCATE ON config.{table}\n    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();", sql, StringComparison.Ordinal);
            /* Enabled always, so a session_replication_role of replica does not skip them. */
            Assert.Contains($"ALTER TABLE config.{table} ENABLE ALWAYS TRIGGER trg_{table}_owner_only_row;", sql, StringComparison.Ordinal);
            Assert.Contains($"ALTER TABLE config.{table} ENABLE ALWAYS TRIGGER trg_{table}_owner_only_truncate;", sql, StringComparison.Ordinal);
        }

        Assert.Equal(8, Regex.Matches(sql, @"(?m)^CREATE TRIGGER ").Count);
        Assert.Equal(8, Regex.Matches(sql, @"(?m)^DROP TRIGGER IF EXISTS ").Count);
        Assert.Equal(8, Regex.Matches(sql, @"ENABLE ALWAYS TRIGGER").Count);
        Assert.DoesNotContain("CREATE OR REPLACE TRIGGER", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePasswordKeyTable_CarriesTheAlgorithmSizeAndReplacedColumnChecks()
    {
        var sql = Statements();
        Assert.Contains("CONSTRAINT ck_password_key_algorithm CHECK (algorithm IN ('RSA3072-OAEP-SHA256/A256GCM'))", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT ck_password_key_public_key_size CHECK (octet_length(public_key) BETWEEN 256 AND 2048)", sql, StringComparison.Ordinal);
        Assert.Contains("(state = 'current' AND replaced_reason IS NULL AND replaced_at IS NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("OR (state = 'replaced' AND replaced_reason IS NOT NULL AND replaced_at IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT ck_password_key_id_matches_key CHECK (key_id = left(encode(sha256(public_key), 'hex'), 16))", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTwoPinTables_HaveRowSecurityOn_WithNoPolicy()
    {
        var sql = Statements();
        Assert.Contains("ALTER TABLE config.legacy_secret_pin ENABLE ROW LEVEL SECURITY;", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.legacy_secret_pin_marker ENABLE ROW LEVEL SECURITY;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE POLICY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheProbeCarriesTheKeyTable_AsTheNewestArm_AndMapsFullyMigratedToTheNewestRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "to_regclass('config.password_key') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.LastIndexOf("EXISTS", StringComparison.Ordinal) < probe.IndexOf(arm, StringComparison.Ordinal),
            "the new arm is the probe's last EXISTS, so it reads at the next ordinal");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal(ProbeOrdinal, parameters.Length - 1);
        Assert.Equal("hasPasswordKey", parameters[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(PgMigrations.Scripts[^1].Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PgMigrations.Scripts[^1].Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasPasswordKey)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasQueryStatsHourLedger)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && thisArm < previousArm, "this arm sits above the previous rung's");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        var armProseStart = viewer.LastIndexOf("/* V165 (#5366)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain("password_key", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesTheTables_LeavesOnePendingMarker_AndRerunningChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(4L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'config' AND c.relkind = 'r' AND c.relname IN ('password_key','password_key_service','legacy_secret_pin','legacy_secret_pin_marker')", ct));
            Assert.Equal(8L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid WHERE NOT t.tgisinternal AND p.proname = 'password_key_owner_only'", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_marker WHERE id = 1 AND state = 'pending'", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_marker", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM config.password_key", ct));

            /* The function is not executable by PUBLIC. */
            Assert.Equal(false, await ScalarAsync(connection,
                "SELECT has_function_privilege('public', 'config.password_key_owner_only()', 'EXECUTE')", ct));

            /* A re-run (the rung applied again) keeps the marker row exactly as it was. */
            await ExecAsync(connection, "UPDATE config.legacy_secret_pin_marker SET state = 'done', changed_at = '2026-01-05 10:00:00'", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + PasswordKeyTables.RungVersion, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_marker WHERE id = 1 AND state = 'done' AND changed_at = '2026-01-05 10:00:00'", ct));
            Assert.Equal(8L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid WHERE NOT t.tgisinternal AND p.proname = 'password_key_owner_only'", ct));

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
