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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the facts mint their own scratch database through ScratchPostgres and never touch another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6334 security review: the known-pattern store accepts only rows the app can read, even from a caller that skips the app and calls the
/// write functions directly with the viewer password, and a row that cannot be read never breaks the read of the others.
/// </summary>
public sealed class KnownPatternStoreHardeningLiveTests
{
    private const int ServerId = 7_633_411;
    private const string RolePassword = "known-pattern-hardening-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        await using var command = source.CreateCommand("SELECT count(*) FROM config.config_known_patterns");
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, NpgsqlDataSource, string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the known-pattern store hardening live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "kp_hard_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "LcKnownHardenSrv", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'kp-a', 'kp-a', TRUE) ON CONFLICT DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct, DarlingManagedRoles.BuildKnownPatternFunctionsSql("config"));
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "viewer"))
            {
                await ExecAsync(owner, statement, ct);
            }

            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.insert_known_pattern({DarlingManagedRoles.InsertKnownPatternSignature}) TO {roleName}", ct);
            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.update_known_pattern({DarlingManagedRoles.UpdateKnownPatternSignature}) TO {roleName}", ct);

            await using var viewer = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString);

            await body(owner, viewer, roleName, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}", connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }

    /// <summary>An insert call with every argument spelled as SQL text, so a value the typed store would never send can be sent.</summary>
    private static string InsertCall(string server = "NULL", string metric = "NULL", string time = "NULL", string zone = "NULL",
        string note = "'n'", string createdBy = "NULL", string expires = "NULL") =>
        $"SELECT config.insert_known_pattern({server}, {metric}, {time}, {zone}, 30, {note}, {createdBy}, {expires})";

    [Fact]
    public async Task ADirectCallToTheInsertFunction_WithAValueTheAppCannotRead_IsRefused_AndWritesNothing()
    {
        await WithStoreAsync(async (owner, viewer, _, ct) =>
        {
            var refusals = new (string Sql, string SqlState)[]
            {
                (InsertCall(server: "987654"), "DL404"),
                (InsertCall(time: "'24:00'::time"), PostgresErrorCodes.CheckViolation),
                (InsertCall(expires: "'infinity'::timestamp"), PostgresErrorCodes.CheckViolation),
                (InsertCall(expires: "'-infinity'::timestamp"), PostgresErrorCodes.CheckViolation),
                (InsertCall(expires: "'10000-01-01'::timestamp"), PostgresErrorCodes.CheckViolation),
                (InsertCall(time: "'02:00'::time", zone: "'" + new string('a', 65) + "'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(time: "'02:00'::time", zone: "'Not a zone'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(time: "'02:00'::time", zone: "'../etc'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(metric: "'" + new string('m', 129) + "'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(createdBy: "'" + new string('c', 257) + "'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(note: "'   '"), PostgresErrorCodes.CheckViolation),
                (InsertCall(note: "E' \\t\\r\\n'"), PostgresErrorCodes.CheckViolation),
                (InsertCall(note: "'" + new string('x', 501) + "'"), PostgresErrorCodes.CheckViolation),
            };

            foreach (var (sql, expected) in refusals)
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer, sql, ct));
                Assert.True(expected == ex.SqlState, $"{sql[..Math.Min(sql.Length, 90)]} answered {ex.SqlState}, expected {expected}");
            }

            Assert.Equal(0, await CountAsync(owner, ct));

            // The same function takes a value inside every limit, and 23:59 and the largest allowed zone length are inside them.
            await ExecAsync(viewer, InsertCall(server: ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), metric: "'" + new string('m', 128) + "'",
                time: "'23:59'::time", zone: "'" + new string('a', 64) + "'", createdBy: "'" + new string('c', 256) + "'",
                expires: "'9999-12-31 23:59:59'::timestamp"), ct);
            await ExecAsync(viewer, InsertCall(time: "'02:00'::time", zone: "'America/Argentina/Buenos_Aires'"), ct);
            Assert.Equal(2, await CountAsync(owner, ct));

            // The owner's own direct write meets the same table limits.
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner,
                "INSERT INTO config.config_known_patterns (note, expires_at_utc) VALUES ('n', 'infinity')", ct));
        });
    }

    [Fact]
    public async Task AnAddPastTheEntryCap_IsRefusedWithTheCountAndTheCap_ForTheStoreAndADirectCall_AndAnEditAndDeleteStillWork()
    {
        await WithStoreAsync(async (owner, viewer, _, ct) =>
        {
            /* The viewer and web editors write through PgKnownPatternStore (as the viewer role); the function itself is the shared check. */
            await ExecAsync(owner, $"INSERT INTO config.config_known_patterns (note) SELECT 'bulk ' || g FROM generate_series(1, {KnownPatternMatcher.MaxEntries - 1}) g", ct);
            var store = new PgKnownPatternStore(viewer);
            var last = await store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "the 500th", null), "erik", ct);
            Assert.Equal(KnownPatternMatcher.MaxEntries, await CountAsync(owner, ct));

            var refused = await Assert.ThrowsAsync<PgKnownPatternStore.StoreRefusedException>(() =>
                store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "the 501st", null), "erik", ct));
            Assert.Equal("DL409", refused.SqlState);
            Assert.Equal("Known patterns are capped at 500 entries and 500 exist. Delete entries you no longer need to make room.", refused.Message);
            var direct = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer, InsertCall(), ct));
            Assert.Equal("DL409", direct.SqlState);
            Assert.Equal(KnownPatternMatcher.MaxEntries, await CountAsync(owner, ct));

            /* Edit and delete are not capped; a delete makes room for exactly one add. */
            Assert.True(await store.UpdateAsync(last, new KnownPatternInput(null, null, null, null, null, "edited at the cap", null), "priya", ct));
            Assert.True(await new PgKnownPatternStore(owner).DeleteAsync(last, ct));
            await store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "back to 500", null), "erik", ct);
            Assert.Equal(KnownPatternMatcher.MaxEntries, await CountAsync(owner, ct));
        });
    }

    [Fact]
    public async Task TheStore_TurnsAServerThatIsNotRegistered_IntoAnArgumentException_ForAnAddAndAnEdit()
    {
        await WithStoreAsync(async (owner, viewer, _, ct) =>
        {
            var store = new PgKnownPatternStore(viewer);
            var id = await store.InsertAsync(new KnownPatternInput(ServerId, null, null, null, null, "first", null), "erik", ct);

            var add = await Assert.ThrowsAsync<PgKnownPatternStore.StoreRefusedException>(() =>
                store.InsertAsync(new KnownPatternInput(987654, null, null, null, null, "ghost", null), "erik", ct));
            Assert.Contains("No monitored server", add.Message, StringComparison.Ordinal);
            var edit = await Assert.ThrowsAsync<PgKnownPatternStore.StoreRefusedException>(() =>
                store.UpdateAsync(id, new KnownPatternInput(987654, null, null, null, null, "ghost", null), "priya", ct));
            Assert.Contains("No monitored server", edit.Message, StringComparison.Ordinal);
            Assert.Equal("DL404", add.SqlState);
            Assert.Equal("DL404", edit.SqlState);

            var only = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Equal("first", only.Note);
            Assert.Null(only.UpdatedBy);
            Assert.Null(only.UpdatedAtUtc);
        });
    }

    [Fact]
    public async Task AnEdit_StampsTheEditorAndTheTime_AndKeepsTheOriginalAuthor_AndAnAddNeverSetsThem()
    {
        await WithStoreAsync(async (owner, viewer, _, ct) =>
        {
            var store = new PgKnownPatternStore(viewer);
            var id = await store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "first", null), "erik", ct);
            var added = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Null(added.UpdatedBy);
            Assert.Null(added.UpdatedAtUtc);

            var before = DateTime.UtcNow.AddMinutes(-1);
            Assert.True(await store.UpdateAsync(id, new KnownPatternInput(null, null, null, null, null, "second", null), "  priya  ", ct));
            var edited = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Equal("erik", edited.CreatedBy);
            Assert.Equal("priya", edited.UpdatedBy);
            Assert.True(edited.UpdatedAtUtc > before && edited.UpdatedAtUtc < DateTime.UtcNow.AddMinutes(1), "updated_at_utc must be the edit's UTC time");

            var listed = System.Text.Json.JsonDocument.Parse(await DarlingMcpKnownPatternTools.GetKnownPatterns(owner, cancellationToken: ct)).RootElement
                .GetProperty("known_patterns")[0];
            Assert.Equal("priya", listed.GetProperty("updated_by").GetString());
            Assert.False(string.IsNullOrEmpty(listed.GetProperty("updated_at_utc").GetString()));
        });
    }

    [Fact]
    public async Task ARowTheReaderCannotHold_IsSkippedAndLoggedOnce_AndEveryOtherRowStillReads()
    {
        await WithStoreAsync(async (owner, viewer, _, ct) =>
        {
            var good = new KnownPatternStoreIds();
            var store = new PgKnownPatternStore(viewer);
            good.First = await store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "good before", null), "erik", ct);

            /* Rows a store older than the table limits (or a restore) can hold: the owner removes the limits here to stand in for them. */
            await ExecAsync(owner, "ALTER TABLE config.config_known_patterns DROP CONSTRAINT ck_known_patterns_expires, DROP CONSTRAINT ck_known_patterns_time_of_day", ct);
            await ExecAsync(owner, "INSERT INTO config.config_known_patterns (id, note, expires_at_utc) OVERRIDING SYSTEM VALUE VALUES (6334001, 'far expiry', '200000-01-01')", ct);
            await ExecAsync(owner, "INSERT INTO config.config_known_patterns (id, note, time_of_day) OVERRIDING SYSTEM VALUE VALUES (6334002, 'late time', '24:00')", ct);

            good.Last = await store.InsertAsync(new KnownPatternInput(null, null, null, null, null, "good after", null), "erik", ct);

            var log = new CapturingTestLogger();
            var reader = new PgKnownPatternStore(owner, log);
            var first = await reader.LoadAllAsync(ct);
            Assert.Equal(new[] { good.First, good.Last }, first.Select(e => e.Id).ToArray());
            var second = await reader.LoadAllAsync(ct);
            Assert.Equal(first.Count, second.Count);

            Assert.Contains("6334001", log.Joined, StringComparison.Ordinal);
            Assert.Contains("6334002", log.Joined, StringComparison.Ordinal);
            Assert.Equal(2, log.CountAtLevel(LogLevel.Warning));

            // The alert-side loader goes through the same reader, so it returns the good rows rather than throwing.
            var forAlerts = await DarlingMcpKnownPatternTools.LoadForAlertsAsync(owner, ct);
            Assert.Equal(new[] { good.First, good.Last }, forAlerts.Select(e => e.Id).ToArray());
        });
    }

    private sealed class KnownPatternStoreIds
    {
        public long First { get; set; }

        public long Last { get; set; }
    }
}
