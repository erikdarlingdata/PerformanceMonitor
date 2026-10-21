/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one,
   so it is not serialized against the live-postgres collection. */

/// <summary>
/// A server's remediation login follows darling.json on every start when the entry names both keys (#6262). The seed reads the two keys
/// only for a new row, so a change to an existing server was dropped without a word. The sync never clears a login, writes only to the
/// registered server that pairs with the entry and connects the same way, and never lets one failing row stop the next. These tests start the store the way the service does
/// (<see cref="StoreConfigProvider.SeedIfEmptyAsync"/>), restart it with a changed file, and read the registry row back.
/// </summary>
public sealed class RemediationLoginSyncTests
{
    private const string FirstReference = "env:ALPHA_REMEDIATION_PASSWORD_6262";
    private const string SecondReference = "file:/run/secrets/alpha-remediation-6262";
    private const string ThirdReference = "env:BRAVO_REMEDIATION_PASSWORD_6262";

    private static string Json(params object[] servers) => JsonSerializer.Serialize(new { servers });

    private static object Entry(string name, string? user = null, string? secret = null)
    {
        var entry = new Dictionary<string, object> { ["name"] = name, ["host"] = name + ".example.test" };
        if (user is not null)
        {
            entry["remediationUsername"] = user;
        }

        if (secret is not null)
        {
            entry["remediationEncryptedPassword"] = secret;
        }

        return entry;
    }

    private static async Task<(string? User, string? Secret)> SlotAsync(NpgsqlDataSource owner, string name, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(
            "SELECT remediation_username, remediation_encrypted_password FROM config_monitored_servers WHERE name = $1");
        command.Parameters.AddWithValue(name);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "no registry row for " + name);
        return (reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>An existing server gets a login added or replaced by a restart with a changed darling.json; an entry without the keys leaves
    /// the login it has (it was set in the store, or the keys were removed, and neither disarms anything); a plain password is not written;
    /// a server added elsewhere is never touched; the log says what happened and never prints the login or the secret.</summary>
    [Fact]
    public async Task ARestartWithAChangedFile_SetsAndReplacesTheLogin_NeverClearsOne_AndLeavesOtherServersAlone()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        /* First start: alpha has no login, bravo has one, charlie has one. */
        var first = DarlingConfig.Parse(Json(
            Entry("alpha-sql-01"),
            Entry("bravo-sql-01", "bravo_fix", ThirdReference),
            Entry("charlie-sql-01", "charlie_fix", FirstReference),
            Entry("delta-sql-01")));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(first, ct);
        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));

        /* A server added from the web or MCP after the first start: not in darling.json, with a login of its own. */
        await using (var insert = owner.CreateCommand(
            "INSERT INTO config_monitored_servers (server_id, name, host, remediation_username, remediation_encrypted_password) "
            + "VALUES (6262001, 'web-added-sql-01', 'web-added-sql-01.example.test', 'web_fix', 'env:WEB_ADDED_PASSWORD_6262')"))
        {
            await insert.ExecuteNonQueryAsync(ct);
        }

        logger.Lines.Clear();

        /* Restart with a changed file: alpha gains a login, bravo's secret is replaced, charlie loses both keys, delta names a plain password. */
        var second = DarlingConfig.Parse(Json(
            Entry("alpha-sql-01", "alpha_fix", SecondReference),
            Entry("bravo-sql-01", "bravo_fix", FirstReference),
            Entry("charlie-sql-01"),
            Entry("delta-sql-01", "delta_fix", "hunter2-plain-6262")));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(second, ct);

        Assert.Equal(("alpha_fix", SecondReference), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal(("bravo_fix", FirstReference), await SlotAsync(owner, "bravo-sql-01", ct));
        Assert.Equal(("charlie_fix", FirstReference), await SlotAsync(owner, "charlie-sql-01", ct));
        Assert.Equal((null, null), await SlotAsync(owner, "delta-sql-01", ct));
        Assert.Equal(("web_fix", "env:WEB_ADDED_PASSWORD_6262"), await SlotAsync(owner, "web-added-sql-01", ct));

        var said = string.Join("\n", logger.Lines);
        Assert.Contains("'alpha-sql-01' set", said, StringComparison.Ordinal);
        Assert.Contains("'bravo-sql-01' changed", said, StringComparison.Ordinal);
        Assert.DoesNotContain("charlie-sql-01", said, StringComparison.Ordinal);
        Assert.Contains("'delta-sql-01' not applied", said, StringComparison.Ordinal);
        Assert.Contains("plain text value", said, StringComparison.Ordinal);
        Assert.DoesNotContain("web-added-sql-01", said, StringComparison.Ordinal);
        foreach (var secret in new[] { FirstReference, SecondReference, ThirdReference, "ALPHA_REMEDIATION", "BRAVO_REMEDIATION", "alpha_fix", "bravo_fix", "charlie_fix", "delta_fix", "hunter2" })
        {
            Assert.DoesNotContain(secret, said, StringComparison.Ordinal);
        }

        /* A third start with the same file changes nothing and says nothing. */
        logger.Lines.Clear();
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(second, ct);
        Assert.DoesNotContain(logger.Lines, line => line.Contains("Remediation login", StringComparison.Ordinal) && !line.Contains("delta-sql-01", StringComparison.Ordinal));
    }

    /// <summary>One key alone in the file changes nothing: the login the server has stays, and the log says why. (A half login written as it
    /// stood used to unarm the server; removing keys no longer disarms anything.)</summary>
    [Fact]
    public async Task OneKeyAlone_LeavesTheLoginAsItIs_AndTheLogSaysSo()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(
            DarlingConfig.Parse(Json(Entry("alpha-sql-01", "alpha_fix", FirstReference))), ct);
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(
            DarlingConfig.Parse(Json(Entry("alpha-sql-01", "alpha_fix"))), ct);

        var (user, secret) = await SlotAsync(owner, "alpha-sql-01", ct);
        Assert.Equal("alpha_fix", user);
        Assert.Equal(FirstReference, secret);
        Assert.Contains(logger.Lines, line => line.Contains("'alpha-sql-01' not changed", StringComparison.Ordinal)
            && line.Contains("only one of", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, line => line.Contains("alpha_fix", StringComparison.Ordinal) || line.Contains(FirstReference, StringComparison.Ordinal));
    }

    private static object HypoEntry(string name, string? user = null, string? secret = null)
    {
        var entry = new Dictionary<string, object> { ["name"] = name, ["host"] = name + ".example.test" };
        if (user is not null)
        {
            entry["hypotheticalIndexUsername"] = user;
        }

        if (secret is not null)
        {
            entry["hypotheticalIndexEncryptedPassword"] = secret;
        }

        return entry;
    }

    private static async Task<(string? User, string? Secret)> HypoSlotAsync(NpgsqlDataSource owner, string name, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(
            "SELECT hypothetical_index_username, hypothetical_index_encrypted_password FROM config_monitored_servers WHERE name = $1");
        command.Parameters.AddWithValue(name);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "no registry row for " + name);
        return (reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>The "Test an index" login (#6170) follows darling.json on a restart when BOTH keys are in the file: set, then replaced.
    /// It is never cleared and never half written, because an owner's UPDATE on the store row is a documented way to set it; a server
    /// that is not in the file is untouched; the log names the server and the secret's shape and never a login or a reference; and the
    /// row the sync writes still earns the "declared by the file" mark that lets darling.json name one of Darling's own files.</summary>
    [Fact]
    public async Task HypotheticalIndexLogin_FollowsTheFileWhenBothKeysArePresent_AndIsNeverClearedOrHalfWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        /* First start: alpha has none, bravo and charlie have a login, delta and echo have none. */
        var first = DarlingConfig.Parse(Json(
            HypoEntry("alpha-sql-01"),
            HypoEntry("bravo-sql-01", "bravo_check", ThirdReference),
            HypoEntry("charlie-sql-01", "charlie_check", FirstReference),
            HypoEntry("delta-sql-01"),
            HypoEntry("echo-sql-01")));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(first, ct);

        /* An owner set delta's login with an UPDATE (the README's way); another server was added outside the file with its own. */
        await using (var update = owner.CreateCommand(
            "UPDATE config_monitored_servers SET hypothetical_index_username = 'delta_check', "
            + "hypothetical_index_encrypted_password = 'env:DELTA_CHECK_PASSWORD_6270' WHERE name = 'delta-sql-01'"))
        {
            await update.ExecuteNonQueryAsync(ct);
        }

        await using (var insert = owner.CreateCommand(
            "INSERT INTO config_monitored_servers (server_id, name, host, hypothetical_index_username, hypothetical_index_encrypted_password) "
            + "VALUES (6270001, 'web-added-sql-01', 'web-added-sql-01.example.test', 'web_check', 'env:WEB_ADDED_CHECK_PASSWORD_6270')"))
        {
            await insert.ExecuteNonQueryAsync(ct);
        }

        logger.Lines.Clear();

        /* Restart: alpha gains a login, bravo's secret is replaced, charlie loses both keys in the file, delta has no keys in the file,
           echo has only a username in the file. */
        var second = DarlingConfig.Parse(Json(
            HypoEntry("alpha-sql-01", "alpha_check", SecondReference),
            HypoEntry("bravo-sql-01", "bravo_check", FirstReference),
            HypoEntry("charlie-sql-01"),
            HypoEntry("delta-sql-01"),
            HypoEntry("echo-sql-01", "echo_check")));
        var provider = new StoreConfigProvider(owner, logger);
        await provider.SeedIfEmptyAsync(second, ct);

        Assert.Equal(("alpha_check", SecondReference), await HypoSlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal(("bravo_check", FirstReference), await HypoSlotAsync(owner, "bravo-sql-01", ct));
        Assert.Equal(("charlie_check", FirstReference), await HypoSlotAsync(owner, "charlie-sql-01", ct));
        Assert.Equal(("delta_check", "env:DELTA_CHECK_PASSWORD_6270"), await HypoSlotAsync(owner, "delta-sql-01", ct));
        Assert.Equal((null, null), await HypoSlotAsync(owner, "echo-sql-01", ct));
        Assert.Equal(("web_check", "env:WEB_ADDED_CHECK_PASSWORD_6270"), await HypoSlotAsync(owner, "web-added-sql-01", ct));

        var said = string.Join("\n", logger.Lines);
        Assert.Contains("Hypothetical-index login for server 'alpha-sql-01' set", said, StringComparison.Ordinal);
        Assert.Contains("Hypothetical-index login for server 'bravo-sql-01' changed", said, StringComparison.Ordinal);
        Assert.Contains("env reference", said, StringComparison.Ordinal);
        Assert.DoesNotContain("Remediation login", said, StringComparison.Ordinal);
        Assert.Contains("'echo-sql-01' not changed", said, StringComparison.Ordinal);
        foreach (var untouched in new[] { "charlie-sql-01", "delta-sql-01", "web-added-sql-01" })
        {
            Assert.DoesNotContain(untouched, said, StringComparison.Ordinal);
        }

        foreach (var secret in new[]
                 {
                     FirstReference, SecondReference, ThirdReference, "ALPHA_REMEDIATION", "BRAVO_REMEDIATION", "DELTA_CHECK", "WEB_ADDED_CHECK",
                     "alpha_check", "bravo_check", "charlie_check", "delta_check", "echo_check", "web_check",
                 })
        {
            Assert.DoesNotContain(secret, said, StringComparison.Ordinal);
        }

        /* The row the sync wrote equals the file's text, so the "declared by the file" mark (which lets darling.json name one of
           Darling's own files for this server) is still earned; before the sync it was not (bravo's row held the old reference). */
        var view = await provider.LoadViewAsync(second, ct);
        Assert.NotNull(view);
        var servers = view!.EnabledServers.ToDictionary(s => s.DisplayName, StringComparer.OrdinalIgnoreCase);
        Assert.True(servers["alpha-sql-01"].HypotheticalIndexEncryptedPasswordDeclaredByFile);
        Assert.True(servers["bravo-sql-01"].HypotheticalIndexEncryptedPasswordDeclaredByFile);

        /* A third start with the same file changes nothing and says nothing. */
        logger.Lines.Clear();
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(second, ct);
        Assert.DoesNotContain(logger.Lines, line => line.Contains("Hypothetical-index login", StringComparison.Ordinal) && !line.Contains("echo-sql-01", StringComparison.Ordinal));
    }

    /// <summary>A sync that fails says so with the exception's type and SQLSTATE and nothing else: a PostgreSQL error can carry the row's
    /// values, and the row holds a secret reference and a login. The trigger below makes the store refuse the write with a message that
    /// repeats the new values, so a log line built from the message would carry them.</summary>
    [Fact]
    public async Task AFailingSync_LogsTheErrorTypeAndSqlState_AndNoSecretReferenceOrLogin()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(
            DarlingConfig.Parse(Json(HypoEntry("alpha-sql-01"), Entry("bravo-sql-01"))), ct);

        foreach (var statement in new[]
                 {
                     "CREATE FUNCTION refuse_login_6270() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                     + "RAISE EXCEPTION 'refused % / % / % / %', NEW.hypothetical_index_username, NEW.hypothetical_index_encrypted_password, "
                     + "NEW.remediation_username, NEW.remediation_encrypted_password; END $$",
                     "CREATE TRIGGER refuse_login_6270 BEFORE UPDATE OF hypothetical_index_username, remediation_username "
                     + "ON config_monitored_servers FOR EACH ROW EXECUTE FUNCTION refuse_login_6270()",
                 })
        {
            await using var command = owner.CreateCommand(statement);
            await command.ExecuteNonQueryAsync(ct);
        }

        logger.Lines.Clear();
        var config = DarlingConfig.Parse(JsonSerializer.Serialize(new
        {
            servers = new object[]
            {
                new Dictionary<string, object>
                {
                    ["name"] = "alpha-sql-01", ["host"] = "alpha-sql-01.example.test",
                    ["hypotheticalIndexUsername"] = "alpha_check", ["hypotheticalIndexEncryptedPassword"] = SecondReference,
                },
                new Dictionary<string, object>
                {
                    ["name"] = "bravo-sql-01", ["host"] = "bravo-sql-01.example.test",
                    ["remediationUsername"] = "bravo_fix", ["remediationEncryptedPassword"] = ThirdReference,
                },
            },
        }));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(config, ct);

        Assert.Equal((null, null), await HypoSlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal((null, null), await SlotAsync(owner, "bravo-sql-01", ct));

        var failures = logger.Lines.Where(line => line.Contains("Could not sync", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, failures.Count);
        foreach (var line in failures)
        {
            Assert.Contains("SQLSTATE P0001", line, StringComparison.Ordinal);
            Assert.Contains("PostgresException", line, StringComparison.Ordinal);
        }

        Assert.Contains(failures, line => line.Contains("'alpha-sql-01'", StringComparison.Ordinal));
        Assert.Contains(failures, line => line.Contains("'bravo-sql-01'", StringComparison.Ordinal));

        var said = string.Join("\n", logger.Lines);
        foreach (var secret in new[] { SecondReference, ThirdReference, "alpha_check", "bravo_fix", "ALPHA_REMEDIATION", "BRAVO_REMEDIATION", "alpha-remediation" })
        {
            Assert.DoesNotContain(secret, said, StringComparison.Ordinal);
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource owner, string sql, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static DarlingConfig OneServer(string name, string user, string secret)
    {
        return DarlingConfig.Parse(Json(Entry(name, user, secret)));
    }

    /// <summary>A row whose host was changed by a direct store UPDATE since darling.json was written is not armed on the next start: the
    /// login would go to a host the file never named. Both slots, and the log names the server and not the login or the reference.</summary>
    [Fact]
    public async Task ARowWhoseHostChanged_IsNotArmed_ByTheNextStart()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(DarlingConfig.Parse(Json(Entry("alpha-sql-01"))), ct);
        await ExecAsync(owner, "UPDATE config_monitored_servers SET host = 'elsewhere.example.test' WHERE name = 'alpha-sql-01'", ct);
        logger.Lines.Clear();

        var config = DarlingConfig.Parse(Json(new Dictionary<string, object>
        {
            ["name"] = "alpha-sql-01", ["host"] = "alpha-sql-01.example.test",
            ["remediationUsername"] = "alpha_fix", ["remediationEncryptedPassword"] = FirstReference,
            ["hypotheticalIndexUsername"] = "alpha_check", ["hypotheticalIndexEncryptedPassword"] = SecondReference,
        }));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(config, ct);

        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal((null, null), await HypoSlotAsync(owner, "alpha-sql-01", ct));
        var said = string.Join("\n", logger.Lines);
        Assert.Contains("'alpha-sql-01' not applied: the registered server connects differently", said, StringComparison.Ordinal);
        foreach (var secret in new[] { FirstReference, SecondReference, "alpha_fix", "alpha_check" })
        {
            Assert.DoesNotContain(secret, said, StringComparison.Ordinal);
        }
    }

    /// <summary>A server removed and added again under the same name at another host is not armed with the old entry's login: the name pairs,
    /// the connection does not.</summary>
    [Fact]
    public async Task AServerRemovedAndReAddedAtAnotherHost_IsNotArmed()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(DarlingConfig.Parse(Json(Entry("alpha-sql-01"))), ct);
        await ExecAsync(owner, "DELETE FROM config_monitored_servers WHERE name = 'alpha-sql-01'", ct);
        await ExecAsync(owner,
            "INSERT INTO config_monitored_servers (server_id, name, host) VALUES (6262002, 'alpha-sql-01', 'other-host.example.test')", ct);
        logger.Lines.Clear();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(OneServer("alpha-sql-01", "alpha_fix", FirstReference), ct);

        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Contains(logger.Lines, line => line.Contains("'alpha-sql-01' not applied: the registered server connects differently", StringComparison.Ordinal));
    }

    /// <summary>A row that is edited between the sync's read and its write is left as the edit made it: the write is conditional on the
    /// row's modified_at. The next start plans it again.</summary>
    [Fact]
    public async Task ARowEditedBetweenTheReadAndTheWrite_IsNotOverwritten()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();
        var provider = new StoreConfigProvider(owner, logger);

        await provider.SeedIfEmptyAsync(DarlingConfig.Parse(Json(Entry("alpha-sql-01"))), ct);
        var config = OneServer("alpha-sql-01", "alpha_fix", FirstReference);
        logger.Lines.Clear();

        await using (var connection = await owner.OpenConnectionAsync(ct))
        {
            await provider.SyncRemediationLoginsAsync(
                connection, config, ct,
                afterRead: token => ExecAsync(
                    owner,
                    "UPDATE config_monitored_servers SET host = 'moved.example.test', modified_at = modified_at + interval '1 second' WHERE name = 'alpha-sql-01'",
                    token));
        }

        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Contains(logger.Lines, line => line.Contains("'alpha-sql-01' not applied: the registry row changed", StringComparison.Ordinal));

        /* The next start sees the moved host and still does not arm the row. */
        logger.Lines.Clear();
        await provider.SeedIfEmptyAsync(config, ct);
        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Contains(logger.Lines, line => line.Contains("connects differently", StringComparison.Ordinal));
    }

    /// <summary>One row the store refuses does not stop the next server: the failure is logged with the server name, the exception type and
    /// the SQLSTATE, and the following server in the file is written.</summary>
    [Fact]
    public async Task AFailingRow_DoesNotStopTheNextServer()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var logger = new CapturingLogger();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(
            DarlingConfig.Parse(Json(Entry("alpha-sql-01"), Entry("bravo-sql-01"), Entry("charlie-sql-01"))), ct);
        await ExecAsync(owner,
            "CREATE FUNCTION refuse_boom_6262() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            + "IF NEW.remediation_username = 'boom_fix' THEN RAISE EXCEPTION 'refused %', NEW.remediation_username; END IF; RETURN NEW; END $$", ct);
        await ExecAsync(owner,
            "CREATE TRIGGER refuse_boom_6262 BEFORE UPDATE OF remediation_username ON config_monitored_servers "
            + "FOR EACH ROW EXECUTE FUNCTION refuse_boom_6262()", ct);
        logger.Lines.Clear();

        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(
            DarlingConfig.Parse(Json(
                Entry("alpha-sql-01", "boom_fix", FirstReference),
                Entry("bravo-sql-01", "bravo_fix", SecondReference),
                Entry("charlie-sql-01", "charlie_fix", ThirdReference))), ct);

        Assert.Equal((null, null), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal(("bravo_fix", SecondReference), await SlotAsync(owner, "bravo-sql-01", ct));
        Assert.Equal(("charlie_fix", ThirdReference), await SlotAsync(owner, "charlie-sql-01", ct));
        var failure = Assert.Single(logger.Lines, line => line.Contains("Could not sync", StringComparison.Ordinal));
        Assert.Contains("'alpha-sql-01'", failure, StringComparison.Ordinal);
        Assert.Contains("PostgresException", failure, StringComparison.Ordinal);
        Assert.Contains("SQLSTATE P0001", failure, StringComparison.Ordinal);
        Assert.DoesNotContain("boom_fix", string.Join("\n", logger.Lines), StringComparison.Ordinal);
    }

    /// <summary>A DPAPI blob as the remediation secret is warned about by <c>--validate-config</c>, with the server's name and the
    /// shape and not the value; a reference and an unarmed entry are not.</summary>
    [Fact]
    public async Task ValidateConfig_WarnsAboutADpapiRemediationSecret_AndNotAboutAReference()
    {
        const string blob = "AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAAdpapi6262blobXYZ==";
        var root = Directory.CreateTempSubdirectory("darling-remediation-6262-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            var closedPort = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
            closedPort.Start();
            var port = ((System.Net.IPEndPoint)closedPort.LocalEndpoint).Port;
            closedPort.Stop();
            await File.WriteAllTextAsync(configPath, JsonSerializer.Serialize(new
            {
                postgres = new { connectionString = $"Host=127.0.0.1;Port={port};Username=darling;Database=darlingtest;Timeout=2" },
                servers = new object[]
                {
                    Entry("alpha-sql-01", "alpha_fix", blob),
                    Entry("bravo-sql-01", "bravo_fix", FirstReference),
                    Entry("charlie-sql-01", "charlie_fix"),
                },
            }), TestContext.Current.CancellationToken);

            var output = new StringWriter();
            await DarlingCliCommands.ValidateConfigAsync(configPath, output, new StringWriter(), CancellationToken.None);

            var text = output.ToString();
            var warnings = text.Split('\n').Where(line => line.StartsWith("WARNING:", StringComparison.Ordinal)).ToList();
            var warning = Assert.Single(warnings);
            Assert.Contains("alpha-sql-01", warning, StringComparison.Ordinal);
            Assert.Contains("DPAPI blob", warning, StringComparison.Ordinal);
            Assert.Contains("env:", warning, StringComparison.Ordinal);
            Assert.DoesNotContain(blob, text, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }
}
