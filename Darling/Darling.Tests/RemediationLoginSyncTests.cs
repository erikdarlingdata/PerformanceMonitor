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
/// A server's remediation login follows darling.json on every start (#6262). The seed reads the two keys only for a new row, so a
/// change to an existing server was dropped without a word. These tests start the store the way the service does
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

    /// <summary>An existing server gets a login added, replaced or removed by a restart with a changed darling.json; a server added
    /// elsewhere is never touched; the log says what happened and never prints the login or the secret.</summary>
    [Fact]
    public async Task ARestartWithAChangedFile_SetsReplacesAndClearsTheLogin_AndLeavesOtherServersAlone()
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
            Entry("charlie-sql-01", "charlie_fix", FirstReference)));
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

        /* Restart with a changed file: alpha gains a login, bravo's secret is replaced, charlie loses both keys. */
        var second = DarlingConfig.Parse(Json(
            Entry("alpha-sql-01", "alpha_fix", SecondReference),
            Entry("bravo-sql-01", "bravo_fix", FirstReference),
            Entry("charlie-sql-01")));
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(second, ct);

        Assert.Equal(("alpha_fix", SecondReference), await SlotAsync(owner, "alpha-sql-01", ct));
        Assert.Equal(("bravo_fix", FirstReference), await SlotAsync(owner, "bravo-sql-01", ct));
        Assert.Equal((null, null), await SlotAsync(owner, "charlie-sql-01", ct));
        Assert.Equal(("web_fix", "env:WEB_ADDED_PASSWORD_6262"), await SlotAsync(owner, "web-added-sql-01", ct));

        var said = string.Join("\n", logger.Lines);
        Assert.Contains("'alpha-sql-01' set", said, StringComparison.Ordinal);
        Assert.Contains("'bravo-sql-01' changed", said, StringComparison.Ordinal);
        Assert.Contains("'charlie-sql-01' cleared", said, StringComparison.Ordinal);
        Assert.DoesNotContain("web-added-sql-01", said, StringComparison.Ordinal);
        foreach (var secret in new[] { FirstReference, SecondReference, ThirdReference, "ALPHA_REMEDIATION", "BRAVO_REMEDIATION", "alpha_fix", "bravo_fix", "charlie_fix" })
        {
            Assert.DoesNotContain(secret, said, StringComparison.Ordinal);
        }

        /* A third start with the same file changes nothing and says nothing. */
        logger.Lines.Clear();
        await new StoreConfigProvider(owner, logger).SeedIfEmptyAsync(second, ct);
        Assert.DoesNotContain(logger.Lines, line => line.Contains("Remediation login", StringComparison.Ordinal));
    }

    /// <summary>One key alone in the file is written as it is, which leaves the server unarmed, and the log says why.</summary>
    [Fact]
    public async Task OneKeyAlone_LeavesTheServerUnarmed_AndTheLogSaysSo()
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
        Assert.Null(secret);
        Assert.Contains(logger.Lines, line => line.Contains("'alpha-sql-01' changed", StringComparison.Ordinal)
            && line.Contains("unarmed", StringComparison.Ordinal));
        Assert.Equal(ServerCredentialState.Missing, InvestigationRequestStore.ClassifyCredential(user, secret));
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
