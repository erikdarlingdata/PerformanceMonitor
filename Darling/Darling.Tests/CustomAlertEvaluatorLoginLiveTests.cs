/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and a login with a per-run
   name, and touches nothing on the shared one, so it is not serialized against the live-postgres collection. */

/// <summary>The custom alert evaluator's sign-in on a bring-your-own store against a real server (#9760): a login the
/// store accepts is returned as written, and a login the store refuses keeps the evaluator down with the store's own
/// error in the warning and never the password. The refused login is a NOLOGIN role, which every store refuses, a
/// trust-authenticated one included. A wrong password is checked too wherever the store checks passwords.</summary>
public sealed class CustomAlertEvaluatorLoginLiveTests
{
    [Fact]
    public async Task ConfiguredCustomAlertLoginAsync_AcceptsAGoodLogin_AndKeepsARefusedLoginDown_WithTheStoresError()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live custom alert evaluator sign-in test (it mints its own scratch database and login).");

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var loginName = "alert_web_" + suffix;
        var noLoginName = "alert_nologin_" + suffix;
        var password = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        var wrongPassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var create = owner.CreateCommand(
                $"CREATE ROLE {loginName} LOGIN NOSUPERUSER PASSWORD '{password}'; CREATE ROLE {noLoginName} NOLOGIN NOSUPERUSER PASSWORD '{password}'"))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            string As(string user, string pw) => new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = user,
                Password = pw,
                Pooling = false,
            }.ConnectionString;

            var accepted = new CapturingTestLogger();
            var good = As(loginName, password);
            Assert.Equal(good, await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                new PostgresConfig { WebConnectionString = good }, accepted, ct));
            Assert.Equal(0, accepted.CountAtLevel(LogLevel.Warning));
            Assert.Equal(0, accepted.CountAtLevel(LogLevel.Error));

            var refused = new CapturingTestLogger();
            Assert.Null(await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                new PostgresConfig { WebConnectionString = As(noLoginName, password) }, refused, ct));
            Assert.Equal(1, refused.CountAtLevel(LogLevel.Warning));
            Assert.Contains("postgres.webConnectionString", refused.Joined, StringComparison.Ordinal);
            Assert.Contains(noLoginName + " login", refused.Joined, StringComparison.Ordinal);
            Assert.Contains("is not permitted to log in", refused.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain(password, refused.Joined, StringComparison.Ordinal);

            if (await StoreRefusesAWrongPasswordAsync(As(loginName, wrongPassword), ct))
            {
                var wrong = new CapturingTestLogger();
                Assert.Null(await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                    new PostgresConfig { WebConnectionString = As(loginName, wrongPassword) }, wrong, ct));
                Assert.Equal(1, wrong.CountAtLevel(LogLevel.Warning));
                Assert.Contains(loginName + " login", wrong.Joined, StringComparison.Ordinal);
                Assert.Contains("password authentication failed", wrong.Joined, StringComparison.Ordinal);
                Assert.DoesNotContain(wrongPassword, wrong.Joined, StringComparison.Ordinal);
                Assert.DoesNotContain(password, wrong.Joined, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {loginName}; DROP ROLE IF EXISTS {noLoginName}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>Whether the store checks the password for this address. A store with trust authentication (CI's
    /// throwaway server) accepts any password, so the wrong-password case can only be shown where this is true.</summary>
    private static async Task<bool> StoreRefusesAWrongPasswordAsync(string wrongPasswordConnectionString, CancellationToken ct)
    {
        await using var probe = new NpgsqlConnection(wrongPasswordConnectionString);
        try
        {
            await probe.OpenAsync(ct);
            return false;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InvalidPassword)
        {
            return true;
        }
    }
}
