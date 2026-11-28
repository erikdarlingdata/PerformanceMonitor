/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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

    /// <summary>#9937: the evaluator runs user rules as the login <c>postgres.webConnectionString</c> names, and the owner-by-name warning
    /// cannot see a login that holds the <c>admin</c> role's grants. The sign-in is accepted and the login is still returned, so the
    /// evaluator runs, with one Warning that names the setting and the web role. The provisioned <c>web</c> login logs nothing, and a login
    /// that is the owner by name gets the owner warning alone, not a second one.</summary>
    [Fact]
    public async Task ConfiguredCustomAlertLoginAsync_WarnsOnceForALoginHoldingTheAdminGrants_AndNotForTheWebRole()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live custom alert evaluator sign-in test (it mints its own scratch database and login).");

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var adminLike = "alert_admin_" + suffix;
        var webLike = "alert_webrole_" + suffix;
        var password = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var create = owner.CreateCommand(
                $"CREATE ROLE {adminLike} LOGIN NOSUPERUSER PASSWORD '{password}'; CREATE ROLE {webLike} LOGIN NOSUPERUSER PASSWORD '{password}'"))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            foreach (var statement in ViewerGrantReplay.StatementsFor(adminLike, scratch.DatabaseName, "admin")
                .Concat(ViewerGrantReplay.StatementsFor(webLike, scratch.DatabaseName, "web")))
            {
                await using var grant = owner.CreateCommand(statement);
                await grant.ExecuteNonQueryAsync(ct);
            }

            string As(string user) => new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = user,
                Password = password,
                Pooling = false,
            }.ConnectionString;

            /* The owner string names a different user, so neither login is the owner by name. */
            var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = "someone_else_" + suffix }.ConnectionString;

            var adminLog = new CapturingTestLogger();
            var adminString = As(adminLike);
            Assert.Equal(adminString, await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                new PostgresConfig { ConnectionString = ownerString, WebConnectionString = adminString }, adminLog, ct));
            Assert.Equal(1, adminLog.CountAtLevel(LogLevel.Warning));
            Assert.Contains("custom alert evaluator", adminLog.Joined, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(adminLike + " login", adminLog.Joined, StringComparison.Ordinal);
            Assert.Contains("postgres.webConnectionString", adminLog.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain(password, adminLog.Joined, StringComparison.Ordinal);

            var webLog = new CapturingTestLogger();
            var webString = As(webLike);
            Assert.Equal(webString, await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                new PostgresConfig { ConnectionString = ownerString, WebConnectionString = webString }, webLog, ct));
            Assert.Equal(0, webLog.CountAtLevel(LogLevel.Warning));

            /* The owner by name: the owner warning is the one Warning, and the probe's is not added to it. */
            var ownerLog = new CapturingTestLogger();
            var ownerAsWeb = As(adminLike);
            Assert.Equal(ownerAsWeb, await DarlingStoreLogins.ResolveConfiguredCustomAlertLoginAsync(
                new PostgresConfig { ConnectionString = ownerAsWeb, WebConnectionString = ownerAsWeb }, ownerLog, ct));
            Assert.Equal(1, ownerLog.CountAtLevel(LogLevel.Warning));
            Assert.Contains("the owner login", ownerLog.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY {adminLike}; DROP ROLE IF EXISTS {adminLike}; DROP OWNED BY {webLike}; DROP ROLE IF EXISTS {webLike}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>#9937: a probe that could not run is a Warning that says so with its error class, and the Unprivileged verdict is none.</summary>
    [Fact]
    public void CustomAlertLoginProbeWarning_SaysAFailedProbeCouldNotConfirm_AndIsSilentForAnUnprivilegedLogin()
    {
        Assert.Null(DarlingStoreLogins.CustomAlertLoginProbeWarning(
            "web", new DarlingStoreLogins.LoginProbeResult(DarlingStoreLogins.LoginProbeVerdict.Unprivileged)));

        var failed = DarlingStoreLogins.CustomAlertLoginProbeWarning(
            "web", new DarlingStoreLogins.LoginProbeResult(DarlingStoreLogins.LoginProbeVerdict.Failed, "TimeoutException"));
        Assert.NotNull(failed);
        Assert.Contains("TimeoutException", failed, StringComparison.Ordinal);
        Assert.Contains("could not confirm", failed, StringComparison.Ordinal);
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
