/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>A logger that keeps every line, so a test can search the lines for a secret.</summary>
internal sealed class CapturingLogger : ILogger
{
    public List<string> Lines { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (Lines)
        {
            Lines.Add(formatter(state, exception) + (exception is null ? "" : " " + exception));
        }
    }
}

/// <summary>
/// The server write code, without a server (#5632, step 6): the sign-in refuses each bad slot with its own error and never repeats a secret,
/// the connection reaches the server the way the monitoring connection does under its own application name, a plan handle is read strictly,
/// and no write can be called without the gate's permit.
/// </summary>
public sealed class RemediationOfflineTests
{
    private const string Secret = "S3cret-Value-For-Tests!";

    private static SqlServerRemediationExecutor Executor(string? login, string? slot, CapturingLogger? logger = null, int serverId = 7) =>
        new(serverId, new RemediationServer("unreachable.invalid"), login, slot, logger);

    private static ServerActionPermit Permit(string serverAction, string? id, string? database = "db1", int serverId = 7) =>
        new(new ServerActionCall("tool", serverAction,
            new AgentAction(Guid.NewGuid(), Guid.NewGuid(), "agent-a", "why", "[]"),
            new AgentActionTarget(serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), id, database)));

    [Fact]
    public void EachSignInRefusal_HasItsOwnError_AndNoneRepeatsTheSlot()
    {
        var name = "PM_REM_UNSET_" + Guid.NewGuid().ToString("N");
        var cases = new (string? Login, string? Slot, RemediationLoginError Error)[]
        {
            (null, "env:X", RemediationLoginError.LoginMissing),
            ("  ", "env:X", RemediationLoginError.LoginMissing),
            ("svc", null, RemediationLoginError.SecretSlotMissing),
            ("svc", "", RemediationLoginError.SecretSlotMissing),
            ("svc", "AQAAANCMnd8BFdERjHoAwE-stored-" + Secret, RemediationLoginError.StoredPasswordRefused),
            ("svc", "sealed:" + Secret, RemediationLoginError.StoredPasswordRefused),
            ("svc", "ENV:" + name, RemediationLoginError.StoredPasswordRefused),
            ("svc", "env:" + name, RemediationLoginError.SecretEmpty),
            ("svc", "env:", RemediationLoginError.SecretUnreadable),
            ("svc", "file:", RemediationLoginError.SecretUnreadable),
            ("svc", "file:" + Path.Combine(Path.GetTempPath(), "pm-rem-missing-" + Guid.NewGuid().ToString("N")), RemediationLoginError.SecretUnreadable),
        };

        foreach (var (login, slot, error) in cases)
        {
            var resolution = RemediationLoginResolver.Resolve(login, slot);
            Assert.Null(resolution.Login);
            Assert.Equal(error, resolution.Error);
            Assert.False(string.IsNullOrWhiteSpace(resolution.Message));
            Assert.DoesNotContain(Secret, resolution.Message, StringComparison.Ordinal);
            if (slot is { Length: > 5 })
                Assert.DoesNotContain(slot, resolution.Message, StringComparison.Ordinal);
        }

        /* Five reasons, five distinct sentences. */
        Assert.Equal(5, cases.Select(c => RemediationLoginResolver.Resolve(c.Login, c.Slot)).GroupBy(r => r.Error).Select(g => g.First().Message).Distinct().Count());
    }

    [Fact]
    public void AnEmptyOrBlankSecret_IsRefused_FromAnEnvVariableAndFromAFile()
    {
        var name = "PM_REM_BLANK_" + Guid.NewGuid().ToString("N");
        var path = Path.Combine(Path.GetTempPath(), "pm-rem-" + Guid.NewGuid().ToString("N"));
        try
        {
            Environment.SetEnvironmentVariable(name, "   ");
            Assert.Equal(RemediationLoginError.SecretEmpty, RemediationLoginResolver.Resolve("svc", "env:" + name).Error);
            File.WriteAllText(path, " \r\n");
            Assert.Equal(RemediationLoginError.SecretEmpty, RemediationLoginResolver.Resolve("svc", "file:" + path).Error);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            File.Delete(path);
        }
    }

    [Fact]
    public void AReference_ResolvesFromTheEnvironment_AndFromATrimmedFile_AndTheLoginNeverPrintsTheSecret()
    {
        var name = "PM_REM_OK_" + Guid.NewGuid().ToString("N");
        var path = Path.Combine(Path.GetTempPath(), "pm-rem-" + Guid.NewGuid().ToString("N"));
        try
        {
            Environment.SetEnvironmentVariable(name, Secret);
            var fromEnv = RemediationLoginResolver.Resolve(" svc_fix ", "env:" + name);
            Assert.Equal("svc_fix", fromEnv.Login!.UserName);
            File.WriteAllText(path, Secret + "\r\n");
            var fromFile = RemediationLoginResolver.Resolve("svc_fix", "file:" + path);
            Assert.Equal(RemediationLoginError.None, fromFile.Error);
            Assert.Equal(fromEnv.Login.Secret, fromFile.Login!.Secret);
            Assert.Equal(Secret, fromFile.Login.Secret);
            Assert.DoesNotContain(Secret, fromEnv.Login.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(Secret, System.Text.Json.JsonSerializer.Serialize(fromEnv.Login, new System.Text.Json.JsonSerializerOptions { IncludeFields = true }),
                StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("env:NAME")]
    [InlineData("file:/run/secrets/x")]
    [InlineData("ENV:NAME")]
    [InlineData("xenv:NAME")]
    [InlineData(" env:NAME")]
    [InlineData("AQAAANCMnd8BFdERjHoAwE")]
    [InlineData("plain")]
    public void WhatIsAReference_AgreesWithTheServicesSecretSource_AndWithTheGate(string slot)
    {
        var refused = RemediationLoginResolver.Resolve("svc", slot).Error == RemediationLoginError.StoredPasswordRefused;
        Assert.Equal(!DarlingSecretSource.IsReference(slot), refused);
        Assert.Equal(DarlingSecretSource.IsReference(slot), InvestigationRequestStore.ClassifyCredential("svc", slot) == ServerCredentialState.Reference);
    }

    [Fact]
    public void TheRemediationConnection_ReachesTheServerLikeTheMonitoringOne_UnderItsOwnApplicationName()
    {
        var login = RemediationLoginResolverFor("svc_fix", Secret);
        foreach (var encrypt in new[] { "Strict", "Optional", "Mandatory", "garbage", null })
        {
            var server = new MonitoredServer
            {
                Name = "srv", Host = "sql.example,1444", Database = "app", Auth = "sql", Username = "monitor", Password = "x",
                EncryptMode = encrypt!, TrustServerCertificate = encrypt == "Optional", MultiSubnetFailover = true, ReadOnlyIntent = true,
                RemediationUsername = "svc_fix", RemediationEncryptedPassword = "env:X",
            };
            var service = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildRemediationConnectionString(server, Secret));
            var here = new SqlConnectionStringBuilder(RemediationConnection.BuildConnectionString(
                new RemediationServer("sql.example,1444", "app", encrypt, encrypt == "Optional", true), login));

            Assert.Equal(MonitoredServerConnection.RemediationApplicationName, RemediationConnection.ApplicationName);
            Assert.Equal(RemediationConnection.ApplicationName, here.ApplicationName);
            Assert.NotEqual("PerformanceMonitorDarling", here.ApplicationName);
            Assert.Equal(ApplicationIntent.ReadWrite, here.ApplicationIntent);
            Assert.False(here.Pooling);
            service.Pooling = false;
            foreach (string key in service.Keys)
                Assert.Equal(service[key]?.ToString(), here[key]?.ToString());
        }
    }

    private static RemediationLogin RemediationLoginResolverFor(string user, string secret)
    {
        var name = "PM_REM_CONN_" + Guid.NewGuid().ToString("N");
        try
        {
            Environment.SetEnvironmentVariable(name, secret);
            return RemediationLoginResolver.Resolve(user, "env:" + name).Login!;
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Theory]
    [InlineData("0x0600050031E6C2B0B0A1AE1C0A00000001000000000000000000000000000000000000000000000000000000", true)]
    [InlineData("0xAB", true)]
    [InlineData("0xab", true)]
    [InlineData("0X0600", true)]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("0x", false)]
    [InlineData("0x1", false)]
    [InlineData("0xGG", false)]
    [InlineData("06000500", false)]
    [InlineData("0x0600; DROP TABLE t", false)]
    [InlineData("0x0600\n", false)]
    [InlineData(" 0x0600", false)]
    [InlineData("0x0600,0x0700", false)]
    public void APlanHandle_IsReadStrictly(string? text, bool valid)
    {
        Assert.Equal(valid, PlanHandleText.TryParse(text, out var bytes));
        Assert.Equal(valid, bytes.Length > 0);
    }

    [Fact]
    public void APlanHandle_IsAtMostSixtyFourBytes()
    {
        Assert.True(PlanHandleText.TryParse("0x" + new string('A', 128), out var max));
        Assert.Equal(64, max.Length);
        Assert.False(PlanHandleText.TryParse("0x" + new string('A', 130), out _));
    }

    [Theory]
    [InlineData("0x")]
    [InlineData("0xZZ")]
    [InlineData("0x0600; DROP TABLE t")]
    [InlineData("12345")]
    public async Task AMalformedHandle_IsRefused_BeforeAnySecretIsReadOrAnyConnectionOpens(string handle)
    {
        var logger = new CapturingLogger();
        /* A server that cannot be reached and a slot that cannot resolve: had either step been reached the answer would be another reason. */
        var result = await Executor("svc", "env:PM_REM_NEVER_SET_" + Guid.NewGuid().ToString("N"), logger)
            .EvictPlanAsync(Permit(ServerActionNames.EvictPlan, handle), handle, dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        Assert.Equal(RemediationResult.Reasons.BadHandle, result.Reason);
        Assert.False(result.Wrote);
    }

    [Fact]
    public async Task ASessionId_MustBeAPositiveInteger()
    {
        var ct = TestContext.Current.CancellationToken;
        foreach (var id in new[] { 0, -1, int.MinValue })
        {
            var result = await Executor("svc", "env:PM_REM_NEVER_SET").KillSessionAsync(Permit(ServerActionNames.KillSession, id.ToString(CultureInfo.InvariantCulture)), id, DateTime.UtcNow, false, ct);
            Assert.Equal(RemediationResult.Reasons.BadSession, result.Reason);
        }
    }

    [Fact]
    public async Task APermitForAnotherAction_Server_Database_OrObject_IsRefused_BeforeTheSecretIsRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var executor = Executor("svc", "env:PM_REM_NEVER_SET_" + Guid.NewGuid().ToString("N"));
        var handle = "0x0600AA";
        var wrongPermits = new (string Case, Func<Task<RemediationResult>> Run)[]
        {
            ("force with an evict permit", () => executor.ForcePlanAsync(Permit(ServerActionNames.EvictPlan, "5"), 1, 5, false, ct)),
            ("force with another server's permit", () => executor.ForcePlanAsync(Permit(ServerActionNames.ForcePlan, "5", serverId: 8), 1, 5, false, ct)),
            ("force with another plan's permit", () => executor.ForcePlanAsync(Permit(ServerActionNames.ForcePlan, "6"), 1, 5, false, ct)),
            ("unforce with a kill permit", () => executor.UnforcePlanAsync(Permit(ServerActionNames.KillSession, "5"), 1, 5, false, ct)),
            ("evict with a force permit", () => executor.EvictPlanAsync(Permit(ServerActionNames.ForcePlan, handle), handle, false, ct)),
            ("evict with another handle's permit", () => executor.EvictPlanAsync(Permit(ServerActionNames.EvictPlan, "0x0600BB"), handle, false, ct)),
            ("kill with another session's permit", () => executor.KillSessionAsync(Permit(ServerActionNames.KillSession, "51"), 52, DateTime.UtcNow, false, ct)),
            ("kill with another server's permit", () => executor.KillSessionAsync(Permit(ServerActionNames.KillSession, "52", serverId: 9), 52, DateTime.UtcNow, false, ct)),
            ("kill with no permit", () => executor.KillSessionAsync(null!, 52, DateTime.UtcNow, false, ct)),
        };

        foreach (var (name, run) in wrongPermits)
        {
            var result = await run();
            Assert.True(result.Reason == RemediationResult.Reasons.PermitMismatch, name + " answered " + result.Reason);
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        }
    }

    [Fact]
    public async Task APlanForce_NeedsAPermitThatNamesTheDatabase()
    {
        var result = await Executor("svc", "env:PM_REM_NEVER_SET").ForcePlanAsync(Permit(ServerActionNames.ForcePlan, "5", database: null), 1, 5, false,
            TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.BadDatabase, result.Reason);
    }

    [Fact]
    public async Task ARefusedSignIn_ReachesTheCaller_AsRefusedWithItsOwnReason_AndNeverAsAWrite()
    {
        var ct = TestContext.Current.CancellationToken;
        var permit = Permit(ServerActionNames.KillSession, "52");
        Assert.Equal(RemediationResult.Reasons.LoginMissing, (await Executor(null, "env:X").KillSessionAsync(permit, 52, DateTime.UtcNow, false, ct)).Reason);
        Assert.Equal(RemediationResult.Reasons.SecretSlotMissing, (await Executor("svc", null).KillSessionAsync(permit, 52, DateTime.UtcNow, false, ct)).Reason);
        Assert.Equal(RemediationResult.Reasons.StoredPassword, (await Executor("svc", Secret).KillSessionAsync(permit, 52, DateTime.UtcNow, false, ct)).Reason);
        Assert.Equal(RemediationResult.Reasons.SecretEmpty, (await Executor("svc", "env:PM_REM_NEVER_SET_X").KillSessionAsync(permit, 52, DateTime.UtcNow, false, ct)).Reason);
    }

    [Fact]
    public void AMessageFromTheServer_IsScrubbedOfTheSecret()
    {
        var login = RemediationLoginResolverFor("svc", Secret);
        var scrubbed = SqlServerRemediationExecutor.Scrub($"Login failed for user 'svc' with {Secret} and again {Secret}.", login);
        Assert.DoesNotContain(Secret, scrubbed, StringComparison.Ordinal);
        Assert.Contains("***", scrubbed, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryWrite_TakesTheGatesPermitFirst_AndAPermitCannotBeBuiltOutsideTheStorageAssembly()
    {
        var writes = typeof(SqlServerRemediationExecutor).GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
            .Where(m => m.ReturnType == typeof(Task<RemediationResult>)).ToList();
        Assert.Equal(8, writes.Count); /* force, unforce, evict, kill, the Azure SQL Database kill (#5632), recompile a module, and create and drop a plan guide (#6569) */
        Assert.All(writes, m => Assert.Equal(typeof(ServerActionPermit), m.GetParameters()[0].ParameterType));
        /* No other public way to reach the server: the executor exposes nothing but those eight methods, the one read-only state read the force
           tools journal their before and after image from (#5632, step 7, which takes the permit first too), and its constructor. */
        var publicMethods = typeof(SqlServerRemediationExecutor).GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.DeclaredOnly);
        Assert.Equal(9, publicMethods.Length);
        var read = Assert.Single(publicMethods, m => m.ReturnType == typeof(Task<PlanStateRead>));
        Assert.Equal(typeof(ServerActionPermit), read.GetParameters()[0].ParameterType);

        Assert.DoesNotContain(typeof(ServerActionPermit).GetConstructors(BindingFlags.Instance | BindingFlags.Public), _ => true);
        Assert.DoesNotContain(typeof(ServerActionPermit).GetProperties(), p => p.SetMethod?.IsPublic == true);
    }
}

/// <summary>
/// The check every database-scoped remediation makes right after it connects: the database the connection really landed in must be the one
/// the caller's exclusion check was made on. The server resolves a sign-in database name under its own collation, which can ignore character
/// width and accents, so a requested name with one fullwidth letter can land in the database it is not equal to.
/// </summary>
public sealed class RemediationLandedDatabaseTests
{
    private const string FullwidthT = "Ｔ"; /* a fullwidth capital T */

    [Theory]
    [InlineData("tempdb", "tempdb")]
    [InlineData("tempdb", "TEMPDB")]
    [InlineData(" tempdb ", "tempdb")]
    public void SameName_IsNotRefused(string requested, string landed) =>
        Assert.Null(SqlServerRemediationExecutor.LandedElsewhere("force_plan", false, requested, landed));

    [Theory]
    [InlineData(FullwidthT + "empdb", "tempdb")]
    [InlineData("tempdb", FullwidthT + "empdb")]
    [InlineData("cafédb", "cafedb")]
    [InlineData("tempdb", null)]
    public void NameThatDiffersBeyondCase_IsRefused_AndSaysNothingWasRun(string requested, string? landed)
    {
        var refused = SqlServerRemediationExecutor.LandedElsewhere("force_plan", true, requested, landed);

        Assert.NotNull(refused);
        Assert.Equal(RemediationResult.Reasons.WrongDatabase, refused!.Reason);
        Assert.True(refused.DryRun);
        Assert.Contains(requested.Trim(), refused.Message, StringComparison.Ordinal);
        if (landed is not null)
            Assert.Contains(landed, refused.Message, StringComparison.Ordinal);
        Assert.EndsWith("Nothing was run.", refused.Message, StringComparison.Ordinal);
    }
}
