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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6476: <c>--test-connection</c> and <c>--add-server</c> test a Windows-authentication server as the person at the console, not as the
/// service. These run the code both verbs run: the per-target loop (<see cref="DarlingCliCommands.ProbeTargetsAsync"/>), the queue
/// wait that asks the running service, and the flag parsing and password prompt of <c>--add-server</c>. The service and the in-process
/// probe are stood in, so no SQL Server and no store is needed.
/// </summary>
public sealed class DarlingServiceConnectionTestTests
{
    private const string Console_ = @"CONSOLE\erik";

    private static MonitoredServer Windows(string host = "sql01") => new() { Name = host, Host = host, Auth = "integrated" };

    private static MonitoredServer SqlLogin(string host = "sql02") => new() { Name = host, Host = host, Auth = "sql", Username = "darling_monitor" };

    private static ConnectionProbeResult Good(string facts = "SQL Server 2019") =>
        new(true, 15, 3, facts, false, false, false, true, null);

    private static ConnectionProbeResult Bad(string error) =>
        new(false, 0, 0, null, false, false, false, false, error);

    private sealed class FakeTester : DarlingServiceConnectionTest.IServiceConnectionTester
    {
        private readonly DarlingServiceConnectionTest.ServiceTestOutcome _outcome;

        public FakeTester(DarlingServiceConnectionTest.ServiceTestOutcome outcome) => _outcome = outcome;

        public int Calls { get; private set; }

        public Task<DarlingServiceConnectionTest.ServiceTestOutcome> TestAsync(MonitoredServer server, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(_outcome);
        }
    }

    private sealed class ConsoleProbe
    {
        private readonly ConnectionProbeResult _result;

        public ConsoleProbe(ConnectionProbeResult result) => _result = result;

        public int Calls { get; private set; }

        public Task<ConnectionProbeResult> RunAsync(MonitoredServer server, CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(_result);
        }
    }

    [Fact]
    public async Task AWindowsServerTheServiceAnswersFor_IsTestedByTheService_AndTheConsoleProbeIsNotRun()
    {
        var tester = new FakeTester(new(Good("SQL Server 2022"), null));
        var console = new ConsoleProbe(Good("SQL Server 2019"));
        var output = new StringWriter();

        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { Windows() }, output, tester, console.RunAsync, windowsHost: true, Console_, CancellationToken.None);

        Assert.True(ok);
        Assert.Equal(1, tester.Calls);
        Assert.Equal(0, console.Calls);
        var text = output.ToString();
        Assert.Contains("[PASS] sql01", text, StringComparison.Ordinal);
        Assert.Contains("Tested by the running service", text, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE LOGIN", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServiceSideLoginFailure_FailsTheTest_EvenWhenTheConsoleUserCouldConnect()
    {
        var tester = new FakeTester(new(Bad("Login failed for user 'NT SERVICE\\PerformanceMonitor Darling'."), null));
        var console = new ConsoleProbe(Good());
        var output = new StringWriter();

        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { Windows() }, output, tester, console.RunAsync, windowsHost: true, Console_, CancellationToken.None);

        Assert.False(ok);
        Assert.Equal(0, console.Calls);
        Assert.Contains("[FAIL] sql01: Login failed for user 'NT SERVICE", output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task WhenTheServiceCannotBeAsked_TheConsoleTestSaysWhoseIdentityItUsed_AndTheLoginToGrant()
    {
        var tester = new FakeTester(new(null, "The service is not running (Stopped), so it could not be asked."));
        var console = new ConsoleProbe(Good());
        var output = new StringWriter();

        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { Windows() }, output, tester, console.RunAsync, windowsHost: true, Console_, CancellationToken.None);

        Assert.True(ok);
        Assert.Equal(1, console.Calls);
        var text = output.ToString();
        Assert.Contains("[PASS] sql01", text, StringComparison.Ordinal);
        Assert.Contains(@"ran as CONSOLE\erik", text, StringComparison.Ordinal);
        Assert.Contains("does not prove the service can connect", text, StringComparison.Ordinal);
        Assert.Contains("The service is not running (Stopped)", text, StringComparison.Ordinal);
        Assert.Contains(@"CREATE LOGIN [NT SERVICE\PerformanceMonitor Darling] FROM WINDOWS", text, StringComparison.Ordinal);
        Assert.Contains("--test-connection", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANoServiceTester_StillSaysTheResultIsForTheConsoleUser()
    {
        var console = new ConsoleProbe(Bad("Cannot open database"));
        var output = new StringWriter();

        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { Windows() }, output, serviceTester: null, console.RunAsync, windowsHost: true, Console_, CancellationToken.None);

        Assert.False(ok);
        var text = output.ToString();
        Assert.Contains("[FAIL] sql01: Cannot open database", text, StringComparison.Ordinal);
        Assert.Contains(@"ran as CONSOLE\erik", text, StringComparison.Ordinal);
        Assert.Contains("failure here does not prove it cannot", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASqlLoginServer_IsProbedAsBefore_WithNoIdentityNote_AndTheServiceIsNeverAsked()
    {
        var tester = new FakeTester(new(Good(), null));
        var console = new ConsoleProbe(Good());
        var output = new StringWriter();

        var ok = await DarlingCliCommands.ProbeTargetsAsync(
            new[] { SqlLogin() }, output, tester, console.RunAsync, windowsHost: true, Console_, CancellationToken.None);

        Assert.True(ok);
        Assert.Equal(0, tester.Calls);
        Assert.Equal(1, console.Calls);
        Assert.DoesNotContain("NOTE", output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OffWindows_NoIdentityNoteIsPrinted()
    {
        var console = new ConsoleProbe(Good());
        var output = new StringWriter();

        await DarlingCliCommands.ProbeTargetsAsync(
            new[] { Windows() }, output, serviceTester: null, console.RunAsync, windowsHost: false, "erik", CancellationToken.None);

        Assert.DoesNotContain("NOTE", output.ToString(), StringComparison.Ordinal);
    }

    /// <summary>A queue that answers with a canned row after a number of reads (never, when null) and records what was done to it.</summary>
    private sealed class FakeQueue : DarlingServiceConnectionTest.IServiceCommandPlane
    {
        private readonly (string Status, string? ResultStatus, string? ResultJson)? _reply;
        private readonly int _readsBeforeReply;
        private int _reads;

        public FakeQueue((string Status, string? ResultStatus, string? ResultJson)? reply, int readsBeforeReply = 1)
        {
            _reply = reply;
            _readsBeforeReply = readsBeforeReply;
        }

        public List<string> Enqueued { get; } = new();

        public List<long> Cancelled { get; } = new();

        public List<long> Deleted { get; } = new();

        public Task<long> EnqueueAsync(string commandType, string argsJson, CancellationToken cancellationToken)
        {
            Assert.Equal("test_connect", commandType);
            Enqueued.Add(argsJson);
            return Task.FromResult((long)Enqueued.Count);
        }

        public Task<(string Status, string? ResultStatus, string? ResultJson)?> ReadAsync(long commandId, CancellationToken cancellationToken)
        {
            _reads++;
            return Task.FromResult<(string, string?, string?)?>(
                _reply is not null && _reads >= _readsBeforeReply ? _reply : ("pending", null, null));
        }

        public Task<bool> CancelPendingAsync(long commandId, CancellationToken cancellationToken)
        {
            Cancelled.Add(commandId);
            return Task.FromResult(true);
        }

        public Task DeleteAsync(long commandId, CancellationToken cancellationToken)
        {
            Deleted.Add(commandId);
            return Task.CompletedTask;
        }
    }

    private static DarlingServiceConnectionTest.QueuedServiceConnectionTester Tester(FakeQueue queue, string state, TimeSpan? budget = null) =>
        new(queue, _ => Task.FromResult(state), budget ?? TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(5));

    [Fact]
    public async Task ARunningService_IsAsked_ThePayloadCarriesNoPassword_AndTheAnsweredRowIsDeleted()
    {
        var (status, json) = DarlingCommandExecutor.MapProbeResult(Good("SQL Server 2022") with { ConnectedDatabase = "master", MachineIdentity = "SQL01" });
        var queue = new FakeQueue(("succeeded", status, json), readsBeforeReply: 3);

        var outcome = await Tester(queue, "Running").TestAsync(Windows(), CancellationToken.None);

        Assert.True(outcome.Answered);
        Assert.True(outcome.Probe!.Success);
        Assert.Equal(15, outcome.Probe.MajorVersion);
        Assert.Equal("master", outcome.Probe.ConnectedDatabase);
        Assert.Equal("SQL01", outcome.Probe.MachineIdentity);
        Assert.Single(queue.Enqueued);
        Assert.Contains("\"sql01\"", queue.Enqueued[0], StringComparison.Ordinal);
        Assert.Contains("\"password\":null", queue.Enqueued[0], StringComparison.Ordinal);
        Assert.Contains("\"encryptedPassword\":null", queue.Enqueued[0], StringComparison.Ordinal);
        Assert.Equal(new long[] { 1 }, queue.Deleted);
        Assert.Empty(queue.Cancelled);
    }

    [Fact]
    public async Task TheServiceReadsTheQueuedServer_AsTheSameServer()
    {
        var queue = new FakeQueue(("succeeded", "connected", "{\"success\":true}"));
        await Tester(queue, "Running").TestAsync(Windows("sql09"), CancellationToken.None);

        /* The service's own plan for the row: a probe of the server the argument names. */
        var plan = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(1, "test_connect", null, queue.Enqueued[0], "cli"));
        Assert.Equal(CommandKind.Probe, plan.Kind);
    }

    [Fact]
    public async Task AServiceFailure_ComesBackAsTheFailureText()
    {
        var (status, json) = DarlingCommandExecutor.MapProbeResult(Bad("Login failed for user X"));
        var queue = new FakeQueue(("failed", status, json));

        var outcome = await Tester(queue, "Running").TestAsync(Windows(), CancellationToken.None);

        Assert.True(outcome.Answered);
        Assert.False(outcome.Probe!.Success);
        Assert.Equal("Login failed for user X", outcome.Probe.Error);
    }

    [Theory]
    [InlineData("Stopped")]
    [InlineData("not installed (or status unavailable)")]
    public async Task AServiceThatIsNotRunning_IsNotAsked_SoTheInstallersPreflightDoesNotWait(string state)
    {
        var queue = new FakeQueue(null);
        var tester = Tester(queue, state);

        var first = await tester.TestAsync(Windows(), CancellationToken.None);
        var second = await tester.TestAsync(Windows("sql03"), CancellationToken.None);

        Assert.False(first.Answered);
        Assert.False(second.Answered);
        Assert.Contains("not running", first.NotAnsweredReason, StringComparison.Ordinal);
        Assert.Empty(queue.Enqueued);
    }

    [Fact]
    public async Task ARunningServiceThatNeverAnswers_HasItsRowCancelled_AndIsNotAskedAgain()
    {
        var queue = new FakeQueue(null);
        var tester = Tester(queue, "Running", budget: TimeSpan.FromMilliseconds(60));

        var first = await tester.TestAsync(Windows(), CancellationToken.None);
        var second = await tester.TestAsync(Windows("sql03"), CancellationToken.None);

        Assert.False(first.Answered);
        Assert.Contains("did not answer", first.NotAnsweredReason, StringComparison.Ordinal);
        Assert.False(second.Answered);
        Assert.Single(queue.Enqueued);
        Assert.Equal(new long[] { 1 }, queue.Cancelled);
        Assert.Empty(queue.Deleted);
    }

    [Fact]
    public void AReplyWithNoReadableBody_IsAFailureThatNamesTheStatus()
    {
        var probe = DarlingServiceConnectionTest.ParseReply("refused", "not json");

        Assert.False(probe.Success);
        Assert.Contains("refused", probe.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFlags_BuildOneEntry_AndAWindowsServerIsNeverAskedForAPassword()
    {
        var asked = 0;
        var (json, problem) = DarlingCliCommands.BuildAddServerJson(
            new DarlingCliCommands.AddServerFlags(Host: "sql01", DisplayName: "Prod", Auth: "windows"),
            _ => { asked++; return "x"; });

        Assert.Null(problem);
        Assert.Equal(0, asked);
        Assert.Equal("""[{"host":"sql01","display_name":"Prod","auth":"windows"}]""", json);
    }

    [Fact]
    public void ASqlLogin_IsAskedForItsPasswordOnce_ThePasswordRidesOnlyInTheRequestText_AndIsNeverWrittenToTheOutput()
    {
        var prompts = new List<string>();
        var (json, problem) = DarlingCliCommands.BuildAddServerJson(
            new DarlingCliCommands.AddServerFlags(Host: "sql02", Auth: "SQL", Username: "darling_monitor"),
            label => { prompts.Add(label); return "S3cret!pw"; });

        Assert.Null(problem);
        Assert.Single(prompts);
        Assert.Equal("Password for darling_monitor: ", prompts[0]);
        Assert.DoesNotContain("S3cret", prompts[0], StringComparison.Ordinal);
        Assert.Contains("\"password\":\"S3cret!pw\"", json, StringComparison.Ordinal);
        Assert.Contains("\"username\":\"darling_monitor\"", json, StringComparison.Ordinal);
    }

    [Fact]
    public void ASqlLoginWithNoPasswordTyped_AddsNothing()
    {
        var (json, problem) = DarlingCliCommands.BuildAddServerJson(
            new DarlingCliCommands.AddServerFlags(Host: "sql02", Auth: "sql", Username: "u"), _ => string.Empty);

        Assert.Null(json);
        Assert.Contains("No password was entered", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void ASqlLoginWithNoUsername_IsRefusedBeforeAnyPrompt()
    {
        var asked = false;
        var (json, problem) = DarlingCliCommands.BuildAddServerJson(
            new DarlingCliCommands.AddServerFlags(Host: "sql02", Auth: "sql"), _ => { asked = true; return "x"; });

        Assert.Null(json);
        Assert.False(asked);
        Assert.Contains("--username", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void TheArgumentGrammar_TakesFlagsInBothSpellings_AndAConfigPath()
    {
        var ok = DarlingCliCommands.TryParseAddServerArgs(
            new[] { "C:\\x\\darling.json", "--host", "sql01", "--auth=sql", "--login", "svc", "--name", "Prod" },
            out var config, out var flags, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Equal("C:\\x\\darling.json", config);
        Assert.Equal(new DarlingCliCommands.AddServerFlags("sql01", "Prod", "sql", "svc"), flags);
        Assert.True(flags.Any);
    }

    /* --grant-script takes "integrated", --add-server took only "windows": the same word now works on both, and reaches the shared
       server rules as Windows authentication. */
    [Theory]
    [InlineData("windows", "windows")]
    [InlineData("integrated", "windows")]
    [InlineData("Integrated", "windows")]
    [InlineData("sql", "sql")]
    public void TheAuthWords_WindowsAndIntegrated_AreTheSameOnTheAddServerVerb(string word, string expected)
    {
        Assert.True(DarlingCliCommands.TryParseAddServerArgs(new[] { "--host", "sql01", "--auth", word }, out _, out var flags, out var error));
        Assert.Null(error);
        Assert.Equal(expected, flags.Auth);

        var (json, problem) = DarlingCliCommands.BuildAddServerJson(flags with { Username = "svc" }, _ => "pw");
        Assert.Null(problem);
        Assert.NotNull(json);
        Assert.Null(DarlingMcpServerAdminTools.ResolveAuth(flags.Auth).Error);
    }

    [Fact]
    public void NoArguments_IsTheStdinFormAsBefore()
    {
        Assert.True(DarlingCliCommands.TryParseAddServerArgs(Array.Empty<string>(), out var config, out var flags, out _));
        Assert.Null(config);
        Assert.False(flags.Any);
    }

    [Theory]
    [InlineData("--password")]
    [InlineData("--password=hunter2")]
    [InlineData("--PWD")]
    public void ThereIsNoPasswordFlag_AndTheRefusalNeverEchoesTheValue(string flag)
    {
        var ok = DarlingCliCommands.TryParseAddServerArgs(new[] { "--host", "sql01", flag, "hunter2" }, out _, out _, out var error);

        Assert.False(ok);
        Assert.Contains("no password flag", error, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--bogus")]
    [InlineData("--host")]
    public void AnUnknownOrValuelessFlag_IsRefused(string flag)
    {
        Assert.False(DarlingCliCommands.TryParseAddServerArgs(new[] { flag }, out _, out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public async Task FlagsWithNoHost_FailOnTheErrorStream_WithoutReadingStdinOrTouchingTheStore()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.AddServerAsync(
            null, new ThrowingReader(), output, error, CancellationToken.None,
            flags: new DarlingCliCommands.AddServerFlags(Auth: "sql", Username: "u"));

        Assert.Equal(1, exit);
        Assert.Contains("--host is required", error.ToString(), StringComparison.Ordinal);
    }

    private sealed class ThrowingReader : TextReader
    {
        public override string ReadToEnd() => throw new InvalidOperationException("stdin must not be read when flags name the server");

        public override string? ReadLine() => throw new InvalidOperationException("stdin must not be read when flags name the server");
    }
}
