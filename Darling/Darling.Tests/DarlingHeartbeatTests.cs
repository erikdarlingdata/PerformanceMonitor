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
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5450 proposal 2: the outbound heartbeat. The config block (file-only, off by default, fatal validation, env:/file:
/// references), the ping decision, the HTTP behavior against a local stub, the failure-log throttle, and the rule that
/// the URL (a secret) never reaches a log line or a validation problem.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
[Collection("gap-cache-serial")]
public sealed class DarlingHeartbeatTests
{
    private static readonly CancellationToken Ct = TestContext.Current.CancellationToken;
    private static readonly DateTime Now = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);
    private const string Token = "SECRET-TOKEN-5450";

    /* ───────────────────────── config ───────────────────────── */

    [Fact]
    public void AbsentBlock_IsOff_WithTheDefaultInterval()
    {
        var config = DarlingConfig.Parse("{}");
        Assert.False(config.Heartbeat.IsConfigured);
        Assert.Equal(300, config.Heartbeat.IntervalSeconds);
        Assert.Empty(HeartbeatConfig.Validate(config.Heartbeat));
    }

    [Theory]
    [InlineData("{\"heartbeat\":{}}")]
    [InlineData("{\"heartbeat\":{\"url\":\"\"}}")]
    [InlineData("{\"heartbeat\":{\"url\":null}}")]
    [InlineData("{\"heartbeat\":{\"url\":\"   \"}}")]
    public void EmptyOrNullUrl_IsOff_AndValid(string json)
    {
        var config = DarlingConfig.Parse(json);
        Assert.False(config.Heartbeat.IsConfigured);
        Assert.Empty(HeartbeatConfig.Validate(config.Heartbeat));
    }

    [Theory]
    [InlineData("ftp://hc-ping.example/your-check-id")]
    [InlineData("/relative/path")]
    [InlineData("hc-ping.example/your-check-id")]
    [InlineData("not a url")]
    [InlineData("https://")]
    public void BadScheme_Relative_OrMalformedUrl_IsFatal_AndTheProblemNeverEchoesIt(string url)
    {
        var heartbeat = new HeartbeatConfig { Url = url };
        var problems = HeartbeatConfig.Validate(heartbeat);
        Assert.NotEmpty(problems);
        Assert.All(problems, p => Assert.DoesNotContain(url.Trim(), p, StringComparison.Ordinal));
    }

    [Fact]
    public void HttpAndHttpsUrls_AreValid()
    {
        Assert.Empty(HeartbeatConfig.Validate(new HeartbeatConfig { Url = "https://hc-ping.example/your-check-id" }));
        Assert.Empty(HeartbeatConfig.Validate(new HeartbeatConfig { Url = "http://hc-ping.example:8080/ping/" + Token }));
    }

    [Theory]
    [InlineData(59, false)]
    [InlineData(60, true)]
    [InlineData(300, true)]
    [InlineData(3600, true)]
    [InlineData(3601, false)]
    [InlineData(0, false)]
    [InlineData(-5, false)]
    public void IntervalRange_IsSixtyToThirtySixHundred(int seconds, bool valid)
    {
        var problems = HeartbeatConfig.Validate(new HeartbeatConfig { Url = "https://hc-ping.example/x", IntervalSeconds = seconds });
        Assert.Equal(valid, problems.Count == 0);
        if (!valid)
        {
            Assert.Contains(problems, p => p.Contains("heartbeat.intervalSeconds", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void ABadBlock_SurfacesThroughTheWholeConfigValidate_EvenWithNoServers()
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { ConnectionString = "Host=localhost;Database=darling" },
            Heartbeat = new HeartbeatConfig { Url = "ftp://x.example/" + Token, IntervalSeconds = 5 },
        };

        var problems = config.Validate();
        Assert.Contains(problems, p => p.Contains("heartbeat.url", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.Contains("heartbeat.intervalSeconds", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.Contains("servers must contain at least one entry", StringComparison.Ordinal));
        Assert.DoesNotContain(problems, p => p.Contains(Token, StringComparison.Ordinal));
    }

    [Fact]
    public void EnvReference_Resolves_AndAMissingVariableIsFatalWithoutTheValue()
    {
        var name = "DARLING_TEST_HEARTBEAT_URL_5450";
        try
        {
            Environment.SetEnvironmentVariable(name, "https://hc-ping.example/" + Token);
            var heartbeat = new HeartbeatConfig { Url = "env:" + name };
            Assert.Empty(HeartbeatConfig.Validate(heartbeat));
            Assert.True(heartbeat.TryResolveUrl(out var uri, out _));
            Assert.Equal("hc-ping.example", uri!.Host);

            Environment.SetEnvironmentVariable(name, "ftp://hc-ping.example/" + Token);
            Assert.NotEmpty(HeartbeatConfig.Validate(heartbeat));
            Assert.All(HeartbeatConfig.Validate(heartbeat), p => Assert.DoesNotContain(Token, p, StringComparison.Ordinal));

            Environment.SetEnvironmentVariable(name, null);
            var missing = HeartbeatConfig.Validate(heartbeat);
            Assert.Contains(missing, p => p.Contains("heartbeat.url", StringComparison.Ordinal) && p.Contains(name, StringComparison.Ordinal));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void FileReference_Resolves_AndAMissingFileIsFatal()
    {
        var path = Path.Combine(Path.GetTempPath(), "heartbeat-5450-" + Guid.NewGuid().ToString("N") + ".txt");
        try
        {
            File.WriteAllText(path, "https://hc-ping.example/" + Token + "\n");
            var heartbeat = new HeartbeatConfig { Url = "file:" + path };
            Assert.Empty(HeartbeatConfig.Validate(heartbeat));
            Assert.True(heartbeat.TryResolveUrl(out var uri, out _));
            Assert.EndsWith(Token, uri!.AbsolutePath, StringComparison.Ordinal);

            File.Delete(path);
            Assert.NotEmpty(HeartbeatConfig.Validate(heartbeat));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void TheReferenceIsCaptured_AsAWrittenSecret()
    {
        /* The owned-secrets walker is reflective: a reference in heartbeat.url is found with the others. */
        var config = DarlingConfig.Parse("{\"heartbeat\":{\"url\":\"env:HEARTBEAT_URL_X\"}}");
        Assert.Contains("env:HEARTBEAT_URL_X", config.SecretReferencesAsWritten);
    }

    [Fact]
    public void NoReadSurface_ReferencesTheHeartbeatBlock()
    {
        /* File-only and secret: the web, MCP, triage and CLI code never read heartbeat.url. Only the config class that
           declares and validates it, the loop, the worker that starts it, and the diagnostics bundle's classification
           list (which EXCLUDES it from the bundle) name it. A new file that does must be added here on purpose. */
        var allowed = new HashSet<string>(StringComparer.Ordinal)
        {
            "DarlingConfig.cs", "DarlingHeartbeat.cs", "DarlingWorker.cs", "DiagnosticsBundle.cs",
        };
        var serviceDir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");
        var offenders = Directory.EnumerateFiles(serviceDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                        && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(f => !allowed.Contains(Path.GetFileName(f)))
            .Where(f => System.Text.RegularExpressions.Regex.IsMatch(File.ReadAllText(f), @"HeartbeatConfig|DarlingHeartbeat|\.Heartbeat\b|""heartbeat"""))
            .Select(Path.GetFileName)
            .ToList();
        Assert.Empty(offenders);
    }

    private static string RepoRoot([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    /* ───────────────────────── decision ───────────────────────── */

    [Fact]
    public void Decide_FreshPings_StaleSkips_NoEnabledServersPings_ReadFailureSkips()
    {
        Assert.Equal(HeartbeatDecision.Ping, DarlingHeartbeat.Decide(new HeartbeatRead(true, 3, Now.AddMinutes(-1)), Now));
        Assert.Equal(HeartbeatDecision.Ping, DarlingHeartbeat.Decide(new HeartbeatRead(true, 3, Now.AddMinutes(-14).AddSeconds(-59)), Now));
        Assert.Equal(HeartbeatDecision.SkipStale, DarlingHeartbeat.Decide(new HeartbeatRead(true, 3, Now.AddMinutes(-15)), Now));
        Assert.Equal(HeartbeatDecision.SkipStale, DarlingHeartbeat.Decide(new HeartbeatRead(true, 3, Now.AddHours(-6)), Now));
        Assert.Equal(HeartbeatDecision.Ping, DarlingHeartbeat.Decide(new HeartbeatRead(true, 0, null), Now));
        Assert.Equal(HeartbeatDecision.SkipStale, DarlingHeartbeat.Decide(new HeartbeatRead(true, 2, null), Now));
        Assert.Equal(HeartbeatDecision.SkipReadFailed, DarlingHeartbeat.Decide(new HeartbeatRead(false, 0, null), Now));
        /* A time after now (clock skew) is fresh, like #5454's negative gap. */
        Assert.Equal(HeartbeatDecision.Ping, DarlingHeartbeat.Decide(new HeartbeatRead(true, 1, Now.AddMinutes(2)), Now));
    }

    [Fact]
    public void TheThreshold_IsTheSharedGapConstant_AndTheReadReusesTheSharedSql()
    {
        Assert.Equal(15, DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold.TotalMinutes);
        Assert.Contains(DarlingSelfAlertEvaluator.NewestCollectionTimeSql, DarlingHeartbeat.ReadSql, StringComparison.Ordinal);
        Assert.Contains("is_enabled", DarlingHeartbeat.ReadSql, StringComparison.Ordinal);
    }

    /* ───────────────────────── failure-log throttle ───────────────────────── */

    [Fact]
    public void Gate_WarnsFirst_ThenOncePerHour_ThenOneRecovery()
    {
        var gate = new HeartbeatFailureGate();
        Assert.Equal(HeartbeatLogEdge.None, gate.OnSuccess());
        Assert.Equal(HeartbeatLogEdge.Warn, gate.OnFailure(Now));
        Assert.Equal(HeartbeatLogEdge.None, gate.OnFailure(Now.AddMinutes(5)));
        Assert.Equal(HeartbeatLogEdge.None, gate.OnFailure(Now.AddMinutes(59)));
        Assert.Equal(HeartbeatLogEdge.Warn, gate.OnFailure(Now.AddMinutes(60)));
        Assert.Equal(HeartbeatLogEdge.None, gate.OnFailure(Now.AddMinutes(90)));
        Assert.Equal(HeartbeatLogEdge.Recovered, gate.OnSuccess());
        Assert.Equal(HeartbeatLogEdge.None, gate.OnSuccess());
        /* A new failure after recovery warns at once. */
        Assert.Equal(HeartbeatLogEdge.Warn, gate.OnFailure(Now.AddMinutes(91)));
    }

    /* ───────────────────────── HTTP against a local stub ───────────────────────── */

    private sealed class Stub : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly Func<string, string?> _respond;
        private int _requests;

        /// <param name="respond">Gets the request line's path; returns the full raw response, or null to never answer.</param>
        public Stub(Func<string, string?> respond)
        {
            _respond = respond;
            _listener.Start();
            _ = Task.Run(AcceptLoopAsync);
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        public int Requests => Volatile.Read(ref _requests);

        public string Url(string path) => "http://127.0.0.1:" + Port + path;

        private async Task AcceptLoopAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _ = Task.Run(() => HandleAsync(client));
                }
            }
            catch (Exception)
            {
                /* Stopping. */
            }
        }

        private async Task HandleAsync(TcpClient client)
        {
            using (client)
            {
                try
                {
                    var stream = client.GetStream();
                    var buffer = new byte[4096];
                    var read = await stream.ReadAsync(buffer, _stop.Token);
                    var head = Encoding.ASCII.GetString(buffer, 0, read);
                    var path = head.Split(' ')[1];
                    Interlocked.Increment(ref _requests);
                    var response = _respond(path);
                    if (response is null)
                    {
                        await Task.Delay(Timeout.Infinite, _stop.Token);
                        return;
                    }

                    await stream.WriteAsync(Encoding.ASCII.GetBytes(response), _stop.Token);
                }
                catch (Exception)
                {
                    /* Stopping or the client gave up. */
                }
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }

    private static string Reply(int code, string? location = null) =>
        $"HTTP/1.1 {code} X\r\nContent-Length: 0\r\nConnection: close\r\n" + (location is null ? "" : $"Location: {location}\r\n") + "\r\n";

    [Fact]
    public async Task Ping_2xx_IsSuccess_AndSendsAGet()
    {
        string? seen = null;
        using var stub = new Stub(path => { seen = path; return Reply(200); });
        var (ok, reason) = await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri(stub.Url("/ping/" + Token)), TimeSpan.FromSeconds(5), Ct);
        Assert.True(ok);
        Assert.Null(reason);
        Assert.Equal("/ping/" + Token, seen);
        Assert.Equal(1, stub.Requests);
    }

    [Theory]
    [InlineData(204, true)]
    [InlineData(299, true)]
    [InlineData(400, false)]
    [InlineData(404, false)]
    [InlineData(500, false)]
    [InlineData(503, false)]
    public async Task Ping_OnlyA2xxIsSuccess(int code, bool expected)
    {
        using var stub = new Stub(_ => Reply(code));
        var (ok, reason) = await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri(stub.Url("/x")), TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(expected, ok);
        Assert.Equal(expected, reason is null);
        if (!expected)
        {
            Assert.Equal("HTTP " + code, reason);
        }
    }

    [Fact]
    public async Task Ping_ANoAnswer_TimesOut_AsAFailureNotACancellation()
    {
        using var stub = new Stub(_ => null);
        var (ok, reason) = await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri(stub.Url("/x")), TimeSpan.FromMilliseconds(300), Ct);
        Assert.False(ok);
        Assert.Contains("no answer within", reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Ping_ACallerCancel_Propagates()
    {
        using var stub = new Stub(_ => null);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(200);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri(stub.Url("/x")), TimeSpan.FromSeconds(30), cts.Token));
    }

    [Fact]
    public async Task Ping_A3xx_IsNotFollowed_AndIsAFailure()
    {
        using var target = new Stub(_ => Reply(200));
        using var redirector = new Stub(_ => Reply(302, target.Url("/elsewhere")));
        var (ok, reason) = await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri(redirector.Url("/x")), TimeSpan.FromSeconds(5), Ct);
        Assert.False(ok);
        Assert.Equal("HTTP 302", reason);
        Assert.Equal(1, redirector.Requests);
        Assert.Equal(0, target.Requests);
    }

    [Fact]
    public async Task Ping_AConnectionFailure_IsAFailure_WithAReasonThatHasNoUrl()
    {
        int port;
        using (var l = new TcpListener(IPAddress.Loopback, 0))
        {
            l.Start();
            port = ((IPEndPoint)l.LocalEndpoint).Port;
        }

        var (ok, reason) = await DarlingHeartbeat.PingAsync(DarlingHeartbeat.SharedClient, new Uri($"http://127.0.0.1:{port}/{Token}"), TimeSpan.FromSeconds(5), Ct);
        Assert.False(ok);
        Assert.DoesNotContain(Token, reason, StringComparison.Ordinal);
    }

    /* ───────────────────────── the loop's tick, and the log ───────────────────────── */

    private sealed class Clock
    {
        public DateTime Value = Now;
    }

    private static (DarlingHeartbeat Heartbeat, CapturingTestLogger Logger, Clock Clock) Build()
    {
        var logger = new CapturingTestLogger();
        var clock = new Clock();
        var client = new HttpClient(new SocketsHttpHandler { AllowAutoRedirect = false }) { Timeout = TimeSpan.FromSeconds(30) };
        return (new DarlingHeartbeat(logger, client, () => clock.Value), logger, clock);
    }

    private static Func<CancellationToken, Task<HeartbeatRead>> ReadOf(HeartbeatRead read) => _ => Task.FromResult(read);

    [Fact]
    public async Task Tick_PingsWhenFresh_SkipsWhenStaleOrUnreadable_PingsWithNoEnabledServers()
    {
        using var stub = new Stub(_ => Reply(200));
        var (hb, _, _) = Build();
        var config = new HeartbeatConfig { Url = stub.Url("/ping/" + Token) };

        Assert.Equal(HeartbeatDecision.Ping, await hb.TickAsync(config, ReadOf(new HeartbeatRead(true, 2, Now.AddMinutes(-2))), Ct));
        Assert.Equal(1, stub.Requests);

        Assert.Equal(HeartbeatDecision.SkipStale, await hb.TickAsync(config, ReadOf(new HeartbeatRead(true, 2, Now.AddMinutes(-40))), Ct));
        Assert.Equal(HeartbeatDecision.SkipReadFailed, await hb.TickAsync(config, ReadOf(new HeartbeatRead(false, 0, null)), Ct));
        Assert.Equal(1, stub.Requests);

        Assert.Equal(HeartbeatDecision.Ping, await hb.TickAsync(config, ReadOf(new HeartbeatRead(true, 0, null)), Ct));
        Assert.Equal(2, stub.Requests);
    }

    [Fact]
    public async Task Tick_TheUrlNeverReachesALogLine_OnFailure_Recovery_Skip_OrABadReference()
    {
        var ok = true;
        using var stub = new Stub(_ => ok ? Reply(200) : Reply(500));
        var (hb, logger, clock) = Build();
        var url = stub.Url("/ping/" + Token);
        var config = new HeartbeatConfig { Url = url };
        /* Fresh against the test clock as it moves: the newest collection is always a minute old. */
        Func<CancellationToken, Task<HeartbeatRead>> fresh = _ => Task.FromResult(new HeartbeatRead(true, 1, clock.Value.AddMinutes(-1)));

        ok = false;
        await hb.TickAsync(config, fresh, Ct);                       /* failure: one warning */
        clock.Value = Now.AddMinutes(5);
        await hb.TickAsync(config, fresh, Ct);                       /* throttled: no second warning */
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));

        clock.Value = Now.AddMinutes(61);
        await hb.TickAsync(config, fresh, Ct);                       /* an hour on: one more */
        Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));

        ok = true;
        await hb.TickAsync(config, fresh, Ct);                       /* recovery line, once */
        await hb.TickAsync(config, fresh, Ct);
        Assert.Equal(1, logger.Lines.Count(l => l.Contains("succeeded again", StringComparison.Ordinal)));

        await hb.TickAsync(config, ReadOf(new HeartbeatRead(false, 0, null)), Ct);   /* skip line */
        await hb.TickAsync(config, ReadOf(new HeartbeatRead(true, 1, clock.Value.AddHours(-3))), Ct);

        var bad = new HeartbeatConfig { Url = "env:DARLING_TEST_HEARTBEAT_UNSET_5450" };
        await hb.TickAsync(bad, fresh, Ct);                          /* bad reference line */

        Assert.NotEmpty(logger.Lines);
        Assert.All(logger.Lines, line =>
        {
            Assert.DoesNotContain(Token, line, StringComparison.Ordinal);
            Assert.DoesNotContain("/ping/", line, StringComparison.Ordinal);
            Assert.DoesNotContain(url, line, StringComparison.Ordinal);
        });

        /* The scheme and host ARE named. */
        Assert.Contains(logger.Lines, l => l.Contains("http://127.0.0.1", StringComparison.Ordinal));
        Assert.Empty(logger.ExceptionsAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task Tick_SkipLogsOnTheEdge_AndLogsTheResume()
    {
        using var stub = new Stub(_ => Reply(200));
        var (hb, logger, _) = Build();
        var config = new HeartbeatConfig { Url = stub.Url("/x") };
        var stale = ReadOf(new HeartbeatRead(true, 1, Now.AddHours(-1)));

        await hb.TickAsync(config, stale, Ct);
        await hb.TickAsync(config, stale, Ct);
        Assert.Equal(1, logger.Lines.Count(l => l.Contains("ping skipped", StringComparison.Ordinal)));

        await hb.TickAsync(config, ReadOf(new HeartbeatRead(true, 1, Now.AddMinutes(-1))), Ct);
        Assert.Equal(1, logger.Lines.Count(l => l.Contains("pings resume", StringComparison.Ordinal)));
        Assert.Equal(1, stub.Requests);
    }

    [Fact]
    public async Task Run_WithNoUrl_ReturnsAtOnce_AndWithAUrl_StopsCleanlyOnCancel()
    {
        var (hb, _, _) = Build();
        await hb.RunAsync(new HeartbeatConfig(), null!, Ct);     /* off: never touches the store, returns */

        /* On: a cancelled token ends the loop without throwing, before it reaches the store. */
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await hb.RunAsync(new HeartbeatConfig { Url = "https://hc-ping.example/x" }, null!, cts.Token);
    }

    /* ───────────────────────── the read, against a real store ───────────────────────── */

    [Fact]
    public async Task ReadAsync_ReportsTheEnabledCountAndNewestTime_OnARealStore()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, Ct);
        try
        {
            await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrate.OpenAsync(Ct);
                await PgMigrations.MigrateAsync(migrate, Ct);
            }

            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hb = new DarlingHeartbeat(new CapturingTestLogger());

            var empty = await hb.ReadAsync(store, Ct);
            Assert.Equal(new HeartbeatRead(true, 0, null), empty);

            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (7,'hb-7','h',true),(8,'hb-8','h',false)");
            Assert.Equal(new HeartbeatRead(true, 1, null), await hb.ReadAsync(store, Ct));

            var newest = DateTime.UtcNow.AddMinutes(-3);
            var micro = new DateTime(newest.Ticks - (newest.Ticks % 10), DateTimeKind.Unspecified);
            await using (var insert = store.CreateCommand(
                "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES (1, 7, 'hb', 'cpu', @t, 'SUCCESS'), (2, 8, 'hb', 'cpu', @t2, 'SUCCESS')"))
            {
                insert.Parameters.Add(new NpgsqlParameter("t", NpgsqlDbType.Timestamp) { Value = micro });
                insert.Parameters.Add(new NpgsqlParameter("t2", NpgsqlDbType.Timestamp) { Value = micro.AddMinutes(2) });
                await insert.ExecuteNonQueryAsync(Ct);
            }

            var read = await hb.ReadAsync(store, Ct);
            Assert.True(read.Ok);
            Assert.Equal(1, read.EnabledServers);
            /* The disabled server's newer row does not count. */
            Assert.Equal(DateTime.SpecifyKind(micro, DateTimeKind.Utc), read.NewestUtc);
            Assert.Equal(HeartbeatDecision.Ping, DarlingHeartbeat.Decide(read, DateTime.UtcNow));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource store, string sql)
    {
        await using var command = store.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(Ct);
    }
}
