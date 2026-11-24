using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9423, #9424: every MCP call is timed by the filter the host registers first, and a call that takes the slow threshold or longer
/// writes one mcp_slow_calls row. Each case drives a real in-process MCP server with the host's REAL call-tool filter list and a
/// probe tool, so the filter, the recorder and the store row are all the shipped code. The threshold is the recorder's own test seam,
/// never a real slow read.
/// </summary>
public sealed class McpCallTimingTests : IClassFixture<SharedDuckDbFixture>, IAsyncDisposable
{
    private const string ServerName = "TimingSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _data;
    private ServerHost? _host;

    public McpCallTimingTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-timing-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _serverManager.AddServer(new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true });
        _data = new LocalDataService(_duckDb);
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null) await _host.DisposeAsync();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [ModelContextProtocol.Server.McpServerToolType]
    private sealed class TimingProbeTools
    {
        [McpServerTool(Name = "timing_probe"), System.ComponentModel.Description("Test-only: answers after a short delay.")]
        public static async Task<string> Probe(
            [System.ComponentModel.Description("A value the row must never hold.")] string secret,
            [System.ComponentModel.Description("The window.")] int hours_back = 24,
            [System.ComponentModel.Description("Delay.")] int delay_ms = 0)
        {
            if (delay_ms > 0) await Task.Delay(delay_ms);
            return "{\"answer\":\"" + new string('x', 100) + "\"}";
        }

        [McpServerTool(Name = "timing_counted"), System.ComponentModel.Description("Test-only: a reply with a server, a count and an array.")]
        public static string Counted([System.ComponentModel.Description("The server.")] string? server_name = null) =>
            "{\"server\":\"Resolved Name\",\"count\":7,\"items\":[1,2]}";

        [McpServerTool(Name = "timing_array"), System.ComponentModel.Description("Test-only: a reply with one array and no server or count.")]
        public static string OneArray([System.ComponentModel.Description("The server.")] string? server_name = null) =>
            "{\"queries\":[1,2,3]}";

        [McpServerTool(Name = "timing_two_arrays"), System.ComponentModel.Description("Test-only: a reply with two arrays and no count.")]
        public static string TwoArrays() =>
            "{\"a\":[1],\"b\":[1,2]}";

        [McpServerTool(Name = "timing_throw"), System.ComponentModel.Description("Test-only: throws an McpException.")]
        public static string Throw([System.ComponentModel.Description("The message.")] string message) =>
            throw new McpException(message);
    }

    // ── a call under the threshold ──

    [Fact]
    public async Task ACallUnderTheThreshold_WritesNoRow_ButCountsInThePerToolFigures()
    {
        var writes = new ConcurrentBag<McpSlowCall>();
        var recorder = new McpCallTimingRecorder(c => { writes.Add(c); return Task.CompletedTask; }, () => null)
        {
            SlowThreshold = TimeSpan.FromHours(1),
        };

        await CallAsync(recorder, "timing_probe", ("secret", "s1"));
        await CallAsync(recorder, "timing_probe", ("secret", "s2"));
        await recorder.WhenWritesDoneAsync();

        Assert.Empty(writes);
        var figures = Assert.Single(recorder.Snapshot());
        Assert.Equal("timing_probe", figures.Tool);
        Assert.Equal(2, figures.Count);
        Assert.Equal(0, figures.SlowCalls);
    }

    // ── a slow call ──

    [Fact]
    public async Task ASlowCall_WritesOneRow_WithItsToolTotalWindowAndReplyBytes_AndNoArgumentValue()
    {
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };

        await CallAsync(recorder, "timing_probe", ("secret", "query-text-canary-9423"), ("hours_back", 6), ("delay_ms", 40));
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, null));
        Assert.Equal("timing_probe", row.Tool);
        Assert.Equal("ok", row.Outcome);
        Assert.True(row.TotalMs >= 30, $"total_ms was {row.TotalMs}");
        Assert.Equal(6d, row.HoursBack);
        Assert.True(row.ResponseBytes >= 100, $"response_bytes was {row.ResponseBytes}");
        Assert.InRange(row.CallTimeUtc, DateTime.UtcNow.AddMinutes(-2), DateTime.UtcNow.AddMinutes(1));
        /* Only the window is kept of the arguments: the string argument's value is nowhere in the stored row. */
        Assert.DoesNotContain("query-text-canary-9423", System.Text.Json.JsonSerializer.Serialize(row));
        /* A read that keeps no phase split stores NULL for it, not zero. */
        Assert.Null(row.HotMs);
        Assert.Null(row.ArchiveMs);
        Assert.Null(row.ArchiveFiles);
        Assert.NotNull(row.LockWaitMs);
    }

    [Fact]
    public async Task ASlowCall_CarriesTheProcessWorkingSetPeakAndDuckDbMemory_AndNamesTheirSource()
    {
        /* The DuckDB memory read answers NULL while another class holds the store lock, so this test hands the recorder its own reading. */
        var recorder = new McpCallTimingRecorder(_data.RecordMcpSlowCallAsync, () => 123_456) { SlowThreshold = TimeSpan.Zero };

        await CallAsync(recorder, "timing_probe", ("secret", "s"), ("delay_ms", 200));
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, null));
        Assert.True(row.ProcessPeakWorkingSetBytes > 0, "no working-set peak");
        Assert.Equal(123_456, row.DuckDbPeakMemoryBytes);
        Assert.Contains("working set", row.MemorySource);
        Assert.Contains("duckdb_memory()", row.MemorySource);
    }

    [Fact]
    public async Task TheWorkingSetPeak_IsTheHighestReadingPolledDuringTheCall()
    {
        long reading = 1_000;
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero, SampleInterval = TimeSpan.FromMilliseconds(10), SamplingStartDelay = TimeSpan.Zero };
        /* The readings climb to 5000 while the call runs, then fall back: the row keeps the top, not the last. */
        recorder.ReadWorkingSet = () => { var r = Interlocked.Add(ref reading, 1_000); return r <= 5_000 ? r : 2_000; };

        await CallAsync(recorder, "timing_probe", ("secret", "s"), ("delay_ms", 300));
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, null));
        Assert.Equal(5_000, row.ProcessPeakWorkingSetBytes);
    }

    [Fact]
    public async Task AToolThatKeepsAPhaseSplit_StoresItsHotAndArchiveFigures()
    {
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };

        /* get_query_store_top keeps a split (ReadPhaseSplit), so its row has the figures filled; the server has no data, which is an empty answer. */
        await CallAsync(recorder, "get_query_store_top", ("server_name", ServerName));
        await recorder.WhenWritesDoneAsync();

        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "get_query_store_top"));
        Assert.NotNull(row.HotMs);
        Assert.NotNull(row.ArchiveMs);
        Assert.NotNull(row.ArchiveFiles);
        Assert.True(row.HotMs + row.ArchiveMs <= row.TotalMs + 1);
    }

    // ── the server and the rows ──

    [Fact]
    public async Task ASlowCall_StoresTheServerAndTheRows_FromTheReplyOrTheArgument_AndNullWhereTheReplyHasNone()
    {
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };

        await CallAsync(recorder, "timing_counted", ("server_name", "typed partial"));
        await CallAsync(recorder, "timing_array", ("server_name", "  typed name  "));
        await CallAsync(recorder, "timing_two_arrays");
        await recorder.WhenWritesDoneAsync();

        var counted = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "timing_counted"));
        /* The reply's own server wins over the typed one, and its count wins over its array. */
        Assert.Equal("Resolved Name", counted.ServerName);
        Assert.Equal(7, counted.RowsReturned);

        var array = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "timing_array"));
        Assert.Equal("typed name", array.ServerName);
        Assert.Equal(3, array.RowsReturned);

        /* Two arrays and no count: the reply does not say how many rows it holds, so the row says nothing. */
        var two = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "timing_two_arrays"));
        Assert.Null(two.ServerName);
        Assert.Null(two.RowsReturned);
    }

    // ── a call that throws ──

    [Fact]
    public async Task ACallThatThrows_IsRecordedWithItsOutcome_AndStillFailsForTheCaller()
    {
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.Zero };

        var (result, text) = await CallRawAsync(recorder, "timing_throw", ("message", "the binding failed"));
        await recorder.WhenWritesDoneAsync();

        /* The caller sees the failure exactly as it does without the filter. */
        Assert.True(result.IsError);
        Assert.Contains("the binding failed", text);
        var row = Assert.Single(await _data.GetMcpSlowCallsAsync(DateTime.UtcNow.AddHours(-1), 50, "timing_throw"));
        Assert.Equal("exception", row.Outcome);
        Assert.Equal(1, Assert.Single(recorder.Snapshot()).Errors);
    }

    // ── a failed write ──

    [Fact]
    public async Task AFailedRowWrite_DoesNotFailTheCall()
    {
        var attempts = 0;
        var recorder = new McpCallTimingRecorder(_ => { Interlocked.Increment(ref attempts); throw new IOException("store gone"); }, () => null)
        {
            SlowThreshold = TimeSpan.Zero,
        };

        var (result, text) = await CallRawAsync(recorder, "timing_probe", ("secret", "s"));
        await recorder.WhenWritesDoneAsync();

        Assert.True(result.IsError != true);
        Assert.Contains("answer", text);
        Assert.Equal(1, attempts);
    }

    // ── the read tool ──

    [Fact]
    public async Task GetSlowReads_ReturnsRowsNewestFirst_InsideHoursBack_AndFiltersByTool()
    {
        var now = DateTime.UtcNow;
        await _data.RecordMcpSlowCallAsync(Row("tool_old", now.AddHours(-30)));
        await _data.RecordMcpSlowCallAsync(Row("tool_a", now.AddHours(-5)));
        await _data.RecordMcpSlowCallAsync(Row("tool_b", now.AddHours(-1)));
        await _data.RecordMcpSlowCallAsync(Row("tool_a", now.AddHours(-3)));
        // #9453: a threshold of one hour keeps the test's own calls out of the slow-call rows, even on a loaded machine.
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.FromHours(1) };

        using var all = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls", ("hours_back", 24))).Text);
        var tools = all.RootElement.GetProperty("slow_calls").EnumerateArray().Select(r => r.GetProperty("tool").GetString()).ToArray();
        Assert.Equal(new[] { "tool_b", "tool_a", "tool_a" }, tools);

        using var one = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls", ("hours_back", 24), ("tool", "tool_a"), ("limit", 1))).Text);
        var only = Assert.Single(one.RootElement.GetProperty("slow_calls").EnumerateArray());
        Assert.Equal("tool_a", only.GetProperty("tool").GetString());
        Assert.Equal("SeedSrv", only.GetProperty("server_name").GetString());
        Assert.Equal(12, only.GetProperty("rows_returned").GetInt64());
        Assert.Equal(1, only.GetProperty("process_peak_working_set_bytes").GetInt64());
        Assert.Equal(1, only.GetProperty("duckdb_peak_memory_bytes").GetInt64());
        Assert.Equal(now.AddHours(-3), only.GetProperty("call_time_utc").GetDateTime().ToUniversalTime(), TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task GetSlowReads_ReportsTruncated_WhenTheWindowHoldsMoreSlowCallsThanLimit_AndNotWhenTheyFit()
    {
        var now = DateTime.UtcNow;
        for (var i = 1; i <= 4; i++)
            await _data.RecordMcpSlowCallAsync(Row("tool_a", now.AddMinutes(-i)));
        // #9453: a threshold of one hour keeps the test's own calls out of the slow-call rows, even on a loaded machine.
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.FromHours(1) };

        using var cut = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls", ("hours_back", 24), ("limit", 3))).Text);
        Assert.True(cut.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(3, cut.RootElement.GetProperty("slow_calls").GetArrayLength());

        using var exact = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls", ("hours_back", 24), ("limit", 4))).Text);
        Assert.False(exact.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(4, exact.RootElement.GetProperty("slow_calls").GetArrayLength());

        using var roomy = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls", ("hours_back", 24), ("limit", 10))).Text);
        Assert.False(roomy.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(4, roomy.RootElement.GetProperty("slow_calls").GetArrayLength());
    }

    [Fact]
    public async Task GetSlowReads_ShowsThePerToolFiguresOfTheCallsMadeSoFar()
    {
        var recorder = new McpCallTimingRecorder(_data) { SlowThreshold = TimeSpan.FromHours(1) };
        await CallAsync(recorder, "timing_probe", ("secret", "s"));
        await CallAsync(recorder, "timing_probe", ("secret", "s"));

        using var doc = JsonDocument.Parse((await CallRawAsync(recorder, "get_slow_mcp_calls")).Text);
        var probe = doc.RootElement.GetProperty("per_tool").EnumerateArray().Single(t => t.GetProperty("tool").GetString() == "timing_probe");
        Assert.Equal(2, probe.GetProperty("count").GetInt64());
        Assert.Equal(0, probe.GetProperty("slow_calls").GetInt64());
        Assert.Empty(doc.RootElement.GetProperty("slow_calls").EnumerateArray());
    }

    // ── plumbing ──

    private static McpSlowCall Row(string tool, DateTime startedUtc) =>
        new(startedUtc, tool, "ok", 2500, 10, null, null, null, 1000, 24, 1, 1, "test", "SeedSrv", 12);

    private async Task CallAsync(McpCallTimingRecorder recorder, string tool, params (string Key, object Value)[] args) =>
        await CallRawAsync(recorder, tool, args);

    private async Task<(CallToolResult Result, string Text)> CallRawAsync(McpCallTimingRecorder recorder, string tool, params (string Key, object Value)[] args)
    {
        _host ??= await ServerHost.StartAsync(_data, _serverManager, recorder);
        var arguments = args.ToDictionary(a => a.Key, a => (object?)a.Value);
        var result = await _host.Client.CallToolAsync(tool, arguments);
        return (result, string.Concat(result.Content.OfType<TextContentBlock>().Select(b => b.Text)));
    }

    internal sealed class ServerHost : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly CancellationTokenSource _stop;
        private readonly Task _run;

        private ServerHost(ServiceProvider provider, CancellationTokenSource stop, Task run, McpClient client)
        {
            _provider = provider;
            _stop = stop;
            _run = run;
            Client = client;
        }

        public McpClient Client { get; }

        public static async Task<ServerHost> StartAsync(LocalDataService data, ServerManager servers, McpCallTimingRecorder recorder)
        {
            var c2s = new Pipe();
            var s2c = new Pipe();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(data);
            services.AddSingleton(servers);
            services.AddSingleton(recorder);
            services.AddSingleton(new McpCallTimingLive(recorder));
            var builder = services.AddMcpServer()
                .WithStreamServerTransport(c2s.Reader.AsStream(), s2c.Writer.AsStream())
                .WithTools<McpQueryTools>()
                .WithTools<McpSlowCallTools>()
                .WithTools<TimingProbeTools>();
            builder.WithRequestFilters(McpHostService.AddCallToolFilters);

            var provider = services.BuildServiceProvider();
            var stop = new CancellationTokenSource();
            var run = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);
            var client = await McpClient.CreateAsync(new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));
            return new ServerHost(provider, stop, run, client);
        }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            _stop.Cancel();
            await _provider.DisposeAsync();
            try { await _run; } catch (Exception) { /* shutdown */ }
        }
    }
}
