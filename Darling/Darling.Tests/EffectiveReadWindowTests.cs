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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5814: the slow-read row records the window the read ran with, its default included, and a route whose window is fixed
/// in code records that window. Field: 29 of 30 slow <c>get_query_store_query_history</c> rows and every
/// <c>get_collection_health</c> row carried no window, because the row lifted a window only from an argument the caller sent.
/// </summary>
public sealed class EffectiveReadWindowTests
{
    private const string HistoryTool = "get_query_store_query_history";
    private const string CollectionHealthTool = "get_collection_health";

    private static McpToolParameterTypes Types()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer()
            .WithGeminiCompatibleTools<DarlingMcpQueryStoreHistoryTools>()
            .WithGeminiCompatibleTools<DarlingMcpDataTools>();
        var provider = services.BuildServiceProvider();
        _ = provider.GetServices<McpServerTool>().ToList(); // creating the tools fills the declared defaults
        return provider.GetRequiredService<McpToolParameterTypes>();
    }

    private static Dictionary<string, JsonElement> Sent(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());
    }

    private static double Hours(SlowReadRecord record) => (record.WindowEnd!.Value - record.WindowStart!.Value).TotalHours;

    [Fact]
    public void ADefaultedHistoryCall_RecordsTheTwentyFourHoursItRead()
    {
        var types = Types();
        Assert.True(types.TryGetDefault(HistoryTool, "hours_back", out var declared));
        var hoursBack = Convert.ToInt32(declared, System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(24, hoursBack);

        using var opened = ReadScope.Open(null);
        /* What the tool runs after it resolves its arguments: the declared default, through the shared validator. */
        Assert.Null(DarlingReadWindow.Validate(hoursBack, null, out var end));

        var arguments = new JsonObject { ["server_name"] = "SRV1", ["database_name"] = "db", ["query_id"] = 7 };
        var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, HistoryTool, ReadOutcome.Ok, 9_000, arguments, null, DateTime.UtcNow);

        Assert.NotNull(record.WindowStart);
        Assert.Equal(24, Hours(record), 3);
        Assert.Equal(DateTimeKind.Unspecified, record.WindowEnd!.Value.Kind);
        Assert.Equal(end, DateTime.SpecifyKind(record.WindowEnd.Value, DateTimeKind.Utc));
        Assert.Equal(ReadWindowBand.OneDay, ReadWindowBand.Of(record.WindowStart, record.WindowEnd));

        /* The read-latency band the call is admitted and recorded under is the same band. */
        var band = McpToolLatencyFilter.WindowBandOf(HistoryTool, Sent("""{"server_name":"SRV1","database_name":"db","query_id":7}"""), types);
        Assert.Equal(ReadWindowBand.OneDay, band);
    }

    [Fact]
    public void ACollectionHealthCall_RecordsItsFixedSevenDays()
    {
        Assert.Equal(168, ReadWindowBand.FixedHours[CollectionHealthTool]);

        using var opened = ReadScope.Open(null);
        var now = DateTime.UtcNow;
        ReadScope.NoteWindow(now.AddHours(-ReadWindowBand.CollectionHealthHours), now);
        var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, CollectionHealthTool, ReadOutcome.Ok, 9_000,
            new JsonObject { ["server_name"] = "SRV1" }, null, DateTime.UtcNow);

        Assert.Equal(168, Hours(record), 3);
        Assert.Equal(ReadWindowBand.SevenDays, ReadWindowBand.Of(record.WindowStart, record.WindowEnd));

        /* Admission, the read cache and the read-latency row band it the same, on both surfaces, before it runs. */
        Assert.Equal(ReadWindowBand.SevenDays, McpToolLatencyFilter.WindowBandOf(CollectionHealthTool, Sent("""{"server_name":"SRV1"}"""), Types()));
        Assert.Equal(ReadWindowBand.SevenDays, DarlingWebEndpoints.WebWindowBand(CollectionHealthTool, _ => null));
    }

    [Fact]
    public void AnExplicitSixHourCall_RecordsSixHours()
    {
        using var opened = ReadScope.Open(null);
        Assert.Null(DarlingReadWindow.Validate(6, null, out _));

        var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, HistoryTool, ReadOutcome.Ok, 9_000,
            new JsonObject { ["hours_back"] = 6 }, null, DateTime.UtcNow);

        Assert.Equal(6, Hours(record), 3);
        Assert.Equal(ReadWindowBand.OneDay, ReadWindowBand.Of(record.WindowStart, record.WindowEnd));
        Assert.Equal(ReadWindowBand.OneDay, McpToolLatencyFilter.WindowBandOf(HistoryTool, Sent("""{"hours_back":6}"""), Types()));
    }

    [Fact]
    public void AReadWithNoWindow_RecordsNone()
    {
        using var opened = ReadScope.Open(null);
        var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, "get_server_info", ReadOutcome.Ok, 9_000,
            new JsonObject { ["server_name"] = "SRV1" }, null, DateTime.UtcNow);

        Assert.Null(record.WindowStart);
        Assert.Null(record.WindowEnd);
        Assert.Equal(ReadWindowBand.None, McpToolLatencyFilter.WindowBandOf("get_server_info", Sent("""{"server_name":"SRV1"}"""), Types()));
        Assert.Equal(ReadWindowBand.None, DarlingWebEndpoints.WebWindowBand("get_server_info", _ => null));
    }

    /* #5844: get_investigation_reports reads the window through ReportsAsync, on MCP and on the web panel alike. The data
       source points at a closed port and the token is cancelled before the call, so the read stops at its first
       connection open: the window is noted (or not) before that, and nothing reaches a server. */
    private static async Task<(SlowReadRecord Record, DateTime? NotedStart)> InvestigationReportsRecord(string? requestId,
        int hoursBack, JsonObject arguments)
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=none;Database=none;Timeout=1");
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        using var opened = ReadScope.Open(null);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingMcpInvestigationTools.ReportsAsync(
            postgres, McpDataDetail.Full, null, requestId, hoursBack, cancelled.Token));
        return (SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, InvestigationReportsTool, ReadOutcome.Ok, 9_000, arguments, null,
            DateTime.UtcNow), opened.Scope.WindowStartUtc);
    }

    private const string InvestigationReportsTool = "get_investigation_reports";

    [Fact]
    public async Task ADefaultedInvestigationReportsCall_RecordsTheTwentyFourHoursItRead()
    {
        var declared = typeof(DarlingMcpInvestigationTools).GetMethod(nameof(DarlingMcpInvestigationTools.GetInvestigationReports))!
            .GetParameters().Single(p => p.Name == "hours_back").DefaultValue;
        Assert.Equal(24, declared);

        var (record, _) = await InvestigationReportsRecord(null, (int)declared!, new JsonObject());

        Assert.NotNull(record.WindowStart);
        Assert.Equal(24, Hours(record), 3);
        Assert.Equal(ReadWindowBand.OneDay, ReadWindowBand.Of(record.WindowStart, record.WindowEnd));
    }

    [Fact]
    public async Task AnExplicitInvestigationReportsWindow_RecordsThatSpan_AndARefusedOneRecordsNone()
    {
        /* The tool notes the span itself: the row does not depend on lifting hours_back from the sent arguments. */
        var (record, notedStart) = await InvestigationReportsRecord(null, 72, new JsonObject { ["hours_back"] = 72 });
        Assert.NotNull(notedStart);
        Assert.Equal(72, Hours(record), 3);
        Assert.Equal(ReadWindowBand.SevenDays, ReadWindowBand.Of(record.WindowStart, record.WindowEnd));

        /* Out of range on the window arm: refused with the shared text, before any read, and no window noted. */
        foreach (var refused in new[] { 0, 169 })
        {
            using var opened = ReadScope.Open(null);
            var answer = await DarlingMcpInvestigationTools.ReportsAsync(null!, McpDataDetail.Full, null, null, refused,
                CancellationToken.None);
            Assert.Equal(McpHelpers.ValidateHoursBack(refused), answer);
            Assert.Null(opened.Scope.WindowStartUtc);
        }
    }

    [Fact]
    public async Task AnInvestigationReportsCallForOneRequest_RecordsNoWindow()
    {
        /* hours_back is ignored with request_id, so the row carries no window whatever hours_back says, even when the
           caller sent it and the arguments would otherwise lift it, and an out-of-range hours_back is not refused: the
           call goes on to read the request. A value past DateTime's range must not throw into an error envelope either:
           the helper's read only ends in the cancellation when the call reached the store. */
        var id = Guid.NewGuid().ToString();
        foreach (var hoursBack in new[] { 24, 6, 0, 500, 1_000_000_000 })
        {
            var (record, notedStart) = await InvestigationReportsRecord(id, hoursBack,
                new JsonObject { ["request_id"] = id, ["hours_back"] = hoursBack });
            Assert.Null(notedStart);
            Assert.Null(record.WindowStart);
            Assert.Null(record.WindowEnd);
        }
    }

    [Fact]
    public void ANoteOutsideAScope_ARefusedWindow_AndASecondNote_ChangeNothing()
    {
        ReadScope.NoteWindow(DateTime.UtcNow.AddHours(-1), DateTime.UtcNow); // no scope: ignored, no throw

        using var opened = ReadScope.Open(null);
        Assert.NotNull(DarlingReadWindow.Validate(-5, null, out _)); // refused: notes nothing
        Assert.Null(opened.Scope.WindowStartUtc);

        ReadScope.NoteWindow(DateTime.UtcNow, DateTime.UtcNow.AddHours(-1)); // inverted: ignored
        Assert.Null(opened.Scope.WindowEndUtc);

        Assert.Null(DarlingReadWindow.Validate(2, null, out _));
        Assert.Null(DarlingReadWindow.Validate(100, null, out _)); // a later read inside the tool: the tool's window stays
        Assert.Equal(2, (opened.Scope.WindowEndUtc!.Value - opened.Scope.WindowStartUtc!.Value).TotalHours, 3);
    }

    [Fact]
    public void ANotedWindow_WinsOverTheLiftedArguments_AndNoNoteFallsBackToThem()
    {
        using var noted = ReadScope.Open(null);
        Assert.Null(DarlingReadWindow.ValidateDaysBack(2, 30));
        var record = SlowReadLog.Build(noted.Scope, ReadSurface.Web, "r", ReadOutcome.Ok, 9_000,
            new JsonObject { ["hours"] = 1 }, null, DateTime.UtcNow);
        Assert.Equal(48, Hours(record), 3);

        using var lifted = ReadScope.Open(null);
        var fallback = SlowReadLog.Build(lifted.Scope, ReadSurface.Web, "r", ReadOutcome.Ok, 9_000,
            new JsonObject { ["hours"] = 3 }, null, DateTime.UtcNow);
        Assert.Equal(3, Hours(fallback), 3);
    }

    /// <summary>
    /// #5814 review: <c>get_finops view=storage_growth</c> reads a span its <c>hours_back</c> does not name (30 days at the
    /// default), so it notes that span itself; the arm used to note nothing and its slow-read row came back windowless. This
    /// runs the real read up to its first store call: the token is already cancelled, so the read stops there, after the
    /// note. Remove the note and every expected span below reads null.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task AStorageGrowthOrUtilizationRead_RecordsTheDaysItReads_NotItsHoursBack()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=darling;Database=none");
        using var cancelled = new System.Threading.CancellationTokenSource();
        await cancelled.CancelAsync();

        async System.Threading.Tasks.Task<double?> NotedHours(int hoursBack, string? database)
        {
            using var opened = ReadScope.Open(null);
            var thrown = await Record.ExceptionAsync(() => DarlingMcpFinOpsTools.ReadStorageGrowthAsync(
                postgres, (1, "SRV1"), hoursBack, 10, database, null, cancelled.Token));
            Assert.NotNull(thrown); // the read reached the store, so every check before it passed
            var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, "get_finops", ReadOutcome.Ok, 9_000,
                new JsonObject { ["view"] = DarlingMcpFinOpsTools.StorageGrowthView }, null, DateTime.UtcNow);
            return record.WindowStart is null ? null : Hours(record);
        }

        /* The databases level compares the newest snapshot with the ones 7 and 30 days back, whatever hours_back says. */
        Assert.Equal(30 * 24, await NotedHours(24, null) ?? 0, 3);
        Assert.Equal(30 * 24, await NotedHours(14 * 24, null) ?? 0, 3);
        /* The objects level reads its window: 30 days at the default hours_back, else hours_back's whole days. */
        Assert.Equal(30 * 24, await NotedHours(24, "db") ?? 0, 3);
        Assert.Equal(14 * 24, await NotedHours(14 * 24, "db") ?? 0, 3);
        Assert.Equal(90 * 24, await NotedHours(90 * 24, "db") ?? 0, 3);

        /* utilization's rows span its 7-day trend, the wider of its two windows. */
        using (var utilization = ReadScope.Open(null))
        {
            Assert.NotNull(await Record.ExceptionAsync(() => DarlingMcpFinOpsTools.ReadUtilizationAsync(postgres, (1, "SRV1"), 24, 10, cancelled.Token)));
            var row = SlowReadLog.Build(utilization.Scope, ReadSurface.Mcp, "get_finops", ReadOutcome.Ok, 9_000,
                new JsonObject { ["view"] = DarlingMcpFinOpsTools.UtilizationView }, null, DateTime.UtcNow);
            Assert.Equal(DarlingMcpFinOpsTools.UtilizationTrendDays * 24, Hours(row), 3);
        }

        using (var utilizationRefused = ReadScope.Open(null))
        {
            Assert.NotNull(await DarlingMcpFinOpsTools.ReadUtilizationAsync(postgres, (1, "SRV1"), 48, 10, cancelled.Token));
            Assert.Null(utilizationRefused.Scope.WindowStartUtc);
        }

        /* A refused window notes nothing and never reaches the store. */
        using var refused = ReadScope.Open(null);
        Assert.NotNull(await DarlingMcpFinOpsTools.ReadStorageGrowthAsync(postgres, (1, "SRV1"), 25, 10, "db", null, cancelled.Token));
        Assert.Null(refused.Scope.WindowStartUtc);
    }

    /// <summary>
    /// #5814 review, the negative pin: index_analysis and database_sizes read the newest snapshot, so their slow-read row
    /// records no window (the first fix noted 24 hours for them through the shared check); a view that reads
    /// <c>hours_back</c> records it, and storage_growth and utilization note nothing at this step (each notes its own span
    /// in its read).
    /// </summary>
    [Fact]
    public void ALatestSnapshotFinOpsView_RecordsNoWindow_AndAWindowedViewRecordsItsHoursBack()
    {
        foreach (var view in new[] { DarlingMcpFinOpsTools.DatabaseSizesView, DarlingMcpFinOpsTools.IndexAnalysisView, DarlingMcpFinOpsTools.StorageGrowthView, DarlingMcpFinOpsTools.UtilizationView })
        {
            using var opened = ReadScope.Open(null);
            DarlingMcpFinOpsTools.NoteViewWindow(view, 24);
            var record = SlowReadLog.Build(opened.Scope, ReadSurface.Mcp, "get_finops", ReadOutcome.Ok, 9_000,
                new JsonObject { ["view"] = view, ["server_name"] = "SRV1" }, null, DateTime.UtcNow);
            Assert.Null(record.WindowStart);
            Assert.Null(record.WindowEnd);
        }

        using var windowed = ReadScope.Open(null);
        DarlingMcpFinOpsTools.NoteViewWindow(DarlingMcpFinOpsTools.HighImpactView, 24);
        var row = SlowReadLog.Build(windowed.Scope, ReadSurface.Mcp, "get_finops", ReadOutcome.Ok, 9_000,
            new JsonObject { ["view"] = DarlingMcpFinOpsTools.HighImpactView }, null, DateTime.UtcNow);
        Assert.Equal(24, Hours(row), 3);
    }
}

/// <summary>
/// #5814 census: every MCP tool that takes a window argument (<c>hours</c>, <c>hours_back</c>, <c>days_back</c>) notes the
/// window it ran with, so its slow-read row is never windowless; every route in <see cref="ReadWindowBand.FixedHours"/> is a
/// real tool with no window argument that notes its window; and no Darling read calls the shared validators directly,
/// which would skip the note, unless rostered with its reason.
///
/// <para><b>The bound these rest on, stated.</b> Every scan reads the service source through
/// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>, so a comment or a string that names a validator is neither a
/// note nor an offender. A tool's source is its region (its attribute up to the next tool's) plus every same-file method its
/// region calls by name. Noting is checked per region, not per branch: a tool that notes on one arm and not another passes,
/// which is why the get_finops views carry direct behavior pins in <see cref="EffectiveReadWindowTests"/>.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class EffectiveReadWindowCensusTests
{
    private static readonly string[] WindowParameters = ["hours", "hours_back", "days_back"];

    private static readonly Regex Noting = new(@"DarlingReadWindow\.\w+\(|ReadScope\.NoteWindow\(", RegexOptions.Compiled);

    /// <summary>The direct calls of the shared validators that stay, by file and validator, each with its reason.</summary>
    private static readonly Dictionary<string, string> DirectCallRoster = new(StringComparer.Ordinal)
    {
        ["DarlingMcpHealthTools.cs|ValidateDaysBack"] = "get_daily_summary_range anchors its days on as_of, and the first "
            + "note stays: DarlingReadWindow.ValidateDaysBack would note a span ending now, so the tool checks through the "
            + "shared validator and notes the anchored whole-day range itself with ReadScope.NoteWindow.",
        ["DarlingMcpFinOpsTools.cs|ValidateHoursBack"] = "get_finops checks hours_back for every view but storage_growth, "
            + "and notes per view after its checks in NoteViewWindow: index_analysis and database_sizes read the newest "
            + "snapshot and note nothing, storage_growth notes the span it reads. Noting in the check would note 24 hours "
            + "for a snapshot read.",
    };

    private sealed record Source(string Path, string Raw, string Code);

    /* Walked once per test run: the census reads the same tree in every fact. */
    private static readonly Lazy<IReadOnlyList<Source>> s_sources = new(LoadSources);

    private static IEnumerable<(string Name, MethodInfo Method)> Tools() =>
        typeof(DarlingMcpDataTools).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
            .Where(x => x.Attribute?.Name is { Length: > 0 })
            .Select(x => (x.Attribute!.Name!, x.Method));

    /// <summary>The walked source of tool <paramref name="name"/>: its region (its attribute up to the next tool's
    /// attribute), plus every same-file method its region calls by name (a tool that delegates to its own internal
    /// overload, a CollectAsync, a Core method). The attribute is found in the raw text, since the walk blanks its
    /// string; the walk keeps every offset, so the region is then cut from the walked code.</summary>
    private static string ToolSource(string name, string methodName)
    {
        foreach (var source in s_sources.Value)
        {
            var at = source.Raw.IndexOf($"[McpServerTool(Name = \"{name}\"", StringComparison.Ordinal);
            if (at < 0) continue;

            var next = source.Raw.IndexOf("[McpServerTool(", at + 1, StringComparison.Ordinal);
            var region = next < 0 ? source.Code[at..] : source.Code[at..next];
            var called = Regex.Matches(region, @"\b([A-Z]\w*)(?:<[^>()]*>)?\(").Select(m => m.Groups[1].Value).Append(methodName).ToHashSet(StringComparer.Ordinal);
            /* One declaration scan per file, matched against the called names, rather than one regex per name over the file. */
            var helpers = Regex.Matches(source.Code, @"\b(?:static|async)\b[^\n;=]*?\b([A-Z]\w*)(?:<[^>()]*>)?\(")
                .Where(m => called.Contains(m.Groups[1].Value))
                .Select(m => source.Code.Substring(m.Index, Math.Min(6000, source.Code.Length - m.Index)));
            return region + string.Join("\n", helpers);
        }

        return string.Empty;
    }

    private static IReadOnlyList<Source> LoadSources() => ServiceFiles()
        .Select(f => { var raw = File.ReadAllText(f); return new Source(f, raw, CSharpSourceWalker.StripCommentsAndStrings(raw)); })
        .ToList();

    private static IEnumerable<string> ServiceFiles([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "Darling", "PerformanceMonitor.Darling.Service");
            if (Directory.Exists(candidate))
            {
                return Directory.GetFiles(candidate, "*.cs", SearchOption.AllDirectories)
                    .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                             && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal));
            }
        }

        throw new DirectoryNotFoundException("Could not locate Darling/PerformanceMonitor.Darling.Service walking up from " + thisFile);
    }

    private static List<(string Name, MethodInfo Method)> WindowedTools() => Tools()
        .Where(t => t.Method.GetParameters().Any(p => WindowParameters.Contains(p.Name, StringComparer.Ordinal)))
        .ToList();

    private static int LineOf(string text, int index) => text.AsSpan(0, index).Count('\n') + 1;

    [Fact]
    public void EveryToolWithAWindowArgument_NotesTheWindowItRanWith()
    {
        var windowed = WindowedTools();
        Assert.True(windowed.Count >= 60, $"only {windowed.Count} windowed tools were found: the scan is broken, not the surface");

        var missing = windowed
            .Where(t => !Noting.IsMatch(ToolSource(t.Name, t.Method.Name)))
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(missing.Count == 0, "These tools take a window argument but never note the window they ran with "
            + "(call DarlingReadWindow, or ReadScope.NoteWindow for a window the shared validators do not compute): "
            + string.Join(", ", missing));
    }

    [Fact]
    public void EveryFixedWindowRoute_IsARealToolWithNoWindowArgument_ThatNotesItsWindow()
    {
        var tools = Tools().ToDictionary(t => t.Name, t => t.Method, StringComparer.Ordinal);

        foreach (var (route, hours) in ReadWindowBand.FixedHours)
        {
            Assert.True(tools.TryGetValue(route, out var method), $"{route} is not a registered tool");
            Assert.DoesNotContain(method!.GetParameters(), p => WindowParameters.Contains(p.Name, StringComparer.Ordinal));
            Assert.True(hours > 0);
            Assert.Matches(Noting, ToolSource(route, method.Name));
        }
    }

    /// <summary>All four shared validators, ValidateDaysBack included (the first census left it out, and one direct call
    /// survived unseen). A direct call that stays is rostered by file and validator with its reason; an entry whose call is
    /// gone fails too, so the roster cannot outlive what it excuses.</summary>
    [Fact]
    public void NoDarlingRead_CallsTheSharedWindowValidatorsDirectly_UnlessRostered()
    {
        var raw = new Regex(@"McpHelpers\.(ValidateWindow|ValidateUncappedWindow|ValidateHoursBack|ValidateDaysBack)\(", RegexOptions.Compiled);
        var offenders = new List<string>();
        var rostered = new HashSet<string>(StringComparer.Ordinal);

        foreach (var source in s_sources.Value)
        {
            var file = Path.GetFileName(source.Path);
            if (file == "DarlingReadWindow.cs") continue;

            foreach (Match call in raw.Matches(source.Code))
            {
                var key = $"{file}|{call.Groups[1].Value}";
                if (DirectCallRoster.ContainsKey(key))
                {
                    rostered.Add(key);
                }
                else
                {
                    offenders.Add($"{file}:{LineOf(source.Code, call.Index)}");
                }
            }
        }

        Assert.True(offenders.Count == 0, "Call DarlingReadWindow instead, so the read notes its window (#5814): " + string.Join(", ", offenders));
        Assert.Equal(DirectCallRoster.Keys.OrderBy(k => k, StringComparer.Ordinal), rostered.OrderBy(k => k, StringComparer.Ordinal));
    }
}
