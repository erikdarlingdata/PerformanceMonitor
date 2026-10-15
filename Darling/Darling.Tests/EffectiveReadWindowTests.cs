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
    public async System.Threading.Tasks.Task AStorageGrowthRead_RecordsTheDaysItReads_NotItsHoursBack()
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

        /* A refused window notes nothing and never reaches the store. */
        using var refused = ReadScope.Open(null);
        Assert.NotNull(await DarlingMcpFinOpsTools.ReadStorageGrowthAsync(postgres, (1, "SRV1"), 25, 10, "db", null, cancelled.Token));
        Assert.Null(refused.Scope.WindowStartUtc);
    }

    /// <summary>
    /// #5814 review, the negative pin: index_analysis and database_sizes read the newest snapshot, so their slow-read row
    /// records no window (the first fix noted 24 hours for them through the shared check); a view that reads
    /// <c>hours_back</c> records it, and storage_growth notes nothing at this step (it notes its own span in the read).
    /// </summary>
    [Fact]
    public void ALatestSnapshotFinOpsView_RecordsNoWindow_AndAWindowedViewRecordsItsHoursBack()
    {
        foreach (var view in new[] { DarlingMcpFinOpsTools.DatabaseSizesView, DarlingMcpFinOpsTools.IndexAnalysisView, DarlingMcpFinOpsTools.StorageGrowthView })
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
/// window it ran with, so its slow-read row is never windowless; no such tool notes only on a conditional arm unless it is
/// rostered with its reason; every route in <see cref="ReadWindowBand.FixedHours"/> is a real tool with no window argument
/// that notes its window; no Darling read calls the shared validators directly, which would skip the note, unless rostered;
/// and no caller of the validators that end the window now anchors its read on an <c>as_of</c>.
///
/// <para><b>The bound these rest on, stated.</b> Every scan reads the service source through
/// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>, so a comment or a string that names a validator is neither a
/// note nor an offender. A tool's source is its region (its attribute up to the next tool's) plus every same-file method its
/// region calls by name; a helper in another file is not read, which is why storage_growth's own note has a direct pin
/// (<see cref="EffectiveReadWindowTests.AStorageGrowthRead_RecordsTheDaysItReads_NotItsHoursBack"/>) rather than this census.
/// Noting is checked per region, not per branch: the conditional-arm check sees a note that sits on the arm of a
/// <c>?:</c>, a <c>??</c> or a <c>switch</c> expression, and does NOT see one guarded by an <c>if</c> or a <c>case</c>
/// label (<c>get_pvs_stats</c> notes its trend window only when <c>trend_hours_back</c> is not 0, correctly, and passes
/// unrostered). A per-branch census would need a parse; the direct pins carry the branches that matter.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class EffectiveReadWindowCensusTests
{
    private static readonly string[] WindowParameters = ["hours", "hours_back", "days_back"];

    private static readonly Regex Noting = new(@"DarlingReadWindow\.\w+\(|ReadScope\.NoteWindow\(", RegexOptions.Compiled);

    /* The validators that end the window at the process clock: right only for a read with no as_of. */
    private static readonly Regex NowEnding = new(@"DarlingReadWindow\.(ValidateHoursBack|ValidateDaysBack)\(", RegexOptions.Compiled);

    /* A note on an arm: the code before it, past whitespace (line breaks included) and an await, ends in ?, ?? or : (a
       ternary arm, or a case label) or in => (a switch-expression arm, or an expression body: the reason the roster below
       is checked for equality, so a false hit is seen and named rather than ignored). */
    private static readonly Regex ArmBefore = new(@"(?:\?|:|=>)\s*(?:await\s+)?$", RegexOptions.Compiled);

    /// <summary>The tools whose note sits on a conditional arm, each with the reason the other arms need none.</summary>
    private static readonly Dictionary<string, string> ConditionalNoteRoster = new(StringComparer.Ordinal)
    {
        /* Empty today. get_finops was the case that prompted this check (a ternary whose storage_growth arm noted nothing);
           it now checks hours_back through the shared validator and notes per view in NoteViewWindow, whose arms the direct
           pins in EffectiveReadWindowTests carry. */
    };

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

    /// <summary>The callers of the now-ending validators that are not MCP tools, each with the reason it has no as_of.</summary>
    private static readonly Dictionary<string, string> NowEndingNonToolRoster = new(StringComparer.Ordinal)
    {
        ["DarlingWebEndpoints.cs"] = "FleetParamRefusal: /api/fleet takes hours and worst, no as_of, and its note is "
            + "inert (it runs before the route's ReadScope opens).",
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

    /// <summary>The bot's #5830 counterexample: get_finops passed the census on the one arm of a ternary that noted while
    /// its storage_growth arm, a 30 to 90 day read, noted nothing. A tool whose note sits on an arm is a finding unless the
    /// roster names it with the reason its other arms need none; a roster entry that no longer matches fails too.</summary>
    [Fact]
    public void NoWindowedTool_NotesOnAConditionalArm_UnlessRostered()
    {
        var tools = WindowedTools().Concat(Tools().Where(t => ReadWindowBand.FixedHours.ContainsKey(t.Name)));
        var conditional = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var (name, method) in tools)
        {
            var source = ToolSource(name, method.Name);
            foreach (Match note in Noting.Matches(source))
            {
                var before = source[Math.Max(0, note.Index - 200)..note.Index];
                if (ArmBefore.IsMatch(before))
                {
                    conditional.Add(name);
                }
            }
        }

        Assert.Equal(ConditionalNoteRoster.Keys.OrderBy(k => k, StringComparer.Ordinal), conditional);
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

    /// <summary>The bot's #5830 riskiest line 3: ValidateHoursBack and ValidateDaysBack note a window that ends at the
    /// process clock, which is right only because no caller resolves an as_of. A tool that calls either one must take no
    /// as_of and resolve none; a caller that is not a tool is rostered with the reason it has none. A tool that anchors its
    /// read calls DarlingReadWindow.Validate with the as_of instead, which ends the window at the anchor.</summary>
    [Fact]
    public void NoCallerOfTheNowEndingValidators_AnchorsItsReadOnAsOf()
    {
        var checkedFiles = new HashSet<string>(StringComparer.Ordinal);
        var anchored = new List<string>();

        foreach (var (name, method) in Tools())
        {
            var source = ToolSource(name, method.Name);
            if (!NowEnding.IsMatch(source)) continue;

            checkedFiles.Add(s_sources.Value.First(s => s.Raw.Contains($"[McpServerTool(Name = \"{name}\"", StringComparison.Ordinal)).Path);
            if (method.GetParameters().Any(p => p.Name == "as_of") || source.Contains("ResolveAsOf(", StringComparison.Ordinal))
            {
                anchored.Add(name);
            }
        }

        /* Seven tool files when this landed. */
        Assert.True(checkedFiles.Count >= 6, $"only {checkedFiles.Count} tool files call the now-ending validators: the scan is broken");
        Assert.True(anchored.Count == 0, "These tools anchor on as_of but note a window ending now; call DarlingReadWindow.Validate "
            + "with the as_of instead: " + string.Join(", ", anchored));

        var unaccounted = s_sources.Value
            .Where(s => Path.GetFileName(s.Path) != "DarlingReadWindow.cs" && NowEnding.IsMatch(s.Code))
            .Where(s => !checkedFiles.Contains(s.Path) && !NowEndingNonToolRoster.ContainsKey(Path.GetFileName(s.Path)))
            .Select(s => Path.GetFileName(s.Path))
            .ToList();
        Assert.True(unaccounted.Count == 0, "These files call a now-ending validator outside a tool the census checks; "
            + "roster each with the reason it has no as_of: " + string.Join(", ", unaccounted));
    }
}
