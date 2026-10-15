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
}

/// <summary>
/// #5814 census: every MCP tool that takes a window argument (<c>hours</c>, <c>hours_back</c>, <c>days_back</c>) notes the
/// window it ran with, so its slow-read row is never windowless; every route in <see cref="ReadWindowBand.FixedHours"/> is a
/// real tool with no window argument that notes its window; and no Darling read calls the shared validators directly,
/// which would skip the note.
/// </summary>
public sealed class EffectiveReadWindowCensusTests
{
    private static readonly string[] WindowParameters = ["hours", "hours_back", "days_back"];

    private static readonly Regex Noting = new(@"DarlingReadWindow\.\w+\(|ReadScope\.NoteWindow\(", RegexOptions.Compiled);

    private static IEnumerable<(string Name, MethodInfo Method)> Tools() =>
        typeof(DarlingMcpDataTools).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
            .Where(x => x.Attribute?.Name is { Length: > 0 })
            .Select(x => (x.Attribute!.Name!, x.Method));

    /// <summary>The source of tool <paramref name="name"/>: its attribute up to the next tool's attribute, plus every
    /// method of the same name in that file (a tool that delegates to its own internal overload).</summary>
    private static string ToolSource(string name, string methodName, IReadOnlyDictionary<string, string> files)
    {
        foreach (var text in files.Values)
        {
            var at = text.IndexOf($"[McpServerTool(Name = \"{name}\"", StringComparison.Ordinal);
            if (at < 0) continue;

            var next = text.IndexOf("[McpServerTool(", at + 1, StringComparison.Ordinal);
            var region = next < 0 ? text[at..] : text[at..next];
            /* The tool's own overloads, and every same-file helper its body calls (CollectAsync, a Core method). */
            var called = Regex.Matches(region, @"\b([A-Z]\w*)(?:<[^>()]*>)?\(").Select(m => m.Groups[1].Value).Append(methodName).Distinct();
            var helpers = called.SelectMany(helper => Regex.Matches(text, $@"\b(?:static|async)[^\n;=]*\b{Regex.Escape(helper)}(?:<[^>()]*>)?\(")
                .Select(m => text.Substring(m.Index, Math.Min(6000, text.Length - m.Index))));
            return region + string.Join("\n", helpers);
        }

        return string.Empty;
    }

    private static IReadOnlyDictionary<string, string> ServiceSources([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "Darling", "PerformanceMonitor.Darling.Service");
            if (Directory.Exists(candidate))
            {
                return Directory.GetFiles(candidate, "*.cs", SearchOption.AllDirectories)
                    .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                             && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                    .ToDictionary(f => f, File.ReadAllText);
            }
        }

        throw new DirectoryNotFoundException("Could not locate Darling/PerformanceMonitor.Darling.Service walking up from " + thisFile);
    }

    [Fact]
    public void EveryToolWithAWindowArgument_NotesTheWindowItRanWith()
    {
        var files = ServiceSources();
        var windowed = Tools()
            .Where(t => t.Method.GetParameters().Any(p => WindowParameters.Contains(p.Name, StringComparer.Ordinal)))
            .ToList();

        Assert.True(windowed.Count >= 60, $"only {windowed.Count} windowed tools were found: the scan is broken, not the surface");

        var missing = windowed
            .Where(t => !Noting.IsMatch(ToolSource(t.Name, t.Method.Name, files)))
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
        var files = ServiceSources();
        var tools = Tools().ToDictionary(t => t.Name, t => t.Method, StringComparer.Ordinal);

        foreach (var (route, hours) in ReadWindowBand.FixedHours)
        {
            Assert.True(tools.TryGetValue(route, out var method), $"{route} is not a registered tool");
            Assert.DoesNotContain(method!.GetParameters(), p => WindowParameters.Contains(p.Name, StringComparer.Ordinal));
            Assert.True(hours > 0);
            Assert.Matches(Noting, ToolSource(route, method.Name, files));
        }
    }

    [Fact]
    public void NoDarlingRead_CallsTheSharedWindowValidatorsDirectly()
    {
        var raw = new Regex(@"McpHelpers\.(ValidateWindow|ValidateUncappedWindow|ValidateHoursBack)\(", RegexOptions.Compiled);
        var offenders = new List<string>();
        foreach (var (path, text) in ServiceSources())
        {
            if (Path.GetFileName(path) == "DarlingReadWindow.cs") continue;
            var lineNumber = 0;
            foreach (var line in text.Split('\n'))
            {
                lineNumber++;
                var trimmed = line.TrimStart();
                if (trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("*", StringComparison.Ordinal)
                    || trimmed.StartsWith("/*", StringComparison.Ordinal))
                {
                    continue;
                }

                if (raw.IsMatch(line)) offenders.Add($"{Path.GetFileName(path)}:{lineNumber}");
            }
        }

        Assert.True(offenders.Count == 0, "Call DarlingReadWindow instead, so the read notes its window (#5814): " + string.Join(", ", offenders));
    }
}
