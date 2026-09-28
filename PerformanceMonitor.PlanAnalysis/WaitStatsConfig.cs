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
using System.Text.Json.Serialization;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Loads and serves the wait stats configuration from Resources/WaitStats.json (embedded in
/// PerformanceMonitor.PlanAnalysis). This is the single source of truth for per-wait display
/// flags and curated descriptions — no other file duplicates a wait type's classification.
/// </summary>
public static class WaitStatsConfig
{
    public sealed class Entry
    {
        [JsonPropertyName("name")]
        public string Name { get; init; } = "";

        [JsonPropertyName("isPreemptive")]
        public bool IsPreemptive { get; init; }

        [JsonPropertyName("isExternal")]
        public bool IsExternal { get; init; }

        [JsonPropertyName("isImplemented")]
        public bool IsImplemented { get; init; }

        [JsonPropertyName("isEnabled")]
        public bool IsEnabled { get; init; }

        [JsonPropertyName("showWaitCount")]
        public bool? ShowWaitCount { get; init; }

        [JsonPropertyName("showAverageWaitTime")]
        public bool? ShowAverageWaitTime { get; init; }

        [JsonPropertyName("timeCalculationModel")]
        public string? TimeCalculationModel { get; init; }

        [JsonPropertyName("description")]
        public string? Description { get; init; }
    }

    private static readonly Lazy<Dictionary<string, Entry>> _byName = new(Load);

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    public static Entry? Get(string waitType)
    {
        if (string.IsNullOrEmpty(waitType)) return null;
        return _byName.Value.TryGetValue(waitType, out var e) ? e : null;
    }

    /// <summary>
    /// True iff effective per-wait latency (wait_ms / wait_count) should be surfaced alongside
    /// totals. Defaults to false when the wait isn't in the config — unknown waits don't get a
    /// latency line.
    /// </summary>
    public static bool ShowAverageWaitTime(string waitType)
        => Get(waitType)?.ShowAverageWaitTime ?? false;

    /// <summary>
    /// The curated end-user description for a wait type, or null if the config has none for it
    /// (most entries are seeded but not yet described — see Resources/WaitStats.json).
    /// </summary>
    public static string? Description(string waitType)
        => Get(waitType)?.Description;

    private static Dictionary<string, Entry> Load()
    {
        // The JSON ships embedded in this assembly (manifest name
        // PerformanceMonitor.PlanAnalysis.Resources.WaitStats.json) and is referenced by Darling,
        // Lite, and the viewer via the same ProjectReference, so resolve by suffix in case a
        // consumer ever re-embeds it under its own manifest prefix.
        var asm = typeof(WaitStatsConfig).Assembly;
        var resourceName = asm.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith("Resources.WaitStats.json", StringComparison.Ordinal))
            ?? throw new InvalidOperationException(
                $"Embedded resource ending in 'Resources.WaitStats.json' not found in {asm.GetName().Name}. " +
                "Check that Resources/WaitStats.json is included as <EmbeddedResource> in the project.");

        using var stream = asm.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Failed to open embedded resource '{resourceName}'.");

        using var reader = new StreamReader(stream);
        var json = reader.ReadToEnd();

        return ParseDocument(json);
    }

    /// <summary>
    /// Parses a WaitStats.json document (the same shape as the embedded resource) into a
    /// case-insensitive lookup by wait name. Internal seam so tests can pin the parsing/lookup
    /// mechanism (description round-trip, unknown-wait miss) against a small synthetic document
    /// instead of depending on which real entries happen to have a curated description filled in.
    /// </summary>
    internal static Dictionary<string, Entry> ParseDocument(string json)
    {
        var doc = JsonSerializer.Deserialize<Document>(json, _jsonOptions)
            ?? throw new InvalidOperationException("WaitStats.json deserialized to null.");

        var dict = new Dictionary<string, Entry>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in doc.WaitStats)
        {
            if (string.IsNullOrEmpty(entry.Name)) continue;
            dict[entry.Name] = entry;
        }
        return dict;
    }

    private sealed class Document
    {
        [JsonPropertyName("waitStats")]
        public List<Entry> WaitStats { get; init; } = new();
    }
}
