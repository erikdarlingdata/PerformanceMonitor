/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Ported from erikdarlingdata/PerformanceStudio dev (85492a1)
/// <c>src/PlanViewer.Core/Services/ConfigLoader.cs</c>: the JSON options and the malformed-file
/// fallback for <see cref="AnalyzerConfig"/>. Unlike PS's <c>Load</c>, this type does NOT probe
/// <c>./.planview.json</c> or <c>~/.planview.json</c> — that would couple PM to another product's
/// file convention. Each PM host (Darling's <c>darling.json</c>, Lite's <c>settings.json</c>, the
/// Viewer's own config read) resolves its own file path and hands the text (or the file path) to
/// <see cref="Parse"/> / <see cref="LoadFile"/>.
/// </summary>
public static class ConfigLoader
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    /// <summary>
    /// Parses an <c>analyzer</c> JSON fragment (the same shape as PS's <c>.planview.json</c> root:
    /// <c>{ "rules": { "disabled": [...], "severity_overrides": {...} } }</c>) into an
    /// <see cref="AnalyzerConfig"/>. A malformed or empty document returns
    /// <see cref="AnalyzerConfig.Default"/> rather than throwing, matching PS's
    /// <c>config ?? AnalyzerConfig.Default</c> fallback for a JSON <c>null</c> — extended here to also
    /// cover a document that fails to parse at all, since PM's callers hand this a section pulled out
    /// of a larger host config file rather than a dedicated file PS already validated exists.
    /// </summary>
    public static AnalyzerConfig Parse(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return AnalyzerConfig.Default;

        try
        {
            var config = JsonSerializer.Deserialize<AnalyzerConfig>(json, s_jsonOptions);
            return config ?? AnalyzerConfig.Default;
        }
        catch (JsonException)
        {
            return AnalyzerConfig.Default;
        }
    }

    /// <summary>
    /// Reads and parses <paramref name="path"/> the same way <see cref="Parse"/> does. A missing file
    /// or an unreadable one returns <see cref="AnalyzerConfig.Default"/> (PS's <c>Load</c> throws
    /// <see cref="System.IO.FileNotFoundException"/> for an explicit path that does not exist; PM's
    /// callers never pass an explicit user-supplied path here, so a missing file is treated the same
    /// as "no analyzer section" rather than an error).
    /// </summary>
    public static AnalyzerConfig LoadFile(string path)
    {
        try
        {
            if (!System.IO.File.Exists(path))
                return AnalyzerConfig.Default;

            return Parse(System.IO.File.ReadAllText(path));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return AnalyzerConfig.Default;
        }
    }
}
