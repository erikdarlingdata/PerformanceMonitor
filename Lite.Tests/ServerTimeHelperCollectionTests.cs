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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4776: <c>ServerTimeHelper.UtcOffsetMinutes</c> and <c>ServerTimeHelper.CurrentDisplayMode</c> are settings shared
/// by the whole test process. Classes that write them carry <c>[Collection("server-time-helper")]</c> so xUnit runs
/// them one at a time. A class that only READS them (through <c>FormatServerTime</c>, <c>ToServerTime</c> and the
/// like) needs the same collection: if it formats one time twice, a writer running between the two calls makes the
/// two texts differ. <c>QueryWindowTruncationTests</c> did exactly that and failed by a 4 hour difference.
/// </summary>
public sealed class ServerTimeHelperCollectionTests
{
    private const string CollectionName = "server-time-helper";

    /// <summary>The members of <c>ServerTimeHelper</c> that read or write one of the two shared settings.</summary>
    private static readonly Regex ReadsTheSettings = new(
        @"ServerTimeHelper\s*\.\s*(UtcOffsetMinutes|CurrentDisplayMode|ToServerTime|FormatServerTime|FormatServerClock)\b",
        RegexOptions.CultureInvariant);

    private static readonly Regex Comments = new(
        @"//[^\r\n]*|/\*.*?\*/",
        RegexOptions.CultureInvariant | RegexOptions.Singleline);

    /// <summary>
    /// Test files that name a setting in code text without reading it. Each entry says why; add one only with a
    /// reason, because an entry switches the scan off for that file.
    /// </summary>
    private static readonly Dictionary<string, string> NamesWithoutReading = new(StringComparer.OrdinalIgnoreCase)
    {
        ["ComparisonWindowUtcTests.cs"] =
            "names ServerTimeHelper.UtcOffsetMinutes inside a string it searches the product source for",
    };

    [Fact]
    public void QueryWindowTruncationTests_IsInTheServerTimeHelperCollection()
    {
        var collection = typeof(QueryWindowTruncationTests).GetCustomAttribute<CollectionAttribute>();

        Assert.True(
            collection is not null && collection.Name == CollectionName,
            "QueryWindowTruncationTests formats one time twice with ServerTimeHelper.FormatServerTime, which reads "
            + "settings shared by the whole test process, so it must carry "
            + $"[Collection(\"{CollectionName}\")]; found "
            + $"{(collection is null ? "no [Collection] attribute" : $"[Collection(\"{collection.Name}\")]")}.");
    }

    [Fact]
    public void EveryTestFileThatReadsTheSettings_CarriesTheCollection()
    {
        var thisFile = ThisFile();
        var testsDirectory = Path.GetDirectoryName(thisFile)!;
        var offenders = new List<string>();
        var filesThatRead = 0;

        foreach (var path in Directory.EnumerateFiles(testsDirectory, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(testsDirectory, path);
            var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (Array.Exists(segments, s => s is "bin" or "obj")
                || string.Equals(Path.GetFullPath(path), Path.GetFullPath(thisFile), StringComparison.OrdinalIgnoreCase)
                || NamesWithoutReading.ContainsKey(Path.GetFileName(path)))
            {
                continue;
            }

            var code = Comments.Replace(File.ReadAllText(path), string.Empty);
            if (!ReadsTheSettings.IsMatch(code))
            {
                continue;
            }

            filesThatRead++;
            if (!code.Contains($"[Collection(\"{CollectionName}\")]", StringComparison.Ordinal))
            {
                offenders.Add(relative);
            }
        }

        Assert.True(
            filesThatRead >= 10,
            $"Expected the scan to find the many classes that use ServerTimeHelper's shared settings, found {filesThatRead}; "
            + "the scan has stopped seeing the test sources.");
        Assert.True(
            offenders.Count == 0,
            "These test files use ServerTimeHelper's shared settings without "
            + $"[Collection(\"{CollectionName}\")], so a class that changes them can run between two of their reads: "
            + string.Join(", ", offenders));
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
