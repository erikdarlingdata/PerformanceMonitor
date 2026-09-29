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
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4776, #4766: the settings <c>ServerTimeHelper</c> shares with the whole test process are the server clock
/// (<c>ActiveServerClock</c>, which <c>UtcOffsetMinutes</c> now reads and writes: setting the offset installs a
/// fixed-offset clock, and reading it asks the clock for its offset right now) and
/// <c>CurrentDisplayMode</c>. Classes that write them carry <c>[Collection("server-time-helper")]</c> so xUnit runs
/// them one at a time. A class that only READS them (through <c>FormatServerTime</c>, <c>ToServerTime</c>,
/// <c>ServerTimeToUtc</c> and the like) needs the same collection: if it formats one time twice, a writer running
/// between the two calls makes the two texts differ. <c>QueryWindowTruncationTests</c> did exactly that and failed
/// by a 4 hour difference.
/// </summary>
public sealed class ServerTimeHelperCollectionTests
{
    private const string CollectionName = "server-time-helper";

    /// <summary>
    /// The members of <c>ServerTimeHelper</c> that read or write one of the two shared settings.
    /// <c>ActiveServerClock</c> and <c>UtcOffsetMinutes</c> are the server clock itself, read and written.
    /// <c>ToServerTime</c>, <c>ServerTimeToUtc</c>, the conversions (<c>ConvertForDisplay</c>,
    /// <c>DisplayTimeToServerTime</c>) and <c>GetTimezoneLabel</c> read it, in the modes that use it, and
    /// <c>FormatServerTime</c> and <c>FormatServerClock</c> read the display mode as well. The pattern cannot tell
    /// an overload that takes the offset or a <c>ServerClock</c> as an argument (<c>ToServerTime</c>,
    /// <c>ConvertForDisplay</c>, <c>DisplayTimeToServerTime</c>) from the one that reads the shared clock: the
    /// explicit overloads read no setting, so a file that calls only those goes in <c>NamesWithoutReading</c>.
    /// </summary>
    private static readonly Regex ReadsTheSettings = new(
        @"ServerTimeHelper\s*\.\s*(ActiveServerClock|UtcOffsetMinutes|CurrentDisplayMode|ToServerTime|ServerTimeToUtc|ConvertForDisplay|DisplayTimeToServerTime|GetTimezoneLabel|FormatServerTime|FormatServerClock)\b",
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

    /// <summary>
    /// #4766: <c>ActiveServerClock</c> and <c>ServerTimeToUtc</c> were added to <c>ServerTimeHelper</c> and this
    /// guard did not know them, so a class using only those could have run beside a writer with no collection.
    /// Every public static member of <c>ServerTimeHelper</c> reads or writes the server clock or the display mode,
    /// so the pattern must name each one. A member added later fails here until it is in <c>ReadsTheSettings</c>
    /// (one that reads neither costs a class the collection attribute and nothing else).
    /// </summary>
    [Fact]
    public void ReadsTheSettings_NamesEveryPublicStaticMemberOfServerTimeHelper()
    {
        var unnamed = typeof(ServerTimeHelper)
            .GetMembers(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(member => member is not MethodBase { IsSpecialName: true })
            .Select(member => member.Name)
            .Distinct()
            .Where(name => !ReadsTheSettings.IsMatch($"ServerTimeHelper.{name}"))
            .ToList();

        Assert.True(
            unnamed.Count == 0,
            "These public static members of ServerTimeHelper are not in ReadsTheSettings, so a test class that uses "
            + "only them would not be required to carry "
            + $"[Collection(\"{CollectionName}\")]: {string.Join(", ", unnamed)}.");
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
