/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5735: the two products read one log row, so the collector name and status are spelled the same in Lite's
/// <c>CollectionCycleSkip</c> and Darling's <c>CollectionCycleRow</c> (#8464: both read the name from
/// <see cref="CollectionRunSql.CycleRowCollectorName"/>, and Lite may declare it as that constant). A drift would make each product's readers miss the
/// other's rows (a peer disclosure, the shared get_collection_health text, a store read by both). Reads a Lite source file, so
/// it carries the Lite trait and a Lite-only change runs it.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CollectionCycleLiteSpellingTests
{
    [Fact]
    public void LiteAndDarling_SpellTheCollectorNameAndStatus_Identically()
    {
        var lite = File.ReadAllText(Path.Combine(CycleSkipFeedTests.RepoRoot(), "Lite", "Services", "CollectionCycleSkip.cs"));
        var name = Regex.Match(lite, "const string CollectorName = (?:\"([^\"]+)\"|CollectionRunSql\\.CycleRowCollectorName);");
        var status = Regex.Match(lite, "const string Status = \"([^\"]+)\";");
        Assert.True(name.Success && status.Success, "Lite's CollectionCycleSkip no longer declares the collector name as the shared constant or a literal, or the status as a literal");
        var liteName = name.Groups[1].Success ? name.Groups[1].Value : CollectionRunSql.CycleRowCollectorName;
        Assert.Equal(CollectionCycleRow.CollectorName, liteName);
        Assert.Equal(CollectionCycleRow.Status, status.Groups[1].Value);
    }
}
