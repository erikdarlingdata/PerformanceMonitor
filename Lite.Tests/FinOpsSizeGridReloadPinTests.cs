/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// A server added while the FinOps tab is already on screen loads its Database Sizes and Storage Growth grids
/// once, before the first collection, and nothing re-runs that load. The tab therefore remembers whether the
/// last load of each grid produced no rows (empty, failed or superseded) and re-runs just that load when the
/// tab is shown again. WPF cannot run here, so this is a source pin anchored on the method declarations.
/// </summary>
public sealed class FinOpsSizeGridReloadPinTests
{
    private static string ReadFinOpsTab([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var parts = new[] { "Lite", "Controls", "FinOpsTab.xaml.cs" };
        while (dir is not null && !File.Exists(Path.Combine(new[] { dir }.Concat(parts).ToArray())))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()))
            .Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    /// <summary>The body of the method declared as <c>name(</c>, up to the next member declaration.</summary>
    private static string BodyOf(string source, string name)
    {
        var decl = Regex.Matches(source, @"private (?:async )?[\w.<>]+ " + Regex.Escape(name) + @"\(");
        Assert.Single(decl);
        var start = decl[0].Index;
        var next = Regex.Match(source[(start + 10)..], @"\n    (?:private|public|internal|protected) ");
        return next.Success ? source.Substring(start, next.Index + 10) : source[start..];
    }

    [Fact]
    public void IsVisibleChanged_ReloadsBothSizeGridsWhoseLastLoadProducedNoRows()
    {
        var src = ReadFinOpsTab();
        var sub = Regex.Matches(src, @"IsVisibleChanged \+= \(_, _\) => (\w+)\(\);");
        Assert.Single(sub);

        var body = BodyOf(src, sub[0].Groups[1].Value);
        Assert.Contains("if (!IsVisible", body, StringComparison.Ordinal);
        Assert.Contains("if (_dbSizesNeedReload) _ = LoadDatabaseSizesAsync(", body, StringComparison.Ordinal);
        Assert.Contains("if (_storageGrowthNeedReload) _ = LoadStorageGrowthAsync(", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("LoadDatabaseSizesAsync")]
    [InlineData("LoadStorageGrowthAsync")]
    public void SizeGridLoad_MarksItselfNeedingReloadBeforeTheReadAndClearsItOnlyFromTheRowCount(string method)
    {
        var body = BodyOf(ReadFinOpsTab(), method);

        var claim = body.IndexOf("_loads.Claim(", StringComparison.Ordinal);
        var set = Regex.Match(body, @"\b(_\w+) = true;");
        var read = body.IndexOf("Task.Run(", StringComparison.Ordinal);
        Assert.True(claim >= 0 && set.Success && read >= 0, "claim, flag-set and read must all be present");
        Assert.True(claim < set.Index && set.Index < read, "flag is set true after the claim and before the read");

        var clear = Regex.Matches(body, @"\b" + set.Groups[1].Value + @" = data\.Count == 0;");
        Assert.Single(clear);
        Assert.True(clear[0].Index > read, "the row-count assignment follows the read");
        var sup = body.IndexOf("_loads.Superseded(", StringComparison.Ordinal);
        Assert.True(sup > read && clear[0].Index > sup, "the row-count assignment follows the generation check");
        Assert.Equal(2, Regex.Matches(body, @"\b" + set.Groups[1].Value + @" = ").Count);
    }
}
