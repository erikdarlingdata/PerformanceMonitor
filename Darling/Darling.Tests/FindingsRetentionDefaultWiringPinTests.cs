/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Darling half of the findings-retention pin: that the two cleanup defaults in
/// <c>PerformanceMonitor.Darling.Analysis</c> NAME the shared horizon rather than repeating its value.
///
/// <para>Nothing else can carry this claim. <see cref="FindingsRetentionCrossSkuPinTests"/> pins the two
/// editions' constants equal, and the worker's daily sweep passes the horizon in explicitly — so a default
/// reverted to a bare <c>30</c> leaves every value correct, every behavioural assertion passing, and,
/// because the constant it replaced reads 30 today, byte-identical IL. Source text is the only artifact
/// that distinguishes the two states.</para>
///
/// <para>The pair below is deliberate. The source pin catches the revert; the reflection pin states the
/// weaker but independent claim that the default a caller actually receives is the shared horizon, which
/// survives a reformat of the signature and reads the compiled metadata rather than the text.</para>
///
/// <para>If the source pin goes red, do not satisfy it by writing the number back. Either the horizon
/// moved — in which case move <see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/> and let both
/// defaults follow — or the wiring was undone and belongs back.</para>
/// </summary>
public sealed class FindingsRetentionDefaultWiringPinTests
{
    /// <summary>
    /// A numeric default on this parameter, in either file. The revert this pin exists to catch, expressed
    /// as the shape it would take rather than as one exact string.
    /// </summary>
    private static readonly Regex NumericRetentionDefault = new(@"retentionDays\s*=\s*\d", RegexOptions.Compiled);

    [Theory]
    [InlineData("DarlingAnalysisService.cs", "CleanupAsync")]
    [InlineData("PgFindingStore.cs", "CleanupOldFindingsAsync")]
    public void TheCleanupDefaultNamesTheSharedHorizon(string file, string method)
    {
        /* Stripped so a doc comment quoting the old literal cannot satisfy or break either assertion. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadAnalysisSource(file));

        Assert.Contains(
            $"{method}(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)",
            code,
            StringComparison.Ordinal);

        Assert.DoesNotMatch(NumericRetentionDefault, code);
    }

    [Fact]
    public void TheCompiledDefaultsAreTheSharedFindingsHorizon()
    {
        Assert.Equal(
            AnalysisRetentionDefaults.FindingsRetentionDays,
            CompiledRetentionDefault(typeof(DarlingAnalysisService), nameof(DarlingAnalysisService.CleanupAsync)));

        Assert.Equal(
            AnalysisRetentionDefaults.FindingsRetentionDays,
            CompiledRetentionDefault(typeof(PgFindingStore), nameof(PgFindingStore.CleanupOldFindingsAsync)));
    }

    /// <summary>
    /// The default baked into <c>PerformanceMonitor.Darling.Analysis.dll</c> for the method's single
    /// <c>retentionDays</c> parameter — the value a caller that omits the argument actually gets.
    /// </summary>
    private static int CompiledRetentionDefault(Type type, string method)
    {
        var parameter = Assert.Single(type.GetMethod(method)!.GetParameters());

        Assert.Equal("retentionDays", parameter.Name);
        Assert.True(parameter.HasDefaultValue);

        return Assert.IsType<int>(parameter.DefaultValue);
    }

    private static string ReadAnalysisSource(string file) =>
        File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", file));

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
