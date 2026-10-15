/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Darling half of the findings-retention default: the default a caller of either cleanup method in
/// <c>PerformanceMonitor.Darling.Analysis</c> receives is the shared horizon,
/// <see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/>, read from the compiled metadata.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class FindingsRetentionDefaultWiringPinTests
{
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
}
