/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Tier-0 baseline-model collapse (Darling half of the Lite&lt;-&gt;Darling lockstep):
/// Darling's baseline/anomaly brain consumes the SHARED model from the PerformanceMonitor.Analysis
/// assembly (namespace <c>PerformanceMonitor.Analysis.Baselines</c>) and keeps NO per-app copy of it,
/// so Darling and Lite can't silently re-fork the once-triplicated model + constants. The deprecated
/// Dashboard keeps its OWN copy on purpose (<c>PerformanceMonitorDashboard.Analysis</c>) — this pin is
/// Lite&lt;-&gt;Darling only. Lite.Tests carries the mirror-image pin for the Lite half.
/// </summary>
public sealed class SharedBaselineModelPinTests
{
    private const string SharedAssembly = "PerformanceMonitor.Analysis";
    private const string SharedNamespace = "PerformanceMonitor.Analysis.Baselines";

    [Fact]
    public void BaselineModelTypes_LiveInSharedAssemblyAndNamespace()
    {
        Assert.Equal(SharedAssembly, typeof(BaselineBucket).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineTier).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(MetricNames).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineMath).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(AnomalyThresholds).Assembly.GetName().Name);

        Assert.Equal(SharedNamespace, typeof(BaselineBucket).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineTier).Namespace);
        Assert.Equal(SharedNamespace, typeof(MetricNames).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineMath).Namespace);
        Assert.Equal(SharedNamespace, typeof(AnomalyThresholds).Namespace);
    }

    [Fact]
    public void Darling_KeepsNoPrivateCopyOfTheBaselineModel()
    {
        // If someone re-declares any of these in Darling's own Analysis namespace, this fails —
        // the whole point of the collapse is that the model has exactly ONE home.
        var darlingAssembly = typeof(PgBaselineProvider).Assembly;
        foreach (var forkedType in new[]
        {
            "PerformanceMonitor.Darling.Analysis.BaselineBucket",
            "PerformanceMonitor.Darling.Analysis.BaselineTier",
            "PerformanceMonitor.Darling.Analysis.MetricNames",
            "PerformanceMonitor.Darling.Analysis.BaselineMath",
            "PerformanceMonitor.Darling.Analysis.AnomalyThresholds",
        })
        {
            Assert.Null(darlingAssembly.GetType(forkedType));
        }
    }

    [Fact]
    public void PgBaselineProvider_BindsToTheSharedBaselineBucket()
    {
        // GetBaselineAsync returns Task<BaselineBucket>; pin the element type to the shared type —
        // the SAME PerformanceMonitor.Analysis.Baselines.BaselineBucket Lite binds to. BOTH overloads since #3691
        // lane 33 (the unkeyed four-parameter lookup and the keyed five-parameter one): a keyed series returns the
        // same shared bucket shape, never a keyed variant of it — the key belongs to the caller, not the bucket.
        foreach (var signature in new[]
        {
            new[] { typeof(int), typeof(string), typeof(DateTime), typeof(CancellationToken) },
            new[] { typeof(int), typeof(string), typeof(string), typeof(DateTime), typeof(CancellationToken) },
        })
        {
            var method = typeof(PgBaselineProvider).GetMethod(nameof(PgBaselineProvider.GetBaselineAsync), signature);
            Assert.NotNull(method);
            var bucketType = method!.ReturnType.GetGenericArguments()[0];
            Assert.Same(typeof(BaselineBucket), bucketType);
            Assert.Equal(SharedAssembly, bucketType.Assembly.GetName().Name);
        }
    }
}
