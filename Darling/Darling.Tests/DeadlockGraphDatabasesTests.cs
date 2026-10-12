/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The one every-process rule for deadlock graphs: all of a graph's database names must be in the set.
/// Also pins that the alert sweep's excluded-database check agrees with it.
/// </summary>
public sealed class DeadlockGraphDatabasesTests
{
    private static readonly IReadOnlyList<string> Gp = new[] { "GP" };

    private static string Graph(params string[] databases)
    {
        var processes = string.Empty;
        for (var i = 0; i < databases.Length; i++)
            processes += $"<process id=\"p{i}\" currentdbname=\"{databases[i]}\"/>";
        return $"<deadlock><victim-list/><process-list>{processes}</process-list></deadlock>";
    }

    [Fact]
    public void AllIn_EveryProcessInTheSet_IsTrue_CaseInsensitively() =>
        Assert.True(DeadlockGraphDatabases.AllIn(Graph("GP", "gp"), Gp));

    [Fact]
    public void AllIn_AMixedGraph_IsFalse() =>
        Assert.False(DeadlockGraphDatabases.AllIn(Graph("GP", "HS"), Gp));

    [Fact]
    public void AllIn_NoProcesses_IsFalse() =>
        Assert.False(DeadlockGraphDatabases.AllIn(Graph(), Gp));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("<deadlock><process")]
    public void AllIn_MissingOrUnparseableGraph_IsFalse(string? xml) =>
        Assert.False(DeadlockGraphDatabases.AllIn(xml, Gp));

    [Fact]
    public void AllIn_EmptyList_IsFalse() =>
        Assert.False(DeadlockGraphDatabases.AllIn(Graph("GP"), Array.Empty<string>()));

    [Theory]
    [InlineData("GP", "GP")]
    [InlineData("GP", "HS")]
    [InlineData("HS")]
    public void IsDeadlockExcluded_AgreesWithTheSharedRule(params string[] databases)
    {
        var xml = Graph(databases);
        var row = new DeadlockAlertRow { DeadlockGraphXml = xml };
        Assert.Equal(DeadlockGraphDatabases.AllIn(xml, Gp), AlertContextBuilders.IsDeadlockExcluded(row, Gp));
    }
}
