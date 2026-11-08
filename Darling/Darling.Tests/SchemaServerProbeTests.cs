/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#7643: the database probe gives each database one row, so the planner makes one plan and keeps one history per database.</summary>
[Trait("Stage", "Guard")]
public sealed class SchemaServerProbeTests
{
    private static readonly Guid s_local = Guid.Parse("33333333-3333-3333-3333-333333333333");
    private static readonly Guid s_distributed = Guid.Parse("44444444-4444-4444-4444-444444444444");

    private static SchemaDatabaseProbe Probe(int id, Guid? group, bool? primary) =>
        new("SalesDb" + id, id, "ONLINE", false, false, null, true, null, group, group is null ? null : "Ag" + id, primary);

    [Fact]
    public void OneRowPerDatabase_ADistributedGroupRowGivesWayToTheLocalGroup()
    {
        /* In a distributed availability group a database has a local replica row in its own group and one in the distributed group. */
        var rows = new[]
        {
            (Probe(5, s_distributed, false), true),
            (Probe(5, s_local, true), false),
            (Probe(6, null, null), false),
        };

        var probes = SchemaServerProbe.OneRowPerDatabase(rows);

        Assert.Equal(2, probes.Count);
        Assert.Equal(s_local, probes[0].AgGroupId);
        Assert.True(probes[0].IsPrimaryReplica);
        Assert.Equal(6, probes[1].DatabaseId);
    }

    [Fact]
    public void OneRowPerDatabase_KeepsProbeOrder()
    {
        var probes = SchemaServerProbe.OneRowPerDatabase([(Probe(7, null, null), false), (Probe(5, null, null), false)]);

        Assert.Equal([7, 5], new[] { probes[0].DatabaseId, probes[1].DatabaseId });
    }
}
