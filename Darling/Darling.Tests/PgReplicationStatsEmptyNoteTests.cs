/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8920: Aurora readers read the shared storage volume and never appear in pg_stat_replication, so the Replication tab's
/// empty standby grid on an Aurora writer must say that, not "which is either correct or the finding".
/// </summary>
public sealed class PgReplicationStatsEmptyNoteTests
{
    [Fact]
    public void OnAnAuroraTarget_TheNoteSaysReadersDoNotAppear_AndDoesNotCallTheEmptyGridAFinding()
    {
        var note = PgDisplay.ReplicationStatsEmptyNote(MonitoredEngineKind.AuroraPostgres);

        Assert.Contains("Aurora readers", note, System.StringComparison.Ordinal);
        Assert.Contains("never appear in pg_stat_replication", note, System.StringComparison.Ordinal);
        Assert.DoesNotContain("or the finding", note, System.StringComparison.Ordinal);
    }

    [Fact]
    public void OnVanillaPostgres_TheNoteKeepsTheReplicaAndPrimaryExplanation()
    {
        var note = PgDisplay.ReplicationStatsEmptyNote(MonitoredEngineKind.Postgres);

        Assert.Contains("On a REPLICA that is the expected answer", note, System.StringComparison.Ordinal);
        Assert.Contains("either correct or the finding", note, System.StringComparison.Ordinal);
        Assert.DoesNotContain("Aurora", note, System.StringComparison.Ordinal);
    }

    [Fact]
    public void AServerWithNoEngineKindYet_GetsTheVanillaNote()
    {
        Assert.DoesNotContain("Aurora", PgDisplay.ReplicationStatsEmptyNote(null), System.StringComparison.Ordinal);
    }
}
