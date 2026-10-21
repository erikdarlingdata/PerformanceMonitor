/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254 / #6279: which server a Query Store read goes to, where it starts, and the reads that feed the decision.
/// Pure: every case builds the facts the host would read and checks the decision, the cutoff or the parsed rows.
/// </summary>
public sealed class QueryStoreReplicaRoutingTests
{
    private static readonly DateTime Day = new(2026, 10, 21, 0, 0, 0, DateTimeKind.Utc);

    private static DateTime At(int hour, int minute) => Day.AddHours(hour).AddMinutes(minute);

    private static readonly Guid GroupDb = Guid.Parse("11111111-1111-1111-1111-111111111111");

    /// <summary>Facts for a healthy AG database on a server with a replica set; each test breaks one thing.</summary>
    private static QueryStoreRoutingInput Healthy(string db = "Sales") => new(
        db,
        ReplicaConfigured: true,
        IsAzureSqlDb: false,
        ReplicaUnavailable: false,
        PrimaryMajorVersion: 17,
        ReplicaMajorVersion: 17,
        Primary: new QueryStorePrimaryDatabaseState(db, 900),
        Replica: new QueryStoreReplicaDatabaseState(db, ActualState: 1, ReadonlyReason: 8, FlushIntervalSeconds: 900),
        PrimaryRedo: new QueryStoreAgRedoState(db, GroupDb, At(10, 0), true),
        ReplicaRedo: new QueryStoreAgRedoState(db, GroupDb, At(9, 58), false));

    // ---- T3: reasons --------------------------------------------------------------------------------------------

    [Fact]
    public void AllPassing_ReadsTheReplica_WithLookBackFlushPlusFiveMinutes()
    {
        var d = QueryStoreReplicaRouting.Decide(Healthy());

        Assert.Equal(QueryStoreReadSource.Replica, d.Source);
        Assert.Equal(QueryStoreRouteReason.None, d.Reason);
        Assert.Equal(TimeSpan.FromMinutes(20), d.LookBack);
        Assert.True(d.UsesLookBack);
        Assert.True(d.IncludeOpenIntervalEveryRun);
        Assert.False(d.StageOpenIntervalStamp);
        Assert.False(d.FailureCountsTowardShrink);
    }

    [Fact]
    public void ReplicaFlushFasterThanTheLimit_UsesItsOwnFlushForL()
    {
        var input = Healthy() with { Replica = Healthy().Replica! with { FlushIntervalSeconds = 300 } };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreReadSource.Replica, d.Source);
        Assert.Equal(TimeSpan.FromMinutes(10), d.LookBack);
    }

    [Theory]
    [InlineData(2, 0)]   // read-write: not a secondary
    [InlineData(2, 8)]
    [InlineData(1, 0)]   // read-only, but not because it is a readable secondary
    [InlineData(4, 0)]   // a capturing secondary is never read as a replica
    [InlineData(4, 8)]
    public void NotAReadableNonCapturingSecondary_IsNotMirror(int state, int reason)
    {
        var input = Healthy() with { Replica = Healthy().Replica! with { ActualState = state, ReadonlyReason = reason } };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
        Assert.Equal(QueryStoreRouteReason.NotMirror, d.Reason);
        Assert.Equal("not_mirror", d.ReasonText);
        Assert.True(d.FailureCountsTowardShrink);
    }

    [Fact]
    public void ANonCapturingSecondaryReportingStateZero_IsStillReadAsAReplica()
    {
        // Measured on SQL Server 2025 CU9: a readable, non-capturing secondary reports actual_state 0 (OFF),
        // desired_state 0 and readonly_reason 8, while its Query Store tables are readable and match the primary's.
        var input = Healthy() with { Replica = Healthy().Replica! with { ActualState = 0, ReadonlyReason = 8 } };

        Assert.Equal(QueryStoreReadSource.Replica, QueryStoreReplicaRouting.Decide(input).Source);
    }

    [Fact]
    public void DatabaseMissingOnTheReplica_IsNotMirror()
    {
        var d = QueryStoreReplicaRouting.Decide(Healthy() with { Replica = null });

        Assert.Equal(QueryStoreRouteReason.NotMirror, d.Reason);
    }

    [Fact]
    public void AnotherSecondaryCapturing_EvenWithTheReplicaReportingStateOne_IsNotMirror()
    {
        var input = Healthy() with { HasCapturingSecondary = true };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreRouteReason.NotMirror, d.Reason);
    }

    [Theory]
    [InlineData(901, QueryStoreRouteReason.FlushTooSlow)]
    [InlineData(3600, QueryStoreRouteReason.FlushTooSlow)]
    [InlineData(900, QueryStoreRouteReason.None)]
    public void FlushOver900Seconds_ReadsThePrimary(int flush, QueryStoreRouteReason expected)
    {
        var input = Healthy() with { Replica = Healthy().Replica! with { FlushIntervalSeconds = flush } };

        Assert.Equal(expected, QueryStoreReplicaRouting.Decide(input).Reason);
    }

    [Fact]
    public void UnreadableReplicaFlush_ReadsThePrimary()
    {
        var input = Healthy() with { Replica = Healthy().Replica! with { FlushIntervalSeconds = null } };

        Assert.Equal(QueryStoreRouteReason.FlushTooSlow, QueryStoreReplicaRouting.Decide(input).Reason);
    }

    [Theory]
    [InlineData(5, 0, QueryStoreRouteReason.None)]       // exactly 5 minutes: still fine
    [InlineData(5, 1, QueryStoreRouteReason.RedoBehind)] // a second over
    [InlineData(30, 0, QueryStoreRouteReason.RedoBehind)]
    public void RedoLagOverFiveMinutes_ReadsThePrimary(int minutes, int seconds, QueryStoreRouteReason expected)
    {
        var input = Healthy() with
        {
            ReplicaRedo = Healthy().ReplicaRedo! with { LastCommitTime = At(10, 0) - TimeSpan.FromMinutes(minutes) - TimeSpan.FromSeconds(seconds) },
        };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(expected, d.Reason);
        if (expected == QueryStoreRouteReason.RedoBehind)
        {
            Assert.Equal(QueryStoreReadSource.Primary, d.Source);
        }
    }

    [Fact]
    public void NoRedoRowOnTheReplica_Or_NoCommitTime_IsNoRedoState()
    {
        Assert.Equal(QueryStoreRouteReason.NoRedoState, QueryStoreReplicaRouting.Decide(Healthy() with { ReplicaRedo = null }).Reason);
        Assert.Equal(
            QueryStoreRouteReason.NoRedoState,
            QueryStoreReplicaRouting.Decide(Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { LastCommitTime = null } }).Reason);
        Assert.Equal(
            QueryStoreRouteReason.NoRedoState,
            QueryStoreReplicaRouting.Decide(Healthy() with { PrimaryRedo = Healthy().PrimaryRedo! with { LastCommitTime = null } }).Reason);
    }

    [Fact]
    public void DifferentGroupDatabaseId_IsWrongDatabase()
    {
        var input = Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { GroupDatabaseId = Guid.NewGuid() } };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreRouteReason.WrongDatabase, d.Reason);
        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
    }

    [Theory]
    [InlineData(16, 17)]
    [InlineData(17, 16)]
    [InlineData(0, 0)]
    public void DifferentOrUnknownMajorVersion_IsVersionMismatch(int primary, int replica)
    {
        var d = QueryStoreReplicaRouting.Decide(Healthy() with { PrimaryMajorVersion = primary, ReplicaMajorVersion = replica });

        Assert.Equal(QueryStoreRouteReason.VersionMismatch, d.Reason);
    }

    [Fact]
    public void TheNamedHostIsThePrimaryAfterAFailover_IsReplicaIsPrimary()
    {
        var input = Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { IsPrimaryReplica = true } };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreRouteReason.ReplicaIsPrimary, d.Reason);
        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
    }

    [Fact]
    public void AzureSqlDatabase_NeverRoutesToAReplica()
    {
        var d = QueryStoreReplicaRouting.Decide(Healthy() with { IsAzureSqlDb = true });

        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
        Assert.Equal(QueryStoreRouteReason.None, d.Reason);
        Assert.False(d.UsesLookBack);
    }

    [Fact]
    public void ReplicaUnavailable_EveryDatabaseReadsThePrimary_WithThePrimarysOwnFlush()
    {
        var input = Healthy() with
        {
            ReplicaUnavailable = true,
            Replica = null,
            ReplicaRedo = null,
            Primary = new QueryStorePrimaryDatabaseState("Sales", 300),
        };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
        Assert.Equal(QueryStoreRouteReason.ReplicaUnavailable, d.Reason);
        Assert.Equal(300, d.FlushIntervalSeconds);
        Assert.Equal(TimeSpan.FromMinutes(10), d.LookBack);
        Assert.True(d.StageOpenIntervalStamp);
    }

    [Fact]
    public void OneDatabasesReplicaReadFails_FallsBackToThePrimaryInTheSameRun_WithoutShrinkCredit()
    {
        var input = Healthy() with { Primary = new QueryStorePrimaryDatabaseState("Sales", 600) };
        var first = QueryStoreReplicaRouting.Decide(input);
        Assert.Equal(QueryStoreReadSource.Replica, first.Source);
        Assert.False(first.FailureCountsTowardShrink);

        var retry = QueryStoreReplicaRouting.FallbackAfterReplicaReadFailure(input);

        Assert.Equal(QueryStoreReadSource.Primary, retry.Source);
        Assert.Equal(QueryStoreRouteReason.ReplicaUnavailable, retry.Reason);
        Assert.Equal(TimeSpan.FromMinutes(15), retry.LookBack); // the primary's own F (600 s) + 5 min
        Assert.True(retry.StageOpenIntervalStamp);
    }

    // ---- which reads get the look-back ---------------------------------------------------------------------------

    [Fact]
    public void WhichReadsGetTheLookBack()
    {
        // (a) every replica read
        Assert.True(QueryStoreReplicaRouting.Decide(Healthy()).UsesLookBack);

        // (b) a primary read of an AG database on a setting-on server, every fallback reason
        foreach (var broken in new[]
        {
            Healthy() with { Replica = null },
            Healthy() with { Replica = Healthy().Replica! with { FlushIntervalSeconds = 1800 } },
            Healthy() with { ReplicaRedo = null },
            Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { GroupDatabaseId = Guid.NewGuid() } },
            Healthy() with { ReplicaMajorVersion = 16 },
            Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { IsPrimaryReplica = true } },
            Healthy() with { ReplicaUnavailable = true },
            Healthy() with { ReplicaRedo = Healthy().ReplicaRedo! with { LastCommitTime = At(9, 0) } },
        })
        {
            var d = QueryStoreReplicaRouting.Decide(broken);
            Assert.Equal(QueryStoreReadSource.Primary, d.Source);
            Assert.True(d.UsesLookBack, d.Reason.ToString());
            Assert.Equal(TimeSpan.FromMinutes(20), d.LookBack);
        }

        // ... but not a database outside an availability group on that server
        var nonAg = QueryStoreReplicaRouting.Decide(Healthy() with { PrimaryRedo = null, ReplicaRedo = null, Replica = null });
        Assert.False(nonAg.UsesLookBack);
        Assert.False(nonAg.IsAgDatabase);
        Assert.Equal(TimeSpan.Zero, nonAg.LookBack);

        // (c) a primary read of a database with a capturing secondary, setting empty or not
        var capturing = QueryStoreReplicaRouting.Decide(Healthy() with
        {
            ReplicaConfigured = false,
            Replica = null,
            ReplicaRedo = null,
            PrimaryRedo = null,
            Primary = new QueryStorePrimaryDatabaseState("Sales", 900),
            HasCapturingSecondary = true,
        });
        Assert.Equal(QueryStoreReadSource.Primary, capturing.Source);
        Assert.True(capturing.UsesLookBack);
        Assert.Equal(TimeSpan.FromMinutes(25), capturing.LookBack); // F + 10 min

        // a plain database keeps today's cutoff exactly
        var plain = QueryStoreReplicaRouting.Decide(Healthy() with
        {
            ReplicaConfigured = false,
            Replica = null,
            ReplicaRedo = null,
            PrimaryRedo = null,
            Primary = new QueryStorePrimaryDatabaseState("Sales", 900),
        });
        Assert.False(plain.UsesLookBack);
        var cut = QueryStoreReplicaRouting.ComputeCutoff(At(10, 0), At(10, 30), plain.LookBack);
        Assert.Equal(At(10, 0), cut.Cutoff);
        Assert.Null(cut.HoleFrom);
    }

    [Fact]
    public void CapturingSecondary_OnAServerWithAReplicaSet_StillGetsTheLongerL_OnItsPrimaryReads()
    {
        var input = Healthy() with
        {
            Replica = Healthy().Replica! with { ActualState = 4, ReadonlyReason = 0 },
            HasCapturingSecondary = true,
        };

        var d = QueryStoreReplicaRouting.Decide(input);

        Assert.Equal(QueryStoreReadSource.Primary, d.Source);
        Assert.Equal(QueryStoreRouteReason.NotMirror, d.Reason);
        Assert.Equal(TimeSpan.FromMinutes(25), d.LookBack);
    }

    // ---- T1: cutoffs -----------------------------------------------------------------------------------------------

    [Fact]
    public void T1_WatermarkTen_RunAtTenFifty_StartsAtNineForty()
    {
        var cut = QueryStoreReplicaRouting.ComputeCutoff(At(10, 0), At(10, 50), TimeSpan.FromMinutes(20));

        Assert.Equal(At(9, 40), cut.Cutoff);
        Assert.False(cut.ClampFired);
        Assert.Null(cut.HoleFrom);
        Assert.Null(cut.HoleTo);
    }

    [Fact]
    public void T1b_RunAtTwelve_ClampsToEleven_AndTheHoleStartsAtWatermarkMinusL()
    {
        var cut = QueryStoreReplicaRouting.ComputeCutoff(At(10, 0), At(12, 0), TimeSpan.FromMinutes(20));

        Assert.Equal(At(11, 0), cut.Cutoff);
        Assert.True(cut.ClampFired);
        Assert.Equal(At(9, 40), cut.HoleFrom);
        Assert.Equal(At(11, 0), cut.HoleTo);
    }

    [Fact]
    public void T1c_CapturingSecondary_UsesFlushPlusTen()
    {
        var capturing = QueryStoreReplicaRouting.Decide(Healthy() with
        {
            ReplicaConfigured = false,
            Replica = null,
            ReplicaRedo = null,
            PrimaryRedo = null,
            Primary = new QueryStorePrimaryDatabaseState("Sales", 900),
            HasCapturingSecondary = true,
        });

        var cut = QueryStoreReplicaRouting.ComputeCutoff(At(10, 0), At(10, 50), capturing.LookBack);

        Assert.Equal(At(9, 35), cut.Cutoff);
    }

    [Fact]
    public void NoStoredWatermark_KeepsTheFirstRunWindow()
    {
        var cut = QueryStoreReplicaRouting.ComputeCutoff(null, At(10, 0), TimeSpan.FromMinutes(20));

        Assert.Null(cut.Cutoff);
        Assert.Null(cut.HoleFrom);
    }

    [Fact]
    public void NoLookBack_KeepsTodaysClampAndHoleExactly()
    {
        var cut = QueryStoreReplicaRouting.ComputeCutoff(At(10, 0), At(12, 0), TimeSpan.Zero);

        Assert.Equal(At(11, 0), cut.Cutoff);
        Assert.Equal(At(10, 0), cut.HoleFrom);
        Assert.Equal(At(11, 0), cut.HoleTo);
    }

    // ---- T4: a cut read ----------------------------------------------------------------------------------------------

    [Fact]
    public void T4_ACutRead_NextStartsAtW_WithHoleWMinusLToW()
    {
        var w = At(10, 30);
        var l = TimeSpan.FromMinutes(20);

        var (from, to) = QueryStoreReplicaRouting.CutHole(w, l);
        var next = QueryStoreReplicaRouting.ComputeCutoff(w, At(10, 40), l, previousReadWasCut: true);

        Assert.Equal(At(10, 10), from);
        Assert.Equal(w, to);
        Assert.Equal(w, next.Cutoff);
        Assert.Null(next.HoleFrom);
    }

    [Fact]
    public void T4_TheMarker_LivesInTheGuard_AndClearsOnANormalRead()
    {
        var guard = new QueryStoreShipGuard();
        Assert.False(guard.WasCut("Sales"));

        guard.MarkCut("Sales", true);
        Assert.True(guard.WasCut("Sales"));
        Assert.False(guard.WasCut("Other"));

        guard.MarkCut("Sales", false);
        Assert.False(guard.WasCut("Sales"));
    }

    // ---- read_source ---------------------------------------------------------------------------------------------------

    [Fact]
    public void Summary_CountsAgDatabasesOnly_NamesThreeAtMost_AndIsNullWhenOff()
    {
        var ok = Enumerable.Range(0, 12).Select(i => QueryStoreReplicaRouting.Decide(Healthy("ok" + i)));
        var slow = new[] { "A", "B", "C", "D" }.Select(n =>
            QueryStoreReplicaRouting.Decide(Healthy(n) with { ReplicaRedo = Healthy(n).ReplicaRedo! with { LastCommitTime = At(9, 0) } }));
        var nonAg = QueryStoreReplicaRouting.Decide(Healthy("plain") with { PrimaryRedo = null, ReplicaRedo = null, Replica = null });

        var text = QueryStoreReplicaRouting.Summarize(ok.Concat(slow).Append(nonAg).ToList(), replicaConfigured: true);

        Assert.Equal("replica 12, primary 4 (redo_behind: A, redo_behind: B, redo_behind: C, ...)", text);
        Assert.Null(QueryStoreReplicaRouting.Summarize(ok.ToList(), replicaConfigured: false));
        Assert.Null(QueryStoreReplicaRouting.Summarize(new[] { nonAg }, replicaConfigured: true));
    }

    // ---- the reads that feed the decision ------------------------------------------------------------------------------

    [Fact]
    public async Task ReplicaEnumeration_ParsesDatabasesAndProbeFailures()
    {
        var dbs = new DataTable();
        dbs.Columns.Add("name", typeof(string));
        dbs.Columns.Add("actual_state", typeof(int));
        dbs.Columns.Add("readonly_reason", typeof(int));
        dbs.Columns.Add("flush_interval_seconds", typeof(int));
        dbs.Rows.Add("Sales", 0, 8, 900);
        dbs.Rows.Add("Hr", 4, 8, DBNull.Value);
        var failures = new DataTable();
        failures.Columns.Add("name", typeof(string));
        failures.Columns.Add("error_text", typeof(string));
        failures.Rows.Add("Gone", "mid-restore");
        var set = new DataSet();
        set.Tables.Add(dbs);
        set.Tables.Add(failures);

        using var reader = set.CreateDataReader();
        var result = await QueryStoreCollector.ReadReplicaEnumerationAsync(reader, CancellationToken.None);

        Assert.Equal(2, result.Databases.Count);
        Assert.Equal(new QueryStoreReplicaDatabaseState("Sales", 0, 8, 900), result.Databases[0]);
        Assert.Equal(new QueryStoreReplicaDatabaseState("Hr", 4, 8, null), result.Databases[1]);
        var failure = Assert.Single(result.ProbeFailures);
        Assert.Equal("Gone", failure.Item);
    }

    [Fact]
    public async Task PrimaryFacts_AreTheThirdResultSet_AfterTheEnumerationContract()
    {
        var items = new DataTable();
        items.Columns.Add("name", typeof(string));
        items.Rows.Add("Sales");
        var failures = new DataTable();
        failures.Columns.Add("name", typeof(string));
        failures.Columns.Add("error_text", typeof(string));
        var facts = new DataTable();
        facts.Columns.Add("name", typeof(string));
        facts.Columns.Add("flush_interval_seconds", typeof(int));
        facts.Rows.Add("Sales", 900);
        facts.Rows.Add("Hr", DBNull.Value);
        var set = new DataSet();
        set.Tables.Add(items);
        set.Tables.Add(failures);
        set.Tables.Add(facts);

        using var reader = set.CreateDataReader();
        var outcome = await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);
        var read = await QueryStoreCollector.ReadPrimaryFactsAsync(reader, CancellationToken.None);

        Assert.Equal(new[] { "Sales" }, outcome.Items);
        Assert.Equal(2, read.Count);
        Assert.Equal(new QueryStorePrimaryDatabaseState("Sales", 900), read[0]);
        Assert.Equal(new QueryStorePrimaryDatabaseState("Hr", null), read[1]);
    }

    [Fact]
    public async Task PrimaryFacts_WithoutTheThirdSet_AreEmpty()
    {
        var items = new DataTable();
        items.Columns.Add("name", typeof(string));
        items.Rows.Add("Sales");
        var failures = new DataTable();
        failures.Columns.Add("name", typeof(string));
        failures.Columns.Add("error_text", typeof(string));
        var set = new DataSet();
        set.Tables.Add(items);
        set.Tables.Add(failures);

        using var reader = set.CreateDataReader();
        await EnumeratedCollectorDriver.ReadEnumerationAsync(reader, CancellationToken.None);

        Assert.Empty(await QueryStoreCollector.ReadPrimaryFactsAsync(reader, CancellationToken.None));
    }

    [Fact]
    public async Task RedoStates_AreKeyedByDatabase()
    {
        var t = new DataTable();
        t.Columns.Add("database_name", typeof(string));
        t.Columns.Add("group_database_id", typeof(Guid));
        t.Columns.Add("last_commit_time", typeof(DateTime));
        t.Columns.Add("is_primary_replica", typeof(bool));
        t.Rows.Add("Sales", GroupDb, At(10, 0), true);
        t.Rows.Add("Hr", Guid.NewGuid(), DBNull.Value, false);

        using var reader = t.CreateDataReader();
        var states = await QueryStoreCollector.ReadRedoStatesAsync(reader, CancellationToken.None);

        Assert.Equal(new QueryStoreAgRedoState("Sales", GroupDb, At(10, 0), true), states["sales"]);
        Assert.Null(states["Hr"].LastCommitTime);
        Assert.False(states["Hr"].IsPrimaryReplica);
    }

    // ---- the enumeration builder ---------------------------------------------------------------------------------------

    private static CollectorContext Ctx(int version, bool replicaConfigured) => new()
    {
        ServerId = 1,
        ServerName = "t",
        CollectionTime = At(10, 0),
        Deltas = null!,
        Target = new CollectorTargetInfo { SqlMajorVersion = version },
        QueryStoreReplicaConfigured = replicaConfigured,
    };

    [Theory]
    [InlineData(15)]
    [InlineData(17)]
    public void Enumeration_WithNoReplicaSet_IsToday_OnAnyVersion(int version)
    {
        var text = QueryStoreCollector.Instance.BuildEnumerationQuery(Ctx(version, false))!.Text;

        Assert.DoesNotContain("@facts", text, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_replicas", text, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_runtime_stats", text, StringComparison.Ordinal);
        Assert.DoesNotContain("FACTS_", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Enumeration_WithAReplicaSet_ReadsTheFlushInterval_AndTouchesNoOtherQueryStoreView()
    {
        var text = QueryStoreCollector.Instance.BuildEnumerationQuery(Ctx(17, true))!.Text;

        Assert.Contains("@facts", text, StringComparison.Ordinal);
        Assert.Contains("flush_interval_seconds", text, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_replicas", text, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_runtime_stats", text, StringComparison.Ordinal);
        Assert.DoesNotContain("FACTS_", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ReplicaEnumeration_IsNullOnAzureSqlDatabase_AndAdmitsSecondariesElsewhere()
    {
        var azure = new CollectorContext
        {
            ServerId = 1,
            ServerName = "t",
            CollectionTime = At(10, 0),
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
        };
        Assert.Null(QueryStoreCollector.Instance.BuildReplicaEnumerationQuery(azure));

        var text = QueryStoreCollector.Instance.BuildReplicaEnumerationQuery(Ctx(17, true))!.Text;

        Assert.DoesNotContain("is_primary_replica", text, StringComparison.Ordinal);
        Assert.DoesNotContain("readonly_reason & 8 = 0", text, StringComparison.Ordinal);
        Assert.DoesNotContain("EXCLUSION_FILTER", text, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_replicas", text, StringComparison.Ordinal);
    }

    // ---- #6279: capturing secondary, learned from the rows already read -----------------------------------------------------

    private static QueryStoreCollector.Row RoleRow(string? role) => new() { DatabaseName = "Sales", ReplicaRole = role };

    [Fact]
    public void Tracker_FlagsADatabase_OnANonPrimaryRow_AndLapsesAfter24Hours()
    {
        var t = new QueryStoreCapturingSecondaryTracker();
        Assert.False(t.HasCapturingSecondary("Sales", At(10, 0)));

        t.Observe("Sales", new[] { RoleRow("Primary"), RoleRow(null) }, At(10, 0)); // primary and unattributed rows do not count
        Assert.False(t.HasCapturingSecondary("Sales", At(10, 1)));

        t.Observe("Sales", new[] { RoleRow("Primary"), RoleRow("Secondary") }, At(10, 5));
        Assert.True(t.HasCapturingSecondary("Sales", At(10, 6)));
        Assert.True(t.HasCapturingSecondary("sales", Day.AddHours(34).AddMinutes(4))); // 23 h 59 m later
        Assert.False(t.HasCapturingSecondary("Sales", Day.AddHours(34).AddMinutes(5))); // 24 h
        Assert.False(t.HasCapturingSecondary("Sales", Day.AddHours(35)));
    }

    [Fact]
    public void Tracker_AFreshNonPrimaryRow_KeepsTheFlagAlive_AndSeedingNeverMovesItBack()
    {
        var t = new QueryStoreCapturingSecondaryTracker();
        t.Seed(new[] { ("Sales", At(10, 0)) });
        Assert.True(t.HasCapturingSecondary("Sales", At(11, 0)));

        t.Observe("Sales", new[] { RoleRow("Geo Secondary") }, Day.AddHours(30));
        t.Seed(new[] { ("Sales", At(9, 0)) });

        Assert.True(t.HasCapturingSecondary("Sales", Day.AddHours(50))); // 20 h after the observation, not after the seed
        t.Forget("Sales");
        Assert.False(t.HasCapturingSecondary("Sales", Day.AddHours(50)));
    }
}
