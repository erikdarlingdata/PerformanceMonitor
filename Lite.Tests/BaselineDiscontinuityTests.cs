/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A5, the rendered-marker clause: the read side of identity-epoch detection, pinned without a store.
/// <see cref="BaselineDiscontinuities.Compose"/> is the whole rule the three store readers (Darling's MCP and
/// viewer over Npgsql, Lite over DuckDB) hand their rows to, so what is pinned here is pinned for every
/// surface at once: the grammar's inverse reads the count back out of a composed note and nothing else; an
/// identity marker takes its reason from the persisted pair only when the pair is THAT event's; two carriers
/// marking one epoch fold into one discontinuity; the vocabulary is closed; the wire shape spells its three
/// keys once; and both SQL texts are the store-neutral statements the class remarks claim.
/// </summary>
public sealed class BaselineDiscontinuityTests
{
    private static readonly DateTime T0 = new(2026, 9, 20, 3, 12, 0, DateTimeKind.Unspecified);

    /* ---------------------------------------------------------------- the grammar's inverse ------------ */

    [Theory]
    [InlineData("identity_epoch_changes=1", "identity_epoch_changes", true, 1)]
    [InlineData("candidates=1264 visible=0 identity_epoch_changes=1", "identity_epoch_changes", true, 1)]
    [InlineData("wall-clock budget (120s) reached; cycle abandoned; identity_epoch_changes=1", "identity_epoch_changes", true, 1)]
    [InlineData("statements_epoch_changes=3 postmaster_epoch_changes=2", "postmaster_epoch_changes", true, 2)]
    [InlineData("identity_epoch_changes=0", "identity_epoch_changes", true, 0)]
    /* A label that is a PREFIX of the token is not the token. */
    [InlineData("identity_epoch_changes_total=1", "identity_epoch_changes", false, 0)]
    /* Mentioned in host prose, not measured. */
    [InlineData("the identity_epoch_changes marker was not written", "identity_epoch_changes", false, 0)]
    /* Truncated mid-number by the column cut: absent, not a partial number. */
    [InlineData("identity_epoch_changes=", "identity_epoch_changes", false, 0)]
    [InlineData("identity_epoch_changes=1x", "identity_epoch_changes", false, 0)]
    [InlineData(null, "identity_epoch_changes", false, 0)]
    [InlineData("", "identity_epoch_changes", false, 0)]
    [InlineData("identity_epoch_changes=1", "Identity_Epoch_Changes", false, 0)]
    public void TryReadCount_ReadsWholeTokensOnly(string? note, string label, bool expected, long count)
    {
        Assert.Equal(expected, CollectorMeasurementNote.TryReadCount(note, label, out var read));
        Assert.Equal(count, read);
    }

    [Fact]
    public void TryReadCount_ReversesRenderAndCompose()
    {
        var measurements = new List<CollectorMeasurement>
        {
            new(ServerEpoch.IdentityChangesMeasurement, 1),
            new("candidates", 1264),
        };
        var composed = CollectorMeasurementNote.Compose("some host note; with a semicolon", measurements)!;

        Assert.True(CollectorMeasurementNote.TryReadCount(composed, ServerEpoch.IdentityChangesMeasurement, out var identity));
        Assert.Equal(1, identity);
        Assert.True(CollectorMeasurementNote.TryReadCount(composed, "candidates", out var candidates));
        Assert.Equal(1264, candidates);
        Assert.False(CollectorMeasurementNote.TryReadCount(composed, ServerEpoch.StatementsChangesMeasurement, out _));
    }

    /* ---------------------------------------------------------------- the reason split ----------------- */

    [Fact]
    public void DeriveIdentityReason_SplitsByWhichHalfMoved()
    {
        var a = new DateTime(2026, 9, 1, 8, 0, 0);
        var b = new DateTime(2026, 9, 20, 3, 11, 40);

        Assert.Equal(BaselineDiscontinuities.RestartReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "NODE1"), new(b, "NODE1")));
        Assert.Equal(BaselineDiscontinuities.RestartReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "node1"), new(b, "NODE1")));
        Assert.Equal(BaselineDiscontinuities.RenameReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "NODE1"), new(a, "NODE2")));
        Assert.Equal(BaselineDiscontinuities.FailoverReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "NODE1"), new(b, "NODE2")));
        /* Unknown is not different (ServerEpoch.IsNewEpoch's rule): a name known on one side only cannot
           have moved, so a start-time move beside it is a restart, not a failover. */
        Assert.Equal(BaselineDiscontinuities.RestartReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, null), new(b, "NODE1")));
        Assert.Equal(BaselineDiscontinuities.RestartReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "NODE1"), new(b, null)));
        Assert.Equal(BaselineDiscontinuities.IdentityReason, BaselineDiscontinuities.DeriveIdentityReason(new(a, "NODE1"), new(a, "node1")));
        Assert.Equal(BaselineDiscontinuities.IdentityReason, BaselineDiscontinuities.DeriveIdentityReason(new(null, null), new(b, "NODE1")));
    }

    /* ---------------------------------------------------------------- Compose ------------------------- */

    [Fact]
    public void Compose_NoRows_IsEmpty()
    {
        Assert.Empty(BaselineDiscontinuities.Compose(Array.Empty<BaselineDiscontinuities.MarkerRow>(), Array.Empty<BaselineDiscontinuities.StateRow>()));
    }

    [Fact]
    public void Compose_IdentityMarker_WithItsPair_IsARestartNamingBothStartTimes()
    {
        var oldStart = new DateTime(2026, 9, 1, 8, 0, 0);
        var newStart = new DateTime(2026, 9, 20, 3, 11, 40);
        var rows = new[] { Marker(T0, "wait_stats", "identity_epoch_changes=1") };
        var state = Pair("wait_stats", T0.AddSeconds(-2), new(oldStart, "NODE1"), new(newStart, "NODE1"));

        var result = BaselineDiscontinuities.Compose(rows, state);

        var one = Assert.Single(result);
        Assert.Equal(T0, one.At);
        Assert.Equal(BaselineDiscontinuities.RestartReason, one.Reason);
        Assert.Contains("2026-09-01 08:00:00 → 2026-09-20 03:11:40", one.Detail, StringComparison.Ordinal);
        Assert.Contains("server name NODE1 → NODE1", one.Detail, StringComparison.Ordinal);
        Assert.EndsWith("observed by wait_stats", one.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void Compose_BothHalvesMoved_IsAFailover()
    {
        var rows = new[] { Marker(T0, "wait_stats", "identity_epoch_changes=1") };
        var state = Pair("wait_stats", T0, new(new DateTime(2026, 9, 1, 8, 0, 0), "NODE1"), new(new DateTime(2026, 9, 20, 3, 11, 40), "NODE2"));

        Assert.Equal(BaselineDiscontinuities.FailoverReason, Assert.Single(BaselineDiscontinuities.Compose(rows, state)).Reason);
    }

    /// <summary>The two SQL Server carriers mark one epoch on the same pass, minutes apart on a heavy instance:
    /// one discontinuity, at the FIRST observation, naming both.</summary>
    [Fact]
    public void Compose_TwoCarriersOnOneEpoch_FoldToOneMarkerAtTheFirstObservation()
    {
        var rows = new[]
        {
            Marker(T0, "wait_stats", "identity_epoch_changes=1"),
            Marker(T0.AddMinutes(4), "cpu_utilization", "identity_epoch_changes=1"),
        };
        var state = Pair("wait_stats", T0, new(new DateTime(2026, 9, 1), "NODE1"), new(new DateTime(2026, 9, 20), "NODE1"))
            .Concat(Pair("cpu_utilization", T0.AddMinutes(4), new(new DateTime(2026, 9, 1), "NODE1"), new(new DateTime(2026, 9, 20), "NODE1")))
            .ToList();

        var one = Assert.Single(BaselineDiscontinuities.Compose(rows, state));
        Assert.Equal(T0, one.At);
        Assert.Equal(BaselineDiscontinuities.RestartReason, one.Reason);
        Assert.Contains("observed by wait_stats, cpu_utilization", one.Detail, StringComparison.Ordinal);
        Assert.Contains("2 collector runs marked one epoch", one.Detail, StringComparison.Ordinal);
    }

    /// <summary>A carrier observes one event once, so its second marker is the NEXT event — no time tolerance
    /// decides it. Two restarts, each seen by both carriers, are two markers; the older one's pair has been
    /// overwritten, so it takes the generic reason and says why.</summary>
    [Fact]
    public void Compose_ARepeatedCarrier_OpensTheNextEvent_AndOnlyTheLatestEventHasItsPair()
    {
        var t1 = T0.AddHours(-2);
        var rows = new[]
        {
            Marker(t1, "wait_stats", "identity_epoch_changes=1"),
            Marker(t1.AddSeconds(7), "cpu_utilization", "identity_epoch_changes=1"),
            Marker(T0, "wait_stats", "identity_epoch_changes=1"),
            Marker(T0.AddSeconds(9), "cpu_utilization", "identity_epoch_changes=1"),
        };
        var state = Pair("wait_stats", T0, new(new DateTime(2026, 9, 20, 1, 0, 0), "NODE1"), new(new DateTime(2026, 9, 20, 3, 11, 0), "NODE1"))
            .Concat(Pair("cpu_utilization", T0.AddSeconds(9), new(new DateTime(2026, 9, 20, 1, 0, 0), "NODE1"), new(new DateTime(2026, 9, 20, 3, 11, 0), "NODE1")))
            .ToList();

        var result = BaselineDiscontinuities.Compose(rows, state);

        Assert.Equal(2, result.Count);
        Assert.Equal(t1, result[0].At);
        Assert.Equal(BaselineDiscontinuities.IdentityReason, result[0].Reason);
        Assert.Contains("not recoverable from the store", result[0].Detail, StringComparison.Ordinal);
        Assert.Contains("observed by wait_stats, cpu_utilization", result[0].Detail, StringComparison.Ordinal);
        Assert.Equal(T0, result[1].At);
        Assert.Equal(BaselineDiscontinuities.RestartReason, result[1].Reason);
    }

    /// <summary>A carrier that was disabled and comes back observes an OLD change late: it joins the event the
    /// other carrier opened, because that is the same event, and the instant stays the first observation.</summary>
    [Fact]
    public void Compose_ALateObserverOfTheSameChange_JoinsTheEventRatherThanOpeningOne()
    {
        var rows = new[]
        {
            Marker(T0, "cpu_utilization", "identity_epoch_changes=1"),
            Marker(T0.AddHours(3), "wait_stats", "identity_epoch_changes=1"),
        };

        var one = Assert.Single(BaselineDiscontinuities.Compose(rows, Array.Empty<BaselineDiscontinuities.StateRow>()));
        Assert.Equal(T0, one.At);
        Assert.Equal(BaselineDiscontinuities.IdentityReason, one.Reason);
        Assert.Contains("observed by cpu_utilization, wait_stats", one.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void Compose_APairOutsideTheTolerance_IsSomeOtherEvents_AndTheMarkerStaysGeneric()
    {
        var rows = new[] { Marker(T0, "wait_stats", "identity_epoch_changes=1") };
        var state = Pair("wait_stats", T0.AddMinutes(11), new(new DateTime(2026, 9, 1), "NODE1"), new(new DateTime(2026, 9, 20), "NODE1"));

        Assert.Equal(BaselineDiscontinuities.IdentityReason, Assert.Single(BaselineDiscontinuities.Compose(rows, state)).Reason);

        var inside = Pair("wait_stats", T0.AddMinutes(9), new(new DateTime(2026, 9, 1), "NODE1"), new(new DateTime(2026, 9, 20), "NODE1"));
        Assert.Equal(BaselineDiscontinuities.RestartReason, Assert.Single(BaselineDiscontinuities.Compose(rows, inside)).Reason);
    }

    [Fact]
    public void Compose_AHalfPair_OrAMalformedOne_IsGeneric()
    {
        var rows = new[] { Marker(T0, "wait_stats", "identity_epoch_changes=1") };

        var currentOnly = new[] { new BaselineDiscontinuities.StateRow("wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(new DateTime(2026, 9, 20), "NODE1")), T0) };
        Assert.Equal(BaselineDiscontinuities.IdentityReason, Assert.Single(BaselineDiscontinuities.Compose(rows, currentOnly)).Reason);

        var malformed = new[]
        {
            new BaselineDiscontinuities.StateRow("wait_stats", ServerEpoch.IdentityStateKey, "not a stamp", T0),
            new BaselineDiscontinuities.StateRow("wait_stats", ServerEpoch.IdentityPreviousStateKey, "|", T0),
        };
        Assert.Equal(BaselineDiscontinuities.IdentityReason, Assert.Single(BaselineDiscontinuities.Compose(rows, malformed)).Reason);

        /* Another carrier's pair is not this carrier's. */
        var otherCarrier = Pair("cpu_utilization", T0, new(new DateTime(2026, 9, 1), "NODE1"), new(new DateTime(2026, 9, 20), "NODE1"));
        Assert.Equal(BaselineDiscontinuities.IdentityReason, Assert.Single(BaselineDiscontinuities.Compose(rows, otherCarrier)).Reason);
    }

    [Fact]
    public void Compose_TheStatementsAndPostmasterEpochs_TakeTheirOwnReasons_AndDoNotFoldTogether()
    {
        var rows = new[]
        {
            Marker(T0, "pg_statement_stats", "statements_epoch_changes=1"),
            Marker(T0.AddSeconds(3), "pg_wait_stats", "postmaster_epoch_changes=1"),
        };

        var result = BaselineDiscontinuities.Compose(rows, Array.Empty<BaselineDiscontinuities.StateRow>());

        Assert.Equal(2, result.Count);
        Assert.Equal(BaselineDiscontinuities.StatsResetReason, result[0].Reason);
        Assert.Contains("stats_reset", result[0].Detail, StringComparison.Ordinal);
        Assert.EndsWith("observed by pg_statement_stats", result[0].Detail, StringComparison.Ordinal);
        Assert.Equal(BaselineDiscontinuities.PostmasterRestartReason, result[1].Reason);
        Assert.Contains("pg_postmaster_start_time()", result[1].Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void Compose_AZeroCount_OrALabelInHostProse_OrANullNote_IsNoMarker()
    {
        var rows = new[]
        {
            Marker(T0, "wait_stats", "identity_epoch_changes=0"),
            Marker(T0.AddMinutes(1), "wait_stats", "the identity_epoch_changes note was not written this pass"),
            Marker(T0.AddMinutes(2), "wait_stats", null),
            Marker(T0.AddMinutes(3), "wait_stats", "candidates=3 visible=3"),
        };

        Assert.Empty(BaselineDiscontinuities.Compose(rows, Array.Empty<BaselineDiscontinuities.StateRow>()));
    }

    [Fact]
    public void Compose_ResultIsOldestFirstAcrossFamilies()
    {
        var rows = new[]
        {
            Marker(T0.AddMinutes(5), "pg_wait_stats", "postmaster_epoch_changes=1"),
            Marker(T0, "wait_stats", "identity_epoch_changes=1"),
            Marker(T0.AddMinutes(2), "pg_statement_stats", "statements_epoch_changes=1"),
        };

        var result = BaselineDiscontinuities.Compose(rows, Array.Empty<BaselineDiscontinuities.StateRow>());

        Assert.Equal(new[] { T0, T0.AddMinutes(2), T0.AddMinutes(5) }, result.Select(r => r.At).ToArray());
    }

    /* ---------------------------------------------------------------- the wire and the sentence -------- */

    [Fact]
    public void ToPayload_SpellsTheThreeKeys_AndTheInstantInTheStoresFrame()
    {
        var items = new[] { new BaselineDiscontinuity(DateTime.SpecifyKind(T0, DateTimeKind.Utc), BaselineDiscontinuities.RestartReason, "d") };

        var json = JsonSerializer.Serialize(BaselineDiscontinuities.ToPayload(items));

        Assert.Equal("[{\"at\":\"2026-09-20T03:12:00.0000000\",\"reason\":\"restart\",\"detail\":\"d\"}]", json);
        Assert.Equal("[]", JsonSerializer.Serialize(BaselineDiscontinuities.ToPayload(Array.Empty<BaselineDiscontinuity>())));
    }

    [Fact]
    public void Sentence_IsTheSharedRenderText()
    {
        Assert.Equal("baseline discontinuity at 2026-09-20 03:12 (restart)", BaselineDiscontinuities.Sentence(T0, BaselineDiscontinuities.RestartReason));
    }

    [Fact]
    public void TheVocabularyIsClosed_AndTheKeyAndSentenceNameIt()
    {
        Assert.Equal(
            new[] { "restart", "rename", "failover", "identity", "stats_reset", "postmaster_restart" },
            BaselineDiscontinuities.Reasons);
        Assert.Equal("discontinuities", BaselineDiscontinuities.PayloadKey);
        Assert.StartsWith(" discontinuities[] ", BaselineDiscontinuities.DescriptionSentence, StringComparison.Ordinal);
        Assert.Contains("{ at, reason, detail }", BaselineDiscontinuities.DescriptionSentence, StringComparison.Ordinal);
        foreach (var reason in BaselineDiscontinuities.Reasons.Where(r => r != BaselineDiscontinuities.IdentityReason))
        {
            Assert.Contains(reason, BaselineDiscontinuities.DescriptionSentence, StringComparison.Ordinal);
        }
    }

    /* ---------------------------------------------------------------- the SQL texts ------------------- */

    [Fact]
    public void MarkerRowsSql_WindowsBothSides_PrefiltersOnTheThreeLabels_AndReadsTheMarkerHome()
    {
        var sql = BaselineDiscontinuities.MarkerRowsSql;

        Assert.Contains("FROM collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("error_message IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("LIKE '%identity_epoch_changes=%'", sql, StringComparison.Ordinal);
        Assert.Contains("LIKE '%statements_epoch_changes=%'", sql, StringComparison.Ordinal);
        Assert.Contains("LIKE '%postmaster_epoch_changes=%'", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time, collector_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);
        AssertStoreNeutral(sql);
    }

    [Fact]
    public void IdentityPairSql_ReadsBothKeysOfEveryCarrier()
    {
        var sql = BaselineDiscontinuities.IdentityPairSql;

        Assert.Contains("FROM collector_state", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("'identity_epoch', 'identity_epoch_previous'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collector_name =", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        AssertStoreNeutral(sql);
    }

    /* Both texts run unchanged on PostgreSQL and DuckDB: no schema prefix (the search path and DuckDB's main
       schema resolve the bare names), no engine-only function, no T-SQL-ism. */
    private static void AssertStoreNeutral(string sql)
    {
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("collect.", lower);
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("ilike", lower);
        Assert.DoesNotContain("::", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /* ---------------------------------------------------------------- helpers ------------------------- */

    private static BaselineDiscontinuities.MarkerRow Marker(DateTime at, string collector, string? note) => new(at, collector, note);

    private static List<BaselineDiscontinuities.StateRow> Pair(string collector, DateTime updatedAt, ServerEpoch.Stamp replaced, ServerEpoch.Stamp current) =>
        new()
        {
            new BaselineDiscontinuities.StateRow(collector, ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(current), updatedAt),
            new BaselineDiscontinuities.StateRow(collector, ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(replaced), updatedAt),
        };
}
