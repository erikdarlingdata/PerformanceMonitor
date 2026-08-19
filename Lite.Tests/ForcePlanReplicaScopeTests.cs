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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the replica scope of the force-plan recommendation (#1882) — WHICH replica's regression drives
/// the recommendation when a query regressed on more than one, and what the rendered advice discloses.
///
/// <para><b>The fact the whole suite turns on</b>, verified against
/// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql">
/// the sp_query_store_force_plan reference</see> rather than inferred from the row shape:
/// "You can force plans on a secondary replica when Query Store for readable secondaries is enabled.
/// Execute sp_query_store_force_plan and sp_query_store_unforce_plan on the primary replica. Using the
/// @replica_group_id argument defaults to the primary replica." So forcing IS scopeable per replica, it
/// always EXECUTES on the primary, and a call that omits @replica_group_id — which is exactly what this
/// codebase renders — targets the PRIMARY.</para>
///
/// <para>That is what makes "worst regression on any replica wins" wrong rather than merely undecided.
/// #1850 de-duped on (database, query_id) keeping the worst row, which meant a secondary's 12x could
/// outrank a primary's 3x and hand the operator a statement that forces on the primary using a read-only
/// replica's evidence, silently. The rule is now: the primary's row wins, and regression factor only
/// breaks ties among rows of equal standing.</para>
///
/// <para><b>Note what the pre-existing #1850 test could not catch.</b> It seeds the PRIMARY row with the
/// WORSE regression, so "worst wins" and "primary wins" agree on it and it passes under either rule. The
/// discriminating seed — a secondary that regressed harder than the primary — is
/// <see cref="BothReplicasRegressed_PrimaryWins_EvenWhenTheSecondaryRegressedHarder"/>, and it is the
/// reason this file exists as well as that test.</para>
/// </summary>
public sealed class ForcePlanReplicaScopeTests
{
    private const string ServerName = "SQL01";

    private static AnalysisFinding PlanRegressionFinding(params Dictionary<string, object>[] rows)
    {
        return new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 42,
            ServerName = ServerName,
            DatabaseName = "MyDb",
            TimeRangeStart = new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc),
            TimeRangeEnd = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            Severity = 1.6,
            Confidence = 1.0,
            Category = "queries",
            StoryPath = "PLAN_REGRESSION",
            StoryPathHash = "hash_PLAN_REGRESSION",
            StoryText = "story",
            RootFactKey = "PLAN_REGRESSION",
            DrillDown = new Dictionary<string, object> { ["regressed_queries"] = rows },
        };
    }

    /// <summary>
    /// One <c>regressed_queries</c> row. <paramref name="replicaRole"/> null omits the key entirely,
    /// which is what a pre-#1850 drill-down and the deprecated Dashboard's collector both produce;
    /// empty string is what Lite's and Darling's readers write when the server did not attribute the
    /// row (<c>IsDBNull(11) ? "" : GetString(11)</c>). Both must behave identically.
    /// </summary>
    private static Dictionary<string, object> Row(
        long queryId, long bestPlanId, double regressionFactor, string? replicaRole,
        string database = "MyDb")
    {
        var row = new Dictionary<string, object>
        {
            ["database"] = database,
            ["query_id"] = queryId,
            ["best_plan_id"] = bestPlanId,
            ["regression_factor"] = regressionFactor,
        };

        if (replicaRole is not null)
            row["replica_role"] = replicaRole;

        return row;
    }

    /* ---------------- which row wins ---------------- */

    [Fact]
    public void BothReplicasRegressed_PrimaryWins_EvenWhenTheSecondaryRegressedHarder()
    {
        /* THE discriminating case. Rows arrive regression_factor DESC, so the secondary's 12x comes
           first and #1850's "keep the first" kept it — producing a recommendation to force plan 99 on
           the PRIMARY (the documented default of the omitted @replica_group_id) because a READ-ONLY
           replica ran a different plan better. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "SECONDARY"),
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 3.0, replicaRole: "PRIMARY"));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));

        Assert.Equal(7L, target.PlanId);
        Assert.Equal(3.0, target.RegressionFactor);
        Assert.Equal("PRIMARY", target.ReplicaRole);
    }

    [Fact]
    public void BothReplicasRegressed_PrimaryWins_RegardlessOfRowOrder()
    {
        /* The preference must not depend on which row the SQL happened to emit first — the same result
           when the primary leads, which is the case #1850's rule already got right by accident. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 12.0, replicaRole: "PRIMARY"),
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 3.0, replicaRole: "SECONDARY"));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(7L, target.PlanId);
        Assert.Equal("PRIMARY", target.ReplicaRole);
    }

    [Theory]
    [InlineData("Primary")]     /* what the collector actually stores — sys.query_store_replicas.replica_name */
    [InlineData("PRIMARY")]     /* what the seeded drill-downs and the #1850 test use */
    [InlineData("primary")]
    public void PrimaryMatch_IsCaseInsensitive(string primaryRole)
    {
        /* The casing differs between production and every seeded fixture in this repo: the collector
           passes SQL Server's own "Primary" through verbatim, while the tests write "PRIMARY". An
           ordinal comparison would pass one and silently fail the other, and the one it failed would be
           production. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "Secondary"),
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 3.0, replicaRole: primaryRole));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(7L, target.PlanId);
    }

    [Theory]
    [InlineData("Geo Secondary")]
    [InlineData("Geo HA Secondary")]
    public void GeoReplicaRoles_LoseToThePrimary(string geoRole)
    {
        /* sys.query_store_replicas' role_type enumerates Geo-Primary, Geo-Secondary and named replicas
           beyond the plain Primary/Secondary pair, and the collector stores replica_name verbatim, so
           these reach the extractor on a geo-replicated Azure database. Anything that is not the primary
           is a replica whose workload is not the one a default-scoped force acts on. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: geoRole),
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 3.0, replicaRole: "Primary"));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(7L, target.PlanId);
    }

    [Fact]
    public void TwoSecondariesAndNoPrimary_KeepsTheWorst()
    {
        /* With no primary row to prefer, #1850's "worst wins" is still the rule — the preference orders
           replica classes, it does not replace the regression ordering inside one. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "Secondary"),
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 3.0, replicaRole: "Geo Secondary"));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(99L, target.PlanId);
        Assert.Equal("Secondary", target.ReplicaRole);
    }

    [Fact]
    public void ALatePrimaryRowCanStillUpgradeAQueryInsideTheCapOfFive()
    {
        /* The cap counts DISTINCT queries, and the loop no longer stops at it — a sixth ROW that is the
           primary's view of a query already holding a slot must still be able to take that slot.
           Stopping at the cap (which is what the pre-#1882 loop did) would have made the preference
           depend on how many other queries regressed harder. */
        var rows = new List<Dictionary<string, object>>();
        for (var q = 1; q <= 5; q++)
            rows.Add(Row(queryId: q, bestPlanId: 900 + q, regressionFactor: 20.0 - q, replicaRole: "Secondary"));

        /* Query 5's primary row arrives sixth, after the cap is already full. */
        rows.Add(Row(queryId: 5, bestPlanId: 55, regressionFactor: 2.0, replicaRole: "Primary"));

        var targets = FactRemediation.ExtractPlanRegressionTargets(PlanRegressionFinding(rows.ToArray()));

        Assert.Equal(5, targets.Count);
        Assert.Equal(55L, targets[4].PlanId);
        Assert.Equal("Primary", targets[4].ReplicaRole);

        /* And the upgrade changed the PLAN, not the position — the list is still ordered worst-first by
           each query's own worst row, which is the order the SQL delivered. */
        Assert.Equal(1L, targets[0].QueryId);
        Assert.Equal(5L, targets[4].QueryId);
    }

    [Fact]
    public void SixDistinctQueries_StillCapAtFive()
    {
        var rows = new List<Dictionary<string, object>>();
        for (var q = 1; q <= 6; q++)
            rows.Add(Row(queryId: q, bestPlanId: 900 + q, regressionFactor: 20.0 - q, replicaRole: "Primary"));

        Assert.Equal(5, FactRemediation.ExtractPlanRegressionTargets(PlanRegressionFinding(rows.ToArray())).Count);
    }

    [Fact]
    public void SameQueryIdInDifferentDatabases_AreNotCollapsed()
    {
        /* The de-dup key is (database, query_id) and the replica preference must not have widened it —
           query_id is only unique within a database. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 12.0, replicaRole: "Primary", database: "A"),
            Row(queryId: 123, bestPlanId: 8, regressionFactor: 3.0, replicaRole: "Primary", database: "B"));

        Assert.Equal(2, FactRemediation.ExtractPlanRegressionTargets(finding).Count);
    }

    /* ---------------- what the recommendation discloses ---------------- */

    private const string ForceDocUrl =
        "https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql";

    [Fact]
    public void SecondaryOnlyRegression_IsStillRecommended_ButDisclosesThatItForcesOnThePrimary()
    {
        /* A regression seen ONLY on a read-only secondary is not discarded: forcing executes on the
           primary in every case, so the recommendation is actionable. What it must not do is stay silent
           about the mismatch between where the evidence came from and where the statement acts. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "Secondary"));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(99L, target.PlanId);

        var sql = FactRemediation.GenerateForFinding(finding);
        Assert.NotNull(sql);

        /* The replica is NAMED, both as the measurement's provenance and in the warning. */
        Assert.Contains("measured on replica: Secondary", sql!, StringComparison.Ordinal);
        Assert.Contains("HEADS UP", sql, StringComparison.Ordinal);
        Assert.Contains("forces on the PRIMARY", sql, StringComparison.Ordinal);

        /* The mechanism is stated, not just the warning — an operator has to be able to check it. */
        Assert.Contains("@replica_group_id", sql, StringComparison.Ordinal);
        Assert.Contains("sys.query_store_replicas", sql, StringComparison.Ordinal);
        Assert.Contains(ForceDocUrl, sql, StringComparison.Ordinal);

        /* The RUNNABLE statement is unchanged: we do NOT emit @replica_group_id, because the collector
           stores replica_name (a role) and not the replica_group_id, so there is no correct value to
           put there. Emitting a guess would be worse than disclosing the default. */
        Assert.Contains("EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99;", sql, StringComparison.Ordinal);

        /* Asserted over the EXECUTABLE lines only, not the whole blob. #1914 added the scoped form to the
           disclosure — as a COMMENT, so the operator can see the shape they would have to write — and a
           bare DoesNotContain over the whole string could not tell that apart from us emitting it. What
           must stay true is that nothing SQL Server would run carries a replica group id. */
        var runnableLines = sql!.Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal));

        Assert.DoesNotContain(runnableLines, line => line.Contains("@replica_group_id", StringComparison.Ordinal));
    }

    [Fact]
    public void TheScopedFormItSuggests_IsTheOneThatActuallyWorks()
    {
        /* #1914 probed sp_query_store_force_plan live, because it is an EXTENDED_STORED_PROCEDURE and
           sys.system_parameters is empty for it — its argument surface can only be established by
           executing it. Measured on SQL Server 2022 (16.0.4255.1) and 2025 (17.0.4045.5), each form run
           from a freshly-unforced plan:

             @query_id, @plan_id, @replica_group_id = 1                              -> OK on both
             @query_id, @plan_id, @disable_optimized_plan_forcing = 0, @rg = 1        -> ERROR 12463 on both

           The four-argument form the reference page's syntax block documents is the one that FAILS, with
           "Role id should be between (including) 1 and 4" for a role id of 1. #1882 shipped guidance
           telling the operator to pass it "as the fourth argument" because "the documented order matters"
           — which would have sent them to the failing form. This pins the corrected shape. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "Secondary"));

        var sql = FactRemediation.GenerateForFinding(finding);

        /* The three-argument named form, which is what works. */
        Assert.Contains(
            "EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99, @replica_group_id = <id>;",
            sql!, StringComparison.Ordinal);

        /* And an explicit steer AWAY from the documented four-argument form. */
        Assert.Contains("@disable_optimized_plan_forcing", sql, StringComparison.Ordinal);
        Assert.Contains("12463", sql, StringComparison.Ordinal);

        /* The stale claim must be gone: named arguments were measured to work in ANY order (@plan_id
           before @query_id succeeds), so "the documented order matters" was never true of them. */
        Assert.DoesNotContain("documented order", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("fourth argument", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ItTellsTheOperatorToVerifyTheGroupId_BecauseSql2025DoesNotValidateIt()
    {
        /* The finding that most justifies not COMPUTING a group id for them (#1914). On SQL Server 2025 —
           the version where Query Store for secondary replicas is GA — @replica_group_id = 99 SUCCEEDED
           on a standalone server with one replica, and wrote a row into
           sys.query_store_plan_forcing_locations naming replica group 99. SQL Server 2022 rejects the
           same call with 12463. So on the version that matters, a wrong id does not fail loudly; it
           records a forcing against a replica that does not exist. Anything we computed and shipped —
           including a correct id that later re-pointed across a failover — would land the same way. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: "Secondary"));

        var sql = FactRemediation.GenerateForFinding(finding);

        Assert.Contains("SELECT replica_group_id, replica_name, role_type FROM sys.query_store_replicas;",
                        sql!, StringComparison.Ordinal);
        Assert.Contains("does", sql, StringComparison.Ordinal);
        Assert.Contains("not exist", sql, StringComparison.Ordinal);
        Assert.Contains("2025", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PrimaryRegression_NamesTheReplicaButRaisesNoWarning()
    {
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 12.0, replicaRole: "Primary"));

        var sql = FactRemediation.GenerateForFinding(finding);

        Assert.Contains("measured on replica: Primary", sql!, StringComparison.Ordinal);
        Assert.DoesNotContain("HEADS UP", sql, StringComparison.Ordinal);

        /* The unforce asymmetry note DOES apply to a correctly-scoped primary force: per
           https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-unforce-plan-transact-sql
           unforce's @replica_group_id "defaults to the local replica where the command is being
           executed", where force's defaults to the primary. So the back-out line, which reads as the
           exact inverse of the force line, is not one when it is run anywhere but the primary. */
        Assert.Contains("Run the unforce ON THE PRIMARY", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]   /* the key is absent — pre-#1850 drill-downs, and the deprecated Dashboard's */
    [InlineData("")]     /* the key is present and empty — what both readers write for an unattributed row */
    public void NoReplicaAttribution_RendersExactlyWhatItAlwaysDid(string? replicaRole)
    {
        /* Every standalone server, every non-AG server and everything below SQL Server 2022 lands here,
           which is the overwhelming majority of installations. Nothing #1882 added may appear. */
        var finding = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 7, regressionFactor: 12.0, replicaRole: replicaRole));

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Null(target.ReplicaRole);

        var sql = FactRemediation.GenerateForFinding(finding);
        Assert.DoesNotContain("measured on replica", sql!, StringComparison.Ordinal);
        Assert.DoesNotContain("HEADS UP", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("replica_group_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("Run the unforce", sql, StringComparison.Ordinal);
    }

    /* ---------------- the copy-paste surface ---------------- */

    [Fact]
    public void CopyPasteSql_SecondaryTarget_CarriesTheDisclosureWithTheStatement()
    {
        /* This is the text an operator PASTES, so the disclosure has to travel with it — a warning that
           only exists in the preview is a warning the person running the statement never sees. */
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force",
            new[] { new ForcePlanTarget("MyDb", 123, 99, ReplicaRole: "Secondary") });

        var sql = FactRemediation.RenderCopyPasteCommand(action);

        Assert.NotNull(sql);
        Assert.Contains("Measured on the Secondary replica", sql!, StringComparison.Ordinal);
        Assert.Contains("forces on the PRIMARY", sql, StringComparison.Ordinal);
        Assert.Contains("USE [MyDb];", sql, StringComparison.Ordinal);
        Assert.Contains("EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void CopyPasteSql_PrimaryAndUnattributedTargets_AreUnchanged()
    {
        /* The byte-exact goldens over this renderer (LiteRecommendationsReaderTests and
           ViewerRecommendationsTests both pin it) cover the unattributed case; this states the same
           thing for a primary-attributed target, which no golden covers. */
        foreach (var role in new[] { null, "", "Primary" })
        {
            var action = new RemediationAction(
                "PLAN_REGRESSION", "force",
                new[] { new ForcePlanTarget("MyDb", 123, 7, ReplicaRole: role) });

            var sql = FactRemediation.RenderCopyPasteCommand(action);

            Assert.Equal(
                "USE [MyDb];" + Environment.NewLine +
                "EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 7;",
                sql);
        }
    }

    /* ---------------- the persisted round trip ---------------- */

    [Fact]
    public void ReplicaRole_SurvivesThePersistedActionRoundTrip()
    {
        /* The recommendation is rebuilt from remediation_action_json long after the ephemeral drill-down
           is gone, so a field that does not survive here is a field the operator never sees. */
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force",
            new[]
            {
                new ForcePlanTarget("MyDb", 123, 99, "0xBEST", "0xLATEST", 9000, 1200, 12.0, "Secondary"),
                new ForcePlanTarget("MyDb", 124, 7, "0xB2", "0xL2", 500, 100, 3.0, "Primary"),
            });

        var json = AlertContextSerializer.SerializeAction(action);
        Assert.NotNull(json);
        var restored = AlertContextSerializer.DeserializeAction(json);

        Assert.NotNull(restored);
        Assert.Equal(2, restored!.Targets.Count);
        Assert.Equal("Secondary", restored.Targets[0].ReplicaRole);
        Assert.Equal("Primary", restored.Targets[1].ReplicaRole);

        /* And the disclosure re-renders off the restored action, not just off the live one. */
        var sql = FactRemediation.RenderCopyPasteCommand(restored);
        Assert.Contains("Measured on the Secondary replica", sql!, StringComparison.Ordinal);
    }

    [Fact]
    public void LegacyPersistedAction_WithoutReplicaRole_DeserializesToNull()
    {
        /* Rows written before #1882 have no such property. They must read back as "not attributed" —
           the same state a non-AG server produces — rather than throwing or inventing a role. */
        const string legacyJson = """
        {
          "FactKey": "PLAN_REGRESSION",
          "Verb": "force",
          "Targets": [
            {
              "Database": "MyDb",
              "QueryId": 123,
              "PlanId": 7,
              "BestPlanHash": "0xBEST",
              "LatestPlanHash": "0xLATEST",
              "LatestCpuPerExecUs": 9000,
              "BestCpuPerExecUs": 1200,
              "RegressionFactor": 7.5
            }
          ]
        }
        """;

        var restored = AlertContextSerializer.DeserializeAction(legacyJson);

        Assert.NotNull(restored);
        var target = Assert.Single(restored!.Targets);
        Assert.Equal(123L, target.QueryId);
        Assert.Equal(7L, target.PlanId);
        Assert.Null(target.ReplicaRole);

        /* A legacy row renders exactly what it always rendered. */
        var sql = FactRemediation.RenderCopyPasteCommand(restored);
        Assert.DoesNotContain("Measured on the", sql!, StringComparison.Ordinal);
        /* #2138 gap 3: the flag defaults FALSE off legacy JSON, so no caution appears either. */
        Assert.DoesNotContain("CAUTION", sql!, StringComparison.Ordinal);
    }

    /* ---------------- #2138 gap 3: the parameter-sensitivity caution ---------------- */

    [Fact]
    public void PspCoFiredTarget_CarriesTheFlag_AndRendersTheCaution()
    {
        var row = Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: null);
        row["parameter_sensitivity_cofired"] = true;
        var finding = PlanRegressionFinding(row);

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.True(target.ParameterSensitivityCoFired);

        var sql = FactRemediation.GenerateForFinding(finding);
        Assert.NotNull(sql);
        Assert.Contains("CAUTION: this query also shows the parameter-sensitivity signature", sql!, StringComparison.Ordinal);
        /* The gentler levers are NAMED — the caution is advice with a next step, not just a wince. */
        Assert.Contains("update statistics", sql, StringComparison.Ordinal);

        /* The caution is comment-only: the runnable statement is byte-identical to the unflagged one. */
        var runnableLines = sql!.Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal));
        Assert.Contains("EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99;", runnableLines);
    }

    [Fact]
    public void UnflaggedTarget_RendersExactly_WhatItRenderedBeforeTheFlagExisted()
    {
        /* An absent key (an old persisted drill-down, the deprecated Dashboard) and an explicit false
           must render byte-identically — and contain no caution — so the render-stability discipline
           the #1882 replica disclosure established holds for this flag too. */
        var absent = PlanRegressionFinding(
            Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: null));

        var flaggedFalse = Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: null);
        flaggedFalse["parameter_sensitivity_cofired"] = false;

        var absentSql = FactRemediation.GenerateForFinding(absent);
        var falseSql = FactRemediation.GenerateForFinding(PlanRegressionFinding(flaggedFalse));

        Assert.NotNull(absentSql);
        Assert.Equal(absentSql, falseSql);
        Assert.DoesNotContain("CAUTION", absentSql!, StringComparison.Ordinal);
    }

    [Fact]
    public void PspCoFiredFlag_SurvivesThePersistedActionRoundTrip()
    {
        /* Review catch on #2140: the flag existed on ForcePlanTarget but not on its JSON mirror
           (ForcePlanTargetDto), so SerializeAction dropped it on the FIRST write and every read
           reconstructed false — and both apps render the copy-paste command from the DESERIALIZED
           action, so the caution never reached the pasted surface at all, and a future bot reading
           persisted actions would have seen false for every flagged target. This is the test that
           was missing: a TRUE flag through the actual persistence path. */
        var row = Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: null);
        row["parameter_sensitivity_cofired"] = true;

        var action = FactRemediation.BuildAction(PlanRegressionFinding(row));
        Assert.NotNull(action);

        var json = AlertContextSerializer.SerializeAction(action!);
        var restored = AlertContextSerializer.DeserializeAction(json);

        Assert.NotNull(restored);
        Assert.True(Assert.Single(restored!.Targets).ParameterSensitivityCoFired);

        /* And the surface that gets executed renders the caution from the RESTORED action. */
        var sql = FactRemediation.RenderCopyPasteCommand(restored);
        Assert.Contains("CAUTION: parameter-sensitive", sql!, StringComparison.Ordinal);
    }

    [Fact]
    public void PspCoFiredTarget_CautionAlsoRidesTheCopyPasteSurface()
    {
        /* The paste surface is the one that gets EXECUTED, so the warning must survive the trip through
           the persisted action — flag into BuildAction, out through RenderCopyPasteCommand — in its
           compact two-line form, with the runnable statement untouched. */
        var row = Row(queryId: 123, bestPlanId: 99, regressionFactor: 12.0, replicaRole: null);
        row["parameter_sensitivity_cofired"] = true;

        var action = FactRemediation.BuildAction(PlanRegressionFinding(row));
        Assert.NotNull(action);
        Assert.True(Assert.Single(action!.Targets).ParameterSensitivityCoFired);

        var sql = FactRemediation.RenderCopyPasteCommand(action);
        Assert.NotNull(sql);
        Assert.Contains("CAUTION: parameter-sensitive", sql!, StringComparison.Ordinal);
        Assert.Contains("EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 99;", sql, StringComparison.Ordinal);
    }
}
