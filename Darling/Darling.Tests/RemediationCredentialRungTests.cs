/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V113 (#2138 phase 1): the per-server remediation credential and the journal's <c>actor</c>.
///
/// <para>This suite also carries the TOP-RUNG guard, which moved here from
/// <see cref="CollectorStallProbeViewerGateTests"/> when V113 dethroned V112. That guard has to live with
/// whichever rung is actually top: it asserts that a store with every sentinel true maps to exactly the
/// head of the ladder, and its whole point is to catch a later rung that appends a sentinel without adding
/// its own arm — which would leave the viewer refusing a store that is perfectly current, permanently,
/// because no further upgrade changes the answer. A rung that keeps claiming the title after losing it
/// breaks every older rung's suite instead.</para>
/// </summary>
public class RemediationCredentialRungTests
{
    internal const int RungVersion = 113;

    /// <summary>The version a store one rung behind this one reports.</summary>
    private const int PreviousVersion = 112;

    /// <summary>This rung's sentinel ordinal in the viewer probe. Its OWN ordinal, which never moves.</summary>
    internal const int ProbeOrdinal = 88;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "remediation-credential-and-actor",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung's four statements. Both ALTERs are schema-qualified for the reason every rung here is: the
    /// migrate session's <c>search_path</c> puts <c>collect</c> first, so a bare name resolves wherever
    /// that points rather than where the rung meant.
    /// </summary>
    [Fact]
    public void TheRungAddsTheCredentialColumnsTheActorAndItsIndex()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        Assert.Contains(
            "ALTER TABLE config.config_monitored_servers", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ADD COLUMN IF NOT EXISTS remediation_username text", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ADD COLUMN IF NOT EXISTS remediation_encrypted_password text", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ALTER TABLE collect.plan_force_actions", sql, StringComparison.Ordinal);
        Assert.Contains(
            "ADD COLUMN IF NOT EXISTS actor text NOT NULL DEFAULT 'bot'", sql, StringComparison.Ordinal);
        Assert.Contains(
            "idx_plan_force_actions_actor", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE reason the rung has four statements instead of three: the actor's DEFAULT is added and then
    /// dropped, in that order, in the same rung.
    ///
    /// <para>Added because every row that predates the column really was written by the bot, so
    /// <c>'bot'</c> is the only honest backfill. Dropped because leaving it in place would make an INSERT
    /// that FORGETS <c>actor</c> silently claim to be the bot — and a bot row is the kind the self-review
    /// is allowed to unforce, so the default fails in the one direction that costs. The ordering is
    /// asserted by POSITION rather than by presence: both statements present in the wrong order would
    /// leave the column with a default and every existing row null-violating, which is the worst of both.</para>
    /// </summary>
    [Fact]
    public void TheActorDefaultIsAddedForTheBackfillThenDroppedSoAForgottenInsertFails()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        var added = sql.IndexOf("DEFAULT 'bot'", StringComparison.Ordinal);
        var dropped = sql.IndexOf("ALTER COLUMN actor DROP DEFAULT", StringComparison.Ordinal);

        Assert.True(added >= 0, "the actor column must carry DEFAULT 'bot' so existing rows backfill honestly");
        Assert.True(dropped >= 0, "the actor DEFAULT must be dropped so an INSERT that omits it fails loudly");
        Assert.True(
            added < dropped,
            "the DEFAULT must be added BEFORE it is dropped — the reverse order leaves a live default and a "
            + "NOT NULL column full of nulls");
    }

    /// <summary>
    /// The journal's INSERT names <c>actor</c>. With the DEFAULT dropped this is not a style point: an
    /// INSERT that omits the column raises 23502 against a live store, so the writer and the rung have to
    /// agree. Read off the shipped SQL rather than a copy.
    /// </summary>
    [Fact]
    public void TheJournalWriterNamesTheActorColumn()
    {
        var sql = ParitySourceLocal.ReadFile(
            "Darling/PerformanceMonitor.Darling.Service/PgPlanForceActionStore.cs");

        Assert.Contains("action, mode, actor, decision, reasons,", sql, StringComparison.Ordinal);

        /* And the read the invariant rests on filters on it. A writer that stamps the actor while the
           review read ignores it would leave own-forces-only broken with every row correctly labelled. */
        Assert.Contains("pfa.actor = 'bot'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The actor is a REQUIRED record member, so a construction site that forgets it does not compile.
    /// Asserted through reflection on the primary constructor because that is the property being claimed —
    /// a defaulted parameter would let a new writer journal as whichever actor the default named, and one
    /// of the two is the one the review may act on.
    /// </summary>
    [Fact]
    public void TheActorIsRequiredOnTheRecord_SoTheCompilerEnumeratesCallSites()
    {
        var constructor = typeof(PlanForceActionRecord)
            .GetConstructors()
            .OrderByDescending(c => c.GetParameters().Length)
            .First();

        var actor = Assert.Single(
            constructor.GetParameters().Where(p => p.Name == "Actor"));

        Assert.False(
            actor.HasDefaultValue,
            "PlanForceActionRecord.Actor must have no default: a construction site that omits it has to "
            + "fail to compile rather than silently pick an actor.");

        /* Positive control for the reflection: the record really does carry defaulted parameters elsewhere
           in the codebase's style, so "HasDefaultValue is false" is a fact about this parameter rather
           than about the reflection call always returning false. ReplicaRole on ForcePlanTarget is the
           nearest example of the appended-with-a-default pattern this one deliberately does not follow. */
        var replicaRole = Assert.Single(
            typeof(PerformanceMonitor.Analysis.ForcePlanTarget)
                .GetConstructors()
                .OrderByDescending(c => c.GetParameters().Length)
                .First()
                .GetParameters()
                .Where(p => p.Name == "ReplicaRole"));
        Assert.True(replicaRole.HasDefaultValue);
    }

    /// <summary>
    /// The connect-time gate. A COLUMN sentinel on <c>actor</c>, because both objects this rung touches
    /// already exist — the registry since V17, the journal since V107 — so table existence cannot separate
    /// the rungs, and the actor is the one the own-forces-only invariant turns on. Deliberately not a
    /// credential column: those are the secret and non-secret halves of one optional feature, and a probe
    /// line naming one reads as though the viewer needed to see it.
    ///
    /// <para><b>The top-rung guard.</b> Every sentinel true must map to exactly this version, or the
    /// viewer refuses a fully-migrated store forever. The all-true argument list is built by reflection so
    /// the arity tracks the signature: the literal-true form silently defaults a newly added sentinel to
    /// false and maps one version low, which is the failure this guard exists for.</para>
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheActorColumn_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            "table_name = 'plan_force_actions' AND column_name = 'actor'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = ParitySourceLocal.ReadFile(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasRemediationCredentialAndActor", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* As the TOP rung, this sentinel is the last one — and that equality is what catches a later rung
           appending a sentinel without adding its own arm. When a later rung lands, this clause moves to
           it and becomes ProbeOrdinal < arity here. */
        Assert.Equal(arity - 1, ProbeOrdinal);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel EXCEPT this one must report 112. Without this the arm above
           could be satisfied by an unconditional return and nothing would notice. */
        var allButMine = Enumerable.Repeat((object)true, arity).ToArray();
        allButMine[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, allButMine)!);
    }

    /// <summary>
    /// The two new registry columns are classified in the viewer's column ACL, and on the right sides. The
    /// live security gate already asserts the union covers the table; what it cannot assert is that the
    /// SECRET one landed in the secret list rather than being waved through to fix a failing build.
    /// </summary>
    [Fact]
    public void TheCredentialColumnsAreClassifiedWithTheSecretOnTheSecretSide()
    {
        var acl = Assert.Single(
            DarlingManagedRoles.ViewerRestrictedConfigTables
                .Where(t => t.Table == "config_monitored_servers"));

        Assert.Contains("remediation_username", acl.NonSecretColumns);
        Assert.Contains("remediation_encrypted_password", acl.SecretColumns);
        Assert.DoesNotContain("remediation_encrypted_password", acl.NonSecretColumns);
    }
}

/// <summary>
/// Reads a repo file by a path relative to the repo root. A local helper because
/// <c>Lite.Tests.ParitySource</c> is in the other test assembly and <c>Darling.Tests.RepoFile</c> takes
/// path segments; both resolve the same root the same way.
/// </summary>
internal static class ParitySourceLocal
{
    internal static string ReadFile(string relativePath) =>
        RepoFile.ReadRepoFile(relativePath.Split('/'));
}
