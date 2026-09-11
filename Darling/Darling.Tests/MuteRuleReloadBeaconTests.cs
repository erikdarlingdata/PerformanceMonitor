/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V117 / #3315: <c>config.config_mute_rules</c> carries a <c>config_version</c> bump trigger, so every
/// writer of that table makes the service reload its in-memory mute cache on the next sweep.
///
/// <para>The cache is the whole reason this rung exists. <c>MuteRuleService</c> holds the rules the alert
/// engine consults, and <c>LoadAsync()</c> is reached from exactly two places: service start, and the
/// control-plane reload gated on a <c>config_version</c> change. A mute that lands in the table without
/// bumping the beacon is therefore persisted and inert, and every surface an operator would check to
/// confirm it — the rule list, the tool's own success reply — reads the TABLE, so they all agree the mute is
/// in place while matching alerts keep being delivered.</para>
///
/// <para>This class also carries the "I am the top rung" claims, handed over from
/// <see cref="CustomAlertCoreMigrationTests"/> (V116) when this rung landed: a fully-migrated store must map
/// to EXACTLY this version, or the viewer's connect-time gate refuses a store that is current.</para>
/// </summary>
public sealed class MuteRuleReloadBeaconTests
{
    private const int RungVersion = 117;
    private const int PreviousVersion = 116;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 92;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "mute-rules-reload-beacon",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count).ToList(), above);
    }

    [Fact]
    public void TheRungInstallsTheBumpTrigger_OnAllThreeWriteKinds_StatementLevel()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema. */
        Assert.Contains("ON config.config_mute_rules", rung, StringComparison.Ordinal);

        /* DELETE is the direction that costs most — an operator un-mutes, believes alerting is restored, and
           a stale cache keeps suppressing — so it is asserted by name rather than left to the event list. */
        Assert.Contains("AFTER INSERT OR UPDATE OR DELETE", rung, StringComparison.Ordinal);

        /* V17's function verbatim, statement-level like the four beacon triggers it joins. */
        Assert.Contains("FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version()", rung, StringComparison.Ordinal);

        /* Replay-safe: this rung runs again on any store the upgrade-ladder fixtures replay. */
        Assert.Contains("DROP TRIGGER IF EXISTS trg_bump_mute_rules", rung, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryMuteRuleWriteStatementTargetsTheTriggeredTable()
    {
        /* The trigger fires per STATEMENT on config_mute_rules, so what makes it cover "every writer" is that
           every writer's statement names that table. Asserted against the shipped SQL of both store
           implementations — the service's PgMuteRuleStore (which the MCP tools construct) and the viewer's
           duplicated consts — because a writer that reached the rows another way would leave this rung
           installed and the cache stale, which is the failure the rung exists to end. */
        var store = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "PgMuteRuleStore.cs");
        var mutations = new[]
        {
            "INSERT INTO config_mute_rules",
            "UPDATE config_mute_rules SET",
            "DELETE FROM config_mute_rules",
        };

        foreach (var statement in mutations)
        {
            Assert.Contains(statement, store, StringComparison.Ordinal);
        }

        Assert.Contains("INSERT INTO config_mute_rules", ViewerDataService.MuteRuleInsertSql, StringComparison.Ordinal);
        Assert.Contains("UPDATE config_mute_rules SET", ViewerDataService.MuteRuleUpdateSql, StringComparison.Ordinal);
        Assert.Contains("UPDATE config_mute_rules SET", ViewerDataService.MuteRuleSetEnabledSql, StringComparison.Ordinal);
        Assert.Contains("DELETE FROM config_mute_rules", ViewerDataService.MuteRuleDeleteSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        /* pg_trigger, not information_schema.triggers: that view lists only triggers on relations the current
           role owns or holds a non-SELECT privilege on, and the read-only viewer role holds neither here, so
           it would report this rung absent on a store that is fully migrated.

           Read over the probe with its block comments REMOVED, because the sentinel's own comment states
           that reasoning and so spells the view name this asserts the absence of — the unstripped form finds
           its own rationale and fails. Same blind spot the V71 arm records from the other direction: the
           coverage ratchet strips the probe's information_schema LINES but cannot strip a comment, so a
           prose mention there made a table look read. A text scan over SQL that carries prose has to decide
           which it is looking at. */
        var probe = WithoutSqlComments(ViewerDataService.StoreSchemaProbeSql);
        Assert.Contains("t.tgname = 'trg_bump_mute_rules'", probe, StringComparison.Ordinal);
        Assert.Contains("FROM pg_trigger t", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("information_schema.triggers", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasMuteRuleReloadBeacon", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. */
        Assert.Equal(ProbeOrdinal, arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly this version. Built by
           reflection so the arity tracks the signature. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel EXCEPT this one reports 116 (the previous top rung). Without this
           the arm above could be satisfied by an unconditional return and nothing would notice. */
        var behind = Enumerable.Repeat((object)true, arity).ToArray();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }

    /// <summary>
    /// The probe SQL with its block comments removed, so a scan for a catalog name reads the QUERY and
    /// not the prose beside it. Local and deliberately small: the shared
    /// <see cref="CSharpSourceWalker"/> strips C# and this is SQL inside a verbatim string, where the
    /// only comment form the probe uses is the block one.
    /// </summary>
    private static string WithoutSqlComments(string sql) =>
        Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
}
