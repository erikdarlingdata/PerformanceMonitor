/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V32 fleet-tag schema and the viewer's tag SQL. Ungated — pure shape assertions.
/// </summary>
public sealed class DarlingServerTagsTests
{
    private static string V32Sql =>
        PgMigrations.Scripts.Single(s => s.Version == 32).Sql;

    private static string ViewerFile(string name) =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", name);

    [Fact]
    public void ViewerTagService_HoldsNoTagSql_TheStoreOwnsIt()
    {
        var src = ViewerFile("ViewerDataService.ServerTags.cs");

        foreach (var token in new[] { "FROM server_tags", "INSERT INTO server_tag", "UPDATE server_tags", "DELETE FROM server_tag" })
        {
            Assert.DoesNotContain(token, src, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("MainWindow.Tags.cs")]
    [InlineData("ManageTagsWindow.xaml.cs")]
    public void TagDialogs_TakeTheDepthCapFromServerTagRules(string file)
    {
        var src = ViewerFile(file);

        Assert.Contains("ServerTagRules.MaxDepth", src, StringComparison.Ordinal);
        Assert.DoesNotContain("MaxTagDepth", src, StringComparison.Ordinal);
        Assert.DoesNotContain("int MaxDepth = 4;", src, StringComparison.Ordinal);
    }

    [Fact]
    public void StaleNeverReadsTagsComments_AreGone()
    {
        Assert.DoesNotContain("service never reads", ViewerFile("ViewerDataService.ServerTags.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("service never reads", PgMigrations_Source(), StringComparison.Ordinal);
        Assert.DoesNotContain("service consumes nothing" + " about tags", RepoFile.ReadRepoFile("Darling", "Darling.Tests", "DarlingServerTagsTests.cs"),
            StringComparison.Ordinal);
    }

    private static string PgMigrations_Source() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");

    [Theory]
    [InlineData("MainWindow.Tags.cs")]
    [InlineData("ManageTagsWindow.xaml.cs")]
    public void TagDeleteDialogs_ReadTheImpact_AndUseTheSharedWarning_WithoutBlockingOnAFailedRead(string file)
    {
        var src = ViewerFile(file);

        Assert.Contains("GetServerTagDeleteImpactAsync", src, StringComparison.Ordinal);
        Assert.Contains("ServerTagCoverage.DeleteWarning(", src, StringComparison.Ordinal);
        Assert.Contains("Delete impact read failed", src, StringComparison.Ordinal);
    }

    [Fact]
    public void V32_IsSchemaQualified_AndTheNewestMigrationTracksTheBuildVersion()
    {
        var v32 = PgMigrations.Scripts.Single(s => s.Version == 32);

        Assert.Equal("server-tags", v32.Name);

        /* V33 (#1659 connection-alert opt-ins): config.-qualified like every additive control-plane
           migration, and every added column NOT NULL DEFAULT so a pre-V33 row needs no backfill. */
        var v33 = PgMigrations.Scripts.Single(s => s.Version == 33);
        Assert.Equal("connection-alert-refire", v33.Name);
        Assert.Contains("ALTER TABLE config.config_alert_settings", v33.Sql, StringComparison.Ordinal);
        Assert.Contains("notify_connection_down_at_startup boolean NOT NULL DEFAULT false", v33.Sql, StringComparison.Ordinal);
        Assert.Contains("connection_refire_minutes integer NOT NULL DEFAULT 0", v33.Sql, StringComparison.Ordinal);

        /* V35 (#991 Availability Group alert knobs). Same discipline: config.-qualified, every column
           NOT NULL DEFAULT — and the two thresholds carry the SHIPPED defaults (lag on at 300s, redo queue
           off), which the service's fallback seams and the viewer's parse fallbacks both mirror.
           No longer the newest migration (V36 appends the AG latency columns), so the "tracks the build
           version" assertion moved to the newest by IDENTITY rather than by number — a positional or
           literal pin here turns every stacked branch into a conflict, which is the lesson this file's
           own ordinal-free rewrite already records. */
        var v35 = PgMigrations.Scripts.Single(s => s.Version == 35);
        Assert.Equal("availability-group-alerts", v35.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Contains("ALTER TABLE config.config_alert_settings", v35.Sql, StringComparison.Ordinal);
        Assert.Contains("notify_ag_health boolean NOT NULL DEFAULT true", v35.Sql, StringComparison.Ordinal);
        Assert.Contains("ag_lag_alert_seconds integer NOT NULL DEFAULT 300", v35.Sql, StringComparison.Ordinal);
        Assert.Contains("ag_redo_queue_alert_kb bigint NOT NULL DEFAULT 0", v35.Sql, StringComparison.Ordinal);

        /* config.-QUALIFIED: the migrate session runs under search_path = collect, config, public, so an
           unqualified CREATE TABLE would land in collect — the wrong schema AND the wrong ACL. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.server_tags", V32Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.server_tag_map", V32Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE IF NOT EXISTS server_tags", V32Sql, StringComparison.Ordinal);

        /* Idempotent idioms throughout — the runner skips applied versions, but the repo treats
           IF NOT EXISTS as mandatory anyway. */
        Assert.Contains("CREATE UNIQUE INDEX IF NOT EXISTS", V32Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS", V32Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V32_RootTagNamesAreDeduped_ViaCoalesceNotAPlainUnique()
    {
        /* The subtle one. A plain UNIQUE (parent_id, lower(name)) would NOT dedupe ROOT tags at all:
           parent_id is NULL for roots and Postgres treats NULLs as DISTINCT, so any number of root tags
           named 'prod' would insert. COALESCE(parent_id, 0) makes roots comparable; identity starts at 1,
           so 0 is never a real id. */
        Assert.Contains("COALESCE(parent_id, 0)", V32Sql, StringComparison.Ordinal);
        Assert.Contains("lower(name)", V32Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V32_TagTreeCascades_ButNeverReachesTheServerRegistry()
    {
        /* Deleting a tag removes its subtree and those assignments. It must never be able to reach
           config_monitored_servers: there is no FK to it, so no server row, credential blob, or collected
           history can be cascaded away by a tag delete. */
        Assert.Contains("REFERENCES config.server_tags(id) ON DELETE CASCADE", V32Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config_monitored_servers", V32Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V32_MapIsManyToMany_AndCarriesNoServerFk()
    {
        /* Composite PK = a server may carry any number of tags (tags, not exclusive groups). */
        Assert.Contains("PRIMARY KEY (server_id, tag_id)", V32Sql, StringComparison.Ordinal);

        /* Deliberately NO FK on server_id: GetManagedServersAsync falls back to the observed servers
           registry on a pre-seed store, so the sidebar can legitimately show ids absent from
           config_monitored_servers, and an FK would reject tagging them. The cleanup is owned explicitly
           by the remove-server path instead. */
        Assert.DoesNotContain("server_id integer NOT NULL REFERENCES", V32Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V32_HasNoReloadBeaconTrigger()
    {
        /* The alert evaluator reads tags, but through its own cache refresh, so no beacon is needed. A
           config_bump_version trigger would force a full fleet reconcile on every tag edit (V31 custom_views precedent). */
        Assert.DoesNotContain("config_bump_version", V32Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TRIGGER", V32Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BulkAssign_IsASingleStatement_NotARoundTripPerServer()
    {
        /* Tagging 100 servers must be one statement. A row-per-server loop is the difference between an
           instant bulk tag and 100 round trips. */
        Assert.Contains("unnest($1)", ServerTagStore.AssignSql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT DO NOTHING", ServerTagStore.AssignSql, StringComparison.Ordinal);
        Assert.Contains("= ANY($1)", ServerTagStore.UnassignSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RemovingAServer_HasAWayToClearItsTags_SoAReAddCannotResurrectThem()
    {
        /* server_id is a deterministic hash of host+database+read-only-intent, so a removed-then-re-added
           server gets the SAME id. Orphaned map rows would silently restore its old tags. */
        Assert.Contains("DELETE FROM server_tag_map WHERE server_id = $1", ServerTagStore.ClearForServerSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TagSql_UsesBareTableNames_MatchingEveryOtherConfigWrite()
    {
        /* Bare names resolve through the connection search_path to config, the same as
           config_monitored_servers. Schema-qualifying here would diverge from every sibling write. */
        foreach (var sql in new[]
                 {
                     ServerTagStore.SelectTagsSql,
                     ServerTagStore.SelectAssignmentsSql,
                     ServerTagStore.InsertSql,
                     ServerTagStore.RenameSql,
                     ServerTagStore.SetColourSql,
                     ServerTagStore.ReparentSql,
                     ServerTagStore.DeleteSql,
                     ServerTagStore.GetSql,
                     ServerTagStore.TagScopedRulesSql,
                     ServerTagStore.RegisteredServerIdsSql,
                 })
        {
            Assert.DoesNotContain("config.server_tag", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void V50_AddsANullableColourColumn_ToServerTags()
    {
        var v50 = PgMigrations.Scripts.Single(s => s.Version == 50);
        Assert.Equal("server-tag-colour", v50.Name);

        /* Additive, config.-qualified, idempotent — and NULLABLE (no NOT NULL / DEFAULT), so existing tags
           stay NULL (a neutral pill) until coloured and no backfill runs. */
        Assert.Contains("ALTER TABLE config.server_tags", v50.Sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS colour text", v50.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", v50.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TagColourSql_ReadsAndWritesTheColumn()
    {
        /* The read must SELECT colour (the sidebar swatch + the Overview pills need it); the write targets it. */
        Assert.Contains("colour", ServerTagStore.SelectTagsSql, StringComparison.Ordinal);
        Assert.Contains(
            "UPDATE server_tags SET colour = $2 WHERE id = $1",
            ServerTagStore.SetColourSql,
            StringComparison.Ordinal);
    }
}
