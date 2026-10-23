/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// #6399: <c>purge_expired_mute_rules</c> as the provisioned, NOSUPERUSER <c>mcp</c> role (the shipped provisioning batch run whole, so a
/// missing grant fails here and not on an owner connection). Only the expired rules go, each is returned as it was, and a rule with no
/// end time or an end ahead is left alone. The tool is a write tool: with the writes off it is not offered.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PurgeExpiredMuteRulesLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void WithWritesOff_TheToolIsNotServed_AndAnInvestigationAgentNeverGetsIt()
    {
        const string tool = "purge_expired_mute_rules";
        Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers(tool));
        Assert.True(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false).Offers(tool));
        Assert.False(new DarlingMcpWritePolicy.Effective(Writes: false, ServerAdmin: false, Investigations: true).Offers(tool));
    }

    [Fact]
    public async Task TheMcpRole_PurgesOnlyTheExpiredRule_AndReturnsItAsItWas()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the purge_expired_mute_rules live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using var provision = new NpgsqlCommand(batch, owner);
                await provision.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            var store = new PgMuteRuleStore(mcp);

            var now = DateTime.UtcNow;
            var expired = new MuteRule { Id = "n6399-expired", ExpiresAtUtc = now.AddHours(-2), CreatedAtUtc = now.AddDays(-1), MetricName = "High CPU", Reason = "n6399 expired" };
            var active = new MuteRule { Id = "n6399-active", ExpiresAtUtc = now.AddHours(2), CreatedAtUtc = now.AddDays(-1), MetricName = "High CPU", Reason = "n6399 active" };
            var permanent = new MuteRule { Id = "n6399-permanent", ExpiresAtUtc = null, CreatedAtUtc = now.AddDays(-1), MetricName = "Blocking", Reason = "n6399 permanent" };
            await store.InsertAsync(expired);
            await store.InsertAsync(active);
            await store.InsertAsync(permanent);

            var answer = JsonNode.Parse(await DarlingMcpAlertTools.PurgeExpiredMuteRules(mcp))!;
            Assert.Equal("purged", (string)answer["status"]!);
            Assert.Equal(1, (int)answer["count"]!);
            var previous = answer["previous"]!.AsArray().Single()!;
            Assert.Equal("n6399-expired", (string)previous["id"]!);
            Assert.Equal("n6399 expired", (string)previous["reason"]!);
            Assert.Equal("High CPU", (string)previous["metric_name"]!);
            Assert.Null(answer["undo"]);
            Assert.False(string.IsNullOrWhiteSpace((string?)answer["undo_note"]));

            var left = (await store.LoadAllAsync(ct)).Select(r => r.Id).OrderBy(i => i, StringComparer.Ordinal).ToList();
            Assert.Equal(new[] { "n6399-active", "n6399-permanent" }, left);

            var again = JsonNode.Parse(await DarlingMcpAlertTools.PurgeExpiredMuteRules(mcp))!;
            Assert.Equal("none_expired", (string)again["status"]!);
            Assert.Equal(0, (int)again["count"]!);
            Assert.Equal(2, (await store.LoadAllAsync(ct)).Count);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
