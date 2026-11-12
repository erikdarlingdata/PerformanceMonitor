/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// V236 (#7083): a mute rule's action. A store at V235 with rules in it reads <c>action = 'mute'</c> for every one of them after the upgrade
/// (the rule keeps muting), the column's CHECK refuses any other word, and a digest rule round-trips through <see cref="PgMuteRuleStore"/>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to create and drop its own database. */
public sealed class MuteRuleActionV236LiveTests
{
    private const string ActionsSql = "SELECT string_agg(id || '=' || action, ',' ORDER BY id) FROM config.config_mute_rules";

    private static async Task<string> ActionsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ActionsSql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    [Fact]
    public async Task AnExistingRuleReadsMute_AfterTheUpgrade_AndTheStoreRoundTripsADigestRule()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* Back to V235: the action column is gone, so a rule written now is one an older build wrote. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 235, ct);
        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 236", ct);
        await ExecAsync(connection,
            "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name) VALUES ('old', TRUE, now() AT TIME ZONE 'UTC', 'older rule', 'High CPU')", ct);
        Assert.Equal(0, await CountAsync(connection,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_mute_rules' AND column_name = 'action'", ct));

        Assert.Equal(StorageVersion.SchemaVersion - 235, await PgMigrations.MigrateAsync(connection, ct)); /* V236 and every rung above it */

        Assert.Equal("old=mute", await ActionsAsync(connection, ct));

        /* The CHECK admits only the two words. */
        var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
            "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, action) VALUES ('bad', TRUE, now() AT TIME ZONE 'UTC', 'bad', 'snooze')", ct));
        Assert.Equal("23514", refused.SqlState);

        /* A digest rule round-trips: written as digest, read as digest, flipped back to mute by an update. */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new PgMuteRuleStore(dataSource);
        var digest = new MuteRule { Action = MuteRuleActions.Digest, MetricName = "Blocking Detected", Reason = "nightly delete" };
        await store.InsertAsync(digest);

        var loaded = (await store.LoadAllAsync(ct)).ToDictionary(r => r.Id);
        Assert.True(loaded[digest.Id].IsDigestAction);
        Assert.False(loaded["old"].IsDigestAction);
        Assert.Equal(MuteRuleActions.Mute, loaded["old"].Action);

        digest.Action = MuteRuleActions.Mute;
        await store.UpdateAsync(digest);
        Assert.False((await store.LoadAllAsync(ct)).Single(r => r.Id == digest.Id).IsDigestAction);
        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
    }
}
