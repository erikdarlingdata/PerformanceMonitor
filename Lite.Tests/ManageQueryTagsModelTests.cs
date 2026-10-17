/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Behaviour pins for the Manage Query Tags window's model (#5993): the fifth level is refused with the shared message, a bad
/// pattern is refused with the validator's code and stores nothing, the Test count matches the shared resolver over seeded
/// sightings, and a write while a restore is pending is refused with the store's message.
/// </summary>
public sealed class ManageQueryTagsModelTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly SharedDuckDbFixture _fixture;
    private readonly LocalDataService _data;
    private readonly ManageQueryTagsModel _model;

    public ManageQueryTagsModelTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _data = new LocalDataService(fixture.DuckDb);
        _model = new ManageQueryTagsModel(_data);
    }

    private async Task ExecAsync(string sql)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CreatingATagAtTheFifthLevel_IsRefusedWithTheSharedMessage_AndStoresNothing()
    {
        int? parent = null;
        for (var level = 1; level <= 4; level++)
        {
            parent = await _data.CreateQueryTagAsync("L" + level, parent);
        }

        var outcome = await _model.CreateTagAsync("L5", parent);

        Assert.False(outcome.Ok);
        Assert.Equal(TagTreeRules.DepthLimit, outcome.Code);
        Assert.Equal(TagTreeRules.DepthLimitMessage, outcome.Message);
        Assert.Equal(4, (await _data.GetQueryTagsAsync()).Count);
    }

    [Fact]
    public async Task ARuleWithAnInvalidPattern_IsRefusedAsABadRule_AndNothingIsStored()
    {
        var tag = await _data.CreateQueryTagAsync("Reports", null);

        var blank = await _model.AddRuleAsync(new QueryTagRuleForm(QueryTagRuleField.Login, "  ", tag));
        var tooLong = await _model.AddRuleAsync(new QueryTagRuleForm(QueryTagRuleField.Login, new string('x', QueryTagLimits.MaxPatternLength + 1), tag));

        Assert.False(blank.Ok);
        Assert.Equal(QueryTagStoreCodes.BadRule, blank.Code);
        Assert.Equal(QueryTagStoreCodes.BadRule, tooLong.Code);
        Assert.Empty(await _data.GetQueryTagRulesAsync());
    }

    [Fact]
    public async Task ASuccessfulWrite_SaysTheMapAppliesItOnItsNextPass()
    {
        var outcome = await _model.CreateTagAsync("Reports", null);

        Assert.True(outcome.Ok);
        Assert.Contains("next pass", outcome.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheTestButtonCount_MatchesTheSharedResolverOverSeededSightings()
    {
        var tag = await _data.CreateQueryTagAsync("Reports", null);
        var other = await _data.CreateQueryTagAsync("Etl", null);
        await ExecAsync(@"INSERT INTO query_tag_sightings (server_id, key_kind, key, database_name, application_name) VALUES
            (1, 'hash', '0xA1', 'Sales', 'Report Server'),
            (1, 'hash', '0xA2', 'Sales', 'Report Builder'),
            (1, 'hash', '0xA3', 'Sales', 'Nightly Etl'),
            (2, 'hash', '0xA4', 'Sales', 'Report Server')");
        // 0xA2 is held by a hand assignment, so the rule matches it but would not change it.
        await _data.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(other, QueryTagKeyKind.Hash, 1, QueryHash: "0xA2"));
        var form = new QueryTagRuleForm(QueryTagRuleField.Application, "report*", tag);

        var result = await _model.TestRuleAsync(form);

        var sightings = await _data.GetQueryTagSightingsAsync();
        var expected = sightings
            .GroupBy(s => (s.ServerId, s.Kind, s.Key))
            .Count(g => QueryTagResolver.Resolve(g.Key.ServerId, g.Select(x => x.Sighting).ToList(), [],
                [new QueryTagRule(1, QueryTagRuleField.Application, "report*", 100, true, tag)], new System.Collections.Generic.Dictionary<string, long>()).HasTag);
        Assert.Equal(3, expected);
        Assert.Equal(expected, result.Matches);
        Assert.Equal(1, result.Held);
        Assert.Equal(4, result.KeysTested);
        Assert.Equal(3, result.Samples.Count);

        var oneServer = await _model.TestRuleAsync(form, serverId: 2);
        Assert.Equal(1, oneServer.Matches);
    }

    [Fact]
    public async Task TheTestButton_ReportsAnInvalidDraftWithoutRunning()
    {
        var tag = await _data.CreateQueryTagAsync("Reports", null);

        var result = await _model.TestRuleAsync(new QueryTagRuleForm(QueryTagRuleField.Login, "", tag));

        Assert.True(result.Refused);
        Assert.Equal(QueryTagRuleError.EmptyPattern, result.Error);
    }

    [Fact]
    public void TheTree_NestsChildrenAndCountsDepthFromZero()
    {
        var now = DateTime.UtcNow;
        var tree = ManageQueryTagsModel.BuildTree([
            new QueryTagRow(1, "A", null, 0, null, now), new QueryTagRow(2, "B", 1, 0, null, now),
            new QueryTagRow(3, "C", 2, 0, null, now), new QueryTagRow(4, "D", 3, 0, null, now)]);

        var d = tree.Single().Children.Single().Children.Single().Children.Single();
        Assert.Equal(3, d.Depth);
        Assert.False(d.CanAddChild);
        Assert.True(tree.Single().CanAddChild);
    }
}

/// <summary>A write from the window while a restore of the query-tag tables is pending is refused with the store's message.</summary>
[Collection("CollectionResetGate")]
public sealed class ManageQueryTagsRestorePendingTests : IDisposable
{
    private readonly string _tempDir;

    public ManageQueryTagsRestorePendingTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
    }

    public void Dispose()
    {
        CollectionResetGate.ResetForTests();
        try { Directory.Delete(_tempDir, true); } catch (IOException) { /* temp cleanup */ }
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task AWriteWhileARestoreIsPending_IsRefusedWithTheStoresMessage()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var data = new LocalDataService(duckDb);
        await data.CreateQueryTagAsync("Kept", null);
        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "query_tags") throw new InvalidOperationException("restore failed");
        };
        try
        {
            await new ArchiveService(duckDb, Path.Combine(_tempDir, "archive"), NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();
        }
        finally
        {
            ArchiveService.BeforePreservedTableRestoreForTests = null;
        }

        var storeMessage = (await Assert.ThrowsAsync<PendingRestoreException>(() => data.CreateQueryTagAsync("Mine", null))).Message;
        var outcome = await new ManageQueryTagsModel(data).CreateTagAsync("Mine", null);

        Assert.False(outcome.Ok);
        Assert.Equal(storeMessage, outcome.Message);
    }
}
