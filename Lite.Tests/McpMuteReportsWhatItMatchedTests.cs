/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3541 A14 — <c>mute_analysis_finding</c> reports what the write DID, on Lite.
///
/// <para>The defect: the verb returned <c>{status: "muted"}</c> for any <c>story_path_hash</c> at all — one
/// copied wrong, one from a different store, one that never existed — and an agent reading that walked away
/// believing a pattern was silenced. The registry it writes is a PATTERN registry (nothing references a
/// finding row), so the write itself is legitimate for a hash that matches nothing today; what was missing
/// is the disclosure. The response now carries <c>registered</c> and <c>matched_now</c>, and its
/// <c>status</c> names the unmatched case rather than folding it into success.</para>
///
/// <para>Planted findings rather than a full analysis pass: the count is a read of <c>analysis_findings</c>
/// by hash and server, and planting two rows under a known hash makes the expected number a fact of the
/// test rather than of the analyser's output on a seeded server. The Darling twin's pin is in
/// <c>DarlingMcpToolsTests</c>' live round-trip.</para>
/// </summary>
public sealed class McpMuteReportsWhatItMatchedTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string PlantedHash = "an3541-lite-planted";
    private const string OtherServerHash = "an3541-lite-elsewhere";
    private const string NeverSeenHash = "an3541-lite-never-seen";

    /// <summary>The <c>story_path</c> every planted finding carries — what the registry row must name once the
    /// store resolves the path from the retained findings (#3653 A15/A16).</summary>
    private const string PlantedStoryPath = "AN3541_LITE";

    /// <summary>A second server id, never registered with the ServerManager, so a hash planted under it is
    /// visible to a fleet-wide count and invisible to the resolved server's. NEGATIVE on purpose: a server_id is
    /// an FNV hash cast to int, so about half of all real ids are negative, and the count's scope test must key
    /// on "null or the sentinel 0" — a <c>&gt; 0</c> test (the first draft of this very read) counted every
    /// negative-id server's scoped mute fleet-wide, and a positive test id here would never have seen it.</summary>
    private const int OtherServerId = -3541_0002;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -3_541_000;
    private DuckDBConnection? _seedConn;

    public McpMuteReportsWhatItMatchedTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpMuteTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store — no DPAPI side effects. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        /* The id the TOOL resolves "TestServer" to; the planted rows must sit under it or every scoped
           assertion below would be about the wrong server. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>The normal case: the hash came from real findings on the named server. Two planted occurrences
    /// → <c>matched_now: 2</c>, <c>status: "muted"</c>, and the registry row exists under the server's id.</summary>
    [Fact]
    public async Task AHashWithStoredFindings_IsMuted_AndSaysHowManyItMatched()
    {
        await PlantFindingAsync(_serverId, DateTime.UtcNow.AddHours(-2), PlantedHash);
        await PlantFindingAsync(_serverId, DateTime.UtcNow.AddHours(-1), PlantedHash);

        var json = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, PlantedHash, "TestServer", "planted");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("muted", root.GetProperty("status").GetString());
        Assert.True(root.GetProperty("registered").GetBoolean());
        Assert.Equal(2, root.GetProperty("matched_now").GetInt64());
        Assert.Equal(PlantedHash, root.GetProperty("story_path_hash").GetString());
        Assert.Equal("TestServer", root.GetProperty("server").GetString());
        Assert.Equal("planted", root.GetProperty("reason").GetString());
        Assert.False(root.GetProperty("already_muted").GetBoolean());
        /* #3653 A15/A16: the row names the CHAIN, read off the retained finding that carries the hash — not
           the hash echoed into the path column, which is what this entry point wrote before. */
        Assert.Equal(PlantedStoryPath, root.GetProperty("story_path").GetString());

        Assert.Equal(1, await CountMuteRowsAsync(PlantedHash, _serverId));
        Assert.Equal(PlantedStoryPath, await StoredPathAsync(PlantedHash, _serverId));
    }

    /// <summary>
    /// #3653 A15/A16, THE idempotence case: the same hash in the same scope a second time. Before, a second
    /// registry row landed and the envelope said <c>muted</c> twice — "registered" was true for a write that
    /// changed nothing. Now nothing is written, the envelope says so (<c>registered: false</c>,
    /// <c>already_muted: true</c>, <c>status: "already_muted"</c>), <c>matched_now</c> is still read against the
    /// same moment, and the registry holds exactly one row for the pair.
    /// </summary>
    [Fact]
    public async Task TheSameHashInTheSameScope_ASecondTime_WritesNothing_AndSaysAlreadyMuted()
    {
        await PlantFindingAsync(_serverId, DateTime.UtcNow.AddHours(-1), PlantedHash);

        var first = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, PlantedHash, "TestServer", "first");
        using (var doc = JsonDocument.Parse(first))
        {
            Assert.Equal("muted", doc.RootElement.GetProperty("status").GetString());
            Assert.True(doc.RootElement.GetProperty("registered").GetBoolean());
        }

        var second = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, PlantedHash, "TestServer", "second, different reason");
        using (var doc = JsonDocument.Parse(second))
        {
            var root = doc.RootElement;
            Assert.Equal("already_muted", root.GetProperty("status").GetString());
            Assert.False(root.GetProperty("registered").GetBoolean());
            Assert.True(root.GetProperty("already_muted").GetBoolean());
            Assert.Equal(1, root.GetProperty("matched_now").GetInt64());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("story_path").ValueKind);
            Assert.Contains("nothing was written", root.GetProperty("note").GetString(), StringComparison.Ordinal);
            Assert.Contains("not recorded", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        }

        Assert.Equal(1, await CountMuteRowsAsync(PlantedHash, _serverId));
        /* The FIRST call's reason is the one on the row — the second changed nothing, as its note said. */
        Assert.Equal("first", await StoredReasonAsync(PlantedHash, _serverId));
    }

    /// <summary>
    /// Scope is part of the key: a server-scoped mute and an all-servers mute of the same hash are two
    /// different registrations, and each is idempotent only against its own kind — the second global write is
    /// the one refused, not the first. The legacy all-servers spelling (<c>server_id = 0</c>, which the pre-fix
    /// tool wrote and every reader still honours as global) counts as the same scope as NULL, so a store that
    /// carries an old 0 row does not grow a NULL twin beside it.
    /// </summary>
    [Fact]
    public async Task Idempotence_IsPerScope_AndTheLegacyZeroRowCountsAsGlobal()
    {
        var store = new FindingStore(_duckDb);

        Assert.Equal(MuteRegistration.Registered, (await store.MuteStoryAsync(_serverId, NeverSeenHash, null)).Registration);
        Assert.Equal(MuteRegistration.Registered, (await store.MuteStoryAsync(0, NeverSeenHash, null)).Registration);
        Assert.Equal(MuteRegistration.AlreadyMuted, (await store.MuteStoryAsync(0, NeverSeenHash, null)).Registration);
        Assert.Equal(MuteRegistration.AlreadyMuted, (await store.MuteStoryAsync(_serverId, NeverSeenHash, null)).Registration);
        Assert.Equal(MuteRegistration.Registered, (await store.MuteStoryAsync(OtherServerId, NeverSeenHash, null)).Registration);

        Assert.Equal(1, await CountMuteRowsAsync(NeverSeenHash, _serverId));
        Assert.Equal(1, await CountMuteRowsAsync(NeverSeenHash, null));
        Assert.Equal(1, await CountMuteRowsAsync(NeverSeenHash, OtherServerId));

        /* A legacy 0 row planted directly: the global write sees it as already registered. */
        await PlantLegacyZeroMuteAsync(OtherServerHash);
        Assert.Equal(MuteRegistration.AlreadyMuted, (await store.MuteStoryAsync(0, OtherServerHash, null)).Registration);
        Assert.Equal(0, await CountMuteRowsAsync(OtherServerHash, null));
    }

    /// <summary>
    /// The path rule at the store: a caller that holds the path writes it; a caller that holds only the hash
    /// (null or empty) gets it resolved from the newest retained finding; and when no finding carries the hash
    /// the column — NOT NULL, no rung — takes the hash as a placeholder, which the result reports as
    /// <c>StoryPath: null</c> rather than as a path.
    /// </summary>
    [Fact]
    public async Task TheStoredPath_IsTheCallers_ElseTheRetainedFindings_ElseTheHashPlaceholderReportedAsUnknown()
    {
        var store = new FindingStore(_duckDb);

        var held = await store.MuteStoryAsync(_serverId, OtherServerHash, "HELD → BY_CALLER");
        Assert.Equal(MuteRegistration.Registered, held.Registration);
        Assert.Equal("HELD → BY_CALLER", held.StoryPath);
        Assert.Equal("HELD → BY_CALLER", await StoredPathAsync(OtherServerHash, _serverId));

        await PlantFindingAsync(OtherServerId, DateTime.UtcNow.AddHours(-1), PlantedHash);
        var resolved = await store.MuteStoryAsync(_serverId, PlantedHash, string.Empty);
        Assert.Equal(MuteRegistration.Registered, resolved.Registration);
        Assert.Equal(PlantedStoryPath, resolved.StoryPath);
        Assert.Equal(PlantedStoryPath, await StoredPathAsync(PlantedHash, _serverId));

        var unknown = await store.MuteStoryAsync(_serverId, NeverSeenHash, null);
        Assert.Equal(MuteRegistration.Registered, unknown.Registration);
        Assert.Null(unknown.StoryPath);
        Assert.Equal(NeverSeenHash, await StoredPathAsync(NeverSeenHash, _serverId));
    }

    /// <summary>
    /// THE case: a hash no stored finding carries. The mute is still registered (pattern registry — it will
    /// bite if the pattern ever appears), but the status says <c>muted_unmatched</c> and <c>matched_now</c> is 0,
    /// which is what a mistyped hash looks like and what the old envelope never said.
    /// </summary>
    [Fact]
    public async Task AHashNoFindingCarries_IsRegistered_ButReportedUnmatched()
    {
        var json = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, NeverSeenHash, "TestServer");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("muted_unmatched", root.GetProperty("status").GetString());
        Assert.True(root.GetProperty("registered").GetBoolean());
        Assert.Equal(0, root.GetProperty("matched_now").GetInt64());
        Assert.Contains("no stored finding", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        Assert.Contains("mistyped", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        /* Registered, as the field says: the row is in the registry under this server. */
        Assert.Equal(1, await CountMuteRowsAsync(NeverSeenHash, _serverId));
    }

    /// <summary>
    /// The count honours the mute's SCOPE. A hash whose only findings sit under another server is unmatched
    /// for the named server and matched fleet-wide — the same hash, two truthful answers, because the mute
    /// being registered is a different mute in each case (server-scoped vs global).
    /// </summary>
    [Fact]
    public async Task MatchedNow_IsCountedInTheMuteScope_ServerOrFleet()
    {
        await PlantFindingAsync(OtherServerId, DateTime.UtcNow.AddHours(-1), OtherServerHash);

        var scoped = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, OtherServerHash, "TestServer");
        using (var doc = JsonDocument.Parse(scoped))
        {
            Assert.Equal("muted_unmatched", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal(0, doc.RootElement.GetProperty("matched_now").GetInt64());
        }

        var fleet = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, OtherServerHash);
        using (var doc = JsonDocument.Parse(fleet))
        {
            Assert.Equal("muted", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal(1, doc.RootElement.GetProperty("matched_now").GetInt64());
            Assert.Equal("(all servers)", doc.RootElement.GetProperty("server").GetString());
        }

        /* Both writes landed, as their own registered flags said: one scoped row, one global (NULL) row. */
        Assert.Equal(1, await CountMuteRowsAsync(OtherServerHash, _serverId));
        Assert.Equal(1, await CountMuteRowsAsync(OtherServerHash, null));
    }

    /// <summary>A blank hash mutes nothing and is refused before any write — an empty-string pattern in the
    /// registry would be a row that can never match and never be found.</summary>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ABlankHash_IsInvalid_AndWritesNothing(string hash)
    {
        var json = await McpAnalysisTools.MuteAnalysisFinding(CreateTestService(), _serverManager, hash, "TestServer");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(0, await CountMuteRowsAsync(hash, _serverId));
    }

    /// <summary>The store read the disclosure rests on: scoped to a server, or fleet-wide for null AND for the
    /// legacy all-servers sentinel 0 (the value the tool still passes through <c>AnalysisFinding.ServerId</c>).</summary>
    [Fact]
    public async Task CountStoredFindings_ScopesByServer_AndTreatsNullAndZeroAsFleetWide()
    {
        await PlantFindingAsync(_serverId, DateTime.UtcNow.AddHours(-3), PlantedHash);
        await PlantFindingAsync(_serverId, DateTime.UtcNow.AddHours(-2), PlantedHash);
        await PlantFindingAsync(OtherServerId, DateTime.UtcNow.AddHours(-1), PlantedHash);

        var store = new FindingStore(_duckDb);

        Assert.Equal(2, await store.CountStoredFindingsAsync(_serverId, PlantedHash));
        Assert.Equal(1, await store.CountStoredFindingsAsync(OtherServerId, PlantedHash));
        Assert.Equal(3, await store.CountStoredFindingsAsync(null, PlantedHash));
        Assert.Equal(3, await store.CountStoredFindingsAsync(0, PlantedHash));
        Assert.Equal(0, await store.CountStoredFindingsAsync(_serverId, NeverSeenHash));
    }

    /// <summary>The description promises the disclosure in the words a caller will look for, on both SKUs'
    /// spellings of the same tool (the Darling twin's description is held to the same tokens by its own pin).</summary>
    [Fact]
    public void TheDescription_NamesRegistered_MatchedNow_AndTheUnmatchedStatus()
    {
        var method = typeof(McpAnalysisTools)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "mute_analysis_finding");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        foreach (var token in new[] { "registered", "matched_now", "\"muted_unmatched\"", "mistyped hash", "already_muted", "\"already_muted\"", "story_path is", "placeholder" })
        {
            Assert.Contains(token, description, StringComparison.Ordinal);
        }
    }

    /* ---------------- plumbing ---------------- */

    private AnalysisService CreateTestService() => new(_duckDb) { MinimumDataHours = 0 };

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task PlantFindingAsync(int serverId, DateTime analysisTime, string storyPathHash)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id,
     remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, 'TestServer', NULL, $4, $5, 0.9, 0.8, 'waits',
        'AN3541_LITE', $6, 'planted for the #3541 A14 mute disclosure',
        'AN3541_LITE', 1, NULL, NULL, 1, NULL, NULL, NULL)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(analysisTime);
        P(serverId);
        P(analysisTime.AddHours(-4));
        P(analysisTime);
        P(storyPathHash);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>A pre-fix all-servers mute: <c>server_id = 0</c>, the sentinel the old MCP path persisted and
    /// every reader still honours as global (<c>FindingStore.GetMutedHashesSql</c>).</summary>
    private async Task PlantLegacyZeroMuteAsync(string storyPathHash)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES ($1, 0, $2, $2, $3, 'legacy zero row')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task<string?> StoredPathAsync(string storyPathHash, int serverId) =>
        ReadMuteColumnAsync("story_path", storyPathHash, serverId);

    private Task<string?> StoredReasonAsync(string storyPathHash, int serverId) =>
        ReadMuteColumnAsync("reason", storyPathHash, serverId);

    private async Task<string?> ReadMuteColumnAsync(string column, string storyPathHash, int serverId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {column} FROM analysis_muted WHERE story_path_hash = $1 AND server_id = $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await cmd.ExecuteScalarAsync() as string;
    }

    /// <summary>Registry rows for a hash under one server, or the GLOBAL rows (server_id IS NULL) when
    /// <paramref name="serverId"/> is null — the store writes the all-servers mute as NULL.</summary>
    private async Task<int> CountMuteRowsAsync(string storyPathHash, int? serverId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = serverId is null
            ? "SELECT COUNT(*) FROM analysis_muted WHERE story_path_hash = $1 AND server_id IS NULL"
            : "SELECT COUNT(*) FROM analysis_muted WHERE story_path_hash = $1 AND server_id = $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        if (serverId is not null)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
        }

        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }
}
