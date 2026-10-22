/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6334 security review round 1, matcher and MCP side: the zone cache cannot be grown by a caller, a zone lookup never throws, a write
/// refuses an odd zone or a long metric name before any lookup, the two MCP reads mask the operator's note and the author at
/// metadataOnly, and the output of both reads is bounded.
/// </summary>
public sealed class KnownPatternSecurityReviewTests
{
    private const string NoteCanary = "canary note 77881234 about the nightly job";
    private const string LoginCanary = "canary_author_6334";
    private static readonly DateTime Created = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Now = new(2026, 3, 4, 0, 0, 0, DateTimeKind.Utc);

    private static KnownPatternInput Input(string? zone = null, string? metric = null) =>
        new(null, metric, zone is null ? null : new TimeOnly(2, 0), zone, null, "a note", null);

    private static KnownPattern Entry(long id, string note = NoteCanary, string? createdBy = LoginCanary, string? zone = null) =>
        new(id, null, null, zone is null ? null : new TimeOnly(2, 0), zone, 30, note, createdBy, Created, null);

    [Fact]
    public void ManyDistinctUnknownZones_DoNotGrowTheZoneCache()
    {
        var before = KnownPatternMatcher.CachedZoneCount;
        for (var i = 0; i < 300; i++)
        {
            var zone = "Nowhere/Zone" + i.ToString(CultureInfo.InvariantCulture);
            Assert.False(KnownPatternMatcher.IsKnownTimeZone(zone));
            Assert.NotNull(Input(zone).Validate());
        }

        // Other tests may add the few real zones they use; 300 cached misses would be far past this.
        Assert.True(KnownPatternMatcher.CachedZoneCount - before < 20, $"the cache grew by {KnownPatternMatcher.CachedZoneCount - before}");
    }

    [Fact]
    public void AFolderNamedZone_ThatMakesTheHostLookupThrow_IsNotFound_NotAnException()
    {
        var real = KnownPatternMatcher.ZoneLookup;
        // Unix throws SecurityException for a folder such as /usr/share/zoneinfo/America; fake it on any host. Every other id
        // goes to the real lookup, so a test running beside this one is not disturbed.
        KnownPatternMatcher.ZoneLookup = id => id == "America" ? throw new System.Security.SecurityException("folder") : real(id);
        try
        {
            Assert.False(KnownPatternMatcher.IsKnownTimeZone("America"));
            Assert.Contains("not known", Input("America").Validate(), StringComparison.Ordinal);

            var logged = new List<string>();
            var alert = new DateTime(2026, 3, 4, 2, 5, 0, DateTimeKind.Unspecified);
            Assert.True(KnownPatternMatcher.Matches(Entry(1, zone: "America"), 7, "High CPU", alert, logged.Add), "an unreadable zone is read as UTC");
            Assert.Single(logged);
        }
        finally
        {
            KnownPatternMatcher.ZoneLookup = real;
        }
    }

    [Theory]
    [InlineData("Eastern Standard Time")]
    [InlineData("America/New York")]
    [InlineData("../etc/passwd")]
    [InlineData("America//New_York")]
    [InlineData("/America/New_York")]
    [InlineData("America/New_York\nX")]
    [InlineData("Amérique/Montréal")]
    public void AZoneThatIsNotAnIanaShapedId_IsRefused_WithTheExample(string zone)
    {
        var reason = Input(zone).Validate();
        Assert.NotNull(reason);
        Assert.Contains("America/New_York", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void AZoneOver64Characters_IsRefusedBeforeAnyLookup()
    {
        var calls = 0;
        var real = KnownPatternMatcher.ZoneLookup;
        KnownPatternMatcher.ZoneLookup = id => { calls++; return real(id); };
        try
        {
            var tooLong = "Area/" + new string('x', 60);
            Assert.Equal(65, tooLong.Length);
            Assert.NotNull(Input(tooLong).Validate());
            Assert.NotNull(Input(new string('a', 16 * 1024)).Validate());
            Assert.Equal(0, calls);
        }
        finally
        {
            KnownPatternMatcher.ZoneLookup = real;
        }
    }

    [Theory]
    [InlineData("America/New_York")]
    [InlineData("UTC")]
    [InlineData("Etc/GMT+5")]
    [InlineData(null)]
    [InlineData("  ")]
    public void AnIanaZone_OrNone_StillPasses(string? zone) => Assert.Null(Input(zone).Validate());

    [Fact]
    public void AMetricNameOf128Characters_Passes_And129IsRefused()
    {
        Assert.Null(Input(metric: new string('m', 128)).Validate());
        var reason = Input(metric: new string('m', 129)).Validate();
        Assert.NotNull(reason);
        Assert.Contains("129", reason, StringComparison.Ordinal);
        Assert.Contains("128", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void GetKnownPatterns_AtMetadataOnly_WithholdsTheNote_AndTheHostFilterMasksTheAuthor()
    {
        var entries = new[] { Entry(1) };

        var full = Reply(entries, McpDataDetail.Full);
        Assert.Contains(NoteCanary, full, StringComparison.Ordinal);
        Assert.Contains(LoginCanary, full, StringComparison.Ordinal);

        // The tool masks the note; the host's key-name filter (McpDataDetailMasker.Answer) masks the author.
        var tool = Reply(entries, McpDataDetail.MetadataOnly);
        Assert.DoesNotContain(NoteCanary, tool, StringComparison.Ordinal);
        var served = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly).Answer(tool);
        Assert.DoesNotContain(NoteCanary, served, StringComparison.Ordinal);
        Assert.DoesNotContain(LoginCanary, served, StringComparison.Ordinal);

        // maskLiterals keeps the words and replaces the literal digits.
        var literals = JsonDocument.Parse(Reply(entries, McpDataDetail.MaskLiterals)).RootElement
            .GetProperty("known_patterns")[0].GetProperty("note").GetString()!;
        Assert.Contains("nightly", literals, StringComparison.Ordinal);
        Assert.DoesNotContain("77881234", literals, StringComparison.Ordinal);
    }

    [Fact]
    public void AnAlertRow_AtMetadataOnly_WithholdsTheNote_AndTheHostFilterMasksTheAuthor()
    {
        var entries = new[] { Entry(1) };
        var alert = new DateTime(2026, 3, 4, 2, 5, 0, DateTimeKind.Unspecified);

        var (kept, truncated) = DarlingMcpKnownPatternTools.ForAlert(entries, 7, "High CPU", alert, McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly));
        Assert.False(truncated);
        var row = JsonSerializer.Serialize(new { alerts = new[] { new { known_patterns = kept } } }, McpHelpers.JsonOptions);
        Assert.DoesNotContain(NoteCanary, row, StringComparison.Ordinal);
        var served = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly).Answer(row);
        Assert.DoesNotContain(LoginCanary, served, StringComparison.Ordinal);

        var (fullKept, _) = DarlingMcpKnownPatternTools.ForAlert(entries, 7, "High CPU", alert, McpDataDetailMasker.ForAnswer(McpDataDetail.Full));
        Assert.Contains(NoteCanary, JsonSerializer.Serialize(fullKept, McpHelpers.JsonOptions), StringComparison.Ordinal);
    }

    [Fact]
    public void TwentyUnscopedEntries_AreCappedPerAlertRow_AndSayTheyWereCut()
    {
        var entries = Enumerable.Range(1, 20).Select(i => Entry(i)).ToList();
        var alert = new DateTime(2026, 3, 4, 2, 5, 0, DateTimeKind.Unspecified);
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.Full);

        var (kept, truncated) = DarlingMcpKnownPatternTools.ForAlert(entries, 7, "High CPU", alert, masker);
        Assert.Equal(5, kept.Length);
        Assert.True(truncated);

        var (few, fewTruncated) = DarlingMcpKnownPatternTools.ForAlert(entries.Take(5).ToList(), 7, "High CPU", alert, masker);
        Assert.Equal(5, few.Length);
        Assert.False(fewTruncated);
    }

    [Fact]
    public void GetKnownPatterns_OverTheBudget_IsCutFromTheTail_WithTheHouseFields()
    {
        var note = new string('n', KnownPatternMatcher.MaxNoteLength);
        var entries = Enumerable.Range(1, 200).Select(i => Entry(i, note)).ToList();
        var reply = Reply(entries, McpDataDetail.Full);

        Assert.True(Encoding.UTF8.GetByteCount(reply) <= McpResponseBudget.DefaultBytes, "the reply is held to the response budget");
        var root = JsonDocument.Parse(reply).RootElement;
        var returned = root.GetProperty("entries_returned").GetInt32();
        Assert.True(returned is > 0 and < 200);
        Assert.Equal(returned, root.GetProperty("known_patterns").GetArrayLength());
        Assert.Equal(200, root.GetProperty("entries_in_scope").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Contains("TRUNCATED", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("known_patterns")[0].GetProperty("id").GetInt64());

        var small = JsonDocument.Parse(Reply(entries.Take(3).ToList(), McpDataDetail.Full)).RootElement;
        Assert.False(small.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, small.GetProperty("budget_note").ValueKind);
    }

    private static string Reply(IReadOnlyList<KnownPattern> entries, McpDataDetail mode) =>
        DarlingMcpKnownPatternTools.Reply(entries, Now, McpDataDetailMasker.ForAnswer(mode));
}
