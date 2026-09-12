/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An <see cref="IMuteRuleStore"/> whose READ is scripted per call, so a reload can be made to fail the way
/// a store blip makes it fail — and only the read. The mutating members record what the service asked of
/// them, because "does a failed reload also write to the store it could not read" is a question about this
/// seam rather than about the cache.
///
/// <para>Shared with <c>DarlingSelfAlertTests</c>, whose stale-mute fixtures feed a real
/// <see cref="MuteRuleService"/> cache into the condition: that condition's input IS this cache, so its
/// fixture has to be able to produce a cache whose last load failed.</para>
/// </summary>
internal sealed class ScriptedMuteRuleStore : IMuteRuleStore
{
    private readonly Queue<Func<IReadOnlyList<MuteRule>>> _reads = new();

    /// <summary>Ids handed to <see cref="DeleteExpiredAsync"/>, in call order.</summary>
    public List<string> PurgedIds { get; } = new();

    /// <summary>How many times the service asked this store for the rules.</summary>
    public int Loads { get; private set; }

    /// <summary>Queues a successful read returning exactly these rules.</summary>
    public ScriptedMuteRuleStore Returning(params MuteRule[] rules)
    {
        var snapshot = rules.ToList();
        _reads.Enqueue(() => snapshot);
        return this;
    }

    /// <summary>Queues a read that faults the way an unreachable or locked store faults.</summary>
    public ScriptedMuteRuleStore Failing(string message = "store read boom")
    {
        _reads.Enqueue(() => throw new InvalidOperationException(message));
        return this;
    }

    public Task<IReadOnlyList<MuteRule>> LoadAllAsync()
    {
        Loads++;

        Assert.True(_reads.Count > 0, $"read #{Loads} was not scripted by this fixture");

        /* Invoked OUTSIDE the returned task on purpose: both real stores fault while opening a connection
           or executing, so the exception leaves LoadAllAsync rather than arriving on an awaited task. */
        return Task.FromResult(_reads.Dequeue()());
    }

    public Task InsertAsync(MuteRule rule) => Task.CompletedTask;

    public Task UpdateAsync(MuteRule rule) => Task.CompletedTask;

    public Task SetEnabledAsync(string ruleId, bool enabled) => Task.CompletedTask;

    public Task DeleteAsync(string ruleId) => Task.CompletedTask;

    public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
    {
        PurgedIds.AddRange(expiredIds);
        return Task.CompletedTask;
    }
}

/// <summary>
/// #3354: <b>a failed reload must not reduce the set of rules in force.</b>
///
/// <para>The defect was three links, each of which read correctly on its own. Both stores swallowed a
/// failed <c>LoadAllAsync</c> and returned an empty list; <c>MuteRuleService.LoadAsync</c> replaced its
/// cache with whatever came back, with nothing distinguishing "this store has no rules" from "I could not
/// read them"; and <c>LoadAsync</c> is reached on every control-plane reload, not only at startup. So one
/// failed read dropped every mute an operator had in force, the alert engine delivered the flood those
/// rules existed to stop, and the change was invisible — the rule list and the stale-mute condition read
/// the same wrongly-empty cache.</para>
///
/// <para><b>Why the original rationale was right where it was written.</b> At first load an empty set is a
/// real answer: a fresh or unmigrated store has no rules, and refusing to start over that would be worse.
/// That case still works and needs no special case — the cache is empty at first load, so retaining it IS
/// starting clean. One rule covers startup and reload, which is why the fix is a deletion.</para>
///
/// <para>The pins below come in PAIRS wherever they can. A retention arm alone is satisfied by a cache that
/// never updates at all, so each one has an arm beside it proving a legitimately-empty read still empties
/// the cache.</para>
/// </summary>
public sealed class MuteRuleReloadRetentionTests
{
    private static MuteRule Rule(string id, string? metric = "High CPU") => new()
    {
        Id = id,
        Enabled = true,
        CreatedAtUtc = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc),
        MetricName = metric,
    };

    private static MuteRuleService Service(IMuteRuleStore store) =>
        new(store, NullLogger<MuteRuleService>.Instance);

    private static AlertMuteContext Cpu =>
        new() { ServerName = "srv-1", MetricName = "High CPU" };

    [Fact]
    public async Task AFailedReload_LeavesThePreviouslyLoadedRulesInForce()
    {
        var store = new ScriptedMuteRuleStore().Returning(Rule("a"), Rule("b")).Failing();
        var service = Service(store);

        await service.LoadAsync();
        Assert.Equal(2, service.GetRules().Count);
        Assert.True(service.IsAlertMuted(Cpu));

        /* The reload the control plane fires on any config write. The read faults — which is not evidence
           that the operator deleted the rules they put in force a minute ago. */
        await Assert.ThrowsAsync<InvalidOperationException>(service.LoadAsync);

        Assert.Equal(new[] { "a", "b" }, service.GetRules().Select(r => r.Id).OrderBy(id => id, StringComparer.Ordinal));
        Assert.True(service.IsAlertMuted(Cpu));
        Assert.Equal(2, store.Loads);
    }

    [Fact]
    public async Task ALegitimatelyEmptyReload_StillEmptiesTheCache()
    {
        /* The arm that makes the one above mean something. An operator deleting their last rule is the case
           a "never shrink the cache" fix would break, and it is indistinguishable from a failed read unless
           the store answers the two differently — which is the whole change. */
        var store = new ScriptedMuteRuleStore().Returning(Rule("a"), Rule("b")).Returning();
        var service = Service(store);

        await service.LoadAsync();
        Assert.True(service.IsAlertMuted(Cpu));

        await service.LoadAsync();

        Assert.Empty(service.GetRules());
        Assert.False(service.IsAlertMuted(Cpu));
    }

    [Fact]
    public async Task TheFirstLoadOnAnUnreadableStore_StartsCleanRatherThanInventingRules()
    {
        /* The sound half of the original rationale, kept. A headless service against a store that is not
           migrated yet mutes nothing, and the retention rule delivers that for free because there is
           nothing to retain. */
        var store = new ScriptedMuteRuleStore().Failing("relation config_mute_rules does not exist");
        var service = Service(store);

        await Assert.ThrowsAsync<InvalidOperationException>(service.LoadAsync);

        Assert.Empty(service.GetRules());
        Assert.False(service.IsAlertMuted(Cpu));
    }

    [Fact]
    public async Task AFailedReload_WritesNothingToTheStoreItCouldNotRead()
    {
        /* The expiry purge runs after the cache assignment, so a failed read skips it — right for two
           reasons. It is a DELETE against a store that just failed a SELECT, and it is the one path that
           could legitimately shrink the retained set: a rule that expired during the outage must not be
           dropped from the cache on the strength of a read that never happened. */
        var expired = Rule("gone");
        expired.ExpiresAtUtc = new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc);

        var store = new ScriptedMuteRuleStore().Returning(Rule("a")).Failing();
        var service = Service(store);

        await service.LoadAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(service.LoadAsync);

        Assert.Empty(store.PurgedIds);

        /* The control: a SUCCESSFUL load carrying an expired rule does purge, so the emptiness above is the
           failure path and not a purge that never fires for anyone. */
        var live = new ScriptedMuteRuleStore().Returning(Rule("a"), expired);
        await Service(live).LoadAsync();
        Assert.Equal(new[] { "gone" }, live.PurgedIds);
    }

    /* ---------------- the mechanism, pinned from source ---------------- */

    /// <summary>
    /// The retention is an ORDERING, not a branch: the read is awaited before the cache assignment, and
    /// nothing in between catches. Pinned from source because the behavioural arms above survive neither
    /// edit that would put the defect back — a <c>catch</c> around the read that carried on, or the
    /// assignment hoisted above the await — until somebody makes one, and both read perfectly fine.
    /// </summary>
    [Fact]
    public void TheServiceReadsTheStoreBeforeItTouchesTheCache_AndCatchesNothingInBetween()
    {
        var body = Member(
            RepoFile.ReadRepoFile("PerformanceMonitor.Notifications", "MuteRuleService.cs"),
            "LoadAsync").Stripped;

        var read = body.IndexOf("await _store.LoadAllAsync()", StringComparison.Ordinal);
        var assign = body.IndexOf("_rules = rules.ToList()", StringComparison.Ordinal);

        Assert.True(read >= 0, "LoadAsync no longer reads the store through LoadAllAsync");
        Assert.True(assign >= 0, "LoadAsync no longer replaces the cache — this pin is reading the wrong member");
        Assert.True(read < assign, "the cache is replaced before the read completes, so a failed read empties it");

        Assert.DoesNotContain("catch", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Neither store renders a failed read as an empty rule set. This is the pin that would have caught the
    /// defect: both swallows were deliberate, documented, and correct for the case they were written for,
    /// so neither looked wrong in isolation — and the caller they shared made them wrong together.
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/PgMuteRuleStore.cs")]
    [InlineData("Lite/Services/DuckDbMuteRuleStore.cs")]
    public void NeitherStore_TurnsAFailedReadIntoAnEmptyRuleSet(string relative)
    {
        var body = Member(RepoFile.ReadRepoFile(relative), "LoadAllAsync");

        /* Read over STRIPPED source, so the paragraph in each store's remarks explaining why it no longer
           swallows cannot itself trip a scan for the keyword it names. */
        Assert.DoesNotContain("catch", body.Stripped, StringComparison.Ordinal);

        /* The control for that silence: the scan is reading a real body that really does read the table, so
           finding no catch is an absence rather than an empty span. The table name lives in a verbatim
           string literal, hence the RAW body — which is also why the assertion above cannot use it. */
        Assert.Contains("FROM config_mute_rules", body.Raw, StringComparison.Ordinal);
    }

    /// <summary>
    /// Darling reaches <c>LoadAsync</c> from exactly one place, and that place swallows and reports.
    ///
    /// <para>Two call sites are what made this a standing defect rather than a startup one, and two call
    /// sites are also how a fix gets applied to one of them. One helper means the guard cannot be
    /// half-installed, and the counts are what say so.</para>
    ///
    /// <para>The swallow is asserted because an unhandled throw at either site would take down a service
    /// whose collection is healthy. The count is asserted because a cache that is correct and STALE is
    /// indistinguishable from one that is correct and current without an artefact of the failure.</para>
    /// </summary>
    [Fact]
    public void DarlingReachesTheReloadOnlyThroughOneGuardedHelper()
    {
        var raw = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);

        Assert.Equal(1, Occurrences(stripped, "muteRuleService.LoadAsync("));
        Assert.Equal(2, Occurrences(stripped, "await LoadMuteRulesAsync(muteRuleService);"));

        var body = Member(raw, "LoadMuteRulesAsync");

        Assert.Contains("await muteRuleService.LoadAsync();", body.Stripped, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex)", body.Stripped, StringComparison.Ordinal);
        Assert.Contains("RecordReadFailure(null, \"mute-rule reload\"", body.Raw, StringComparison.Ordinal);

        /* It must SWALLOW. A catch that logged and rethrew satisfies every Contains above and still kills
           the service on the startup path. */
        Assert.DoesNotContain("throw", body.Stripped, StringComparison.Ordinal);

        /* And the retained count has to reach the log line: that number is what says the mute set is stale
           rather than gone, and it is the only place the distinction is written down. */
        Assert.Contains("muteRuleService.GetRules().Count", body.Stripped, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite's arm was the worse of the two — a bare <c>catch</c> with no logging, so the event left no
    /// artefact at all on that SKU. Its call site now swallows the throw the store propagates and says so.
    /// </summary>
    [Fact]
    public void LitesCallSite_SwallowsTheThrowAndLeavesAnArtefact()
    {
        var raw = RepoFile.ReadRepoFile("Lite", "MainWindow.xaml.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);

        const string call = "await _muteRuleService.LoadAsync();";
        var at = stripped.IndexOf(call, StringComparison.Ordinal);

        Assert.True(at > 0, "Lite no longer loads its mute rules here — this pin is reading the wrong file");
        Assert.Equal(at, stripped.LastIndexOf(call, StringComparison.Ordinal));

        var guard = EnclosingTryBlock(stripped, at);
        Assert.True(
            guard.HasValue,
            "Lite's mute-rule load is not inside a try, so a store fault aborts the rest of its startup");

        /* The handler, read from the raw source at the same offsets — StripCommentsAndStrings preserves
           length, so they line up — because the artefact this asserts is a log MESSAGE. */
        var handler = raw[guard!.Value.End..];
        var strippedHandler = stripped[guard.Value.End..];

        Assert.StartsWith("catch (Exception", strippedHandler.TrimStart(), StringComparison.Ordinal);

        var end = strippedHandler.IndexOf('}');
        Assert.True(end > 0, "Lite's mute-rule catch block never closes");
        Assert.Contains("AppLogger.Warn(", handler[..end], StringComparison.Ordinal);
        Assert.Contains("stay in force", handler[..end], StringComparison.Ordinal);
    }

    /* ---------------- helpers ---------------- */

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal);
             at >= 0;
             at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// One named member's body, in both spellings at the same offsets.
    ///
    /// <para>Both are needed and for opposite reasons. A keyword scan has to read STRIPPED source or the
    /// paragraph explaining why a <c>catch</c> is gone satisfies a scan for one — every member this file
    /// reads now carries such a paragraph. A scan for a read name or a table name has to read RAW source,
    /// because those live in string literals and stripping blanks them. Returning one value carrying both
    /// keeps a call site from reaching for whichever it happens to have.</para>
    /// </summary>
    private static (string Raw, string Stripped) Member(string source, string member)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);

        var decl = Regex.Match(
            stripped,
            @"^[ \t]*(?:private|internal|public|protected)[^\r\n=]*?\b" + Regex.Escape(member) + @"\s*\(",
            RegexOptions.Multiline);

        Assert.True(decl.Success, $"{member} has no declaration — a rename has moved it out from under this pin");

        var open = stripped.IndexOf('{', decl.Index);
        Assert.True(open > 0, $"{member} has no block body");

        var body = CSharpSourceWalker.BraceBalanced(stripped, open);
        return (source[open..(open + body.Length)], body);
    }

    /// <summary>
    /// The innermost <c>try { … }</c> block containing <paramref name="offset"/>, or null when the offset
    /// sits in no try at all. Every try in the file is brace-balanced and the TIGHTEST enclosing one wins,
    /// rather than trusting the nearest preceding <c>try</c> token — a call in a sibling block after an
    /// unrelated try would otherwise read as guarded.
    /// </summary>
    private static (int Start, int End)? EnclosingTryBlock(string stripped, int offset)
    {
        (int Start, int End)? tightest = null;

        foreach (Match m in Regex.Matches(stripped, @"\btry\s*\{"))
        {
            var open = stripped.IndexOf('{', m.Index);
            var block = CSharpSourceWalker.BraceBalanced(stripped, open);
            var end = open + block.Length;

            if (offset <= open || offset >= end)
            {
                continue;
            }

            if (tightest is null || open > tightest.Value.Start)
            {
                tightest = (open, end);
            }
        }

        return tightest;
    }
}
