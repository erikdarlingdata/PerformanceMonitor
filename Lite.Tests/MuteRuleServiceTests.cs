using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// E3b ordering invariant for the now-shared <see cref="MuteRuleService"/>: the
/// service persists through <see cref="IMuteRuleStore"/> FIRST and only mutates
/// its in-memory cache if the persist succeeded (persist-then-cache). On a persist
/// failure the rule must NOT linger in the cache. This is the deliberate behaviour
/// change for Dashboard (§4.2), which previously mutated the cache before saving.
/// </summary>
public class MuteRuleServiceTests
{
    /// <summary>In-memory fake; the persist and the READ each optionally throw to exercise the two error
    /// paths, which are opposite by design — a failed persist must not enter the cache, and a failed read
    /// must not empty it.</summary>
    private sealed class FakeMuteRuleStore : IMuteRuleStore
    {
        private readonly bool _throwOnInsert;
        public List<MuteRule> Persisted { get; } = new();

        /// <summary>#3354: when set, the READ faults the way a store blip makes it fault.</summary>
        public bool ThrowOnLoad { get; set; }

        public FakeMuteRuleStore(bool throwOnInsert = false) => _throwOnInsert = throwOnInsert;

        public Task<IReadOnlyList<MuteRule>> LoadAllAsync()
        {
            if (ThrowOnLoad) throw new InvalidOperationException("store read boom");
            return Task.FromResult<IReadOnlyList<MuteRule>>(Persisted.ToList());
        }

        public Task InsertAsync(MuteRule rule)
        {
            if (_throwOnInsert) throw new InvalidOperationException("persist boom");
            Persisted.Add(rule);
            return Task.CompletedTask;
        }

        public Task UpdateAsync(MuteRule rule) => Task.CompletedTask;
        public Task SetEnabledAsync(string ruleId, bool enabled) => Task.CompletedTask;
        public Task DeleteAsync(string ruleId) => Task.CompletedTask;
        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds) => Task.CompletedTask;
    }

    private static MuteRule NewRule(string id) => new()
    {
        Id = id,
        Enabled = true,
        CreatedAtUtc = DateTime.UtcNow,
        ServerName = "Srv",
        MetricName = "CPU"
    };

    [Fact]
    public async Task AddRuleAsync_PersistSucceeds_RuleEntersCache()
    {
        var store = new FakeMuteRuleStore();
        var service = new MuteRuleService(store, new AppLoggerAdapter<MuteRuleService>());

        await service.AddRuleAsync(NewRule("rule-1"));

        Assert.Single(store.Persisted);
        Assert.Contains(service.GetRules(), r => r.Id == "rule-1");
    }

    [Fact]
    public async Task AddRuleAsync_PersistFails_RuleDoesNotLingerInCache()
    {
        var store = new FakeMuteRuleStore(throwOnInsert: true);
        var service = new MuteRuleService(store, new AppLoggerAdapter<MuteRuleService>());

        /* AddRuleAsync swallows the persist failure (logs + early-returns) — it must NOT throw. */
        await service.AddRuleAsync(NewRule("rule-1"));

        /* Persist-then-cache: nothing persisted, and the cache must be empty too
           (the deliberate Dashboard behaviour change in §4.2). */
        Assert.Empty(store.Persisted);
        Assert.Empty(service.GetRules());
    }

    /// <summary>
    /// #3354, asserted on THIS SKU as well: a failed reload must not reduce the set of rules in force.
    /// Lite's store used to swallow the read fault and return an empty list with no log line at all, so
    /// Lite lost every mute on a DuckDB blip and left no artefact of having done so. The invariant lives in
    /// the shared service — the read is awaited before the cache assignment and nothing catches — and this
    /// is the arm that says Lite's compilation of it behaves the same way Darling's does.
    /// </summary>
    [Fact]
    public async Task LoadAsync_ReadFails_TheRulesAlreadyInForceStayInForce()
    {
        var store = new FakeMuteRuleStore();
        var service = new MuteRuleService(store, new AppLoggerAdapter<MuteRuleService>());

        await service.AddRuleAsync(NewRule("rule-1"));
        await service.LoadAsync();
        Assert.Single(service.GetRules());

        store.ThrowOnLoad = true;
        await Assert.ThrowsAsync<InvalidOperationException>(service.LoadAsync);

        Assert.Single(service.GetRules());
        Assert.Contains(service.GetRules(), r => r.Id == "rule-1");
    }

    /// <summary>
    /// The arm that keeps the one above from being satisfied by a cache that never updates: a SUCCESSFUL
    /// empty read is an operator deleting their last rule, and it still empties the cache.
    /// </summary>
    [Fact]
    public async Task LoadAsync_ReadSucceedsAndIsEmpty_TheCacheIsEmptied()
    {
        var store = new FakeMuteRuleStore();
        var service = new MuteRuleService(store, new AppLoggerAdapter<MuteRuleService>());

        await service.AddRuleAsync(NewRule("rule-1"));
        Assert.Single(service.GetRules());

        store.Persisted.Clear();
        await service.LoadAsync();

        Assert.Empty(service.GetRules());
    }
}
