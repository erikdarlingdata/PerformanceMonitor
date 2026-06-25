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
    /// <summary>In-memory fake; InsertAsync optionally throws to exercise the error path.</summary>
    private sealed class FakeMuteRuleStore : IMuteRuleStore
    {
        private readonly bool _throwOnInsert;
        public List<MuteRule> Persisted { get; } = new();

        public FakeMuteRuleStore(bool throwOnInsert = false) => _throwOnInsert = throwOnInsert;

        public Task<IReadOnlyList<MuteRule>> LoadAllAsync()
            => Task.FromResult<IReadOnlyList<MuteRule>>(Persisted.ToList());

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
}
