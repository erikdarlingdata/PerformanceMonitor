using System.Linq;
using PerformanceMonitorDashboard.Services.Remediation;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Contract between the remediation BUILDERS (FactRemediation.Build*Action and the recommendations
/// reader) and the registered HANDLERS. Every Apply-able RemediationAction carries a FactKey, and
/// RemediationApplyService dispatches on it; a FactKey with no registered handler makes Apply
/// silently no-op — the "shipped half-built" failure class. These tests fail loudly if the handler
/// set drifts from the agreed Apply-able key set, forcing a conscious update whenever a builder or
/// handler is added or removed.
///
/// Scope note: this locks the HANDLER side (registry integrity + the documented Apply-able set).
/// The complementary dynamic check — feeding real detector drill-down output through each builder
/// and asserting it still yields a non-null, dispatchable action (the autogrowth-CTE-drift class) —
/// lives in <see cref="RemediationGoldenSampleTests"/> (the captured-fixture golden-sample).
/// </summary>
public class RemediationContractTests
{
    // The agreed Apply-able fact keys. Each is produced by a FactRemediation builder or the
    // recommendations reader: PLAN_REGRESSION, DB_CONFIG, FILE_AUTOGROWTH_PERCENT and SERVER_CONFIG
    // are always-safe; RCSI and CLEAR_PLAN are destructive (behind the informed-consent gate).
    // Adding or removing a builder/handler? Update this list deliberately — that is the contract.
    private static readonly string[] ApplyableFactKeys =
    {
        "PLAN_REGRESSION",
        "DB_CONFIG",
        "FILE_AUTOGROWTH_PERCENT",
        "SERVER_CONFIG",
        "RCSI",
        "CLEAR_PLAN",
    };

    [Fact]
    public void DefaultHandlers_HaveDistinctNonEmptyFactKeys()
    {
        var keys = RemediationApplyService.CreateDefaultHandlers().Select(h => h.FactKey).ToList();

        Assert.DoesNotContain(keys, string.IsNullOrWhiteSpace);
        Assert.Equal(keys.Count, keys.Distinct().Count());   // no two handlers claim the same key
    }

    [Fact]
    public void HandlerFactKeys_ExactlyMatch_ApplyableContract()
    {
        var handlerKeys = RemediationApplyService.CreateDefaultHandlers()
            .Select(h => h.FactKey)
            .ToHashSet();

        var orphanBuilderKeys = ApplyableFactKeys.Except(handlerKeys).ToList(); // builder key, no handler -> silent no-op
        var deadHandlerKeys = handlerKeys.Except(ApplyableFactKeys).ToList();    // handler, no producing builder -> dead

        Assert.True(orphanBuilderKeys.Count == 0,
            $"Apply-able fact key(s) with NO registered handler (Apply would silently no-op): {string.Join(", ", orphanBuilderKeys)}");
        Assert.True(deadHandlerKeys.Count == 0,
            $"Registered handler(s) with no Apply-able builder key (dead handler / contract drift): {string.Join(", ", deadHandlerKeys)}");
    }

    [Fact]
    public void EveryApplyableFactKey_ResolvesThroughTheRegistry()
    {
        // The registry is the actual dispatch point used by RunAsync; prove each contract key
        // resolves through it exactly as production constructs it.
        var registry = new RemediationHandlerRegistry(RemediationApplyService.CreateDefaultHandlers());

        foreach (var key in ApplyableFactKeys)
            Assert.True(registry.TryGet(key) is not null, $"No handler resolves for Apply-able fact key '{key}'.");
    }
}
