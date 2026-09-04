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
using System.Reflection;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <b>The family invariant behind #2357's dropped knob:</b> every scalar on <see cref="StoreConfigView"/> that
/// has a same-named, same-typed property on <see cref="DarlingConfig"/> must actually be propagated by
/// <see cref="StoreConfigProvider.ApplyToConfig"/>.
///
/// <para>This is a category pin, not an instance one. <c>ComposeStatementTimeoutSeconds</c> was seeded to the
/// store, read back into the view, clamped, given a doc comment promising the store was authoritative — and
/// then never assigned, so the held config stayed pinned to the darling.json value for the life of the
/// process while the store said something else. Six siblings on the same code path were assigned. Nothing
/// would have caught the seventh, and nothing would catch the eighth, because every existing test names its
/// own knob. Derived from the real types so it covers knobs that do not exist yet -- see
/// <see cref="ScalarViewProperties"/> for exactly what "scalar" admits, and note that a type
/// <see cref="ProbeValues"/> cannot probe fails the build instead of dropping out quietly.</para>
///
/// <para><b>Scope.</b> Propagation only. It says the held <see cref="DarlingConfig"/> tells the truth about the
/// store after a reload; it says nothing about whether a given knob's CONSUMER re-reads the held value.
/// <c>ComposeStatementTimeoutSeconds</c> is precisely the case where it does not — the effective timeout is
/// baked into <c>ALTER ROLE ... SET statement_timeout</c> by startup provisioning, which reads the store column
/// directly, so the operator's change lands on the next service start. Restart-scoped delivery is #2357's
/// design; a permanently stale held value was not.</para>
/// </summary>
public sealed class StoreConfigViewPropagationTests
{
    /// <summary>
    /// The scalars deliberately NOT name-matched to a top-level <see cref="DarlingConfig"/> property, each with
    /// the reason it is out of scope. Pinned as a set so a NEW view scalar cannot land here unnoticed: it either
    /// matches a config property (and the propagation test below covers it automatically) or it fails this
    /// assertion until someone writes down why it does not belong.
    /// </summary>
    private static readonly Dictionary<string, string> ExpectedUnmatched = new(StringComparer.Ordinal)
    {
        ["ConfigVersion"] = "the reload baseline; the worker holds it in _lastConfigVersion, not the config",
        ["Paused"] = "the worker holds it in _paused; the collection loop reads that, not the config",
        ["McpEnabled"] = "nested as config.Mcp.Enabled -- assigned, just not name-matched at the top level",
        ["McpPort"] = "nested as config.Mcp.Port",
        ["WebEnabled"] = "nested as config.Web.Enabled",
        ["WebPort"] = "nested as config.Web.Port",
    };

    /// <summary>
    /// The siblings that were already propagated when the gap was found. Asserting they are still MATCHED is
    /// what stops this whole class from passing vacuously: a reflection pin whose match set silently empties
    /// -- a rename, a type change, a move into a nested section -- would otherwise go green having tested
    /// nothing, which is worse than not having the pin at all.
    /// </summary>
    private static readonly string[] KnownMatched =
    {
        "CapturePlans",
        "ComposeStatementTimeoutSeconds",
        "MaxConcurrentSweeps",
        "PlanContentRetentionDays",
        "PlanXmlCompression",
        "QueryStoreBackfillEnabled",
        "QueryStoreTextBudgetMb",
    };

    [Fact]
    public void EveryMatchedScalar_IsPropagatedByApplyToConfig()
    {
        var pairs = MatchedPairs();

        Assert.Equal(
            KnownMatched.OrderBy(n => n, StringComparer.Ordinal),
            pairs.Select(p => p.View.Name).OrderBy(n => n, StringComparer.Ordinal));

        var dropped = new List<string>();

        foreach (var (viewProperty, configProperty) in pairs)
        {
            var probes = ProbeValues(viewProperty.PropertyType, viewProperty.Name);

            /* Two probes per property, each pre-setting the held config to the OTHER probe value. A single
               probe would pass against an assignment hardcoded to that value, and pre-setting matters because
               several of these defaults are already equal between the two types -- assigning nothing would
               look identical to assigning correctly. */
            for (var i = 0; i < probes.Length; i++)
            {
                var expected = probes[i];
                var config = new DarlingConfig();
                var view = new StoreConfigView();

                viewProperty.SetValue(view, expected);
                configProperty.SetValue(config, probes[(i + 1) % probes.Length]);

                Assert.Equal(expected, viewProperty.GetValue(view)); // the init-accessor write took

                StoreConfigProvider.ApplyToConfig(config, view);

                var actual = configProperty.GetValue(config);
                if (!Equals(expected, actual))
                {
                    dropped.Add($"{viewProperty.Name}: view={expected}, config={actual}");
                }
            }
        }

        Assert.True(
            dropped.Count == 0,
            "StoreConfigProvider.ApplyToConfig does not propagate every store-backed scalar. A knob seeded to "
                + "config_service and read back into StoreConfigView but never assigned here leaves the held "
                + "DarlingConfig reporting the darling.json value for the life of the process, with nothing "
                + "indicating why. Add the assignment in ApplyToConfig. Dropped: "
                + string.Join("; ", dropped));
    }

    [Fact]
    public void TheUnmatchedScalars_AreExactlyTheOnesWithAWrittenReason()
    {
        var unmatched = ScalarViewProperties()
            .Where(p => MatchingConfigProperty(p) is null)
            .Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(ExpectedUnmatched.Keys.OrderBy(n => n, StringComparer.Ordinal), unmatched);
    }

    /// <summary>
    /// <b>The pin's own machinery, covered.</b> No <see cref="StoreConfigView"/> scalar is an enum today,
    /// so the enum arm and the unprobeable-type throw would otherwise ship untested and could rot without
    /// anything noticing -- and they are the two things standing between a future non-<c>int</c> knob and
    /// the silent exclusion this class exists to prevent.
    /// </summary>
    [Fact]
    public void ProbeValues_CoversEnums_AndRefusesWhatItCannotProbe()
    {
        var probes = ProbeValues(typeof(SyntheticKnobMode), nameof(SyntheticKnobMode));
        Assert.Equal(2, probes.Length);
        Assert.NotEqual(probes[0], probes[1]);

        /* A one-member enum cannot distinguish an assignment from an unchanged default. */
        Assert.Throws<InvalidOperationException>(
            () => ProbeValues(typeof(SyntheticSingleMode), nameof(SyntheticSingleMode)));

        /* And a value type with no probe values fails the build rather than dropping out of the family. */
        Assert.Throws<InvalidOperationException>(() => ProbeValues(typeof(Guid), "Synthetic"));

        /* The hole this replaced: every one of these is a value type that IsPrimitive rejects, so the
           original IsPrimitive||string filter would have hidden such a knob from BOTH tests. */
        foreach (var type in new[]
            { typeof(SyntheticKnobMode), typeof(decimal), typeof(DateTime), typeof(Guid), typeof(TimeSpan) })
        {
            Assert.False(type.IsPrimitive, $"{type.Name} is not IsPrimitive -- that was the hole");
            Assert.True(type.IsValueType, $"{type.Name} must be admitted by the widened filter");
        }
    }

    private enum SyntheticKnobMode { First, Second }

    private enum SyntheticSingleMode { Only }

    /* ---------------- reflection over the real types ---------------- */

    /// <summary>
    /// A view "scalar" is any value type or <c>string</c> — NOT <c>IsPrimitive || string</c>, which was the
    /// filter this class shipped with and a hole in it. <c>IsPrimitive</c> is false for <c>enum</c>,
    /// <c>decimal</c>, <c>DateTime</c>, <c>Guid</c> and <c>TimeSpan</c>, so a future knob of any of those
    /// types would have been invisible to BOTH tests: unprobed for propagation, and absent from the
    /// unmatched set that is supposed to force a written reason. <see cref="ProbeValues"/>'s throw could
    /// never have fired for it either, since that only sees types this filter already admitted. Widening
    /// here is what makes the throw load-bearing.
    ///
    /// <para>Everything excluded is a reference type: the four sub-config objects
    /// (<c>Alerts</c>/<c>Analysis</c>/<c>Smtp</c>/<c>Webhooks</c>, swapped wholesale by reference) and the
    /// two collections. A nullable value type would be admitted and would throw in
    /// <see cref="ProbeValues"/> until someone gives it probe values, which is the intended outcome.</para>
    /// </summary>
    private static IEnumerable<PropertyInfo> ScalarViewProperties() =>
        typeof(StoreConfigView)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType.IsValueType || p.PropertyType == typeof(string))
            .OrderBy(p => p.Name, StringComparer.Ordinal);

    private static PropertyInfo? MatchingConfigProperty(PropertyInfo viewProperty)
    {
        var candidate = typeof(DarlingConfig).GetProperty(
            viewProperty.Name, BindingFlags.Public | BindingFlags.Instance);

        return candidate is not null
            && candidate.PropertyType == viewProperty.PropertyType
            && candidate.GetSetMethod() is not null
                ? candidate
                : null;
    }

    private static (PropertyInfo View, PropertyInfo Config)[] MatchedPairs() =>
        ScalarViewProperties()
            .Select(p => (View: p, Config: MatchingConfigProperty(p)))
            .Where(p => p.Config is not null)
            .Select(p => (p.View, Config: p.Config!))
            .ToArray();

    /// <summary>
    /// Two distinct values per scalar type. An unhandled type THROWS rather than skipping: a knob this pin
    /// cannot probe must break the build, not quietly drop out of the family it exists to guard.
    /// </summary>
    private static object[] ProbeValues(Type type, string name)
    {
        if (type == typeof(bool)) return new object[] { true, false };
        if (type == typeof(int)) return new object[] { 137, 268 };
        if (type == typeof(long)) return new object[] { 137L, 268L };
        if (type == typeof(double)) return new object[] { 1.25d, 0.75d };
        if (type == typeof(string)) return new object[] { "probe-alpha", "probe-beta" };
        if (type == typeof(decimal)) return new object[] { 13.7m, 26.8m };

        /* Enums are handled generically rather than left to throw: a store-backed knob is far more likely
           to arrive as an enum than as a Guid, and two DISTINCT declared members are all the probe needs.
           A single-member enum cannot distinguish "assigned" from "left at the default", so it throws
           rather than passing vacuously. */
        if (type.IsEnum)
        {
            var members = Enum.GetValues(type).Cast<object>().Distinct().Take(2).ToArray();
            if (members.Length == 2)
            {
                return members;
            }

            throw new InvalidOperationException(
                $"StoreConfigView.{name} is the enum {type.Name}, which declares fewer than two distinct "
                    + "members. This pin cannot tell an assignment from an unchanged default with one "
                    + "value; give the enum a second member or probe the property by hand.");
        }

        throw new InvalidOperationException(
            $"StoreConfigView.{name} is a {type.Name}, which this pin cannot probe. Add two distinct probe "
                + "values for that type here rather than excluding the property.");
    }
}
