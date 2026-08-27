/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2480: the starter DASHBOARD templates behind the Custom Views first-run hero.
///
/// <para><b>Why templates rather than seeded rows.</b> Seeding curated views as data would need a migration
/// rung, a <c>StorageVersion</c> bump, four pinned test files and a viewer probe sentinel — for content that is
/// not schema — and a seeded row resurrects itself on the next upgrade after the user deletes it, with no reset
/// path if an editing seat breaks one. A template is created only when someone asks for it, and what they get is
/// an ordinary row they own.</para>
///
/// <para><b>What that trades away, and what these pins buy back.</b> A stored definition is validated once, at
/// write time, and never again — so a definition whose <c>read</c> is later renamed becomes a permanently broken
/// panel that nothing greps. These templates are CODE, which is exactly why that is fixable: the same invariant
/// the built-in server page carries applies here, checked against the shipped dispatch and the shipped viz
/// vocabulary rather than a transcribed list.</para>
///
/// <para>Behaviour was verified separately (no JS test runner here): every template was rendered through the
/// shipped <c>renderPanel</c> under a DOM shim across two response shapes, and every definition was fed to the
/// service's own <c>ValidateDefinition</c> — the authority that would refuse the POST. See the PR.</para>
/// </summary>
public sealed class ViewTemplatesTests
{
    private static string TemplatesJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js"));

    private static string ViewsJs => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"));

    /// <summary>
    /// Every read a template names is one the service serves. A stored view's read is checked at write time and
    /// never again, so a template that outlived a renamed read would create dashboards that are broken the moment
    /// they are made — the failure would look like a user's mistake rather than ours.
    /// </summary>
    [Fact]
    public void EveryReadTheTemplatesName_ExistsInTheDispatch()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);
        var named = Regex.Matches(TemplatesJs, "\"(get_[a-z0-9_]+|audit_config|list_servers|compare_analysis)\"")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(named);
        var unknown = named.Where(n => !dispatch.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.True(unknown.Length == 0, "view-templates.js names reads the service does not serve: " + string.Join(", ", unknown));
    }

    /// <summary>
    /// Every parameter key a template sends is one its read binds. Unlike an unknown READ (which the server
    /// refuses at write time), an unknown query KEY is silently ignored: a template asking <c>limit</c> of a read
    /// that binds <c>top</c> saves fine, renders fine, and quietly returns the read's default row count forever.
    /// </summary>
    [Fact]
    public void EveryParameterKeyTheTemplatesSend_IsOneItsReadBinds()
    {
        var problems = new List<string>();

        foreach (Match m in Regex.Matches(TemplatesJs, "read: \"([a-z0-9_]+)\",\\s*params: \\{([^{}]*)\\}", RegexOptions.Singleline))
        {
            var read = m.Groups[1].Value;
            if (!DarlingWebEndpoints.CatalogDescriptors.TryGetValue(read, out var descriptor)) continue;

            var allowed = descriptor.Params.Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
            if (allowed.Contains("hours")) allowed.Add("hours_back");
            if (allowed.Contains("server")) allowed.Add("server_name");

            var keys = Regex.Matches(m.Groups[2].Value, @"(?:^|[,{]\s*)([a-z_][a-z0-9_]*)\s*(?::|,|\})")
                .Select(k => k.Groups[1].Value)
                .Distinct(StringComparer.Ordinal);

            problems.AddRange(keys.Where(k => !allowed.Contains(k))
                .Select(k => $"{read} is sent '{k}' but binds only [{string.Join(", ", allowed.OrderBy(a => a, StringComparer.Ordinal))}]"));
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
    }

    /// <summary>
    /// The structural rules the server's own <c>ValidateDefinition</c> enforces, checked here so a template that
    /// would be REFUSED never ships. A refusal is not a soft failure: the hero's create button is the first thing
    /// a new user clicks, and a 400 there is the worst possible first impression of the feature.
    /// </summary>
    [Fact]
    public void EveryTemplatePanel_IsStorable()
    {
        var js = TemplatesJs;

        /* Raw path mode is never storable — a definition must stay on the read allowlist. */
        Assert.DoesNotContain("path:", js, StringComparison.Ordinal);

        /* span is 1 or 2, and nothing else. */
        foreach (Match m in Regex.Matches(js, @"span: (\d+)"))
        {
            Assert.Contains(m.Groups[1].Value, new[] { "1", "2" });
        }

        /* Every viz is in the shipped vocabulary the validator checks against. */
        var vocabulary = DarlingWebEndpoints.KnownVizList.ToHashSet(StringComparer.Ordinal);
        var named = Regex.Matches(js, "viz: \"([a-z]+)\"").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.NotEmpty(named);
        Assert.All(named, v => Assert.Contains(v, vocabulary));

        /* Every panel names a read (the validator's other accepted form, `source`, is the v2 composed panel;
           these are deliberately v1 read panels, whose params the pin above can check). */
        var reads = Regex.Matches(js, "read: \"").Count;
        var vizzes = Regex.Matches(js, "viz: \"").Count;
        Assert.Equal(reads, vizzes);
        Assert.DoesNotContain("source: \"", js, StringComparison.Ordinal);

        /* No series color at all, so the validator's #rrggbb guard has nothing to refuse — the chart's own
           palette colors them, which is the same choice the built-in pages make. */
        Assert.DoesNotContain("color:", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every template is a distinct, complete offer. A duplicate key would make one unreachable through
    /// <c>findTemplate</c>, and a missing description is a card in the menu that cannot say what it is for.
    /// </summary>
    [Fact]
    public void EveryTemplate_HasAUniqueKey_AndSaysWhatItIsFor()
    {
        var js = TemplatesJs;
        var keys = Regex.Matches(js, "^    key: \"([a-z-]+)\",$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.True(keys.Length >= 5, "expected the full template set; found " + keys.Length);
        Assert.Equal(keys.Length, keys.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(keys.Length, Regex.Matches(js, "^    label: \"", RegexOptions.Multiline).Count);
        Assert.Equal(keys.Length, Regex.Matches(js, "^    description:", RegexOptions.Multiline).Count);

        /* Each make() takes the server and puts it in the NAME, so two servers' copies of one template do not
           collide on the unique-name constraint — which would turn the second click into a 409. */
        Assert.Equal(keys.Length, Regex.Matches(js, @"make: \(server\) => \(\{").Count);
        Assert.Equal(keys.Length, Regex.Matches(js, @"name: "".* — "" \+ server,").Count);
    }

    /// <summary>
    /// The hero's starting points DO something.
    ///
    /// <para>They were three hardcoded strings rendered as spans — "Top waits by server", "CPU trend over time",
    /// "Slowest procedures by database" — with no handler and no href, on the one page a new user arrives at with
    /// nothing. That is the #2437 defect shape: a promise rendered as a caption. This pins that they are gone,
    /// that what replaced them is the template list itself rather than a second hardcoded copy of it, and that
    /// clicking one creates it.</para>
    /// </summary>
    [Fact]
    public void TheHerosStartingPoints_AreTheTemplates_AndCreatingOneWorks()
    {
        var js = ViewsJs;

        /* The three strings themselves are still in the file, quoted in the comment that records what they were
           and why they were a defect — so this pins the CODE that rendered them, not their text. */
        Assert.DoesNotContain("const suggestions =", js, StringComparison.Ordinal);
        Assert.DoesNotContain("suggestions.map((s) =>", js, StringComparison.Ordinal);
        Assert.DoesNotContain("class: \"hero-chip\", text: s", js, StringComparison.Ordinal);

        /* Rendered FROM the registry, so a template added to view-templates.js appears in both affordances with
           no second list to keep in step. */
        Assert.Contains("DASHBOARD_TEMPLATES.map((t) =>", js, StringComparison.Ordinal);
        Assert.Contains("for (const t of DASHBOARD_TEMPLATES)", js, StringComparison.Ordinal);
        Assert.Contains("onClick: () => createFromTemplate(t, select.value, status)", js, StringComparison.Ordinal);
        Assert.Contains("const res = await api.createView(template.make(server));", js, StringComparison.Ordinal);
        Assert.Contains("location.hash = \"#/view/\" + encodeURIComponent(res.data.id);", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// The empty-fleet and failure states are stated, not blank.
    ///
    /// <para>A dashboard template is scoped to one server, so on a fleet with none there is nothing to offer —
    /// and a picker that silently renders zero options is the same dead end in a new costume. The create failure
    /// is surfaced verbatim because the backend re-validates as the authority: flattening its message to "could
    /// not create" would hide the one sentence that says which panel is wrong.</para>
    /// </summary>
    [Fact]
    public void TheTemplateAffordances_DegradeOutLoud()
    {
        var js = ViewsJs;

        Assert.Contains("The ready-made dashboards are scoped to a server, so they appear once the fleet has one.", js, StringComparison.Ordinal);
        Assert.Contains("Scoped to a server", js, StringComparison.Ordinal);
        Assert.Contains("mount(status, errorStrip(\"Pick a server first.\"));", js, StringComparison.Ordinal);
        Assert.Contains("res.message || \"Could not create this dashboard.\"", js, StringComparison.Ordinal);

        /* A 409 on a second click of the same template is the likely failure, and "Request failed" would send
           the reader looking for a bug instead of the view they already made. */
        Assert.Contains("res.status === 409", js, StringComparison.Ordinal);

        /* The fleet read is allowed to fail without taking the page down — every caller handles []. */
        Assert.Contains("if (res.kind !== \"data\" || !res.data) return [];", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every table and chart panel in every template says why it could be empty.
    ///
    /// <para>This matters more here than anywhere else in the product. A starter dashboard is the first screen a
    /// UAT tester opens, on the store with the least data it will ever have — a fresh install has collected
    /// nothing for its first cycles — and a wall of unexplained blank rectangles on day one is a worse first
    /// impression than the feature not existing. The chart case is the one that bites without looking like it:
    /// <c>get_blocking_trend</c> and <c>get_deadlock_trend</c> USED to answer an idle server with <c>trend: []</c>
    /// and NO <c>{status,message}</c> envelope, so the panel had real data in hand and fell through to the chart's
    /// "Not enough data points to chart yet" — a warming-up message on a server that simply never blocked. Those
    /// two now answer with an envelope (#2485), which renderPanel renders instead; the requirement holds for
    /// every other chart in the templates, none of which has one.</para>
    ///
    /// <para>A COUNT rather than a spot-check, and exact rather than a floor: <c>emptyText</c> appears nowhere
    /// else in this module, so one dropped sentence is one panel that will render blank in front of a new
    /// user.</para>
    /// </summary>
    [Fact]
    public void EveryTemplateDataPanel_ExplainsItsOwnEmptyState()
    {
        var js = TemplatesJs;

        var dataPanels = Regex.Matches(js, "viz: \"(table|line)\"").Count;
        var sentences = Regex.Matches(js, "emptyText: \"").Count;

        Assert.True(dataPanels >= 15, "expected the full template panel set; found " + dataPanels);
        Assert.Equal(dataPanels, sentences);

        /* The two reads whose EMPTY ARRAY means "it did not happen" say exactly that, rather than inheriting a
           sentence about collection. Absence and non-occurrence are the distinction this whole codebase keeps
           closing, and a starter dashboard is the worst place to blur it. */
        Assert.Contains("an empty trend here means none happened, not that nothing was collected", js, StringComparison.Ordinal);

        /* And the two reads that CANNOT be honest on day one are deliberately absent, not merely unused: the
           analysis pass writes nothing for 24 hours, and a target with Query Store off has nothing ever. */
        Assert.DoesNotContain("get_analysis_findings", js, StringComparison.Ordinal);
        Assert.DoesNotContain("get_query_store_top", js, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
