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
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>ViewerPostgresTabs.All</c> is the DECLARED intent for where a PostgreSQL panel lives — its
/// <c>Collectors</c> parameter is documented "every collector whose stored rows this tab renders" — and this
/// file is the comparison that was missing: the registry against the XAML that renders the panels, and the
/// registry against the load path that fills them (#3062).
///
/// <para><b>Three artifacts describe one fact, and only one pairing was pinned.</b>
/// <c>ViewerPostgresTabsTests</c> holds the registry to the catalog and to the XAML's tab STRIP — indices,
/// headers, order — but never to the panels inside a tab. <c>PgPanelTabOwnershipTests</c> holds the load path
/// to the XAML. Nothing consulted the registry about a panel, so when the load path and the XAML disagreed
/// there was no third opinion to say which of them was wrong, and three placement defects landed:
/// #3048 (the deadlock pair declared inside <c>PgVacuumTab</c> while the registry's Activity entry named
/// <c>pg_deadlocks</c>) and both of #3050's pairs (<c>pg_wait_sampling</c> registered on Waits and
/// <c>pg_predicate_stats</c> on Storage, both filled only from the Activity load path).</para>
///
/// <para><b>Why the registry is the side that decides.</b> A panel's home is a product decision — which
/// question this screen answers — and the registry is the only artifact that records it in those terms,
/// beside the prose explaining why those panels sit together. The XAML and the load path are both
/// implementations of that decision, so "they disagree" is not the finding; "neither of them is what was
/// declared" is. #3048 is what that costs: it was resolved through five circumstantial signals — the load
/// path, a doc comment in another file, subject matter, a spacer-row convention, a XAML comment — which
/// reached the right answer the long way while the authoritative source sat unread.</para>
///
/// <para><b>The class nothing else can see, measured rather than argued.</b>
/// <c>PgPanelTabOwnershipTests</c> reds on all three of those historical defects, because in each of them
/// the two implementations disagreed with EACH OTHER. Move the deadlock pair onto Vacuum in the XAML AND
/// move the <c>LoadPgDeadlocksAsync</c> call into the Vacuum load path, and it goes green: the two agree,
/// and they are both wrong. That mutation reds only the two rules here, naming <c>pg_deadlocks</c>, the tab
/// the registry gives it and the tab it moved to. A panel can be consistently in the wrong place, and
/// consistency is exactly what the other pin measures.</para>
///
/// <para><b>Collector to panel, and why not by name.</b> Each panel block is loaded by a
/// <c>LoadPg…Async</c> method that names its own collector — <c>PgCollectorIsGatedOff("pg_deadlocks")</c> to
/// skip a permanent gap, and <c>PanelNote("pg_deadlocks", …)</c> to print the reason for one — so the method
/// body is the link from a collector name to the controls that render its rows. Resolving by NAME instead
/// was measured and rejected: 8 of the 27 registry collectors have no control named after them at all, and
/// four notes are named for the tab or the subject rather than the collector (<c>PgWaitsNote</c> for
/// <c>pg_wait_stats</c>, <c>PgIoNote</c> for <c>pg_io_stats</c>, <c>PgReplicationNote</c> for
/// <c>pg_replication_slots</c>, <c>PgDatabasesNote</c> for <c>pg_database_stats</c>).</para>
///
/// <para><b>Both naming forms are read, because one of them covers only 21 of 27.</b>
/// <c>PgCollectorIsGatedOff</c> is called by 21 collectors' loaders; the other six —
/// <c>pg_autovacuum_stats</c>, <c>pg_database_stats</c>, <c>pg_io_stats</c>, <c>pg_replication_slots</c>,
/// <c>pg_wraparound_stats</c>, <c>pg_xmin_horizon</c> — issue their read unconditionally and name themselves
/// only through <c>PanelNote</c>. Reading just the gate call would have covered 21 and skipped six SILENTLY,
/// which is worse than it sounds: the skipped six include the entire Vacuum causal chain and both halves of
/// Replication. <c>PanelNote</c> is not a fallback but the better link of the two — every panel has one by
/// construction (<c>ViewerPostgresTabsTests.EveryPostgresPanel_ExplainsItsOwnEmptyState</c> already refuses
/// a panel without one), and its first argument is keyed on the same collector name the capability sentence
/// takes. Together they resolve 27 of 27, so
/// <see cref="EveryRegistryCollector_ResolvesToAPanel_OrIsNamedAsUnresolved"/> asserts the unresolved set is
/// EMPTY rather than carrying an exemption list — and it names whatever it could not resolve instead of
/// passing on the ones it could.</para>
///
/// <para><b>Ownership resolves to the OUTERMOST named <c>TabItem</c></b>, because Activity holds a sub-tab
/// control and a panel in its Blocking sub-tab still belongs to Activity. Read by parsing the XAML rather
/// than by line range or by the position of a <c>Header</c>: there are two <c>Header="Blocking"</c>
/// TabItems in the file, one SQL Server and one PostgreSQL.</para>
///
/// <para><b>Out of scope, deliberately.</b> This asserts WHICH tab, never in what order. The registry
/// documents its collector list as panel order and the Storage entry orders <c>pg_index_bloat</c> before
/// <c>pg_column_stats</c> while the XAML renders them the other way round; that is pre-existing, untested in
/// either direction, and widening this rule to order is a separate decision. Nothing here reads
/// <c>Grid.Row</c> either — <c>XamlGridRowRangeTests</c> owns the row rules.</para>
/// </summary>
public sealed class PgRegistryPanelPlacementTests
{
    /// <summary>The two forms a loader uses to name the collector whose rows its panels render.</summary>
    private static readonly Regex CollectorNamed = new(
        @"(?:PgCollectorIsGatedOff|PanelNote)\(\s*""(?<collector>pg_[a-z_]+)""", RegexOptions.Compiled);

    /// <summary>A control this method assigns. Same shape as <c>PgPanelTabOwnershipTests</c>' detector.</summary>
    private static readonly Regex ControlAssignment = new(
        @"\b(?<name>Pg[A-Za-z]+(?:Grid|Note|Expander))\s*(?:\.\w+)?\s*=(?!=)", RegexOptions.Compiled);

    private static readonly Regex LoaderCall = new(
        @"\b(?<name>LoadPg[A-Za-z]+Async)\b", RegexOptions.Compiled);

    private static readonly Regex DispatcherArm = new(
        @"case\s+(?<constant>Pg\w+InnerTabIndex):\s*\r?\n\s*await\s+(?<loader>LoadPg\w+Async)\(\);",
        RegexOptions.Compiled);

    private static readonly Regex IndexConstant = new(
        @"internal const int (?<constant>Pg\w+InnerTabIndex)\s*=\s*(?<index>\d+);", RegexOptions.Compiled);

    /// <summary>
    /// One registry entry, read out of <c>ViewerPostgresTabs.cs</c>: the index constant it occupies, its id,
    /// and the collectors it claims. Parsed rather than referenced so the whole rule runs against source
    /// files; the compiled registry's own shape is already pinned by <c>ViewerPostgresTabsTests</c>.
    /// </summary>
    private static readonly Regex RegistryEntry = new(
        @"new ViewerPostgresTab\(\s*ViewerServerTab\.(?<constant>Pg\w+InnerTabIndex),\s*""(?<id>\w+)"",\s*""(?<header>[^""]+)"",\s*new\[\]\s*\{(?<collectors>[^}]*)\}",
        RegexOptions.Compiled);

    /// <summary>
    /// The comparison this issue is about: a collector's panels are declared inside the tab the REGISTRY
    /// places it on. #3048 in one assertion — the deadlock note and grid sat in <c>PgVacuumTab</c> while the
    /// Activity entry had named <c>pg_deadlocks</c> all along.
    /// </summary>
    [Fact]
    public void EveryRegistryCollectorsPanels_AreDeclaredInsideTheTabTheRegistryPlacesItOn()
    {
        var chain = Read();
        var (offenders, compared) = DeclarationOffenders(chain, chain.PlacedOn);

        /* Floors first, because every assertion below is over a set this walk produced: an empty one would
           make the rule pass by having nothing to disagree about, which is the failure this file exists to
           prevent. Derived rather than a round number - each panel block is a note and a grid, so two
           panels per registry collector is the shape, not a guess. */
        Assert.True(compared >= chain.Collectors.Count * 2,
            $"Only {compared} collector/panel pairs were compared for {chain.Collectors.Count} registry "
            + "collectors. Every panel block is a note plus a grid, so the walk is finding less than the "
            + "shipped tree holds and the rule below is checking almost nothing.");

        Assert.True(offenders.Count == 0,
            "A PostgreSQL panel renders on a tab other than the one ViewerPostgresTabs.All places its "
            + "collector on. The registry is the DECLARED intent — its Collectors list is documented as "
            + "'every collector whose stored rows this tab renders' — so either move the declaration into "
            + "the tab the registry names, or change the registry and say why that tab is now the right "
            + "home:\n  " + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// The other half: a collector's panels are filled by the load path of the tab the REGISTRY places it
    /// on. Both of #3050's pairs in one assertion — <c>pg_wait_sampling</c> and <c>pg_predicate_stats</c>
    /// rendered on Waits and Storage exactly as registered, and were populated only once Activity had been
    /// visited.
    ///
    /// <para>Distinct from <c>PgPanelTabOwnershipTests</c>, which compares the load path against the XAML.
    /// That pin can only report that two artifacts disagree; this one reports which of them left the
    /// declared intent, which is the question a fix has to answer.</para>
    /// </summary>
    [Fact]
    public void EveryRegistryCollector_IsFilledByTheLoadPathOfTheTabTheRegistryPlacesItOn()
    {
        var chain = Read();

        Assert.True(chain.LoadPathOf.Count == chain.Registry.Count,
            $"{chain.LoadPathOf.Count} PostgreSQL dispatcher arms resolved to a tab for "
            + $"{chain.Registry.Count} registry entries; the tab-to-loader map is not being read, so this "
            + "rule would pass with nothing to check.");

        var offenders = LoadPathOffenders(chain, chain.PlacedOn);

        Assert.True(offenders.Count == 0,
            "A PostgreSQL panel is filled by the load path of a tab other than the one "
            + "ViewerPostgresTabs.All places its collector on, so it is blank until that other tab is "
            + "visited — and a blank panel reads as 'nothing to report' rather than 'nothing loaded this'. "
            + "Move the loader call onto the tab the registry names:\n  " + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// Which collectors the chain could not resolve to a panel at all, named rather than skipped.
    ///
    /// <para>This is the assertion that keeps the two rules above honest. Reading only
    /// <c>PgCollectorIsGatedOff</c> resolves 21 of the 27 registry collectors and skips six INVISIBLY, so a
    /// green run would have meant "21 are placed correctly and six were never asked" while reading like full
    /// coverage. Reading <c>PanelNote</c> as well resolves all 27, so the expected unresolved set is empty —
    /// and if a 28th collector arrives that neither form names, this names it instead of quietly dropping
    /// it.</para>
    /// </summary>
    [Fact]
    public void EveryRegistryCollector_ResolvesToAPanel_OrIsNamedAsUnresolved()
    {
        var chain = Read();

        Assert.True(chain.Registry.Count == 7,
            $"Parsed {chain.Registry.Count} entries out of ViewerPostgresTabs.All; the registry has seven "
            + "tabs and this file's parse no longer matches its shape.");
        Assert.True(chain.Collectors.Count >= 25,
            $"Parsed only {chain.Collectors.Count} collectors out of the registry, which is fewer than the "
            + "PostgreSQL surface ships. The parse is losing entries and every rule here is running on the "
            + "remainder.");
        Assert.True(chain.NamedBy.Count >= 15,
            $"Only {chain.NamedBy.Count} loader methods name a collector at all; ViewerServerTab.Postgres.cs "
            + "is not being read the way this file assumes.");

        /* Resolved means it reached a PANEL, not merely a method: a collector named inside a method that
           assigns no control would otherwise leave both rules with nothing to compare while this list
           stayed empty — the same invisible skip, one step further along. */
        var unresolved = chain.Collectors
            .Where(c => Panels(chain, c).Count == 0)
            .Select(c => Methods(chain, c).Count == 0
                ? $"{c} — no loader method names it"
                : $"{c} — named by {string.Join(", ", Methods(chain, c))}, which assigns no panel")
            .OrderBy(c => c, StringComparer.Ordinal)
            .ToList();

        Assert.True(unresolved.Count == 0,
            "These registry collectors reach no panel, so nothing here can say which controls render them "
            + "and their placement is NOT checked by either rule in this file. A collector's loader names it "
            + "through PgCollectorIsGatedOff or PanelNote and assigns the controls beside it; if one "
            + "genuinely cannot, the honest fix is a stated exemption in this message rather than a silent "
            + "skip:\n  " + string.Join("\n  ", unresolved));
    }

    /// <summary>
    /// A loader method may not name collectors that the registry places on two different tabs, because the
    /// panels it assigns can only be declared inside one of them. The attribution above is per METHOD — a
    /// method's collectors share its panels — so this is what keeps that coarseness sound: without it, adding
    /// a second tab's <c>PanelNote</c> into an existing loader would make both rules report a confusing
    /// mismatch on panels that never moved.
    /// </summary>
    [Fact]
    public void NoLoaderMethod_NamesCollectorsFromTwoDifferentRegistryTabs()
    {
        var chain = Read();

        var offenders = new List<string>();
        foreach (var (method, named) in chain.NamedBy.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var tabs = named
                .Where(chain.PlacedOn.ContainsKey)
                .Select(c => chain.PlacedOn[c])
                .Distinct(StringComparer.Ordinal)
                .OrderBy(t => t, StringComparer.Ordinal)
                .ToList();

            if (tabs.Count > 1)
            {
                offenders.Add($"{method} names collectors registered on {string.Join(" and ", tabs)} "
                              + $"({string.Join(", ", named.OrderBy(n => n, StringComparer.Ordinal))})");
            }
        }

        Assert.True(offenders.Count == 0,
            "A loader method names collectors that the registry places on different tabs. Its panels are "
            + "attributed to every collector it names, so which panel belongs to which collector is no "
            + "longer derivable and the two rules in this file would blame the wrong one. Split the method, "
            + "one tab's panels each:\n  " + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// The registry parse, read a second and dumber way out of the same file. Without this, a parse that
    /// silently lost an entry would take both rules with it and still report green — the exact shape of
    /// failure the floors above exist for, one level further in.
    /// </summary>
    [Fact]
    public void TheRegistryParse_AgreesWithASecondReadingOfTheSameFile()
    {
        var chain = Read();
        var source = Strip(ViewerFile("ViewerPostgresTabs.cs"));
        var initialiser = source[source.IndexOf("All = new[]", StringComparison.Ordinal)..];

        Assert.Equal(
            Regex.Matches(initialiser, @"new ViewerPostgresTab\(").Count,
            chain.Registry.Count);

        Assert.Equal(
            Regex.Matches(initialiser, @"""pg_[a-z_]+""").Select(m => m.Value.Trim('"')).Order(StringComparer.Ordinal).ToList(),
            chain.Collectors.Order(StringComparer.Ordinal).ToList());

        /* Every entry lands on a NAMED PostgreSQL TabItem. A registry index that resolved to an unnamed
           SQL Server tab would make both rules skip that entry rather than fail it. */
        foreach (var tab in chain.Registry)
        {
            Assert.StartsWith("Pg", chain.XamlTabAt[tab.InnerTabIndex], StringComparison.Ordinal);
        }

        Assert.Equal(chain.Registry.Count, chain.Registry.Select(t => t.XamlTabName).Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// Both detectors fire on a placement planted into the REAL tree, in the shape each historical defect
    /// had. Without this the rules above would pass on any tree where a walk quietly returned nothing, and
    /// an empty offender list would be the only evidence they ever ran.
    /// </summary>
    [Fact]
    public void TheDetectors_ReportAPlantedMisplacementInTheRealTree()
    {
        var chain = Read();

        /* #3048's shape: the registry says one tab, the XAML declares the panels in another. Move a
           resolved collector's registry placement onto a different tab and require it to be named. */
        var moved = chain.Collectors.First(c => Methods(chain, c).Count > 0);
        var elsewhere = chain.Registry.Select(t => t.XamlTabName).First(t => !string.Equals(t, chain.PlacedOn[moved], StringComparison.Ordinal));

        var planted = new Dictionary<string, string>(chain.PlacedOn, StringComparer.Ordinal) { [moved] = elsewhere };

        /* Measured as the DIFFERENCE against the shipped placement rather than as the whole list, so this
           self-test keeps saying something on a tree that already has a real offender: on a clean tree the
           shipped lists are empty and the two are the same claim, and on a broken one this still isolates
           what the plant added instead of drowning in what was already wrong. */
        var declaration = DeclarationOffenders(chain, planted).Offenders
            .Except(DeclarationOffenders(chain, chain.PlacedOn).Offenders, StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(declaration);
        Assert.All(declaration, o => Assert.Contains(moved, o, StringComparison.Ordinal));

        /* #3050's shape: the registry and the XAML agree, and some other tab's load path is what fills the
           panels. Same planted placement reaches it, because the load path still runs from the real tab. */
        var loadPath = LoadPathOffenders(chain, planted)
            .Except(LoadPathOffenders(chain, chain.PlacedOn), StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(loadPath);
        Assert.All(loadPath, o => Assert.Contains(moved, o, StringComparison.Ordinal));

        /* Whether the SHIPPED placement is clean is the two rules above, not this one. Asserting it here as
           well would make one misplaced panel red three tests and cost a reader the attribution. */
    }

    // ── The chain ────────────────────────────────────────────────────────────────────────────────

    private sealed record RegistryTab(string Constant, string Id, string Header, int InnerTabIndex, string XamlTabName, IReadOnlyList<string> Collectors);

    private sealed record PlacementChain(
        IReadOnlyList<RegistryTab> Registry,
        IReadOnlyDictionary<int, string> XamlTabAt,
        IReadOnlyDictionary<string, string> TabOf,
        IReadOnlyDictionary<string, IReadOnlySet<string>> NamedBy,
        IReadOnlyDictionary<string, IReadOnlySet<string>> PanelsOf,
        IReadOnlyDictionary<string, IReadOnlySet<string>> LoadPathOf)
    {
        /// <summary>Collector -> the XAML TabItem the registry places it on.</summary>
        public IReadOnlyDictionary<string, string> PlacedOn { get; } =
            Registry.SelectMany(t => t.Collectors.Select(c => (c, t.XamlTabName)))
                    .ToDictionary(p => p.c, p => p.XamlTabName, StringComparer.Ordinal);

        public IReadOnlyList<string> Collectors { get; } =
            Registry.SelectMany(t => t.Collectors).Distinct(StringComparer.Ordinal).ToList();
    }

    /// <summary>Every loader method that names <paramref name="collector"/>.</summary>
    private static List<string> Methods(PlacementChain chain, string collector) =>
        chain.NamedBy.Where(p => p.Value.Contains(collector))
             .Select(p => p.Key)
             .OrderBy(m => m, StringComparer.Ordinal)
             .ToList();

    /// <summary>Every panel assigned by a method that names <paramref name="collector"/>.</summary>
    private static List<string> Panels(PlacementChain chain, string collector) =>
        Methods(chain, collector)
            .SelectMany(m => chain.PanelsOf[m])
            .Distinct(StringComparer.Ordinal)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// Registry against the XAML, plus how many collector/panel pairs it actually compared — the number the
    /// floor is taken over, so a walk that found nothing cannot report a clean run.
    /// </summary>
    private static (List<string> Offenders, int Compared) DeclarationOffenders(
        PlacementChain chain, IReadOnlyDictionary<string, string> placement)
    {
        var offenders = new List<string>();
        var compared = 0;

        foreach (var collector in chain.Collectors.OrderBy(c => c, StringComparer.Ordinal))
        {
            if (!placement.TryGetValue(collector, out var registryTab))
            {
                continue;
            }

            foreach (var panel in Panels(chain, collector))
            {
                if (!chain.TabOf.TryGetValue(panel, out var declaredIn))
                {
                    continue; /* not declared in this XAML at all — another file's control, not this rule's business */
                }

                compared++;

                if (!string.Equals(declaredIn, registryTab, StringComparison.Ordinal))
                {
                    offenders.Add($"{collector} is registered on {registryTab} but its panel {panel} is "
                                  + $"declared inside {declaredIn} (resolved through "
                                  + $"{string.Join(", ", Methods(chain, collector))})");
                }
            }
        }

        return (offenders, compared);
    }

    /// <summary>Registry against the load path: which tabs' load paths reach the methods naming a collector.</summary>
    private static List<string> LoadPathOffenders(
        PlacementChain chain, IReadOnlyDictionary<string, string> placement)
    {
        var offenders = new List<string>();

        foreach (var collector in chain.Collectors.OrderBy(c => c, StringComparer.Ordinal))
        {
            if (!placement.TryGetValue(collector, out var registryTab))
            {
                continue;
            }

            var methods = Methods(chain, collector);
            if (methods.Count == 0)
            {
                continue; /* reported by EveryRegistryCollector_ResolvesToAPanel_OrIsNamedAsUnresolved */
            }

            var fillers = chain.LoadPathOf
                .Where(p => methods.Any(m => p.Value.Contains(m)))
                .Select(p => p.Key)
                .OrderBy(t => t, StringComparer.Ordinal)
                .ToList();

            if (fillers.Count == 1 && string.Equals(fillers[0], registryTab, StringComparison.Ordinal))
            {
                continue;
            }

            offenders.Add($"{collector} is registered on {registryTab} but its panels are filled by "
                          + (fillers.Count == 0
                              ? "no tab's load path at all"
                              : $"{string.Join(" and ", fillers)}")
                          + $" (resolved through {string.Join(", ", methods)})");
        }

        return offenders;
    }

    private static PlacementChain Read()
    {
        var x = XName.Get("Name", "http://schemas.microsoft.com/winfx/2006/xaml");
        var doc = XDocument.Load(ViewerPath("ViewerServerTab.xaml"));

        /* Parsed, never line ranges and never the position of a Header: two TabItems in this file read
           Header="Blocking" - the SQL Server tab and the PostgreSQL sub-tab - so indexing on the header
           picks whichever comes first. */
        var innerTabs = doc.Descendants().Single(e => e.Attribute(x)?.Value == "InnerTabs");
        var xamlTabAt = innerTabs.Elements()
            .Where(e => e.Name.LocalName == "TabItem")
            .Select((e, i) => (Index: i, Name: e.Attribute(x)?.Value ?? string.Empty))
            .ToDictionary(p => p.Index, p => p.Name);

        var tabOf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var element in doc.Descendants())
        {
            var name = element.Attribute(x)?.Value;
            if (name is null)
            {
                continue;
            }

            /* Outermost, not nearest: a panel in Activity's Blocking sub-tab belongs to Activity. */
            var owner = element.Ancestors()
                .Where(a => a.Name.LocalName == "TabItem" && a.Attribute(x)?.Value is not null)
                .Select(a => a.Attribute(x)!.Value)
                .LastOrDefault(t => t.StartsWith("Pg", StringComparison.Ordinal));

            if (owner is not null)
            {
                tabOf[name] = owner;
            }
        }

        var shell = ViewerFile("ViewerServerTab.xaml.cs");
        var indices = IndexConstant.Matches(shell)
            .ToDictionary(m => m.Groups["constant"].Value, m => int.Parse(m.Groups["index"].Value), StringComparer.Ordinal);

        var registry = new List<RegistryTab>();
        foreach (Match entry in RegistryEntry.Matches(Strip(ViewerFile("ViewerPostgresTabs.cs"))))
        {
            var constant = entry.Groups["constant"].Value;
            Assert.True(indices.ContainsKey(constant),
                $"The registry occupies {constant}, which ViewerServerTab.xaml.cs does not declare — the "
                + "index constants and the registry have drifted apart and nothing here can resolve a tab.");

            var index = indices[constant];
            Assert.True(xamlTabAt.ContainsKey(index),
                $"{constant} is {index}, past the last of the {xamlTabAt.Count} TabItems ViewerServerTab.xaml "
                + "declares inside InnerTabs, so the registry entry occupying it resolves to no tab at all.");
            Assert.False(string.IsNullOrEmpty(xamlTabAt[index]),
                $"The TabItem at index {index} ({constant}) carries no x:Name, so no panel can be resolved "
                + "to it and every collector on that registry entry would go unchecked.");

            registry.Add(new RegistryTab(
                constant,
                entry.Groups["id"].Value,
                entry.Groups["header"].Value,
                index,
                xamlTabAt[index],
                Regex.Matches(entry.Groups["collectors"].Value, @"""(?<name>pg_[a-z_]+)""")
                    .Select(m => m.Groups["name"].Value)
                    .ToList()));
        }

        var bodies = MethodBodies(Strip(ViewerFile("ViewerServerTab.Postgres.cs")));

        var namedBy = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        var panelsOf = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        foreach (var (method, body) in bodies)
        {
            var named = CollectorNamed.Matches(body).Select(m => m.Groups["collector"].Value).ToHashSet(StringComparer.Ordinal);
            if (named.Count > 0)
            {
                namedBy[method] = named;
            }

            panelsOf[method] = ControlAssignment.Matches(body).Select(m => m.Groups["name"].Value).ToHashSet(StringComparer.Ordinal);
        }

        var loadPathOf = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        foreach (Match arm in DispatcherArm.Matches(shell))
        {
            var constant = arm.Groups["constant"].Value;
            if (!indices.TryGetValue(constant, out var index)
                || !xamlTabAt.TryGetValue(index, out var tab)
                || !tab.StartsWith("Pg", StringComparison.Ordinal))
            {
                continue; /* a SQL Server arm — this file only speaks for the PostgreSQL run */
            }

            loadPathOf[tab] = Transitive(arm.Groups["loader"].Value, bodies, new HashSet<string>(StringComparer.Ordinal));
        }

        return new PlacementChain(registry, xamlTabAt, tabOf, namedBy, panelsOf, loadPathOf);
    }

    /// <summary>Every <c>LoadPg…Async</c> reachable from <paramref name="method"/>, itself included.</summary>
    private static IReadOnlySet<string> Transitive(string method, Dictionary<string, string> bodies, HashSet<string> seen)
    {
        var found = new HashSet<string>(StringComparer.Ordinal);
        if (!seen.Add(method) || !bodies.TryGetValue(method, out var body))
        {
            return found;
        }

        found.Add(method);

        foreach (Match m in LoaderCall.Matches(body))
        {
            var callee = m.Groups["name"].Value;
            if (callee != method)
            {
                found.UnionWith(Transitive(callee, bodies, seen));
            }
        }

        return found;
    }

    /// <summary>
    /// Comments out — doc, line and block — so prose naming a collector or a grid cannot read as a call or an
    /// assignment. The registry's own reasoning is block comments INSIDE the initialiser and several of them
    /// name collectors.
    /// </summary>
    private static string Strip(string source)
    {
        source = Regex.Replace(source, @"^[ \t]*///.*$", string.Empty, RegexOptions.Multiline);
        source = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(source, @"//.*$", string.Empty, RegexOptions.Multiline);
    }

    private static Dictionary<string, string> MethodBodies(string source)
    {
        var bodies = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(source, @"private\s+async\s+Task\s+(?<name>LoadPg[A-Za-z]+Async)\s*\("))
        {
            var depth = 0;
            var started = false;
            for (var i = m.Index; i < source.Length; i++)
            {
                if (source[i] == '{')
                {
                    depth++;
                    started = true;
                }
                else if (source[i] == '}')
                {
                    depth--;
                    if (started && depth == 0)
                    {
                        bodies[m.Groups["name"].Value] = source[m.Index..(i + 1)];
                        break;
                    }
                }
            }
        }

        return bodies;
    }

    /// <summary>Same <c>[CallerFilePath]</c> resolver <c>ViewerPostgresTabsTests</c> uses — no walk-up, so it
    /// cannot resolve to the wrong tree or fail to resolve at all.</summary>
    private static string ViewerFile(string fileName, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(ViewerPath(fileName, thisFile));

    private static string ViewerPath(string fileName, [CallerFilePath] string thisFile = "") =>
        Path.Combine(
            Path.GetDirectoryName(Path.GetDirectoryName(thisFile)!)!,
            "PerformanceMonitor.Darling.Viewer",
            fileName);
}
