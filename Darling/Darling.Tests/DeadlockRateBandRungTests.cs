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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V120 / #3368: the deadlock health band's warn/critical tiers move into
/// <c>config.config_alert_settings</c>, in deadlocks per HOUR.
///
/// <para>They were the hardcoded <c>count &gt; 0</c> ladder — no threshold at all, so the number an
/// operator most needs per workload was one no surface exposed. #3297 is the precedent: a threshold whose
/// right value differs per workload does not belong in a compile-time constant.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="RetentionHoldRatioKnobRungTests"/> (V119) when this rung landed — a fully-migrated store must
/// map to EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually
/// current.</para>
/// </summary>
public sealed class DeadlockRateBandRungTests
{
    private const int RungVersion = 120;
    private const int PreviousVersion = 119;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 95;

    private const string WarnColumn = "deadlock_warn_per_hour";
    private const string CriticalColumn = "deadlock_critical_per_hour";

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "deadlock-rate-band-knobs",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds both columns to the singleton settings row, schema-qualified, with the shipped
    /// constants as their defaults.
    ///
    /// <para>The DEFAULTS are compared against the constants rather than against literals, which is the half
    /// that matters: the C# seed, the viewer row, the Restore-Defaults button and the band's own fallback
    /// all name the same two constants, so if the rung text alone held a literal a moved default would
    /// leave the column behind and an upgraded store would band at a threshold no surface reports.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsBothColumns_SchemaQualified_WithTheShippedDefaults()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* config-qualified: the migrate session's search_path puts collect FIRST, so a bare ALTER TABLE
           would look for the wrong relation. */
        Assert.Contains("ALTER TABLE config.config_alert_settings", rung, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(rung, "ALTER TABLE config.config_alert_settings"));

        /* IF NOT EXISTS on both, so re-running the ladder over a store that already has them is a no-op
           rather than a 42701 that aborts the whole migration. */
        Assert.Equal(2, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));

        Assert.Contains($"{WarnColumn} double precision NOT NULL DEFAULT", rung, StringComparison.Ordinal);
        Assert.Contains($"{CriticalColumn} double precision NOT NULL DEFAULT", rung, StringComparison.Ordinal);

        Assert.Contains(
            "DEFAULT " + ServerHealthThresholds.DeadlockWarnPerHourDefault.ToString("0.0", CultureInfo.InvariantCulture),
            rung, StringComparison.Ordinal);
        Assert.Contains(
            "DEFAULT " + ServerHealthThresholds.DeadlockCriticalPerHourDefault.ToString("0.0", CultureInfo.InvariantCulture),
            rung, StringComparison.Ordinal);

        /* No reload beacon of its own: config_alert_settings already carries V17's statement-level
           trg_bump_alert_settings, so a second trigger here would be a duplicate bump per write. */
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);

        /* And no per-table GRANT: this table carries table-level grants with no column carve, which is what
           every earlier knob rung on it says. A grant appearing here would mean the carve had changed and
           this rung had quietly taken on an ACL decision. */
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains($"column_name = '{WarnColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasDeadlockRateBandKnobs", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. */
        Assert.Equal(ProbeOrdinal, arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly this version. Built by
           reflection so the arity tracks the signature. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel EXCEPT this one reports the previous top rung. */
        var behind = Enumerable.Repeat((object)true, arity).ToArray();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }

    /// <summary>
    /// EVERY surface that reads or writes the settings row names BOTH new columns — and that is the defect
    /// class this rung is most exposed to, not the DDL. Every one of these lists drives ordinals or
    /// parameter positions, so a column added to one and not the others re-maps reads and writes at once.
    /// </summary>
    [Fact]
    public void EverySettingsRowSurfaceNamesBothColumns()
    {
        var surfaces = new (string What, string Text)[]
        {
            ("service seed + read", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs")),
            ("viewer select + upsert", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs")),
            ("mcp read", DarlingAlertReader.AlertSettingsSelectSql),
            ("mcp report + accept", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains(WarnColumn, text, StringComparison.Ordinal);
            Assert.Contains(CriticalColumn, text, StringComparison.Ordinal);
        }

        /* The service must READ them, not merely select them — ApplyToConfig replaces config.Alerts
           wholesale, so a selected-but-unread column resets the tier to the shipped default on every worker
           start. Both halves, because one of the two being read is what an off-by-one produces. */
        var service = surfaces.Single(s => s.What == "service seed + read").Text;
        Assert.Contains("DeadlockWarnPerHour = reader.GetDouble(", service, StringComparison.Ordinal);
        Assert.Contains("DeadlockCriticalPerHour = reader.GetDouble(", service, StringComparison.Ordinal);

        /* And the viewer's upsert must WRITE them, or Save silently drops whatever the boxes held. */
        Assert.Contains($"{WarnColumn} = EXCLUDED.{WarnColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains($"{CriticalColumn} = EXCLUDED.{CriticalColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP's ACCEPTED bound equals the band's CLAMP, held STRUCTURALLY — both sites name the same two
    /// constants rather than carrying matching literals.
    ///
    /// <para>The parity is why this matters and it is already pinned three times in this repo: a wider
    /// bound in the tool lets <c>update_alert_settings</c> ACCEPT a value the band then silently rewrites,
    /// which presents as the setting not sticking with nothing saying no.</para>
    /// </summary>
    [Fact]
    public void TheMcpWriteBoundsNameTheSameConstantsTheBandClampsTo()
    {
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");

        Assert.Contains("ServerHealthThresholds.DeadlockRatePerHourFloor", tools, StringComparison.Ordinal);
        Assert.Contains("ServerHealthThresholds.DeadlockRatePerHourCeiling", tools, StringComparison.Ordinal);

        /* Both knobs, not one — two AddDouble calls each naming both bounds. */
        Assert.Equal(2, CountOf(tools, "ServerHealthThresholds.DeadlockRatePerHourFloor"));
        Assert.Equal(2, CountOf(tools, "ServerHealthThresholds.DeadlockRatePerHourCeiling"));

        /* And the Settings window validates against the same pair, so the third surface cannot drift. */
        var settings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");
        Assert.Equal(2, CountOf(settings, "ServerHealthThresholds.DeadlockRatePerHourFloor"));
        Assert.Equal(2, CountOf(settings, "ServerHealthThresholds.DeadlockRatePerHourCeiling"));
    }

    /// <summary>
    /// The band tiers are reported and accepted under their OWN group, not folded into the <c>deadlocks</c>
    /// alert group — the distinction #3368 makes explicitly. <c>deadlocks</c> governs whether an alert is
    /// DELIVERED; these decide what band a card reads. Nesting a band tier under the alert would invite
    /// tuning one and expecting the other to move.
    /// </summary>
    [Fact]
    public void TheTiersAreReportedUnderTheirOwnGroup_NotTheAlert()
    {
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");

        Assert.Contains("health_bands = new", tools, StringComparison.Ordinal);
        Assert.Contains("case \"health_bands\":", tools, StringComparison.Ordinal);
        Assert.Contains("health_bands.deadlock_warn_per_hour", tools, StringComparison.Ordinal);
        Assert.Contains("health_bands.deadlock_critical_per_hour", tools, StringComparison.Ordinal);

        /* And NOT under the alert group, which is the half that could go wrong silently. */
        Assert.DoesNotContain("deadlocks.deadlock_warn_per_hour", tools, StringComparison.Ordinal);
        Assert.DoesNotContain("deadlocks.deadlock_critical_per_hour", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every surface needing the shipped default NAMES the constant rather than restating the number — the
    /// #3060 finding that a number copied into five places is four places that can be left behind.
    /// </summary>
    [Fact]
    public void EverySurfaceNeedingTheShippedDefault_NamesTheConstant()
    {
        var surfaces = new (string What, string Text)[]
        {
            ("AlertsConfig seed", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "DarlingConfig.cs")),
            ("viewer row initializer", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs")),
            ("Restore Defaults button", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains("DeadlockWarnPerHourDefault", text, StringComparison.Ordinal);
            Assert.Contains("DeadlockCriticalPerHourDefault", text, StringComparison.Ordinal);
        }
    }

    /* ─────────────────────── the seam actually reaches the band ─────────────────────── */

    /// <summary>
    /// EVERY production metric bundle declares BOTH the deadlock window and the tiers.
    ///
    /// <para><b>This is the pin that stops the fallback being taken in production.</b>
    /// <c>ServerHealthMetrics.DeadlockRateThresholds</c> is nullable so a path written before the knobs
    /// existed behaves like a store at its defaults — but a PRODUCTION bundle taking that fallback bands on
    /// numbers <c>get_alert_settings</c> does not report, which is the "the setting did not stick" reading.
    /// And a bundle omitting the WINDOW bands every deadlocking server Warning, because
    /// <c>default(TimeSpan)</c> is unrateable.</para>
    ///
    /// <para><b>Keyed on the MEMBER, not on the type name</b>, which is the #3281 finding this reuses: a
    /// scan for <c>new ServerHealthMetrics</c> finds one of the three production sites, because the other
    /// two are target-typed <c>ToHealthMetrics() =&gt; new() { ... }</c> where the type appears only in the
    /// return signature. That is precisely why both survived a review and a file-level census.</para>
    ///
    /// <para><b>And the member is <see cref="ServerHealthMetrics.CpuPercentForAlert"/>, NOT
    /// <see cref="ServerHealthMetrics.DeadlockCount"/>, even though deadlocks are the subject.</b>
    /// <c>DeadlockCount</c> is declared on FOUR types — this bundle, the fleet card DTO, the viewer's
    /// <c>ServerSummaryItem</c> and Lite's overview row — so keying on it matched eight initializers and
    /// reported three files as offenders that do not build this bundle at all. <c>CpuPercentForAlert</c> is
    /// declared here and computed (<c>=&gt;</c>) everywhere else, so an <c>=</c> assignment to it is this
    /// bundle and nothing else; that is the member #3281's own scan uses, and it matches exactly three
    /// sites.</para>
    ///
    /// <para>Comments and strings are stripped with the shared walker first, per the repo pin about
    /// hand-rolled maskers giving different wrong answers.</para>
    /// </summary>
    [Fact]
    public void EveryProductionMetricBundleDeclaresTheWindowAndTheTiers()
    {
        var offenders = new List<string>();
        var found = 0;

        foreach (var file in ProductionCSharpFiles())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(System.IO.File.ReadAllText(file));

            foreach (var initializer in MetricBundleInitializers(code))
            {
                found++;

                if (!AssignsMember(initializer, "DeadlockWindow")
                    || !AssignsMember(initializer, "DeadlockRateThresholds"))
                {
                    offenders.Add(System.IO.Path.GetFileName(file));
                }
            }
        }

        /* The scan has to have FOUND the bundles, or "no offenders" is vacuous. Three production sites,
           the same three #3281's census names: the fleet reader's build, the fleet card's
           ToHealthMetrics, and the viewer card's. Pinned as an exact count rather than a floor — a FOURTH
           bundle is a new surface that has to be looked at, and a floor would let it in silently, while a
           count below three means the regex stopped matching and not that the code got better. */
        Assert.Equal(3, found);

        Assert.True(
            offenders.Count == 0,
            "these production metric bundles carry a deadlock count with no window or no tiers beside it, "
          + "so they band a bare count or band on the shipped pair: "
          + string.Join(", ", offenders.Distinct().OrderBy(f => f, StringComparer.Ordinal)));
    }

    /// <summary>
    /// Both cards READ the tiers from the store rather than constructing the default pair — the half the
    /// bundle census above cannot see, because assigning <c>DeadlockRateThresholds.Default</c> satisfies it
    /// while ignoring the control plane entirely.
    /// </summary>
    [Fact]
    public void BothCardsReadTheTiersFromTheStore()
    {
        var service = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");
        var viewer = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs");

        foreach (var text in new[] { service, viewer })
        {
            Assert.Contains("FROM config_alert_settings", text, StringComparison.Ordinal);
            Assert.Contains(WarnColumn, text, StringComparison.Ordinal);
            Assert.Contains(CriticalColumn, text, StringComparison.Ordinal);
            Assert.Contains("new DeadlockRateThresholds(reader.GetDouble(0), reader.GetDouble(1))", text, StringComparison.Ordinal);
        }

        /* BOTH surfaces read them ONCE per refresh, not per card: a store reload mid-refresh would
           otherwise band some servers on the old pair and the rest on the new one, and the viewer's band
           counts and the service's worst-first ranking would both be derived from a configuration that
           never existed.

           Asserted for BOTH, and that symmetry is the point. An earlier spelling held the discipline for
           the service only, and the viewer read the settings row inside its per-server summary - once per
           card, across concurrent fan-out lanes. One-sided, the pin was satisfied by the side that was
           already right while the side that was wrong went unexamined, which is the shape of a pin that
           cannot fail. */
        Assert.Equal(1, CountOf(service, "ReadDeadlockRateThresholdsAsync(postgres"));
        Assert.Equal(1, CountOf(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"),
            "GetDeadlockRateThresholdsAsync("));

        /* And the viewer's PER-SERVER read must not reach the settings row at all - it takes the tiers as a
           parameter. Stated against the method's own body rather than the file, because the file
           legitimately declares the read its caller hoists. */
        Assert.DoesNotContain(
            "GetDeadlockRateThresholdsAsync(", ViewerServerSummaryBody(), StringComparison.Ordinal);
        Assert.Contains("DeadlockRateThresholds? deadlockTiers", viewer, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>ViewerDataService.GetServerSummaryAsync</c>'s body, sliced out - sliced rather than searched
    /// whole-file because the settings read is DECLARED in the same file, so a whole-file
    /// <c>DoesNotContain</c> could never pass.
    /// </summary>
    private static string ViewerServerSummaryBody()
    {
        var source = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs");

        const string start = "public async Task<ServerSummaryItem> GetServerSummaryAsync(";
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, "GetServerSummaryAsync was not found, so this pin would read nothing");

        const string end = "/// <summary>The deadlock health band's two tiers";
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, "the end of GetServerSummaryAsync was not found, so this pin would read the rest of the file");

        var body = source[from..to];

        /* The slice has to be the method, not a fragment: an off-by-one on either bound silently shrinks it
           and the DoesNotContain above starts passing for the wrong reason. */
        Assert.Contains("DeadlockWindow = window,", body, StringComparison.Ordinal);
        Assert.Contains("ServerSummaryDeadlockSql", body, StringComparison.Ordinal);
        Assert.True(body.Length > 2000, $"the sliced body is only {body.Length} chars, which cannot be this method");

        return body;
    }

    /* ─────────────────────── helpers ─────────────────────── */

    /// <summary>
    /// Assignments of <see cref="ServerHealthMetrics.CpuPercentForAlert"/> in an object initializer,
    /// brace-matched outward to the enclosing <c>{ ... }</c> — #3281's regex, unchanged, because it is the
    /// one proven to select exactly the three production bundle sites.
    ///
    /// <para>The declaration is excluded by requiring an <c>=</c> that is not <c>=&gt;</c>, which is also
    /// what excludes the computed <c>CpuPercentForAlert</c> properties on the two card types; a READ is
    /// excluded by refusing a preceding <c>.</c>.</para>
    /// </summary>
    private static readonly Regex AssignsCpuReading =
        new(@"(?<![.\w])CpuPercentForAlert\s*=(?!=|>)", RegexOptions.Compiled);

    /// <summary>
    /// Every shipping C# file in the repo — the WHOLE repo, not just <c>Darling/</c>.
    ///
    /// <para>Scoping this to one subtree is how a scan of this shape goes quietly wrong: the bundle type
    /// lives in <c>PerformanceMonitor.Common</c> and its <c>InternalsVisibleTo</c> reaches Lite, so a Lite
    /// surface that starts rendering a card would build the bundle outside any subtree a Darling-shaped
    /// census would look in. Tests are excluded because a fixture deliberately omitting the window is a
    /// legitimate pin (the unrateable-window arm is tested that way); <c>deprecated/</c> is excluded because
    /// the old Dashboard has its own frozen copy of this banding.</para>
    /// </summary>
    private static IEnumerable<string> ProductionCSharpFiles()
    {
        foreach (var file in System.IO.Directory.EnumerateFiles(
            RepoFile.Root, "*.cs", System.IO.SearchOption.AllDirectories))
        {
            var segments = file.Split(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj")
                || segments.Contains("deprecated")
                || segments.Any(s => s.EndsWith("Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            yield return file;
        }
    }

    private static IEnumerable<string> MetricBundleInitializers(string code)
    {
        foreach (var match in AssignsCpuReading.Matches(code).Cast<Match>())
        {
            var depth = 0;
            var open = -1;

            for (var i = match.Index; i >= 0; i--)
            {
                if (code[i] == '}')
                {
                    depth++;
                }
                else if (code[i] == '{' && depth-- == 0)
                {
                    open = i;
                    break;
                }
            }

            if (open < 0)
            {
                continue;
            }

            depth = 0;
            for (var i = open; i < code.Length; i++)
            {
                if (code[i] == '{')
                {
                    depth++;
                }
                else if (code[i] == '}' && --depth == 0)
                {
                    yield return code[open..(i + 1)];
                    break;
                }
            }
        }
    }

    private static bool AssignsMember(string initializer, string member) =>
        Regex.IsMatch(initializer, $@"(?<![.\w]){Regex.Escape(member)}\s*=(?!=|>)");

    /// <summary>Non-overlapping occurrences of <paramref name="needle"/>.</summary>
    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (at >= 0)
        {
            count++;
            at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }
}
