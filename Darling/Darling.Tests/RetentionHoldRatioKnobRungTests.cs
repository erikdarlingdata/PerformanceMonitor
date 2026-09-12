/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V119 / #3297: the Retention Held warn/critical ratios move into <c>config.config_alert_settings</c>.
///
/// <para>They were compile-time constants, which made the alert an operator most needs to tune the one alert
/// that could not be — #3296's reporter received an hourly CRITICAL named <c>Retention Held</c> against
/// <c>Monitor Store</c>, went looking in Settings for either phrase, and found neither.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="BuiltinAlertPersistenceRungTests"/> (V118) when this rung landed — a fully-migrated store must
/// map to EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually
/// current.</para>
/// </summary>
public sealed class RetentionHoldRatioKnobRungTests
{
    private const int RungVersion = 119;
    private const int PreviousVersion = 118;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 94;

    private const string WarnColumn = "retention_hold_warn_ratio";
    private const string CriticalColumn = "retention_hold_critical_ratio";

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "retention-hold-ratio-knobs",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds both columns to the singleton settings row, schema-qualified, with the shipped constants
    /// as their defaults.
    ///
    /// <para>The DEFAULTS are compared against the constants rather than against literals, which is the half
    /// that matters: the C# seed, the viewer row, the Restore-Defaults button and the evaluator's fallback
    /// all name the same two constants, so if the rung text alone held a literal a moved default would leave
    /// the column behind and an upgraded store would fire at a threshold no surface reports.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsBothColumns_SchemaQualified_WithTheShippedDefaults()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Equal(2, CountOf(rung, "ALTER TABLE config.config_alert_settings"));
        Assert.DoesNotContain("ALTER TABLE config_alert_settings", rung, StringComparison.Ordinal);

        /* IF NOT EXISTS so a replay is a harmless no-op, the idiom every additive config rung uses. */
        foreach (var column in new[] { WarnColumn, CriticalColumn })
        {
            Assert.Contains(
                $"ADD COLUMN IF NOT EXISTS {column} double precision NOT NULL DEFAULT",
                rung, StringComparison.Ordinal);
        }

        /* double precision, not integer: the value is a ratio whose fractional part is the whole point —
           the healthy ceiling this threshold has to clear is 1.4x. analysis_notify_severity is the same
           type on this table, so this is the established shape rather than a new one. */
        Assert.DoesNotContain("integer", rung, StringComparison.Ordinal);

        /* The defaults ARE the constants. Formatted the way Postgres accepts and the way the rung writes
           them, derived from the constants so a moved default reds here. */
        Assert.Contains(
            $"{WarnColumn} double precision NOT NULL DEFAULT "
            + TimescaleSupport.RetentionHoldWarnRatioDefault.ToString("0.0", CultureInfo.InvariantCulture),
            rung, StringComparison.Ordinal);
        Assert.Contains(
            $"{CriticalColumn} double precision NOT NULL DEFAULT "
            + TimescaleSupport.RetentionHoldCriticalRatioDefault.ToString("0.0", CultureInfo.InvariantCulture),
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
        Assert.Contains("hasRetentionHoldRatioKnobs", viewer, StringComparison.Ordinal);

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
    /// class this rung is most exposed to, not the DDL.
    ///
    /// <para><c>StoreConfigProvider.ApplyToConfig</c> replaces <c>config.Alerts</c> wholesale with what the
    /// store returned, so a column the service SELECTs but does not read — or reads but does not select —
    /// resets the tier to the shipped default on every worker start rather than failing. That is exactly the
    /// "the setting did not stick" reading #3296 produced once already, arriving from the fix for it. The
    /// viewer's list drives its SELECT ordinals AND its upsert parameter positions at once, and the MCP
    /// reader is positional too.</para>
    ///
    /// <para>Asserted against the shipped sources rather than a retyped copy, for the reason the PostgreSQL
    /// slice learned the hard way: a proven statement and a working feature are different claims, and every
    /// blocking defect there was a call site rather than the SQL.</para>
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
            ("mcp read", ViewerAgnosticMcpRead()),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains(WarnColumn, text, StringComparison.Ordinal);
            Assert.Contains(CriticalColumn, text, StringComparison.Ordinal);
        }

        /* The service must READ them, not merely select them — the ApplyToConfig wholesale-replacement trap
           above. Both halves, because one of the two being read is what an off-by-one produces. */
        var service = surfaces.Single(s => s.What == "service seed + read").Text;
        Assert.Contains("RetentionHoldWarnRatio = reader.GetDouble(", service, StringComparison.Ordinal);
        Assert.Contains("RetentionHoldCriticalRatio = reader.GetDouble(", service, StringComparison.Ordinal);

        /* And the viewer's upsert must WRITE them, or Save silently drops whatever the boxes held. */
        Assert.Contains($"{WarnColumn} = EXCLUDED.{WarnColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains($"{CriticalColumn} = EXCLUDED.{CriticalColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP's ACCEPTED bound equals the engine's CLAMP, held STRUCTURALLY — both sites name the same two
    /// constants rather than carrying matching literals.
    ///
    /// <para>The parity itself is why this matters and it is already pinned twice in this repo
    /// (<c>FileGrowthWriteBounds_MatchTheEngineClamps</c>,
    /// <c>AgConnectionAndBlockingWaitWriteBounds_MatchTheEngineClamps</c>): a wider bound in the tool lets
    /// <c>update_alert_settings</c> ACCEPT a value the engine then silently rewrites, which presents as the
    /// setting not sticking with nothing saying no.</para>
    ///
    /// <para>What is asserted here is the shape rather than the equality, because the equality is the
    /// compiler's once both sides share a constant — and an equality assertion over two references to one
    /// constant is an assertion that cannot fail. So this pins that NEITHER site carries a bare literal,
    /// which is the state that would put the two numbers back in a position to drift.</para>
    /// </summary>
    [Fact]
    public void TheMcpWriteBoundAndTheEngineClamp_AreTheSameConstants_NotMatchingLiterals()
    {
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        var settings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs");
        var window = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");

        /* All three writable surfaces reach the same two named bounds. The Settings window is in here
           because it is a THIRD accepted-bound: it assigns the row only when the box parses inside its own
           range, so a range of its own would make the WPF seat and the tool disagree about what is
           settable while both looked right. */
        /* Each bound appears TWICE per surface — once per tier. Zero is the literalised case; ONE is what a
           half-migration looks like, the warn ratio bounded by the constant and the critical ratio by a
           literal beside it. Counted over the whole file rather than per call site because the three
           surfaces spell the call differently and a shared count is the property that holds in all three.
           The surface NAME rides in the failure message: an assertion that merely proves the loop ran is
           one that cannot fail, and a count mismatch with no surface named sends the reader to three files. */
        foreach (var (what, text) in new[]
                 {
                     ("engine clamp", settings), ("mcp write bound", tools), ("settings window", window),
                 })
        {
            Assert.True(
                CountOf(text, "TimescaleSupport.RetentionHoldRatioFloor") == 2,
                $"the {what} must reach the shared FLOOR constant once per tier, not a literal");
            Assert.True(
                CountOf(text, "TimescaleSupport.RetentionHoldRatioCeiling") == 2,
                $"the {what} must reach the shared CEILING constant once per tier, not a literal");
        }

        /* The floor is the shipped warning default, which is the decision: these knobs RAISE the tiers and
           cannot lower them, because healthy whole-chunk granularity reaches 1.4x measured and the band up
           to 2.0x is margin nobody measured. Asserted as an identity rather than as the number, so the
           reasoning and the constant cannot come apart. */
        Assert.Equal(TimescaleSupport.RetentionHoldWarnRatioDefault, TimescaleSupport.RetentionHoldRatioFloor);

        /* And the floor clears the measured healthy ceiling with the margin the reasoning claims. 1.4 is
           the measurement, not a constant, so it is written here and nowhere else. */
        Assert.True(TimescaleSupport.RetentionHoldRatioFloor > 1.5,
            "a floor at or below 1.5x is inside observed healthy behaviour (1.4x measured), so it would fire on a working store");
        Assert.True(TimescaleSupport.RetentionHoldRatioCeiling > TimescaleSupport.RetentionHoldCriticalRatioDefault);
    }

    /// <summary>
    /// The DEFAULT lives in exactly one place, and every surface that needs it names that place.
    ///
    /// <para>Five surfaces need the shipped pair — the rung's column default, <c>AlertsConfig</c>'s seed, the
    /// viewer row's initializer, the Restore-Defaults button, and the evaluator's unsupplied-seam fallback.
    /// #3060 established what a copy costs here: the cadence knob's default was restated in four places and
    /// a moved derivation left them disagreeing, with the surface an operator actually touches holding the
    /// stale one. So this asserts each surface REFERENCES the constant, which is the only form in which they
    /// cannot disagree.</para>
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
            ("evaluator fallback", RepoFile.ReadRepoFile(
                "Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs")),
        };

        foreach (var (what, text) in surfaces)
        {
            Assert.False(string.IsNullOrWhiteSpace(text), $"{what}: read nothing, so this pin would assert nothing");
            Assert.Contains("RetentionHoldWarnRatioDefault", text, StringComparison.Ordinal);
            Assert.Contains("RetentionHoldCriticalRatioDefault", text, StringComparison.Ordinal);
        }

        /* And the values the surfaces resolve to. Asserted through the objects rather than through the
           source, so this half is a behaviour check rather than a second text scan. */
        Assert.Equal(TimescaleSupport.RetentionHoldWarnRatioDefault, new AlertsConfig().RetentionHoldWarnRatio);
        Assert.Equal(TimescaleSupport.RetentionHoldCriticalRatioDefault, new AlertsConfig().RetentionHoldCriticalRatio);
        Assert.Equal(TimescaleSupport.RetentionHoldWarnRatioDefault, AlertSettingsRow.Defaults().RetentionHoldWarnRatio);
        Assert.Equal(TimescaleSupport.RetentionHoldCriticalRatioDefault, AlertSettingsRow.Defaults().RetentionHoldCriticalRatio);

        /* The shipped pair clears the clamp, so a default can never be the value that gets rejected on its
           way back in — the same property ViewerAlertRowCadenceDefaultTests holds for the cadence knob. */
        Assert.InRange(
            AlertSettingsRow.Defaults().RetentionHoldWarnRatio,
            TimescaleSupport.RetentionHoldRatioFloor, TimescaleSupport.RetentionHoldRatioCeiling);
        Assert.InRange(
            AlertSettingsRow.Defaults().RetentionHoldCriticalRatio,
            TimescaleSupport.RetentionHoldRatioFloor, TimescaleSupport.RetentionHoldRatioCeiling);
    }

    /// <summary>
    /// The evaluator judges on the SEAMS, never on the constants — and it says the threshold it judged on.
    ///
    /// <para>This is the whole point of the rung and it is the one thing a store-backed knob can have while
    /// changing nothing: the seam exists, the store row moves, and a bare constant left in the decision
    /// keeps firing at 2.0 while <c>get_alert_settings</c> reports 4.0. There is no test over collected data
    /// that would separate those two worlds, because the shipped default and an untuned store agree.</para>
    ///
    /// <para>So what is pinned is that <c>ApplyRetentionHoldsAsync</c> and <c>ClearRetentionHoldAsync</c>
    /// contain NO reference to either constant — including in the message strings, which is the half review
    /// would be least likely to catch. A resolution that reads "back under 2.0x" on a store configured at
    /// 5.0x is a true sentence about the wrong number.</para>
    /// </summary>
    [Fact]
    public void TheRetentionHoldDecisionAndItsMessages_ReadTheSeams_NotTheConstants()
    {
        var body = RetentionHoldBodies();

        /* The seams are read, and read ONCE each — a per-policy re-read would let one pass judge some
           policies on the pre-reload pair and the rest on the post-reload pair. */
        Assert.Equal(1, CountOf(body, "_retentionHoldWarnRatio()"));
        Assert.Equal(1, CountOf(body, "_retentionHoldCriticalRatio()"));

        /* And neither constant appears anywhere in the two methods — decision or message. */
        Assert.DoesNotContain("RetentionHoldWarnRatio:F1", body, StringComparison.Ordinal);
        Assert.DoesNotContain("RetentionHoldCriticalRatio:F1", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ratio >= RetentionHold", body, StringComparison.Ordinal);
        Assert.DoesNotContain("TimescaleSupport.RetentionHold", body, StringComparison.Ordinal);

        /* The decision compares against the LOCALS the seams were read into. */
        Assert.Contains("ratio >= warnRatio", body, StringComparison.Ordinal);
        Assert.Contains("ratio >= criticalRatio", body, StringComparison.Ordinal);

        /* The resolution's threshold is HANDED IN rather than re-read, which is what makes it the same
           number the fire decision used. */
        Assert.Contains("double warnRatio, CancellationToken cancellationToken", body, StringComparison.Ordinal);
    }

    /// <summary>The worker hands both seams the CLAMPED settings properties, live. A seam wired to
    /// <c>_config.Alerts</c> directly would bypass the clamp and let a hand-edited store row drive a
    /// threshold the tool would have refused.</summary>
    [Fact]
    public void TheWorkerWiresBothSeamsToTheClampedSettings()
    {
        var worker = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        Assert.Contains("retentionHoldWarnRatio: () => alertSettings.RetentionHoldWarnRatio", worker, StringComparison.Ordinal);
        Assert.Contains("retentionHoldCriticalRatio: () => alertSettings.RetentionHoldCriticalRatio", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// The runbook an operator is pointed at no longer says the thresholds cannot be tuned, and states the
    /// floor it now has.
    ///
    /// <para>Pinned because that sentence was the documented answer to "why can I not change this", written
    /// for #3296 and referencing this issue by number — it is the one place a reader would go to find out
    /// whether the gap is closed, and a doc that still admits a gap the code has closed sends them away.</para>
    /// </summary>
    [Fact]
    public void TheRunbookDocumentsTheKnobsRatherThanTheGap()
    {
        var runbook = RepoFile.ReadRepoFile("docs", "retention-hold-runbook.md");

        Assert.DoesNotContain("thresholds are not tunable", runbook, StringComparison.Ordinal);
        Assert.DoesNotContain("cannot be adjusted in Settings", runbook, StringComparison.Ordinal);

        Assert.Contains(WarnColumn, runbook, StringComparison.Ordinal);
        Assert.Contains(CriticalColumn, runbook, StringComparison.Ordinal);

        /* The floor, stated as the number an operator is refused below — derived from the constant so the
           doc cannot outlive it. */
        Assert.Contains(
            "accept " + TimescaleSupport.RetentionHoldRatioFloor.ToString("0.0", CultureInfo.InvariantCulture)
            + " or above",
            runbook, StringComparison.Ordinal);

        /* And the measurement that justifies the floor, which is the part a reader tempted to lower it
           needs. 1.4x is the reading from a healthy production store. */
        Assert.Contains("1.4x", runbook, StringComparison.Ordinal);
    }

    /// <summary>The MCP read constant, fetched through the reader type rather than the file, so this pin
    /// reads the SHIPPED string and not a source file that might merely mention the columns in a
    /// comment.</summary>
    private static string ViewerAgnosticMcpRead() => DarlingAlertReader.AlertSettingsSelectSql;

    /// <summary>
    /// <c>ApplyRetentionHoldsAsync</c> and <c>ClearRetentionHoldAsync</c>, sliced out of the evaluator —
    /// sliced rather than searched whole-file because the constants are DECLARED in the same file, so a
    /// whole-file <c>DoesNotContain</c> could never pass and a whole-file <c>Contains</c> would be satisfied
    /// by the declaration.
    /// </summary>
    private static string RetentionHoldBodies()
    {
        var source = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");

        const string start = "internal async Task ApplyRetentionHoldsAsync(";
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, "ApplyRetentionHoldsAsync was not found, so this pin would read nothing");

        /* To the end of ClearRetentionHoldAsync, which is the next member and the other half of the
           subject — the resolution message is where the threshold is stated back to the operator. */
        const string end = "/// Edge-applies the fleet-level compression-job self-heal machine";
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, "the end of ClearRetentionHoldAsync was not found, so this pin would read the rest of the file");

        var body = source[from..to];

        /* The slice has to be the two methods, not a fragment: an off-by-one on either bound silently
           shrinks it and every DoesNotContain above starts passing for the wrong reason. */
        Assert.Contains("_activeRetentionHold[key] = true;", body, StringComparison.Ordinal);
        Assert.Contains("Retention Hold Cleared", body, StringComparison.Ordinal);
        Assert.True(body.Length > 2000, $"the sliced body is only {body.Length} chars, which cannot be these two methods");

        return body;
    }

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
