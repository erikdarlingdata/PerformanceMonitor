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
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V122 / #3444: the PostgreSQL Deadlocks and Blocking alerts' count thresholds move into
/// <c>config.config_alert_settings</c>.
///
/// <para>They were <c>private const int … = 1</c> in <c>DarlingWorker</c> with no settings, config or JSON
/// path at all, while their SQL Server twins have been settable since V1 — so the same two conditions were
/// tunable on one engine and compile-time on the other. #3297 is the precedent this issue cites: a threshold
/// whose right value differs per workload does not belong in a compile-time constant.</para>
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="OversizedPlanBacklogPins"/> (V121) when this rung landed, the same way they moved off
/// <see cref="DeadlockRateBandRungTests"/> (V120) before that — a fully-migrated store must map to EXACTLY
/// this version, or the viewer's connect-time gate refuses a store that is actually current.</para>
/// </summary>
public sealed class PgAlertCountKnobRungTests
{
    private const int RungVersion = 122;
    private const int PreviousVersion = 121;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 97;

    private const string DeadlockColumn = "pg_deadlock_count_threshold";
    private const string BlockingColumn = "pg_blocking_count_threshold";

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "pg-deadlock-blocking-count-knobs",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds both columns to the singleton settings row, schema-qualified, with the constants they
    /// replace as their defaults.
    ///
    /// <para>The DEFAULTS are compared against the shared constants rather than against literals, which is
    /// the half that matters: the C# seed, the viewer row, the Restore-Defaults button and the worker's
    /// clamp all name the same two constants, so if the rung text alone held a literal a moved default
    /// would leave the column behind and an upgraded store would fire at a threshold no surface reports.
    /// And the constants themselves are pinned to 1 — the exact value the deleted <c>DarlingWorker</c>
    /// consts carried — because #3444's acceptance is that an upgraded, untouched store fires exactly
    /// where it did.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsBothColumns_SchemaQualified_WithTheDefaultsTheConstantsShippedAt()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Equal(2, CountOf(rung, "ALTER TABLE config.config_alert_settings"));
        Assert.DoesNotContain("ALTER TABLE config_alert_settings", rung, StringComparison.Ordinal);

        /* IF NOT EXISTS on both, so re-running the ladder over a store that already has them is a no-op
           rather than a 42701 that aborts the whole migration. */
        Assert.Equal(2, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));

        /* integer, matching the twins on this same table, NOT the double precision the V119/V120 knob
           rungs used: these are counts of discrete events with no meaningful fractional part. */
        Assert.DoesNotContain("double precision", rung, StringComparison.Ordinal);

        /* The defaults ARE the constants — restated as literals in the rung only because a rung is a SQL
           string, and derived from the constants here so a moved default reds this pin. */
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {DeadlockColumn} integer NOT NULL DEFAULT "
            + PostgresAlertEvaluator.DeadlockCountThresholdDefault.ToString(CultureInfo.InvariantCulture) + ";",
            rung, StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {BlockingColumn} integer NOT NULL DEFAULT "
            + PostgresAlertEvaluator.BlockingCountThresholdDefault.ToString(CultureInfo.InvariantCulture) + ";",
            rung, StringComparison.Ordinal);

        /* And the constants equal the replaced consts' value. 1 is written here and in the issue's own
           measurement of the deleted declarations, nowhere else: a default moved away from it is an
           upgrade that changes behaviour on every untouched store, which is the one outcome #3444's
           acceptance forbids. */
        Assert.Equal(1, PostgresAlertEvaluator.DeadlockCountThresholdDefault);
        Assert.Equal(1, PostgresAlertEvaluator.BlockingCountThresholdDefault);

        /* No reload beacon of its own: config_alert_settings already carries V17's statement-level
           trg_bump_alert_settings, so a second trigger here would be a duplicate bump per write. */
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);

        /* And no per-table GRANT: this table carries table-level grants with no column carve, which is what
           every earlier knob rung on it says. A grant appearing here would mean the carve had changed and
           this rung had quietly taken on an ACL decision. */
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm.
    ///
    /// <para>The probe asks the question, the caller reads the answer, the map has the parameter — three
    /// sites, and a sentinel present at only some of them shifts every LATER ordinal onto the wrong column.
    /// Miss all three and a fully-migrated store probes one rung short, so the connect-time gate refuses a
    /// store that is in fact current — permanently, because no later upgrade changes the answer.</para>
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains($"column_name = '{DeadlockColumn}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgAlertCountKnobs", viewer, StringComparison.Ordinal);

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

        /* This rung's own arm answers for a store that stopped here. Expressed as "false above" rather than
           as one named ordinal, so a rung landing on top of this one does not quietly turn this case into a
           test of that rung. */
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        /* One rung behind: the same store WITHOUT this rung's sentinel reports the previous rung. */
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* And in the source, the arm sits ABOVE V121's — newest-first is the whole contract of that method —
           and returns this build's version rather than a literal that could drift from it. This is the
           textual half of the top-arm claim, inherited from OversizedPlanBacklogPins the way that file
           inherited it from DeadlockRateBandRungTests. */
        var v122 = viewer.IndexOf("if (hasPgAlertCountKnobs)", StringComparison.Ordinal);
        var v121 = viewer.IndexOf("if (hasOversizedPlanBacklog)", StringComparison.Ordinal);
        Assert.True(v122 >= 0, "the viewer has no V122 sentinel arm — a fully-migrated store would map to 121");
        Assert.True(v121 >= 0, "the V121 arm is gone, so this pin is comparing against nothing");
        Assert.True(v122 < v121, "the V122 arm sits below V121's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[v122..], StringComparison.Ordinal);
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
            Assert.Contains(DeadlockColumn, text, StringComparison.Ordinal);
            Assert.Contains(BlockingColumn, text, StringComparison.Ordinal);
        }

        /* The service must READ them, not merely select them — ApplyToConfig replaces config.Alerts
           wholesale, so a selected-but-unread column resets the threshold to the shipped default on every
           worker start. Both halves, because one of the two being read is what an off-by-one produces. */
        var service = surfaces.Single(s => s.What == "service seed + read").Text;
        Assert.Contains("PgDeadlockCountThreshold = reader.GetInt32(", service, StringComparison.Ordinal);
        Assert.Contains("PgBlockingCountThreshold = reader.GetInt32(", service, StringComparison.Ordinal);

        /* And the viewer's upsert must WRITE them, or Save silently drops whatever the boxes held. */
        Assert.Contains($"{DeadlockColumn} = EXCLUDED.{DeadlockColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains($"{BlockingColumn} = EXCLUDED.{BlockingColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The MCP's ACCEPTED bound, the settings read's CLAMP and the Viewer's save gate are the SAME named
    /// floor, held STRUCTURALLY — all three sites name <see cref="PostgresAlertEvaluator.CountThresholdFloor"/>
    /// rather than carrying matching literals.
    ///
    /// <para>The parity is why this matters and it is already pinned three times over in this repo: a wider
    /// bound in any writer lets a value in that another surface then silently rewrites, which presents as
    /// the setting not sticking with nothing saying no. What is asserted is the shape rather than the
    /// equality — the equality is the compiler's once every side shares a constant.</para>
    /// </summary>
    [Fact]
    public void TheMcpWriteBound_TheSettingsClamp_AndTheViewerGate_AreTheSameConstant_NotMatchingLiterals()
    {
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        var settings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs");
        var window = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");

        /* The floor appears TWICE per surface — once per knob. ONE is what a half-migration looks like:
           the deadlock knob bounded by the constant and the blocking knob by a literal beside it. The
           surface name rides in the failure message so a count mismatch does not send the reader to
           three files. */
        foreach (var (what, text) in new[]
                 {
                     ("engine clamp", settings), ("mcp write bound", tools), ("settings window gate", window),
                 })
        {
            Assert.True(
                CountOf(text, "PostgresAlertEvaluator.CountThresholdFloor") == 2,
                $"the {what} must reach the shared floor constant once per knob, not a literal");
        }

        /* The floor is what keeps the knob a threshold: at 0 the gate's count >= threshold test is true
           for a count of zero, so a store row hand-edited to 0 would fire on a server with no deadlocks.
           1 is the tightest setting that still describes an occurrence, and it is also both shipped
           defaults — so the shipped pair can never be a value the write path refuses on its way back in. */
        Assert.Equal(1, PostgresAlertEvaluator.CountThresholdFloor);
        Assert.True(PostgresAlertEvaluator.DeadlockCountThresholdDefault >= PostgresAlertEvaluator.CountThresholdFloor);
        Assert.True(PostgresAlertEvaluator.BlockingCountThresholdDefault >= PostgresAlertEvaluator.CountThresholdFloor);
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
            Assert.Contains("DeadlockCountThresholdDefault", text, StringComparison.Ordinal);
            Assert.Contains("BlockingCountThresholdDefault", text, StringComparison.Ordinal);
        }

        /* And the values the surfaces resolve to. Asserted through the objects rather than through the
           source, so this half is a behaviour check rather than a second text scan. */
        Assert.Equal(PostgresAlertEvaluator.DeadlockCountThresholdDefault, new AlertsConfig().PgDeadlockCountThreshold);
        Assert.Equal(PostgresAlertEvaluator.BlockingCountThresholdDefault, new AlertsConfig().PgBlockingCountThreshold);
        Assert.Equal(PostgresAlertEvaluator.DeadlockCountThresholdDefault, AlertSettingsRow.Defaults().PgDeadlockCountThreshold);
        Assert.Equal(PostgresAlertEvaluator.BlockingCountThresholdDefault, AlertSettingsRow.Defaults().PgBlockingCountThreshold);
    }

    /* ─────────────────────── the seam actually reaches the gates ─────────────────────── */

    /// <summary>
    /// The worker's two gates read the CLAMPED settings properties, and the constants this rung replaces
    /// are gone.
    ///
    /// <para>This is the whole point of the rung and it is the one thing a store-backed knob can have while
    /// changing nothing: the columns exist, the store row moves, and a bare constant left in the decision
    /// keeps firing at 1 while <c>get_alert_settings</c> reports 5. There is no test over collected data
    /// that would separate those two worlds, because the shipped default and an untuned store agree —
    /// which is exactly the state the V122 plumbing sat in while the gates still read the consts.</para>
    /// </summary>
    [Fact]
    public void TheWorkerGatesReadTheClampedSettings_AndTheReplacedConstantsAreGone()
    {
        var worker = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        /* Read ONCE each, into the local the gate and the delivered message both use — a second read would
           let a mid-sweep config reload make the message quote a number the decision never saw. */
        Assert.Equal(1, CountOf(worker, "alertSettings.PgDeadlockCountThreshold"));
        Assert.Equal(1, CountOf(worker, "alertSettings.PgBlockingCountThreshold"));

        /* The consts are gone, not shadowed: a surviving copy would quietly outvote the store. */
        Assert.DoesNotContain("const int PgDeadlockCountThreshold", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("const int PgBlockingCountThreshold", worker, StringComparison.Ordinal);

        /* And both evaluators honor the shared enabled switch the tool description promises — one
           "is this condition worth alerting on" preference covering both engines, per-engine volume. */
        Assert.Contains("if (!alertSettings.DeadlockEnabled)", worker, StringComparison.Ordinal);
        Assert.Contains("if (!alertSettings.BlockingEnabled)", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// Behaviour, through the same gate the worker calls: a raised threshold fires at N and not at 1.
    /// Driven through <see cref="DarlingAlertSettings"/> rather than handing the gate a literal, so what is
    /// tested is the seam the worker reads, clamp included.
    /// </summary>
    [Fact]
    public void ARaisedThreshold_FiresAtN_NotAtOne()
    {
        var config = new DarlingConfig();
        config.Alerts.PgDeadlockCountThreshold = 5;
        config.Alerts.PgBlockingCountThreshold = 5;
        var settings = new DarlingAlertSettings(config);

        foreach (var threshold in new[] { settings.PgDeadlockCountThreshold, settings.PgBlockingCountThreshold })
        {
            Assert.Equal(5, threshold);

            /* Four events inside the window: below the raised bar, so quiet — this is the page-per-event
               noise #3444's reporter had no lever against. */
            Assert.False(RollingCountAlertGate.Evaluate(
                4, threshold, watermark: 0, cooldownElapsed: true, suppressed: false).Fire);

            /* The fifth crosses it. */
            Assert.True(RollingCountAlertGate.Evaluate(
                5, threshold, watermark: 0, cooldownElapsed: true, suppressed: false).Fire);
        }
    }

    /// <summary>
    /// The shipped defaults reproduce the fire-at-1 behaviour of the constants they replace — #3444's
    /// acceptance: upgrading changes nothing until an operator chooses to.
    /// </summary>
    [Fact]
    public void TheShippedDefaults_ReproduceTheFireAtOneBehaviour()
    {
        var settings = new DarlingAlertSettings(new DarlingConfig());

        foreach (var threshold in new[] { settings.PgDeadlockCountThreshold, settings.PgBlockingCountThreshold })
        {
            Assert.False(RollingCountAlertGate.Evaluate(
                0, threshold, watermark: 0, cooldownElapsed: true, suppressed: false).Fire);
            Assert.True(RollingCountAlertGate.Evaluate(
                1, threshold, watermark: 0, cooldownElapsed: true, suppressed: false).Fire);
        }
    }

    /// <summary>
    /// The clamp floors a hand-edited store row, so the gate cannot fire on a count of zero — the failure
    /// the floor exists for, stated on <see cref="PostgresAlertEvaluator.CountThresholdFloor"/>'s own doc.
    /// The SQL Server twins take the same write bound and have no read-side floor; that asymmetry is named
    /// on <c>DarlingAlertSettings</c> rather than copied here.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void AHandEditedRowBelowTheFloor_ReadsAsTheFloor_SoNothingFiresOnNothing(int stored)
    {
        var config = new DarlingConfig();
        config.Alerts.PgDeadlockCountThreshold = stored;
        config.Alerts.PgBlockingCountThreshold = stored;
        var settings = new DarlingAlertSettings(config);

        Assert.Equal(PostgresAlertEvaluator.CountThresholdFloor, settings.PgDeadlockCountThreshold);
        Assert.Equal(PostgresAlertEvaluator.CountThresholdFloor, settings.PgBlockingCountThreshold);

        Assert.False(RollingCountAlertGate.Evaluate(
            0, settings.PgDeadlockCountThreshold, watermark: 0, cooldownElapsed: true, suppressed: false).Fire);
    }

    /// <summary>
    /// The viewer row round-trips through the bind at the APPENDED ordinals — the four-parallel-sequences
    /// trap the V119/V120 files name (the column list, the upsert's $N placeholders, the bind order and the
    /// reader ordinals all have to agree), held here for the one sequence no SQL text pin can see: the bind.
    /// A column bound out of order re-maps every later parameter silently, because the types mostly line up.
    /// </summary>
    [Fact]
    public void TheViewerRow_RoundTripsThroughTheBind_AtTheAppendedOrdinals()
    {
        var bind = typeof(ViewerDataService)
            .GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;

        var row = AlertSettingsRow.Defaults();
        /* Deliberately NOT the shipped 1s, and not each other — a bind that dropped one column and shifted
           the rest would otherwise still present the right value at one of the two positions. */
        row.PgDeadlockCountThreshold = 9;
        row.PgBlockingCountThreshold = 11;

        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });

        /* The bind supplies exactly as many parameters as the upsert's highest placeholder. */
        var highestPlaceholder = System.Text.RegularExpressions.Regex
            .Matches(ViewerDataService.AlertSettingsUpsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .Max();
        Assert.Equal(command.Parameters.Count, highestPlaceholder);

        /* The two new columns ride at the END — appended, the rule every knob rung on this table follows,
           so every earlier ordinal keeps its column. */
        Assert.Equal(9, Assert.IsType<NpgsqlParameter<int>>(command.Parameters[^2]).TypedValue);
        Assert.Equal(11, Assert.IsType<NpgsqlParameter<int>>(command.Parameters[^1]).TypedValue);
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
