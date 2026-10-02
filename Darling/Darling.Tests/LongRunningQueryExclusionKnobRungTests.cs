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
/// Pins the Long-Running Query opt-out knob's Darling STORE home (#3653 A5, ruling Q5, as the production-read
/// addendum shaped it): the V135 rung that adds the two <c>text[]</c> columns
/// <c>long_running_query_excluded_program_name_prefixes</c> / <c>long_running_query_excluded_logins</c> to the
/// singleton <c>config_alert_settings</c> row with the read's SEEDED DEFAULTS as their column default, the viewer
/// probe's top arm, and every surface that reads or writes them — the service's seed and read, the MCP
/// read/report/accept trio, the viewer's select/upsert/bind/reader and its Settings window. The shape is
/// <see cref="SelfDiskWarnGbFloorRungTests"/>'s (V126), one knob on, for a knob that is TWO LISTS and not a
/// number, and whose default is not empty.
///
/// <para>The "I am the top rung" claims this file carried when it landed (moved here off <c>TimeHonestyRungTests</c>,
/// V134) moved on again to <c>PgDatabaseSizeAndHostMemoryRungTests</c> when V136 (#3691 — the per-database size
/// series and the Performance Insights host-memory table) landed on top of it. What stays is everything true of
/// this rung wherever it sits: its name, its DDL, its probe sentinel at its own ordinal, and that a store which
/// stopped here maps to exactly 135.</para>
///
/// <para>The engine side — the prefix / exact match rule, the seeds themselves, the two per-arm counts, both
/// SKUs' reads — is <see cref="LongRunningQueryExclusionsTests"/> and <c>AlertEngineTests</c>, and landed one PR
/// ahead of this rung with Darling evaluating on the seeds through <c>DarlingAlertSettings</c>; this PR gives
/// the knob its store home so an operator can change it.</para>
/// </summary>
public sealed class LongRunningQueryExclusionKnobRungTests
{
    private const int RungVersion = 135;
    private const int PreviousVersion = 134;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V136 appended its
    /// own — so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 110;

    private const string ProgramsColumn = "long_running_query_excluded_program_name_prefixes";
    private const string LoginsColumn = "long_running_query_excluded_logins";

    private static PgMigrations.Migration V135 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("lrq-exclusion-knob", V135.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped being
           true when V136 landed. The invariant that outlives the handoff is that the LADDER's top and the
           declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung adds TWO columns to the singleton settings row, schema-qualified, <c>text[] NOT NULL</c> like
    /// <c>excluded_databases</c> — with the production read's seeds as their DEFAULT, not <c>'{}'</c>.
    ///
    /// <para>The DEFAULT is compared against <c>LongRunningQueryExclusions.DefaultProgramNamePrefixes</c> /
    /// <c>DefaultLogins</c> rather than a literal (restated in the rung only because a rung is a SQL string), so a
    /// moved seed cannot leave upgraded stores evaluating a population no surface reports. A NON-EMPTY default on
    /// upgrade deliberately: a pre-rung row must read as exactly what <c>DarlingAlertSettings</c> returned before
    /// the rung (the seeds) and what Lite ships, so the two SKUs and the two sides of the rung evaluate one
    /// population; an operator who clears a list stores an explicit empty array, which nothing re-seeds.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsBothColumns_SchemaQualified_WithTheSeedsAsDefault()
    {
        var rung = V135.Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts collect
           first, so a bare name would resolve to the wrong schema (and the wrong ACL). Two ALTERs, one per column. */
        Assert.Equal(2, CountOf(rung, "ALTER TABLE config.config_alert_settings"));
        Assert.DoesNotContain("ALTER TABLE config_alert_settings", rung, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(rung, "ADD COLUMN IF NOT EXISTS"));

        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {ProgramsColumn} text[] NOT NULL\n        DEFAULT {SqlArray(LongRunningQueryExclusions.DefaultProgramNamePrefixes)};",
            rung.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains(
            $"ADD COLUMN IF NOT EXISTS {LoginsColumn} text[] NOT NULL\n        DEFAULT {SqlArray(LongRunningQueryExclusions.DefaultLogins)};",
            rung.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT '{}'", rung, StringComparison.Ordinal);

        /* And the C# seeds name the same lists on every model, so a fresh file-plane config, a fresh viewer row
           and an upgraded store row agree without any of them citing another. */
        Assert.Equal(LongRunningQueryExclusions.DefaultProgramNamePrefixes, new AlertsConfig().LongRunningQueryExcludedProgramNamePrefixes);
        Assert.Equal(LongRunningQueryExclusions.DefaultLogins, new AlertsConfig().LongRunningQueryExcludedLogins);
        Assert.Equal(LongRunningQueryExclusions.DefaultProgramNamePrefixes, AlertSettingsRow.Defaults().LongRunningQueryExcludedProgramNamePrefixes);
        Assert.Equal(LongRunningQueryExclusions.DefaultLogins, AlertSettingsRow.Defaults().LongRunningQueryExcludedLogins);

        /* The doc comment names the four classes the seeds come from and why the admin login is not one. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var doc = source[source.IndexOf("/// V135 —", StringComparison.Ordinal)..source.IndexOf("private const string V135Sql", StringComparison.Ordinal)];
        Assert.Contains("SQL Agent job-step programs", doc, StringComparison.Ordinal);
        Assert.Contains("NT AUTHORITY\\SYSTEM", doc, StringComparison.Ordinal);
        Assert.Contains("admin login", doc, StringComparison.Ordinal);
        Assert.Contains("named humans", doc, StringComparison.OrdinalIgnoreCase);

        /* No reload beacon of its own: config_alert_settings already carries V17's statement-level
           trg_bump_alert_settings, so a second trigger here would be a duplicate bump per write. */
        Assert.DoesNotContain("config_bump_version", rung, StringComparison.Ordinal);

        /* And no per-table GRANT: this table carries table-level grants with no column carve, which is what every
           earlier knob rung on it says. */
        Assert.DoesNotContain("GRANT", rung, StringComparison.Ordinal);
    }

    /// <summary>A PostgreSQL <c>ARRAY[…]::text[]</c> literal of the seeds, single-quoted — the form the rung spells.</summary>
    private static string SqlArray(System.Collections.Generic.IReadOnlyList<string> values) =>
        "ARRAY[" + string.Join(", ", values.Select(v => "'" + v.Replace("'", "''", StringComparison.Ordinal) + "'")) + "]::text[]";

    private static int CountOf(string haystack, string needle)
    {
        int count = 0, index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /* ---- the probe (three sites) --------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and a store that stopped here maps to it. The
    /// probe asks the question, the caller reads the answer, the map has the parameter — a sentinel present at
    /// only some of them shifts every LATER ordinal onto the wrong column. The top-arm claims (last argument,
    /// returns the build's version) moved to <c>PgDatabaseSizeAndHostMemoryRungTests</c> with V136.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains(
            $"table_name = 'config_alert_settings'\n                                                     AND   column_name = '{ProgramsColumn}'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasLrqExclusionKnob", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V136 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this rung's own literal — not the
           build's version: the "returns StorageVersion.SchemaVersion" half of the top-arm claim moved to V136's
           test with the top. */
        var thisArm = viewer.IndexOf("if (hasLrqExclusionKnob)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasTimeHonesty)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V135 sentinel arm — a store that stopped here would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V135 arm sits below the previous rung's, so a V135 store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table and columns are named in the probe line and nowhere in the arm's prose. */
        Assert.DoesNotContain("config_alert_settings", viewer[thisArm..previousArm], StringComparison.Ordinal);
        Assert.DoesNotContain(ProgramsColumn, viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /* ---- every settings-row surface handles both columns -------------------------------------------- */

    /// <summary>
    /// EVERY surface that reads or writes the settings row names BOTH columns. The wired lists drive ordinals
    /// or parameter positions, so a column added to one and not the others re-maps reads and writes at once;
    /// and a knob the service can read but the viewer cannot write is a knob a Settings-window Save silently
    /// nulls out.
    /// </summary>
    [Fact]
    public void EverySettingsRowSurfaceNamesBothColumns_TheViewerIncluded()
    {
        var service = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");
        var tools = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");

        /* The service must READ them, not merely select them — ApplyToConfig replaces config.Alerts wholesale,
           so a selected-but-unread column resets the knob to empty on every worker start and the excluded
           sessions page again while get_alert_settings still shows the list. Both halves, because one of the
           two being present is what an off-by-one produces. */
        Assert.Contains(ProgramsColumn, service, StringComparison.Ordinal);
        Assert.Contains(LoginsColumn, service, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedProgramNamePrefixes = ReadTextArray(reader, 67)", service, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedLogins = ReadTextArray(reader, 68)", service, StringComparison.Ordinal);
        /* And the seed writes them through the shared normaliser, so the store never holds a raw list. */
        Assert.Contains("AddTextArray(command, LongRunningQueryExclusions.Normalize(a.LongRunningQueryExcludedProgramNamePrefixes));", service, StringComparison.Ordinal);
        Assert.Contains("AddTextArray(command, LongRunningQueryExclusions.Normalize(a.LongRunningQueryExcludedLogins));", service, StringComparison.Ordinal);

        /* The MCP read names both columns and the report/accept pair carries both wire keys — readable AND
           writable, because a read-only knob leaves the UPDATE in someone's runbook. The accept arms pass the
           normaliser, so update_alert_settings stores the same canonical form the seed and the viewer do. */
        Assert.Contains(ProgramsColumn, DarlingAlertReader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains(LoginsColumn, DarlingAlertReader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("excluded_program_name_prefixes = s.LongRunningQueryExcludedProgramNamePrefixes", tools, StringComparison.Ordinal);
        Assert.Contains("excluded_logins = s.LongRunningQueryExcludedLogins", tools, StringComparison.Ordinal);
        Assert.Contains(
            $"case \"excluded_program_name_prefixes\": AddStringArray(\"{ProgramsColumn}\", n, \"long_running_query.excluded_program_name_prefixes\", LongRunningQueryExclusions.Normalize); break;",
            tools, StringComparison.Ordinal);
        Assert.Contains(
            $"case \"excluded_logins\": AddStringArray(\"{LoginsColumn}\", n, \"long_running_query.excluded_logins\", LongRunningQueryExclusions.Normalize); break;",
            tools, StringComparison.Ordinal);

        /* The viewer: its select reads both, its upsert WRITES both (or Save silently drops whatever the boxes
           held), and its reader maps the appended ordinals. */
        var viewerSettings = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs");
        Assert.Contains(ProgramsColumn, ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains(LoginsColumn, ViewerDataService.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains($"{ProgramsColumn} = EXCLUDED.{ProgramsColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains($"{LoginsColumn} = EXCLUDED.{LoginsColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        Assert.Contains("reader.GetFieldValue<string[]>(67)", viewerSettings, StringComparison.Ordinal);
        Assert.Contains("reader.GetFieldValue<string[]>(68)", viewerSettings, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Settings window's two boxes: prefilled from the row as comma-separated text, saved through the
    /// shared normaliser (so the viewer, the seed and the MCP writer store one canonical form), following the
    /// alerts master switch like every threshold box (named <c>Alert…Box</c> so the #1840 wiring census sees
    /// them), and Restore Defaults puts the SEEDS back — not empty boxes: the shipped state is the production
    /// read's two lists, and an emptied box is the operator clearing one.
    /// </summary>
    [Fact]
    public void TheSettingsWindowBoxes_PrefillSaveGateAndRestore_ThroughTheSharedNormaliser_ToTheSeeds()
    {
        var window = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");
        var xaml = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml");

        Assert.Contains("x:Name=\"AlertLrqExcludedProgramNamePrefixesBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"AlertLrqExcludedLoginsBox\"", xaml, StringComparison.Ordinal);
        /* The XAML states the two rules where the operator types, and the four classes beneath them. */
        Assert.Contains("STARTS WITH", xaml, StringComparison.Ordinal);
        Assert.Contains("whole name", xaml, StringComparison.Ordinal);
        Assert.Contains("admin login is NOT a default", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("trailing * matches any suffix", xaml, StringComparison.Ordinal);

        Assert.Contains("AlertLrqExcludedProgramNamePrefixesBox.Text = string.Join(\", \", r.LongRunningQueryExcludedProgramNamePrefixes);", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedLoginsBox.Text = string.Join(\", \", r.LongRunningQueryExcludedLogins);", window, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedProgramNamePrefixes = LongRunningQueryExclusions.Normalize(AlertLrqExcludedProgramNamePrefixesBox.Text.Split(',')).ToList(),", window, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedLogins = LongRunningQueryExclusions.Normalize(AlertLrqExcludedLoginsBox.Text.Split(',')).ToList(),", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedProgramNamePrefixesBox.IsEnabled = enabled;", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedLoginsBox.IsEnabled = enabled;", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedProgramNamePrefixesBox.Text = string.Join(\", \", LongRunningQueryExclusions.DefaultProgramNamePrefixes);", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedLoginsBox.Text = string.Join(\", \", LongRunningQueryExclusions.DefaultLogins);", window, StringComparison.Ordinal);
        Assert.DoesNotContain("AlertLrqExcludedProgramNamePrefixesBox.Text = \"\";", window, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer row round-trips through the bind at the APPENDED ordinals — the four-parallel-sequences trap
    /// (the column list, the upsert's $N placeholders, the bind order, and the reader ordinals), held for the one
    /// sequence no SQL text pin can see: the bind. And the bind NORMALISES: what reaches the store is the
    /// canonical list, whatever the row held.
    /// </summary>
    [Fact]
    public void TheViewerRow_RoundTripsThroughTheBind_AtTheAppendedOrdinals_Normalised()
    {
        var bind = typeof(ViewerDataService)
            .GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;

        var row = AlertSettingsRow.Defaults();
        /* Un-normalised on purpose: a duplicate in different case, padding and a blank — the bind must store the
           canonical form, or the viewer, the seed and the MCP writer disagree about one row. A "*" is a
           character (no wildcard grammar) and is kept as typed, so a misspelling stays visible on the card. */
        row.LongRunningQueryExcludedProgramNamePrefixes = new() { " QueueWorker ", "queueworker", "", "SQLAgent - TSQL JobStep" };
        row.LongRunningQueryExcludedLogins = new() { "svc_replication", "*" };

        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });

        /* The bind supplies exactly as many parameters as the upsert's highest placeholder. */
        var highestPlaceholder = System.Text.RegularExpressions.Regex
            .Matches(ViewerDataService.AlertSettingsUpsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .Max();
        Assert.Equal(command.Parameters.Count, highestPlaceholder);

        /* The two V135 columns ride at their APPENDED positions — the rule every knob rung on this table follows,
           so every earlier ordinal keeps its column — in the column list's order: programs, then logins. They
           were the END until #3712's code half appended the route knob's store column after them
           (UncorroboratedRouteStoreKnobTests pins that one at the end now); 67 and 68 are the V135 slots, and
           the equality is what would catch a rung inserted anywhere but after them. */
        Assert.Equal(new[] { "QueueWorker", "SQLAgent - TSQL JobStep" }, Assert.IsType<string[]>(command.Parameters[67].Value));
        Assert.Equal(new[] { "svc_replication", "*" }, Assert.IsType<string[]>(command.Parameters[68].Value));

        /* And the column list, the select and the upsert agree on the two names at the same two positions. */
        var columns = ViewerDataService.AlertSettingsSelectSql
            .Replace("SELECT ", "", StringComparison.Ordinal)
            .Split(" FROM ")[0]
            .Split(',')
            .Select(c => c.Trim())
            .ToList();
        Assert.Equal(ProgramsColumn, columns[67]);
        Assert.Equal(LoginsColumn, columns[68]);
        Assert.Equal(highestPlaceholder, columns.Count);
    }

    /// <summary>
    /// The MCP reader's positional record and its SELECT agree on the two appended ordinals — the same
    /// four-sequences trap on the MCP side, where the reader is a positional constructor call.
    /// </summary>
    [Fact]
    public void TheMcpReader_ReadsBothColumns_AtTheAppendedOrdinals()
    {
        var columns = DarlingAlertReader.AlertSettingsSelectSql
            .Split("FROM config_alert_settings")[0]
            .Replace("SELECT", "", StringComparison.Ordinal)
            .Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
        Assert.Equal(ProgramsColumn, columns[67]);
        Assert.Equal(LoginsColumn, columns[68]);
        /* At least the V135 pair and whatever appended after it (the #3712 route column is 69) — a count pinned
           to 69 asserted this rung's pair was the end of the SELECT, which stopped being true when that code half
           landed; the top-of-list claim moved with it. */
        Assert.True(columns.Count >= 69);

        var reader = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingAlertReader.cs");
        Assert.Contains("reader.GetFieldValue<string[]>(67)", reader, StringComparison.Ordinal);
        Assert.Contains("reader.GetFieldValue<string[]>(68)", reader, StringComparison.Ordinal);

        /* The record's positional members at 67 and 68 are the two lists, in column order — no longer its last
           two, for the reason one paragraph up. */
        var parameters = typeof(DarlingAlertReader.AlertSettingsReadRow).GetConstructors().Single().GetParameters();
        Assert.Equal("LongRunningQueryExcludedProgramNamePrefixes", parameters[67].Name);
        Assert.Equal("LongRunningQueryExcludedLogins", parameters[68].Name);
        Assert.Equal(columns.Count, parameters.Length);
    }

    /* ---- the seam reaches the engine -------------------------------------------------------------------- */

    /// <summary>
    /// The settings adapter forwards the config's two lists BY REFERENCE, like <c>ExcludedDatabases</c>, so the
    /// store's hot-reload (<c>StoreConfigProvider.ApplyToConfig</c> swaps <c>config.Alerts</c>) reaches the engine
    /// on its next sweep without reconstruction — and the engine, not the seam, normalises: the seam hands
    /// through what the store holds, which the seed and both writers already normalised. A fresh config reads as
    /// the SEEDS (what the seam returned before this rung, and what the rung's DEFAULT gives a pre-rung row), and
    /// a store row that cleared a list reads as empty — present-and-empty is honoured, not re-seeded.
    /// </summary>
    [Fact]
    public void TheSettingsSeam_ForwardsBothListsByReference_AndDefaultsToTheSeeds()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        Assert.Equal(LongRunningQueryExclusions.DefaultProgramNamePrefixes, settings.LongRunningQueryExcludedProgramNamePrefixes);
        Assert.Equal(LongRunningQueryExclusions.DefaultLogins, settings.LongRunningQueryExcludedLogins);

        config.Alerts.LongRunningQueryExcludedProgramNamePrefixes.Add("QueueWorker");
        config.Alerts.LongRunningQueryExcludedLogins.Clear();
        Assert.Equal(new[] { "SQLAgent - TSQL JobStep", "QueueWorker" }, settings.LongRunningQueryExcludedProgramNamePrefixes);
        Assert.Empty(settings.LongRunningQueryExcludedLogins);

        /* The wholesale swap is what the by-reference read is FOR: a new AlertsConfig replaces the lists — and a
           store row whose operator cleared the prefixes and kept one login is exactly that, not the seeds. */
        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = new AlertsConfig { LongRunningQueryExcludedProgramNamePrefixes = new(), LongRunningQueryExcludedLogins = new() { "etl_reader" } },
            Analysis = config.Analysis, Smtp = config.Smtp, Webhooks = config.Webhooks,
            NotificationRoutes = config.NotificationRoutes,
        });
        Assert.Empty(settings.LongRunningQueryExcludedProgramNamePrefixes);
        Assert.Equal(new[] { "etl_reader" }, settings.LongRunningQueryExcludedLogins);
    }

    /// <summary>
    /// The two SKUs' <c>get_alert_settings</c> spell the knob identically inside the <c>long_running_query</c>
    /// group — <c>McpAlertSettingsKeyTests</c> holds Lite's payload to Darling's shape at runtime; this is the
    /// source-side half for the day that WPF-hosted pin cannot run — and the Darling description says what an
    /// exclusion IS (not evaluated) and is NOT (a mute), on both the read and the write tool.
    /// </summary>
    [Fact]
    public void BothMcpPayloads_SpellTheKnobTheSameWay_AndTheDescriptionsNameWhatItIsNot()
    {
        var darling = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        var lite = RepoFile.ReadRepoFile("Lite", "Mcp", "McpAlertTools.cs");

        foreach (var source in new[] { darling, lite })
        {
            Assert.Contains("excluded_program_name_prefixes = ", source, StringComparison.Ordinal);
            Assert.Contains("excluded_logins = ", source, StringComparison.Ordinal);
            Assert.Contains("NOT EVALUATED", source, StringComparison.Ordinal);
            Assert.Contains("mute rule", source, StringComparison.Ordinal);
            /* The two rules, the four classes, the seeds' grounding and why the admin login is absent — in the
               read tool's description on both SKUs. No trace of the retired wildcard grammar. */
            Assert.Contains("STARTS WITH", source, StringComparison.Ordinal);
            Assert.Contains("no wildcard grammar", source, StringComparison.Ordinal);
            Assert.Contains("7-day read of one large production store", source, StringComparison.Ordinal);
            Assert.Contains("SQL Agent job steps", source, StringComparison.Ordinal);
            Assert.Contains("NT AUTHORITY\\\\SYSTEM", source, StringComparison.Ordinal);
            Assert.Contains("admin login", source, StringComparison.Ordinal);
            Assert.Contains("named humans", source, StringComparison.Ordinal);
            Assert.Contains("Excluded By Program Prefix", source, StringComparison.Ordinal);
            Assert.DoesNotContain("trailing * matches any suffix", source, StringComparison.Ordinal);
        }

        /* Darling's write tool describes the replace semantics and that an EMPTY array clears a default. */
        Assert.Contains("Pass the FULL list each time", darling, StringComparison.Ordinal);
        Assert.Contains("an EMPTY array clears that arm and is honoured", darling, StringComparison.Ordinal);
        Assert.DoesNotContain("a bare * is refused", darling, StringComparison.Ordinal);
    }
}
