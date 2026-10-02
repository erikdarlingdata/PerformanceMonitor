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
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the CODE half of the #3712 uncorroborated-finding route knob's store home — the half the V137 rung
/// (<see cref="QsCaptureModeRouteKnobToastRungTests"/>, arm (b)) deliberately left for its own lane: the column
/// <c>config_alert_settings.analysis_uncorroborated_route</c> read by the service, resolved store-over-file by
/// <see cref="DarlingAlertSettings.ResolveUncorroboratedRoute"/>, published with its provenance by
/// <c>get_alert_settings</c>, written tri-state by <c>update_alert_settings</c>, and edited by the Viewer's
/// Settings window through the same select / upsert / bind / reader every knob rung on that row wires. The shape
/// is <see cref="LongRunningQueryExclusionKnobRungTests"/>' (V135, the freshest knob on this row) minus the rung
/// itself, plus the one thing no earlier knob had: TWO homes and a stated precedence between them.
///
/// <para><b>The precedence, once.</b> Store non-NULL wins over file; NULL in the store defers to darling.json's
/// <c>analysis.uncorroboratedRoute</c>; a file value that is neither spelling defers to the shipped
/// <c>digest</c>. The seed leaves the column NULL on purpose (so the third state is reachable on every store, not
/// just upgraded ones), the file half is CARRIED across the wholesale <c>config.Analysis</c> swap rather than
/// overwritten (so clearing the column hands a still-present value back to the file), and a store value the CHECK
/// would have refused is ignored and logged, never obeyed — the one failure a misspelled route could otherwise
/// cause is an operator's PAGE silently reading as the digest.</para>
///
/// <para>The MCP read/accept pair's shape and the writer's three arms are <c>DarlingMcpAlertToolsTests</c>'
/// (the file that already held the pre-rung pin they replace); Lite's deliberate omission of the provenance key
/// is <c>Lite.Tests.McpAlertSettingsKeyTests</c>'. This file is the seam, the surfaces, the Viewer, and the live
/// seed → read → reload round trip on a scratch store.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The one live test reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it — it never touches
   the shared database's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class UncorroboratedRouteStoreKnobTests
{
    private const string RouteColumn = "analysis_uncorroborated_route";

    /// <summary>The column's ordinal on every settings-row SELECT — appended after V135's two lists (67, 68).</summary>
    private const int Ordinal = 69;

    /* ---- the resolver -------------------------------------------------------------------------------- */

    /// <summary>
    /// The four resolutions, and the flag. A stored route wins whatever the file says; a NULL store falls to
    /// the file; a file with no usable route falls to the shipped digest under the name <c>default</c>; and a
    /// stored value that is neither spelling is IGNORED (the file governs) with the flag raised, so the caller
    /// with a logger can say so. Case-insensitive on both arms, the way <see cref="FindingRouting.TryParseRoute"/>
    /// is, because a hand-edited darling.json may well say <c>Page</c>.
    /// </summary>
    [Theory]
    [InlineData("page", "digest", FindingRoute.Page, DarlingAlertSettings.RouteSourceStore, false)]
    [InlineData("digest", "page", FindingRoute.Digest, DarlingAlertSettings.RouteSourceStore, false)]
    [InlineData("PAGE", null, FindingRoute.Page, DarlingAlertSettings.RouteSourceStore, false)]
    [InlineData(null, "page", FindingRoute.Page, DarlingAlertSettings.RouteSourceFile, false)]
    [InlineData(null, "Digest", FindingRoute.Digest, DarlingAlertSettings.RouteSourceFile, false)]
    [InlineData(null, null, FindingRoute.Digest, DarlingAlertSettings.RouteSourceDefault, false)]
    [InlineData(null, "sometimes", FindingRoute.Digest, DarlingAlertSettings.RouteSourceDefault, false)]
    [InlineData("pgae", "page", FindingRoute.Page, DarlingAlertSettings.RouteSourceFile, true)]
    [InlineData("pgae", null, FindingRoute.Digest, DarlingAlertSettings.RouteSourceDefault, true)]
    public void TheResolver_AppliesStoreOverFileOverDefault_AndFlagsAStoreValueItHadToIgnore(
        string? store, string? file, FindingRoute expected, string expectedSource, bool expectedIgnored)
    {
        var resolved = DarlingAlertSettings.ResolveUncorroboratedRoute(store, file);

        Assert.Equal(expected, resolved.Route);
        Assert.Equal(expectedSource, resolved.Source);
        Assert.Equal(expectedIgnored, resolved.StoreValueIgnored);
        Assert.Equal(FindingRouting.RouteText(expected), resolved.RouteText);
    }

    /// <summary>The three wire words are the three the MCP description and the Viewer prose name, and nothing
    /// else — pinned as literals because they are a consumer API (<c>analysis.uncorroborated_route_source</c>).</summary>
    [Fact]
    public void TheSourceWords_AreTheWireVocabulary()
    {
        Assert.Equal("store", DarlingAlertSettings.RouteSourceStore);
        Assert.Equal("file", DarlingAlertSettings.RouteSourceFile);
        Assert.Equal("default", DarlingAlertSettings.RouteSourceDefault);
    }

    /* ---- the seam reaches the engine -------------------------------------------------------------------- */

    /// <summary>
    /// The settings adapter resolves through the config's TWO members by reference, so the store's hot-reload
    /// (<see cref="StoreConfigProvider.ApplyToConfig"/> swaps <c>config.Analysis</c>) reaches the gate on its
    /// next evaluation without reconstruction — and the swap carries BOTH halves: a store-built section whose
    /// store half is set wins, one whose store half is NULL hands the decision to the file half it carried.
    /// </summary>
    [Fact]
    public void TheSettingsSeam_ResolvesThroughBothConfigMembers_AndTheSwapReachesIt()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        /* A fresh config: the file half is the shipped digest, the store half is unset. */
        Assert.Null(config.Analysis.StoreUncorroboratedRoute);
        Assert.Equal(FindingRouting.DigestText, config.Analysis.UncorroboratedRoute);
        Assert.Equal(FindingRoute.Digest, settings.UncorroboratedFindingRoute);

        /* The file alone moves it; the store, once set, wins over the file; clearing the store hands it back. */
        config.Analysis.UncorroboratedRoute = "page";
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);
        config.Analysis.StoreUncorroboratedRoute = "digest";
        Assert.Equal(FindingRoute.Digest, settings.UncorroboratedFindingRoute);
        config.Analysis.StoreUncorroboratedRoute = null;
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);

        /* The wholesale swap is what the by-reference read is FOR: a store-built AnalysisConfig replaces the
           section, and the same adapter instance reads the new pair. */
        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = config.Alerts,
            Analysis = new AnalysisConfig { UncorroboratedRoute = "page", StoreUncorroboratedRoute = "digest" },
            Smtp = config.Smtp, Webhooks = config.Webhooks, NotificationRoutes = config.NotificationRoutes,
        });
        Assert.Equal(FindingRoute.Digest, settings.UncorroboratedFindingRoute);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = config.Alerts,
            Analysis = new AnalysisConfig { UncorroboratedRoute = "page", StoreUncorroboratedRoute = null },
            Smtp = config.Smtp, Webhooks = config.Webhooks, NotificationRoutes = config.NotificationRoutes,
        });
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);
    }

    /// <summary>
    /// The store half has exactly one writer — the store read — and darling.json is not it. <c>JsonIgnore</c> is
    /// the guard: <c>DarlingConfig.Parse</c> is case-insensitive, so without it a file carrying
    /// <c>storeUncorroboratedRoute</c> would populate a member that is supposed to mean "what the store holds",
    /// giving the knob two authors for one home and no tiebreak (the #3314 by-halves shape). Asserted on the
    /// attribute AND by parsing, because the attribute is the mechanism and the parse is the fact.
    /// </summary>
    [Fact]
    public void TheStoreHalf_CannotBeWrittenFromDarlingJson()
    {
        var property = typeof(AnalysisConfig).GetProperty(nameof(AnalysisConfig.StoreUncorroboratedRoute))!;
        Assert.NotNull(property.GetCustomAttribute<JsonIgnoreAttribute>());
        Assert.Null(property.GetCustomAttribute<JsonPropertyNameAttribute>());

        var config = DarlingConfig.Parse(@"{
  ""postgres"": { ""connectionString"": ""Host=localhost;Database=darling"" },
  ""analysis"": { ""uncorroboratedRoute"": ""page"", ""storeUncorroboratedRoute"": ""digest"", ""StoreUncorroboratedRoute"": ""digest"" }
}");
        Assert.Equal("page", config.Analysis.UncorroboratedRoute);
        Assert.Null(config.Analysis.StoreUncorroboratedRoute);
        Assert.Equal(FindingRoute.Page, new DarlingAlertSettings(config).UncorroboratedFindingRoute);
    }

    /* ---- every settings-row surface handles the column ------------------------------------------------ */

    /// <summary>
    /// EVERY surface that reads the settings row names the column at the SAME appended ordinal, and the one
    /// surface that must NOT name it — the seed — does not. The wired lists drive ordinals or parameter
    /// positions, so a column added to one and not the others re-maps reads and writes at once; a knob the
    /// service can read but the viewer cannot write is a knob a Settings-window Save silently nulls out; and a
    /// seed that wrote the file's value into the column would make the store win from the first start on every
    /// install, leaving the tri-state's third state reachable on no store.
    /// </summary>
    [Fact]
    public void EverySettingsRowSurfaceNamesTheColumn_AtTheAppendedOrdinal_AndTheSeedDoesNot()
    {
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

        /* The service READS it into the store half, not merely selects it — ApplyToConfig replaces
           config.Analysis wholesale, so a selected-but-unread column resets the store half to NULL on every
           worker start and an operator's PAGE reads as the file's digest. */
        var serviceSelectStart = service.IndexOf("ReadAlertSettingsAsync(NpgsqlConnection connection", StringComparison.Ordinal);
        Assert.True(serviceSelectStart > 0, "the service's alert-settings read could not be located");
        var serviceSelect = service[serviceSelectStart..];
        serviceSelect = serviceSelect[(serviceSelect.IndexOf("SELECT", StringComparison.Ordinal) + 6)..serviceSelect.IndexOf("FROM config_alert_settings WHERE id = 1", StringComparison.Ordinal)];
        var serviceColumns = serviceSelect.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
        Assert.Equal(RouteColumn, serviceColumns[Ordinal]);
        Assert.Equal(Ordinal + 1, serviceColumns.Count);
        Assert.Contains($"StoreUncorroboratedRoute = reader.IsDBNull({Ordinal}) ? null : reader.GetString({Ordinal}),", service, StringComparison.Ordinal);
        /* The carry survived the column: the file half still crosses the swap from the held config. */
        Assert.Contains("analysis.UncorroboratedRoute = bootstrap.Analysis.UncorroboratedRoute;", service, StringComparison.Ordinal);
        /* And the one caller with a logger says so when it had to ignore a stored value. */
        Assert.Contains("if (resolved.StoreValueIgnored)", service, StringComparison.Ordinal);
        Assert.Contains("_logger?.LogWarning(", service, StringComparison.Ordinal);
        Assert.Contains("which is neither 'digest' nor 'page'", service, StringComparison.Ordinal);

        /* The SEED does not name the column: its INSERT's column list is captured the way
           ConfigSeedStatementArityTests captures it, and the column is absent from it. */
        var seed = Regex.Match(service, @"INSERT\s+INTO\s+config_alert_settings\s*\((?<cols>[^)]*)\)", RegexOptions.Singleline);
        Assert.True(seed.Success, "the alert-settings seed INSERT could not be located");
        Assert.DoesNotContain(RouteColumn, seed.Groups["cols"].Value, StringComparison.Ordinal);
        Assert.Contains("long_running_query_excluded_logins", seed.Groups["cols"].Value, StringComparison.Ordinal);

        /* The MCP read names it LAST, its positional record's last member is the raw column, and the
           report/accept pair carries the wire key both ways — the payload through the resolver, the writer
           through the tri-state adder. */
        var mcpColumns = DarlingAlertReader.AlertSettingsSelectSql
            .Split("FROM config_alert_settings")[0]
            .Replace("SELECT", "", StringComparison.Ordinal)
            .Split(',')
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
        Assert.Equal(RouteColumn, mcpColumns[Ordinal]);
        Assert.Equal(Ordinal + 1, mcpColumns.Count);
        var parameters = typeof(DarlingAlertReader.AlertSettingsReadRow).GetConstructors().Single().GetParameters();
        Assert.Equal("AnalysisUncorroboratedRoute", parameters[Ordinal].Name);
        Assert.Equal(typeof(string), parameters[Ordinal].ParameterType);
        Assert.Equal(Ordinal + 1, parameters.Length);
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingAlertReader.cs");
        Assert.Contains($"reader.IsDBNull({Ordinal}) ? null : reader.GetString({Ordinal}));", reader, StringComparison.Ordinal);

        var tools = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        Assert.Contains("uncorroborated_route = UncorroboratedRoute(s).RouteText,", tools, StringComparison.Ordinal);
        Assert.Contains("uncorroborated_route_source = UncorroboratedRoute(s).Source,", tools, StringComparison.Ordinal);
        Assert.Contains("DarlingAlertSettings.ResolveUncorroboratedRoute(s.AnalysisUncorroboratedRoute, DarlingFileLevelAlertSettings.UncorroboratedRoute)", tools, StringComparison.Ordinal);
        Assert.Contains($"AddNullableRoute(\"{RouteColumn}\", n, \"analysis.uncorroborated_route\");", tools, StringComparison.Ordinal);

        /* The viewer: its select reads it LAST, its upsert WRITES it (or Save silently drops the combo's
           choice), and its reader maps the appended ordinal. */
        var viewerColumns = ViewerDataService.AlertSettingsSelectSql
            .Replace("SELECT ", "", StringComparison.Ordinal)
            .Split(" FROM ")[0]
            .Split(',')
            .Select(c => c.Trim())
            .ToList();
        Assert.Equal(RouteColumn, viewerColumns[Ordinal]);
        Assert.Equal(Ordinal + 1, viewerColumns.Count);
        Assert.Contains($"{RouteColumn} = EXCLUDED.{RouteColumn}", ViewerDataService.AlertSettingsUpsertSql, StringComparison.Ordinal);
        var viewerSettings = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.AlertSettings.cs");
        Assert.Contains($"AnalysisUncorroboratedRoute = reader.IsDBNull({Ordinal}) ? null : reader.GetString({Ordinal}),", viewerSettings, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer row round-trips through the bind at the APPENDED (last) position — the four-parallel-sequences
    /// trap (the column list, the upsert's $N placeholders, the bind order, and the reader ordinals), held for
    /// the one sequence no SQL text pin can see: the bind. And the bind NORMALISES to the tri-state the CHECK
    /// admits: a route in any case lands lower-case, null lands as a typed text NULL, and a value that is neither
    /// lands as NULL too — "the file governs" — rather than as a 23514 the operator cannot see past.
    /// </summary>
    [Theory]
    [InlineData("Page", "page")]
    [InlineData("digest", "digest")]
    [InlineData(null, null)]
    [InlineData("pgae", null)]
    public void TheViewerRow_RoundTripsThroughTheBind_AtTheEnd_Normalised(string? held, string? stored)
    {
        var bind = typeof(ViewerDataService).GetMethod("BindAlertSettings", BindingFlags.NonPublic | BindingFlags.Static)!;

        var row = AlertSettingsRow.Defaults();
        row.AnalysisUncorroboratedRoute = held;

        using var command = new NpgsqlCommand();
        bind.Invoke(null, new object[] { command, row });

        /* The bind supplies exactly as many parameters as the upsert's highest placeholder, and the route is the
           last of them — appended, the rule every knob rung on this table follows. */
        var highestPlaceholder = Regex.Matches(ViewerDataService.AlertSettingsUpsertSql, @"\$(\d+)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .Max();
        Assert.Equal(command.Parameters.Count, highestPlaceholder);
        Assert.Equal(Ordinal + 1, highestPlaceholder);

        var last = command.Parameters[^1];
        Assert.Equal(NpgsqlTypes.NpgsqlDbType.Text, last.NpgsqlDbType);
        Assert.Equal(stored is null ? DBNull.Value : stored, last.Value);
    }

    /// <summary>
    /// The row's default for the store half is NULL — "the file governs", the upgrade-day state — not
    /// <c>digest</c>: a <see cref="AlertSettingsRow.Defaults"/> that held <c>digest</c> would make the migrate-in
    /// (<c>ShouldImportAlerts</c> asks "is the store section untouched") and Restore Defaults both write the
    /// route INTO the store, silently moving the decision off the file for every operator who pressed it.
    /// <see cref="AlertSettingsRow.ValueEquals"/> tells the three states apart and two spellings of one route
    /// not (the reflection census in <c>AlertSettingsRowValueEqualsTests</c> holds that the member is compared
    /// at all; this holds HOW).
    /// </summary>
    [Fact]
    public void TheViewerRowDefault_IsNull_AndValueEquals_TellsTheThreeStatesApart()
    {
        Assert.Null(AlertSettingsRow.Defaults().AnalysisUncorroboratedRoute);

        var stored = AlertSettingsRow.Defaults();
        stored.AnalysisUncorroboratedRoute = "digest";
        Assert.False(AlertSettingsRow.Defaults().ValueEquals(stored));

        var page = AlertSettingsRow.Defaults();
        page.AnalysisUncorroboratedRoute = "page";
        Assert.False(stored.ValueEquals(page));

        var cased = AlertSettingsRow.Defaults();
        cased.AnalysisUncorroboratedRoute = "Digest";
        Assert.True(stored.ValueEquals(cased));
    }

    /// <summary>
    /// The Settings window's combo: three items whose Tags ARE the stored values (<c>digest</c>, <c>page</c>,
    /// and the empty Tag for NULL), prefilled from the row through the same parse the service applies, saved as
    /// the selected Tag, and Restore Defaults lands on the NULL item — the store column cleared — for the reason
    /// the row's default is null. The XAML states the precedence where the operator reads it, and names the MCP
    /// key that reports which home decided.
    /// </summary>
    [Fact]
    public void TheSettingsWindowCombo_PrefillsSavesAndRestores_ThroughTheThreeStates()
    {
        var window = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs");
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml");

        Assert.Contains("x:Name=\"AnalysisUncorroboratedRouteBox\"", xaml, StringComparison.Ordinal);
        Assert.Contains($"Tag=\"{FindingRouting.DigestText}\"", xaml, StringComparison.Ordinal);
        Assert.Contains($"Tag=\"{FindingRouting.PageText}\"", xaml, StringComparison.Ordinal);
        Assert.Contains("darling.json value (store column cleared)\" Tag=\"\"", xaml, StringComparison.Ordinal);
        /* The prose: what a lone finding is, that the store wins and when, and how to hand it back. */
        Assert.Contains("ONE fact in its chain and no matched co-fire check", xaml, StringComparison.Ordinal);
        Assert.Contains("win over darling.json's analysis.uncorroboratedRoute", xaml, StringComparison.Ordinal);
        Assert.Contains("within one collection sweep", xaml, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route_source", xaml, StringComparison.Ordinal);
        Assert.Contains("Escalation is the corroboration event", xaml, StringComparison.Ordinal);

        Assert.Contains("SelectAnalysisUncorroboratedRoute(r.AnalysisUncorroboratedRoute);", window, StringComparison.Ordinal);
        Assert.Contains("AnalysisUncorroboratedRoute = SelectedAnalysisUncorroboratedRoute(),", window, StringComparison.Ordinal);
        Assert.Contains("SelectAnalysisUncorroboratedRoute(null);", window, StringComparison.Ordinal);
        /* The prefill parses through the service's own parser, so the combo and the gate agree about a value. */
        Assert.Contains("FindingRouting.TryParseRoute(stored) is { } route ? FindingRouting.RouteText(route) : \"\"", window, StringComparison.Ordinal);
        /* And Restore Defaults never pins a route into the store. */
        Assert.DoesNotContain("SelectAnalysisUncorroboratedRoute(\"digest\")", window, StringComparison.Ordinal);
        Assert.DoesNotContain("SelectAnalysisUncorroboratedRoute(FindingRouting.DigestText)", window, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prose that pointed at the file alone now points at the store first: the daily digest's footer, the
    /// sample config, and the README's knob row. Each is a place an operator reads to learn how to turn paging
    /// back on, and each said "edit darling.json and restart" — true before V137 and a lie after it.
    /// </summary>
    [Fact]
    public void TheOperatorProse_NamesTheStoreHomeFirst()
    {
        var evaluator = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");
        Assert.Contains("Viewer's Settings > Automated Analysis or through update_alert_settings (analysis.uncorroborated_route;", evaluator, StringComparison.Ordinal);
        Assert.Contains("the store column wins over darling.json's analysis.uncorroboratedRoute", evaluator, StringComparison.Ordinal);
        Assert.DoesNotContain("to 'page' in darling.json.", evaluator, StringComparison.Ordinal);

        var sample = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "darling.sample.json");
        Assert.Contains("This key is the FILE half of a", sample, StringComparison.Ordinal);
        Assert.Contains($"config_alert_settings.{RouteColumn} column", sample, StringComparison.Ordinal);
        Assert.DoesNotContain("FILE-LEVEL like web.publicBaseUrl", sample, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains("**The file half of a two-home knob** since V137", readme, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route_source", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("`update_alert_settings` refuses it by name; edit the file and restart", readme, StringComparison.Ordinal);
        Assert.DoesNotContain("Nothing reads any of the eight yet", readme, StringComparison.Ordinal);

        /* The V137 rung doc and the shim both still describe the FILE as what NULL defers to — neither claims
           the knob is file-level any more. */
        var shim = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFileLevelAlertSettings.cs");
        Assert.Contains("The knob has TWO homes since V137", shim, StringComparison.Ordinal);
        Assert.DoesNotContain("has no column", shim, StringComparison.Ordinal);
    }

    /* ---- live (DARLING_TEST_PG): seed → read → reload on a scratch store --------------------------------- */

    /// <summary>
    /// The round trip the pins above can only describe: on a freshly-migrated scratch store the seed leaves the
    /// column NULL (the file's <c>page</c> is NOT copied in), the read carries the file half across and reads the
    /// store half as NULL, and the resolver says the FILE decided; a route written into the column — the way the
    /// Viewer's upsert or <c>update_alert_settings</c> writes it — bumps <c>config_version</c> through V17's
    /// trigger (the beacon the worker polls every 15 s) and the next read resolves to the STORE; clearing it back
    /// to NULL hands the decision to the still-present file value; and with the V137 CHECK dropped by hand, a
    /// misspelling in the column is read, IGNORED, and logged at Warning naming the value — the file governs,
    /// and nothing turned the operator's page into a digest in silence.
    /// </summary>
    [Fact]
    public async Task SeedLeavesTheColumnNull_TheReadResolvesStoreOverFile_AndGarbageIsLoggedNotObeyed_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the seed/read round-trip (the test mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        /* SeedIfEmptyAsync no-ops on a store any earlier test already seeded, so this test needs a database of
           its own — see ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var logger = new CapturingTestLogger();
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new StoreConfigProvider(dataSource, logger);

        /* darling.json says page. The seed must NOT carry that into the column. */
        var config = new DarlingConfig();
        config.Analysis.UncorroboratedRoute = "page";
        config.Servers.Add(new MonitoredServer { Name = "v137-route", Host = "v137-scratch-host", Auth = "integrated" });
        await provider.SeedIfEmptyAsync(config, ct);

        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        Assert.Equal(DBNull.Value, await ScalarAsync(store, $"SELECT {RouteColumn} FROM config.config_alert_settings WHERE id = 1", ct));

        /* First read: store half NULL, file half carried, the file decides. Applied to the live config, the
           seam reads page. */
        var view = await provider.LoadViewAsync(config, ct);
        Assert.NotNull(view);
        Assert.Null(view!.Analysis.StoreUncorroboratedRoute);
        Assert.Equal("page", view.Analysis.UncorroboratedRoute);
        var resolved = DarlingAlertSettings.ResolveUncorroboratedRoute(view.Analysis.StoreUncorroboratedRoute, view.Analysis.UncorroboratedRoute);
        Assert.Equal(DarlingAlertSettings.RouteSourceFile, resolved.Source);
        StoreConfigProvider.ApplyToConfig(config, view);
        var settings = new DarlingAlertSettings(config);
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);
        Assert.DoesNotContain("neither 'digest' nor 'page'", logger.Joined, StringComparison.Ordinal);

        /* An operator sets digest in the store: the write bumps the beacon, the next read resolves to the store,
           and the SAME adapter instance reads the swapped section. The bootstrap passed to the reload is the
           LIVE config (whose Analysis is now the store-built one) — exactly what the worker passes — so the
           carry chain is exercised, not just the first load. */
        var versionBefore = await provider.ReadConfigVersionAsync(ct);
        await ExecAsync(store, $"UPDATE config.config_alert_settings SET {RouteColumn} = 'digest' WHERE id = 1", ct);
        var versionAfter = await provider.ReadConfigVersionAsync(ct);
        Assert.True(versionAfter.HasValue && versionBefore.HasValue && versionAfter.Value > versionBefore.Value,
            $"config_version should self-bump on a config_alert_settings write (before {versionBefore}, after {versionAfter})");

        view = await provider.LoadViewAsync(config, ct);
        Assert.Equal("digest", view!.Analysis.StoreUncorroboratedRoute);
        Assert.Equal("page", view.Analysis.UncorroboratedRoute);
        StoreConfigProvider.ApplyToConfig(config, view);
        Assert.Equal(FindingRoute.Digest, settings.UncorroboratedFindingRoute);
        Assert.Equal(DarlingAlertSettings.RouteSourceStore,
            DarlingAlertSettings.ResolveUncorroboratedRoute(config.Analysis.StoreUncorroboratedRoute, config.Analysis.UncorroboratedRoute).Source);

        /* Cleared back to NULL: the file's page is still there to govern, two reloads later. */
        await ExecAsync(store, $"UPDATE config.config_alert_settings SET {RouteColumn} = NULL WHERE id = 1", ct);
        view = await provider.LoadViewAsync(config, ct);
        Assert.Null(view!.Analysis.StoreUncorroboratedRoute);
        Assert.Equal("page", view.Analysis.UncorroboratedRoute);
        StoreConfigProvider.ApplyToConfig(config, view);
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);

        /* The CHECK is the store's guard; if someone removes it, the reader is the last one. A misspelling is
           read, ignored, logged with the value — and the file's page still governs. */
        await ExecAsync(store, "ALTER TABLE config.config_alert_settings DROP CONSTRAINT config_alert_settings_analysis_uncorroborated_route_check", ct);
        await ExecAsync(store, $"UPDATE config.config_alert_settings SET {RouteColumn} = 'pgae' WHERE id = 1", ct);
        view = await provider.LoadViewAsync(config, ct);
        Assert.Equal("pgae", view!.Analysis.StoreUncorroboratedRoute);
        StoreConfigProvider.ApplyToConfig(config, view);
        Assert.Equal(FindingRoute.Page, settings.UncorroboratedFindingRoute);
        Assert.Contains("Warning: ", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("holds 'pgae', which is neither 'digest' nor 'page'", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("the file route 'page' governs", logger.Joined, StringComparison.Ordinal);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
