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
/// V131 / #3598: <c>config.config_notification_routes</c>, the sparse routes table that lets alert families
/// land on different channels than the parent <c>config_notification</c> row's. The rung, its trigger, its
/// ACL, the column parity between the three readers/writers that name its columns, and the viewer probe's
/// top arm.
///
/// <para>The "I am the top rung" claims this class carried moved to <c>PerfmonCounterTypeRungTests</c> (V132)
/// when that rung landed, the same handoff this class received from <c>PgLogEventMetricsRungTests</c> (V130).
/// What stays here is the one-rung-behind half: a store carrying this and not V132 maps to 131, which is the
/// honest answer for it and what makes the upgrade banner correct in both directions.</para>
/// </summary>
public sealed class NotificationRoutesRungTests
{
    private const int RungVersion = 131;
    private const int PreviousVersion = 130;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V132 appended
    /// its own — so the invariant that outlives the handoff is that the ordinal is FIXED: a later rung
    /// appends after it and never shifts it.</summary>
    private const int ProbeOrdinal = 106;

    private const string Table = "config_notification_routes";

    private static readonly string[] DestinationColumns = { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key", "smtp_recipients" };

    private static PgMigrations.Migration V131 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("notification-routes", V131.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* One below the top since V132 landed; the "RungVersion == StorageVersion.SchemaVersion" half of
           the top-arm claim moved to PerfmonCounterTypeRungTests with the top. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion, "V131 is expected to sit below the ladder's top now that V132 has landed");
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The table mirrors the parent row's destination columns under the SAME names and the same
    /// <c>NOT NULL DEFAULT ''</c> shape, carries the presence column the carved roles read, is
    /// schema-qualified and idempotent, refuses a blank match, and takes no ACL or data decision.
    /// </summary>
    [Fact]
    public void TheRungCreatesTheTable_MirroringTheParentsDestinations_WithNoAclOrDataDecision()
    {
        var sql = V131.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"CREATE TABLE IF NOT EXISTS config.{Table} (", sql, StringComparison.Ordinal);
        Assert.Contains("route_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY", sql, StringComparison.Ordinal);
        Assert.Contains("metric_match text NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("enabled boolean NOT NULL DEFAULT TRUE", sql, StringComparison.Ordinal);
        Assert.Contains("modified_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
        Assert.Contains("CHECK (btrim(metric_match) <> '')", sql, StringComparison.Ordinal);

        /* Every destination column: the parent's name, the parent's shape, so "inherit" and "unset" are one
           state and the resolver has one test — and the parent's V17/V26/V42 DDL spells each the same way. */
        var parent = PgMigrations.Scripts.Where(m => m.Version is 17 or 26 or 42).Select(m => m.Sql).Aggregate((a, b) => a + b);
        foreach (var column in DestinationColumns)
        {
            Assert.Contains($"    {column} text NOT NULL DEFAULT '',", sql, StringComparison.Ordinal);
            Assert.Contains($"{column} text NOT NULL DEFAULT ''", parent, StringComparison.Ordinal);
        }

        /* The presence column names each channel by the fan-out's own spelling, one CASE per destination,
           and is STORED — a virtual generated column is not readable under PostgreSQL 18's column ACL any
           differently, but STORED is what every supported version accepts. */
        Assert.Contains("configured_channels text[] GENERATED ALWAYS AS (array_remove(ARRAY[", sql, StringComparison.Ordinal);
        Assert.Contains("], NULL)) STORED", sql, StringComparison.Ordinal);
        foreach (var (column, channel) in new[]
        {
            ("teams_url", NotificationRouter.TeamsChannel),
            ("slack_url", NotificationRouter.SlackChannel),
            ("generic_url", NotificationRouter.GenericChannel),
            ("pagerduty_routing_key", NotificationRouter.PagerDutyChannel),
            ("smtp_recipients", NotificationRouter.EmailChannel),
        })
        {
            Assert.Contains($"CASE WHEN {column} <> '' THEN '{channel}' END", sql, StringComparison.Ordinal);
        }

        Assert.Equal(1, CountOf(sql, "CREATE TABLE IF NOT EXISTS"));
        Assert.DoesNotContain("GRANT", sql, StringComparison.Ordinal);

        /* No data movement: the only statements are the CREATE TABLE and the trigger, and the trigger's
           `AFTER INSERT OR UPDATE OR DELETE` is the only place those verbs appear. */
        var table = sql[..sql.IndexOf("DROP TRIGGER", StringComparison.Ordinal)];
        foreach (var shape in new[] { "UPDATE ", "DELETE ", "INSERT ", "ALTER TABLE" })
        {
            Assert.DoesNotContain(shape, table, StringComparison.Ordinal);
        }

        Assert.Equal(1, CountOf(sql, "UPDATE "));
        Assert.Equal(1, CountOf(sql, "DELETE "));
        Assert.Equal(1, CountOf(sql, "INSERT "));
    }

    /// <summary>V117's trigger shape, verbatim: statement-level, all three verbs, the shared bump function,
    /// dropped first so a replay no-ops. Design point 5 — routes ride the same beacon.</summary>
    [Fact]
    public void TheRungInstallsTheReloadBeaconTrigger_InV117sShape()
    {
        var sql = V131.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var v117 = PgMigrations.Scripts.Single(m => m.Version == 117).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"DROP TRIGGER IF EXISTS trg_bump_notification_routes ON config.{Table};", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"CREATE TRIGGER trg_bump_notification_routes\n    AFTER INSERT OR UPDATE OR DELETE ON config.{Table}\n    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();",
            sql, StringComparison.Ordinal);

        /* The same three clauses V117 uses, so this is the sixth instance of one mechanism, not a new one. */
        Assert.Contains("AFTER INSERT OR UPDATE OR DELETE ON config.config_mute_rules\n    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();", v117, StringComparison.Ordinal);
    }

    /* ---- the ACL ------------------------------------------------------------------------------------- */

    /// <summary>
    /// The four webhook-class destinations are carved from the read-only roles exactly as they are on the
    /// parent row; the presence column and the recipient list are not. Every column the rung creates is
    /// classified one way or the other (the live security gate asserts the union against the table; this
    /// is the ungated half), and the two secret sets agree with the parent's for the shared names.
    /// </summary>
    [Fact]
    public void TheViewerCarve_ClassifiesEveryColumn_MatchingTheParentForSharedNames()
    {
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(a => a.Table == Table);
        var parent = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(a => a.Table == "config_notification");

        Assert.Equal(new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key" }, acl.SecretColumns);
        Assert.Equal(new[] { "route_id", "metric_match", "smtp_recipients", "configured_channels", "enabled", "modified_at" }, acl.NonSecretColumns);

        foreach (var column in DestinationColumns)
        {
            var secretHere = acl.SecretColumns.Contains(column);
            var secretOnParent = parent.SecretColumns.Contains(column);
            Assert.True(secretHere == secretOnParent, $"{column} is classified differently on the routes table and the parent row");
        }

        /* Every column in the DDL is in exactly one list. */
        var ddlColumns = new[] { "route_id", "metric_match" }.Concat(DestinationColumns).Concat(new[] { "configured_channels", "enabled", "modified_at" }).ToList();
        var classified = acl.SecretColumns.Concat(acl.NonSecretColumns).ToList();
        Assert.Equal(ddlColumns.OrderBy(c => c, StringComparer.Ordinal), classified.OrderBy(c => c, StringComparer.Ordinal));
        Assert.Equal(classified.Count, classified.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// The mcp role's two writes and no third — <c>UPDATE (enabled, modified_at)</c> and <c>DELETE</c>, never
    /// INSERT and never a destination column — in the managed provisioning DDL, and NOT in the BYO script
    /// (which provisions no mcp role, the asymmetry <c>ProvisionRolesAclDriftTests</c> pins). The viewer
    /// role gets no write at all: there is no web editor for routes, and a seat denied the URLs has nothing
    /// to author.
    /// </summary>
    [Fact]
    public void TheMcpRole_MayToggleAndDelete_AndMayNotAuthorOrRepoint()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql("a", "b", "c", 15);

        Assert.Contains($"GRANT UPDATE (enabled, modified_at), DELETE ON config.{Table} TO mcp;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"GRANT INSERT, UPDATE, DELETE ON config.{Table}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"GRANT INSERT ON config.{Table}", sql, StringComparison.Ordinal);
        /* No write of any kind reaches viewer on this table: the only viewer lines naming it are the carve. */
        var viewerLines = sql.Split('\n').Where(l => l.Contains($"config.{Table}", StringComparison.Ordinal) && l.Contains("viewer", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, viewerLines.Count);
        Assert.All(viewerLines, l => Assert.True(l.StartsWith("REVOKE SELECT", StringComparison.Ordinal) || l.StartsWith("GRANT SELECT (", StringComparison.Ordinal), l));

        /* The carve itself is emitted for both read-only roles. */
        Assert.Contains($"REVOKE SELECT ON config.{Table} FROM viewer;", sql, StringComparison.Ordinal);
        Assert.Contains($"REVOKE SELECT ON config.{Table} FROM mcp;", sql, StringComparison.Ordinal);

        var byo = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");
        Assert.Contains($"REVOKE SELECT ON config.{Table} FROM viewer;", byo, StringComparison.Ordinal);
        Assert.DoesNotContain("TO mcp", byo, StringComparison.Ordinal);
    }

    /// <summary>The MCP store never selects a destination column and writes only what the grant allows —
    /// the read that would 42501 in production is the one a superuser-run test never notices.</summary>
    [Fact]
    public void TheMcpStore_ReadsOnlyNonSecretColumns_AndWritesOnlyTheGrantedOnes()
    {
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(a => a.Table == Table);

        var selected = ColumnsOf(PgNotificationRouteStore.SelectSummariesSql, "SELECT", "FROM");
        Assert.NotEmpty(selected);
        Assert.All(selected, c => Assert.Contains(c, acl.NonSecretColumns));
        Assert.All(acl.SecretColumns, s => Assert.DoesNotContain(s, PgNotificationRouteStore.SelectSummariesSql));

        var set = ColumnsOf(PgNotificationRouteStore.SetEnabledSql.Replace("SET", "SELECT", StringComparison.Ordinal), "SELECT", "WHERE")
            .Select(c => c.Split('=')[0].Trim()).ToList();
        Assert.Equal(new[] { "enabled", "modified_at" }, set);
        Assert.StartsWith("DELETE FROM config_notification_routes WHERE route_id = $1", PgNotificationRouteStore.DeleteSql, StringComparison.Ordinal);
    }

    /* ---- column parity between the readers and the writers ------------------------------------------ */

    /// <summary>
    /// The service's reader, the viewer's reader/writer and the MCP store all name the table's columns
    /// independently — the Storage project cannot reference either of them — so they are pinned to one list
    /// here, the <c>NotificationColumns</c> idiom. The service's SELECT is the resolver's input, so its
    /// order is the <see cref="NotificationRoute"/> constructor's.
    /// </summary>
    [Fact]
    public void TheServiceReader_TheViewerWriter_AndTheDdl_NameTheSameColumnsInTheSameOrder()
    {
        var serviceSelect = ColumnsOf(StoreConfigProvider.NotificationRoutesSelectSql, "SELECT", "FROM");
        Assert.Equal(new[] { "route_id", "metric_match" }.Concat(DestinationColumns).Concat(new[] { "enabled" }), serviceSelect);
        Assert.EndsWith("ORDER BY route_id", StoreConfigProvider.NotificationRoutesSelectSql, StringComparison.Ordinal);

        /* The constructor's parameter order IS the SELECT's order, so a reader ordinal cannot re-map a column. */
        var ctor = typeof(NotificationRoute).GetConstructors().Single().GetParameters().Select(p => p.Name).ToArray();
        Assert.Equal(new[] { "RouteId", "MetricMatch", "TeamsUrl", "SlackUrl", "GenericUrl", "PagerDutyRoutingKey", "SmtpRecipients", "Enabled" }, ctor);

        var viewerSelect = ColumnsOf(ViewerDataService.NotificationRoutesSelectSql, "SELECT", "FROM");
        Assert.Equal(new[] { "route_id", "metric_match" }.Concat(DestinationColumns).Concat(new[] { "enabled", "modified_at", "configured_channels" }), viewerSelect);

        var insert = ColumnsOf(ViewerDataService.NotificationRouteInsertSql, "(", ")");
        Assert.Equal(new[] { "metric_match" }.Concat(DestinationColumns).Concat(new[] { "enabled", "modified_at" }), insert);
        Assert.DoesNotContain("route_id", insert);
        Assert.Contains("RETURNING route_id", ViewerDataService.NotificationRouteInsertSql, StringComparison.Ordinal);

        foreach (var column in DestinationColumns.Concat(new[] { "metric_match", "enabled", "modified_at" }))
        {
            Assert.Contains($"{column} = ", ViewerDataService.NotificationRouteUpdateSql, StringComparison.Ordinal);
        }

        /* The read-only projection names no secret. */
        var acl = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(a => a.Table == Table);
        foreach (var secret in acl.SecretColumns)
        {
            Assert.DoesNotContain(secret, ViewerDataService.NotificationRoutesSelectNoSecretSql, StringComparison.Ordinal);
        }

        /* Every statement resolves the bare name through search_path like its siblings. */
        foreach (var sql in new[]
        {
            StoreConfigProvider.NotificationRoutesSelectSql, ViewerDataService.NotificationRoutesSelectSql,
            ViewerDataService.NotificationRoutesSelectNoSecretSql, ViewerDataService.NotificationRouteInsertSql,
            ViewerDataService.NotificationRouteUpdateSql, ViewerDataService.NotificationRouteSetEnabledSql,
            ViewerDataService.NotificationRouteDeleteSql, PgNotificationRouteStore.SelectSummariesSql,
            PgNotificationRouteStore.SetEnabledSql, PgNotificationRouteStore.DeleteSql,
        })
        {
            Assert.Contains(Table, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("config." + Table, sql, StringComparison.Ordinal);
        }
    }

    /// <summary>The viewer's own validation is the store's CHECK plus the "routes nothing" rule, so a row the
    /// dialog accepts is one the store accepts and one the resolver can act on.</summary>
    [Fact]
    public void TheViewerValidation_RefusesABlankMatch_AndARouteWithNoDestination()
    {
        Assert.NotNull(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "  ", SlackUrl = "x" }));
        Assert.NotNull(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = AlertFamily.Reports }));
        Assert.Null(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = AlertFamily.Reports, SmtpRecipients = "a@example.invalid" }));
        Assert.Null(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "Deadlocks Detected", PagerDutyRoutingKey = "k" }));
    }

    /* ---- the probe (three sites, one rung behind the top) -------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map has an arm for it one rung behind
    /// the top. The probe asks the question, the caller reads the answer, the map has the parameter — a sentinel
    /// present at only some of them shifts every LATER ordinal onto the wrong column, and a missing arm maps a
    /// store that stopped here one rung short.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndAFullyMigratedStoreMapsToTheLaddersTop()
    {
        Assert.Contains($"table_name = '{Table}'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasNotificationRoutes", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung's sentinel sits strictly BELOW the last argument now that V132 has appended its own; the
           "is the last argument" claim moved to PerfmonCounterTypeRungTests with the top. */
        Assert.True(ProbeOrdinal < arity - 1, "V131's sentinel is expected to sit below the top rung's now that V132 has landed");

        /* Every sentinel true = a fully-migrated store, which must map to exactly the ladder's top. Stated
           against StorageVersion rather than this rung's number, so it survives every later rung. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns THIS rung's version. */
        var thisArm = viewer.IndexOf("if (hasNotificationRoutes)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf(PreviousArmSource, StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V131 sentinel arm — a store that stopped here would map to 130");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V131 arm sits below the previous rung's, so a store that stopped here maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>The previous rung's arm as the viewer spells it — V130's sentinel.</summary>
    private const string PreviousArmSource = "if (hasPgLogEventMetrics)";

    /* ---- the MCP surface ----------------------------------------------------------------------------- */

    /// <summary>The tool descriptions state the fall-through semantics in the words an agent will act on —
    /// empty means inherit, exact beats family, routing never decides whether — and the write tools say what
    /// they cannot do.</summary>
    [Fact]
    public void TheMcpDescriptions_StateTheFallThroughSemantics()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs");
        var read = source.IndexOf("[McpServerTool(Name = \"get_notification_routes\")", StringComparison.Ordinal);
        var enabled = source.IndexOf("[McpServerTool(Name = \"set_notification_route_enabled\")", StringComparison.Ordinal);
        var delete = source.IndexOf("[McpServerTool(Name = \"delete_notification_route\")", StringComparison.Ordinal);
        Assert.True(read >= 0 && enabled > read && delete > enabled);

        var readDescription = source[read..enabled];
        Assert.Contains("EMPTY channel on a route INHERITS the parent's", readDescription, StringComparison.Ordinal);
        Assert.Contains("cannot silence a channel the parent has", readDescription, StringComparison.Ordinal);
        Assert.Contains("matching the metric EXACTLY, then one matching its FAMILY, then the parent", readDescription, StringComparison.Ordinal);
        Assert.Contains("Routing sits AFTER the cooldown", readDescription, StringComparison.Ordinal);
        Assert.Contains("are NOT reported", readDescription, StringComparison.Ordinal);

        Assert.Contains("re-point a route: destinations are bearer secrets", source[enabled..delete], StringComparison.Ordinal);
        Assert.Contains("only the Viewer's Settings window can author", source[delete..], StringComparison.Ordinal);

        foreach (var family in AlertFamily.All)
        {
            Assert.False(string.IsNullOrWhiteSpace(DarlingMcpAlertTools.FamilyDescription(family)));
        }
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    private static List<string> ColumnsOf(string sql, string open, string close)
    {
        var start = sql.IndexOf(open, StringComparison.Ordinal) + open.Length;
        var end = sql.IndexOf(close, start, StringComparison.Ordinal);
        return sql[start..end].Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToList();
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}

/// <summary>
/// The live half of V131 (#3598), gated on <c>DARLING_TEST_PG</c>: the DDL parses and creates the generated
/// column; every write kind bumps the reload beacon; the service reader returns the row the viewer's writer
/// inserted; and the resolver, handed that list, routes the family to the route's destination. Serialized
/// against every other live class because it reads the shared store's beacon.
/// </summary>
[Collection("live-postgres")]
public sealed class NotificationRoutesLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task InsertRoute_BumpsTheBeacon_ReloadsThroughTheProvider_AndResolves_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the notification-routes round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await ExecAsync(connection, ct, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var marker = "https://hooks.example.invalid/routes-e2e-" + Guid.NewGuid().ToString("N");
        int routeId = 0;
        var bodySucceeded = false;
        try
        {
            var before = await BeaconAsync(connection, ct);

            /* The viewer's own INSERT statement, bound the way the viewer binds it. */
            using (var insert = new NpgsqlCommand(ViewerDataService.NotificationRouteInsertSql, connection))
            {
                insert.Parameters.AddWithValue(AlertFamily.SelfMonitor);
                insert.Parameters.AddWithValue("");
                insert.Parameters.AddWithValue(marker);
                insert.Parameters.AddWithValue("");
                insert.Parameters.AddWithValue("");
                insert.Parameters.AddWithValue("");
                insert.Parameters.AddWithValue(true);
                routeId = Convert.ToInt32(await insert.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            }

            var afterInsert = await BeaconAsync(connection, ct);
            Assert.True(afterInsert > before, "config_version must bump on a config_notification_routes INSERT (design point 5)");

            /* The generated presence column populated from the one destination. */
            using (var presence = new NpgsqlCommand("SELECT configured_channels FROM config.config_notification_routes WHERE route_id = $1", connection))
            {
                presence.Parameters.AddWithValue(routeId);
                using var reader = await presence.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(new[] { NotificationRouter.SlackChannel }, reader.GetFieldValue<string[]>(0));
            }

            /* The service's reader — the resolver's input — sees the row, and the resolver acts on it. */
            var provider = new StoreConfigProvider(postgres);
            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var route = Assert.Single(view!.NotificationRoutes, r => r.RouteId == routeId);
            Assert.Equal((AlertFamily.SelfMonitor, marker, true), (route.MetricMatch, route.SlackUrl, route.Enabled));

            var config = new DarlingConfig();
            StoreConfigProvider.ApplyToConfig(config, view);
            /* The parent channel AFTER the apply: ApplyToConfig swaps Webhooks wholesale for the store's row,
               and this rig's row is the unseeded default. The point under test is the routes list riding the
               same swap, which the Assert.Same below holds. */
            config.Webhooks.SlackUrl = "https://hooks.example.invalid/parent";
            var settings = new DarlingAlertSettings(config);
            Assert.Same(view.NotificationRoutes, settings.NotificationRoutes);

            var decision = NotificationRouter.Resolve(DarlingSelfAlertEvaluator.CompressionJobMetric, settings.NotificationRoutes, settings);
            Assert.Equal((marker, routeId, RouteSource.Family), (decision.Slack.Destination, decision.Slack.RouteId, decision.Slack.Source));
            Assert.Equal(RouteSource.Default, NotificationRouter.Resolve("Deadlocks Detected", settings.NotificationRoutes, settings).Slack.Source);

            /* The MCP store's non-secret read and its two writes, each bumping the beacon. */
            var mcpStore = new PgNotificationRouteStore(postgres);
            var summary = Assert.Single(await mcpStore.LoadSummariesAsync(ct), s => s.RouteId == routeId);
            Assert.Equal(new[] { NotificationRouter.SlackChannel }, summary.ConfiguredChannels);
            Assert.Equal(1, await mcpStore.SetEnabledAsync(routeId, false, ct));
            var afterUpdate = await BeaconAsync(connection, ct);
            Assert.True(afterUpdate > afterInsert, "config_version must bump on a routes UPDATE");
            Assert.Equal(1, await mcpStore.DeleteAsync(routeId, ct));
            var afterDelete = await BeaconAsync(connection, ct);
            Assert.True(afterDelete > afterUpdate, "config_version must bump on a routes DELETE — the direction that costs most");
            Assert.Equal(0, await mcpStore.DeleteAsync(routeId, ct));
            routeId = 0;

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand("DELETE FROM config.config_notification_routes WHERE slack_url = $1", cleanup);
                command.Parameters.AddWithValue(marker);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<long> BeaconAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
