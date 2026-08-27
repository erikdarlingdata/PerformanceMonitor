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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The V82 rung (#2530) — <c>collect.servers.engine_kind</c>. No longer the top of the ladder: V83 (#2539)
/// took that, and with it the top-of-ladder pins and the sliding <c>mapParameters - 1</c> probe ordinal.
///
/// <para><b>What it fixes.</b> The registry recorded <c>sql_engine_edition</c> and <c>sql_major_version</c>
/// and nothing that says a target is PostgreSQL. <c>SERVERPROPERTY</c> does not exist on PostgreSQL, so the
/// connector stamps edition <c>0</c> — which is byte-identical to a SQL Server that has never completed a
/// connect. Two different facts, one representation, and no reader could tell them apart: the fleet card, both
/// UIs' tab choice and the engine-KIND half of the #2511 capability answer all need to, and none of them could
/// have derived it.</para>
///
/// <para><b>A token rather than a boolean.</b> <c>is_postgres</c> cannot express Aurora, which the collectors
/// already gate on and which the eventual PostgreSQL panels have to gate on too (the <c>aurora_stat_*</c>
/// surface has no equivalent in any core PostgreSQL version). A boolean grows into a second boolean whose
/// meaning depends on the first; a token grows by appending a constant.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MonitoredEngineKindStoreTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int SqlServerRowId = -820011;
    private const int AuroraRowId = -820012;

    private const string SqlServerRowName = "engine-kind-e2e-sqlserver";
    private const string AuroraRowName = "engine-kind-e2e-aurora";

    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("server-engine-kind", PgMigrations.Scripts.Single(s => s.Version == 82).Name);

        /* V83 (#2539) took the top of the ladder, so this rung's own test no longer claims it. What it
           still owns is that 82 EXISTS, is ordered, and sits in the dense run — the invariants that keep a
           rung from being silently skipped. The top-of-ladder pins live with whichever rung is newest;
           leaving a stale copy here would fail every future rung's PR for a reason in someone else's file. */
        Assert.Contains(82, versions);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// One nullable column, no DEFAULT, no backfill. The operational half of that is V80's and V81's: nullable
    /// with no default is a catalog-only change in PostgreSQL. The SEMANTIC half is this rung's own — a
    /// <c>DEFAULT 'sqlserver'</c> would have every PostgreSQL target assert it is SQL Server for the window
    /// between the migration and its next connect, which is precisely the wrong claim to make by default. NULL
    /// says "no connect has stamped this", and every reader treats that as no claim.
    /// </summary>
    [Fact]
    public void TheRungAddsTheColumn_Idempotently_WithoutADefaultOrABackfill()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 82).Sql;

        Assert.Contains("ALTER TABLE collect.servers", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS engine_kind text", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);

        /* And no CHECK constraint: the vocabulary lives in MonitoredEngineKind, so growing it is a code change
           rather than a migration, and a store written by a NEWER service is described conservatively by an
           older reader instead of failing to accept the row at all. */
        Assert.DoesNotContain("CHECK", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Schema-qualified, which is not cosmetic: the migrate session's <c>search_path</c> puts <c>collect</c>
    /// first but a bare name is still a bet on that, and V8 moved these tables between schemas precisely
    /// because the bet was once lost.
    /// </summary>
    [Fact]
    public void TheRungIsSchemaQualified()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == 82).Sql;

        Assert.Contains("collect.servers", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER TABLE servers", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeAsksForTheColumn_AndTheThreePlacesAgree()
    {
        Assert.Contains(
            "table_name = 'servers' AND column_name = 'engine_kind'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewerSource = ReadViewerSource();

        /* Ordinals are positional and this rung's is FIXED at 57 now that V83 (#2539) is the newest — the
           `mapParameters - 1` form belongs to whichever rung is on top, because it slides one place right
           per new rung and would keep passing while quietly testing somebody else's wiring. */
        Assert.Contains("reader.GetBoolean(57)", viewerSource, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAStoreAtThisRungTo82()
    {
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* 57 positional sentinels, then this rung's own, then FALSE for anything a later rung appends. The
           leading count is FIXED at this rung's ordinal deliberately: deriving it from arity reads identically
           while this is the top rung, then slides one place right per new rung — the assertion keeps passing
           while quietly testing a newer arm. */
        var all = Enumerable.Repeat(true, 57).Cast<object>().ToArray();
        object[] Args(bool ownFlag) => all
            .Concat(new object[] { ownFlag })
            .Concat(Enumerable.Repeat((object)false, arity - 58))
            .ToArray();

        Assert.Equal(82, (int)method.Invoke(null, Args(true))!);
        Assert.Equal(81, (int)method.Invoke(null, Args(false))!);
    }

    /* ---------------- the write path ---------------- */

    /// <summary>
    /// The registry upsert writes the column on BOTH arms. The INSERT arm is obvious; the ON CONFLICT arm is
    /// the one worth pinning, because <c>is_enabled</c> deliberately is NOT written there (a control-plane
    /// disable must not be clobbered back to TRUE by a reconnect) and copying that reasoning to a PROBED fact
    /// would leave a re-pointed registration — same storage name, different engine — asserting the old engine
    /// forever.
    /// </summary>
    [Fact]
    public void TheRegistryUpsertWritesTheEngineKind_OnBothArms()
    {
        var sql = DarlingObservability.UpsertServerSql;

        Assert.Contains("engine_kind", sql, StringComparison.Ordinal);
        Assert.Contains("engine_kind = EXCLUDED.engine_kind", sql, StringComparison.Ordinal);

        /* The positional parameters have to stay in step with the column list — this is a $n INSERT and a
           miscount puts the timestamp in the engine column. $7 is bound twice
           (created_date/modified_date) and is_enabled is a literal TRUE, so placeholders are fewer than
           columns.

           What this arm owns is that engine_kind is still $6: V100 (#2653) appended
           postgres_major_version as $9, and appending is the ONLY safe way to add one here, because Npgsql
           binds positionally in add order and inserting a placeholder mid-list would silently shift this
           column's value to its neighbour's. The whole-VALUES literal this used to pin moved to
           PostgresMajorVersionRegistryTests, which asserts the same invariant without a literal that every
           future column has to edit. */
        Assert.Contains("VALUES ($1, $2, $3, TRUE, $4, $5, $6, $7, $7, $8", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The token a live target maps to. Total by construction — every target has a kind — so the upsert never
    /// has to decide whether to write NULL, which is what makes a NULL in the column mean exactly one thing.
    /// </summary>
    [Fact]
    public void EveryTargetShapeClassifies_AndAuroraIsItsOwnToken()
    {
        Assert.Equal(
            MonitoredEngineKind.SqlServer,
            MonitoredEngineKind.For(new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer }));

        /* Hosting flavour does NOT move the kind: RDS for SQL Server and Azure SQL Database run the same
           T-SQL against the same DMVs, which is why they ride on the edition and the flags instead. */
        Assert.Equal(
            MonitoredEngineKind.SqlServer,
            MonitoredEngineKind.For(new CollectorTargetInfo { IsAzureSqlDb = true, IsAwsRds = true }));

        Assert.Equal(
            MonitoredEngineKind.Postgres,
            MonitoredEngineKind.For(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));

        Assert.Equal(
            MonitoredEngineKind.AuroraPostgres,
            MonitoredEngineKind.For(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true }));

        /* Aurora is a PostgreSQL fact only — a SQL Server target carrying the flag is not a thing the
           connector produces, and if it somehow were, it must not become an Aurora token. */
        Assert.Equal(
            MonitoredEngineKind.SqlServer,
            MonitoredEngineKind.For(new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, IsAurora = true }));
    }

    /// <summary>
    /// Absence and unrecognised text both make NO claim, in BOTH directions. This is the assertion the whole
    /// design turns on: "not known to be SQL Server" is not "known to be PostgreSQL", and a store written by a
    /// newer build must be described conservatively rather than guessed at.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("mysql")]
    [InlineData("postgresql")] // deliberately NOT a token — the stored spelling is "postgres"
    public void AnAbsentOrUnrecognisedToken_MakesNoClaimEitherWay(string? token)
    {
        Assert.False(MonitoredEngineKind.IsPostgres(token));
        Assert.False(MonitoredEngineKind.IsSqlServer(token));
        Assert.False(MonitoredEngineKind.IsAurora(token));
        Assert.False(MonitoredEngineKind.IsKnown(token));
        Assert.Null(MonitoredEngineKind.EngineOf(token));
    }

    [Fact]
    public void TheTokensRoundTripThroughEveryPredicate()
    {
        Assert.True(MonitoredEngineKind.IsSqlServer(MonitoredEngineKind.SqlServer));
        Assert.False(MonitoredEngineKind.IsPostgres(MonitoredEngineKind.SqlServer));

        Assert.True(MonitoredEngineKind.IsPostgres(MonitoredEngineKind.Postgres));
        Assert.False(MonitoredEngineKind.IsAurora(MonitoredEngineKind.Postgres));

        Assert.True(MonitoredEngineKind.IsPostgres(MonitoredEngineKind.AuroraPostgres));
        Assert.True(MonitoredEngineKind.IsAurora(MonitoredEngineKind.AuroraPostgres));

        /* Case and surrounding whitespace decode, so a hand-edited row still reads. */
        Assert.True(MonitoredEngineKind.IsAurora("  Aurora-Postgres "));

        Assert.All(MonitoredEngineKind.All, token => Assert.True(MonitoredEngineKind.IsKnown(token)));
        Assert.Equal(MonitoredEngineKind.All.Count, MonitoredEngineKind.All.Distinct(StringComparer.Ordinal).Count());
    }

    /* ---------------- the fleet payload ---------------- */

    [Fact]
    public void TheFleetReadSelectsTheColumn_FromTheSameRegistryRow()
    {
        Assert.Contains("s.engine_kind", DarlingFleetReader.FleetServersSql, StringComparison.Ordinal);
        Assert.Contains("FROM servers s", DarlingFleetReader.FleetServersSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, false, false)]
    [InlineData("sqlserver", false, false)]
    [InlineData("postgres", true, false)]
    [InlineData("aurora-postgres", true, true)]
    public void TheFleetCardDerivesItsBooleans_FromTheStoredToken(string? token, bool expectPostgres, bool expectAurora)
    {
        var (isPostgres, isAurora) = DarlingFleetReader.ClassifyEngineKind(token);

        Assert.Equal(expectPostgres, isPostgres);
        Assert.Equal(expectAurora, isAurora);
    }

    /// <summary>
    /// The serialized contract, which is what <c>docs/uat-onboarding.md</c> §3.4 named as the blocker: the
    /// card the browser and <c>get_fleet_overview</c> both receive carries the raw token, the two derived
    /// booleans, and the token's DESCRIPTION. Raw because a consumer may want the vocabulary itself; derived
    /// because a UI that CHOOSES a tab set should not have to know the vocabulary's spelling; described
    /// because a UI that LABELS the engine should not have to own a second copy of the words — which the web
    /// server page briefly did, in three JavaScript string comparisons, before this field existed.
    ///
    /// <para>The unknown card is asserted alongside deliberately: <c>engine_kind</c> must serialize as JSON
    /// null with both booleans false, so a browser sees "no signal" rather than a default that reads as a
    /// claim. Same discipline the null <c>engine_edition</c> already follows.</para>
    /// </summary>
    [Fact]
    public void TheFleetCardSerializesTheDiscriminator_RawAndDerived()
    {
        var aurora = new FleetServerCard
        {
            ServerId = 1,
            DisplayName = "pg-01",
            ServerName = "pg-01",
            EngineKind = MonitoredEngineKind.AuroraPostgres,
            IsPostgres = true,
            IsAurora = true,
        };

        var auroraJson = System.Text.Json.JsonSerializer.Serialize(aurora, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"engine_kind\": \"aurora-postgres\"", auroraJson);
        JsonAssert.Contains("\"is_postgres\": true", auroraJson);
        JsonAssert.Contains("\"is_aurora\": true", auroraJson);
        JsonAssert.Contains("\"engine_description\": \"Aurora PostgreSQL\"", auroraJson);

        /* A PostgreSQL target has no SQL Server edition at all, and the card says so rather than reporting a
           zero that would read as an edition. */
        JsonAssert.Contains("\"engine_edition\": null", auroraJson);

        var unknown = new FleetServerCard { ServerId = 2, DisplayName = "new", ServerName = "new" };
        var unknownJson = System.Text.Json.JsonSerializer.Serialize(unknown, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"engine_kind\": null", unknownJson);
        JsonAssert.Contains("\"is_postgres\": false", unknownJson);
        JsonAssert.Contains("\"is_aurora\": false", unknownJson);
        /* NULL, not "an unrecognised engine". A surface showing this has nothing to say about a server whose
           engine was never stamped, and the describer's absent-token wording would read as a finding about the
           server rather than about the store. The distinction only exists because the DESCRIPTION is composed
           where the token is decoded; a browser mapping tokens itself would have had to invent it. */
        JsonAssert.Contains("\"engine_description\": null", unknownJson);

        /* And the third answer, which is neither of the two above: a token this build has never heard of — a
           store written by a NEWER build. The booleans stay false, because an unknown token is not a claim
           about either engine. The DESCRIPTION is the token itself rather than DescribeEngineKind's
           "an unrecognised engine": that phrase is worded to sit mid-sentence in the capability messages and
           reads as the wrong part of speech as a UI label, and the token is the more useful of the two anyway,
           being the string an operator would search their own store for. */
        var future = new FleetServerCard
        {
            ServerId = 3,
            DisplayName = "future",
            ServerName = "future",
            EngineKind = "cockroach",
        };
        var futureJson = System.Text.Json.JsonSerializer.Serialize(future, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"engine_kind\": \"cockroach\"", futureJson);
        JsonAssert.Contains("\"engine_description\": \"cockroach\"", futureJson);
        JsonAssert.Contains("\"is_postgres\": false", futureJson);
        JsonAssert.Contains("\"is_aurora\": false", futureJson);
    }

    /* ---------------- gated live E2E: the UPGRADE, not just a fresh store ---------------- */

    /// <summary>
    /// The rung as an operator meets it: a store that ALREADY HAS ROWS, wound back to the previous version and
    /// migrated forward. A fresh-store test cannot see any of this, because on a fresh store there is no
    /// pre-existing row for the column to be absent from.
    ///
    /// <para><b>The claim that matters.</b> A SQL Server row that predates the rung must read as NULL — no
    /// claim — and specifically must NOT read as PostgreSQL. Getting that backwards is not a cosmetic defect:
    /// it would make every historical row in every upgraded store answer <c>not_collected</c> to every SQL
    /// Server read, with a confident sentence naming the wrong engine, until its next connect re-stamped it.
    /// That is the #2511 defect reintroduced by the fix for it.</para>
    ///
    /// <para>The rewind deletes the stamp rows at AND above 82 — the applier reads MAX(version) and skips
    /// anything at or below it, so leaving a higher stamp would make the rewind a no-op that still asserted
    /// green — and drops only this one column. Every other rung's re-run is a no-op, which is what every
    /// migration being idempotent buys. The store is left fully migrated.</para>
    /// </summary>
    [Fact]
    public async Task AnExistingStore_ClimbsToTheRung_AndItsPreRungRowsReadAsNoClaim()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-kind upgrade test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* --- wind the ONE column back, and the stamps with it --- */
            await ExecuteAsync(connection, "ALTER TABLE collect.servers DROP COLUMN IF EXISTS engine_kind", ct);
            await ExecuteAsync(connection, "DELETE FROM collect.darling_schema_version WHERE version >= 82", ct);

            Assert.False(await ColumnExistsAsync(connection, ct));
            Assert.Equal(81, await CurrentVersionAsync(connection, ct));

            /* A row written by the PREVIOUS build: a real SQL Server, registered before the rung existed. It
               cannot carry an engine kind, because the column it would live in does not exist yet. */
            await InsertPreRungServerAsync(connection, ct, SqlServerRowId, SqlServerRowName, engineEdition: 3);

            /* --- the operator's upgrade --- */
            var applied = await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 82), applied);
            Assert.Equal(StorageVersion.SchemaVersion, await CurrentVersionAsync(connection, ct));
            Assert.True(await ColumnExistsAsync(connection, ct));

            /* THE assertion. The pre-rung row survived, kept its edition, and its engine kind is NULL — which
               every predicate reads as "no claim", and specifically not as PostgreSQL. */
            var (edition, kind) = await ReadServerEngineAsync(connection, ct, SqlServerRowId);
            Assert.Equal(3, edition);
            Assert.Null(kind);
            Assert.False(MonitoredEngineKind.IsPostgres(kind));
            Assert.False(MonitoredEngineKind.IsKnown(kind));

            /* And the column accepts what the upsert will write into it on the next connect. */
            await StampEngineKindAsync(connection, ct, SqlServerRowId, MonitoredEngineKind.SqlServer);
            await InsertPreRungServerAsync(connection, ct, AuroraRowId, AuroraRowName, engineEdition: 0);
            await StampEngineKindAsync(connection, ct, AuroraRowId, MonitoredEngineKind.AuroraPostgres);

            var (sqlEdition, sqlKind) = await ReadServerEngineAsync(connection, ct, SqlServerRowId);
            Assert.Equal(3, sqlEdition);
            Assert.True(MonitoredEngineKind.IsSqlServer(sqlKind));
            Assert.False(MonitoredEngineKind.IsPostgres(sqlKind));

            /* The Aurora row is the shape the whole rung exists for: engine edition 0 — identical to a server
               that has never connected — and an engine kind that says exactly what it is. */
            var (pgEdition, pgKind) = await ReadServerEngineAsync(connection, ct, AuroraRowId);
            Assert.Equal(0, pgEdition);
            Assert.True(MonitoredEngineKind.IsPostgres(pgKind));
            Assert.True(MonitoredEngineKind.IsAurora(pgKind));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* Re-migrate FIRST: a body that failed between the DROP and the MigrateAsync would otherwise
                   leave the shared store one rung short for every test after it. */
                await PgMigrations.MigrateAsync(cleanup, cleanupCt);
                await DeleteTestRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /* ---------------- helpers ---------------- */

    /// <summary>Column list copied verbatim from <c>EngineCapabilityMissTests.RegisterAsync</c>, MINUS the one
    /// column this rung adds — the point is a row a pre-rung build could have written.</summary>
    private static async Task InsertPreRungServerAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, int engineEdition)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, 15, $5, $5)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = EXCLUDED.sql_engine_edition;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(engineEdition);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task StampEngineKindAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string engineKind)
    {
        using var command = new NpgsqlCommand(
            "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Reads the two engine facts through the SHIPPED read the capability helper uses, not a
    /// retyped copy — a column that is selected but not mapped, or an ordinal off by one, is invisible to a
    /// query written here.</summary>
    private static async Task<(int Edition, string? Kind)> ReadServerEngineAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId)
    {
        using var command = new NpgsqlCommand(DarlingEngineCapability.ServerEngineSql, connection);
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(ct);

        Assert.True(await reader.ReadAsync(ct), $"no registry row for server_id {serverId}");
        var edition = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        var kind = reader.IsDBNull(1) ? null : reader.GetString(1);
        return (edition, kind);
    }

    private static async Task<bool> ColumnExistsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'servers' AND column_name = 'engine_kind'",
            connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
    }

    private static async Task<int> CurrentVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("SELECT COALESCE(MAX(version), 0) FROM collect.darling_schema_version", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM servers WHERE server_id IN ({SqlServerRowId}, {AuroraRowId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static string ReadViewerSource() =>
        System.IO.File.ReadAllText(System.IO.Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs"));

    private static string RepoRoot([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (System.IO.Directory.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.Common")))
            {
                return dir.FullName;
            }
        }

        throw new System.IO.DirectoryNotFoundException($"Could not locate the repo root walking up from {thisFile}");
    }
}
