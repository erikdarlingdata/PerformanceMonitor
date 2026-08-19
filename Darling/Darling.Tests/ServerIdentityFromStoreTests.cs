/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2218 step one: a monitored server's <c>server_id</c> comes from the STORE, and is derived in exactly one
/// place when there is nothing stored yet.
///
/// <para><b>What was wrong.</b> <c>config.config_monitored_servers.server_id</c> is that table's PRIMARY KEY
/// and is authoritative once seeded — but the read selected fifteen columns and not that one, so the registry's
/// own key was discarded and twelve downstream sites re-derived it from <c>host</c>, <c>database</c> and
/// <c>read_only_intent</c>: every operator-command lookup, the reconcile, the self-alert stamps, the schedule
/// resolution. Identity-derived-from-editable-config cannot be replaced while that is true, because a stored
/// surrogate is only worth anything if nothing re-derives it behind the store's back.</para>
///
/// <para><b>Behaviour is unchanged today and that is the point.</b> The seed and the Viewer both write exactly
/// the hash this used to recompute, so stored and derived are equal on every existing store and no data moves.
/// What changed is that the derivation is now the FALLBACK, in one property, so the later change is that
/// property rather than twelve call sites — see #2158 and #2228 for what it is a prerequisite for.</para>
///
/// <para>The live arm therefore does the one thing production cannot yet produce: a row whose stored
/// <c>server_id</c> DISAGREES with the hash of its own host. Without that, every assertion here would pass
/// just as well against the old code, since the two values coincide.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The one live test reaches DARLING_TEST_PG
   only to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it — it never
   touches the shared database's tables, so it cannot race the live collection and serializing every pure test
   here alongside it would be pure slowdown. Same shape and same reason as DarlingAlertTuningKnobsTests and
   DarlingDeliveryModeTests, which also pair pure pins with one scratch-store seed/read round-trip. Kept in one
   class rather than split, for that symmetry and because the split would leave a single-test file whose
   subject is the same seam the pure pins above cover. This comment is here so the next sweep does not "fix"
   it. */
public sealed class ServerIdentityFromStoreTests
{
    private static int Derived(string host, string? database = null, bool readOnlyIntent = false) =>
        ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent));

    /// <summary>A <c>darling.json</c> entry before the first seed has no store row, so identity is derived —
    /// which is what makes the seed able to mint it, and what keeps <c>--test-connection</c> working against a
    /// file on a host that has never started the service.</summary>
    [Theory]
    [InlineData("sql01", null, false)]
    [InlineData("myazure.database.windows.net", "AdventureWorks", false)]
    [InlineData("ag-listener", null, true)]
    public void WithNothingStored_IdentityIsTheDerivation(string host, string? database, bool readOnlyIntent)
    {
        var server = new MonitoredServer { Host = host, Database = database, ReadOnlyIntent = readOnlyIntent };

        Assert.Null(server.StoredServerId);
        Assert.Equal(Derived(host, database, readOnlyIntent), server.ServerId);
    }

    /// <summary>
    /// THE SEAM. A stored id wins over the derivation, and keeps winning when the fields the derivation reads
    /// change underneath it.
    ///
    /// <para>This is the property the whole change exists for: an operator repointing a server at a new host
    /// — the #2158 case, and what the use1 fleet did on 2026-08-09 — must not move the identity its collected
    /// history and its per-server config are keyed on. Note the assertion is not merely "stored equals
    /// ServerId": it is that ServerId is stable across an edit that changes the hash.</para>
    /// </summary>
    [Fact]
    public void AStoredIdWins_AndSurvivesAnEditThatChangesTheHash()
    {
        var server = new MonitoredServer { Host = "old-host", StoredServerId = 424242 };

        Assert.Equal(424242, server.ServerId);
        Assert.NotEqual(Derived("old-host"), server.ServerId);

        server.Host = "new-host";
        server.ReadOnlyIntent = true;
        server.Database = "someDb";

        /* The derivation moved. The identity did not. */
        Assert.NotEqual(Derived("old-host"), Derived("new-host", "someDb", true));
        Assert.Equal(424242, server.ServerId);
    }

    /// <summary>
    /// The store is the only authority for a stored id — <c>darling.json</c> cannot set one.
    ///
    /// <para>Deliberate rather than incidental: the registry is authoritative for identity once seeded, so a
    /// file that could pin <c>server_id</c> would be a second authority able to disagree with it, and disagree
    /// SILENTLY, because nothing downstream re-checks. Both spellings are tried because a reader guessing at
    /// the property name is exactly who would try this.</para>
    /// </summary>
    [Theory]
    [InlineData("""{"host":"sql01","storedServerId":999}""")]
    [InlineData("""{"host":"sql01","serverId":999}""")]
    [InlineData("""{"host":"sql01","server_id":999}""")]
    public void ADarlingJsonEntryCannotPinAnIdentity(string json)
    {
        var server = JsonSerializer.Deserialize<MonitoredServer>(json,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;

        Assert.Null(server.StoredServerId);
        Assert.Equal(Derived("sql01"), server.ServerId);
    }

    /// <summary>
    /// Nothing in the service re-derives identity any more. Three files, because between them they held every
    /// one of the twelve converted sites — the worker's lookups and stamps, the connect-time stamp the
    /// collectors inherit, and the MCP host's plan-fetch map.
    ///
    /// <para>A source pin rather than a behavioural one because the failure it guards is a NEW call site being
    /// added later, which no test of today's behaviour can see: a fresh
    /// <c>GetDeterministicHashCode(config.StorageName)</c> would agree with the stored id on every store that
    /// exists right now, and only start lying once ids stop being derivable — i.e. long after the commit that
    /// introduced it.</para>
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/DarlingServerConnector.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs")]
    public void TheServiceNoLongerDerivesIdentity(string relativePath)
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, "repo root not found -- the source pin cannot run");

        var offenders = File.ReadAllLines(Path.Combine(root!, relativePath))
            .Select((line, index) => (Line: line, Number: index + 1))
            /* Doc comments name the helper on purpose (the cadence-jitter doc explains what its input is). */
            .Where(l => !l.Line.TrimStart().StartsWith("///", StringComparison.Ordinal))
            .Where(l => l.Line.Contains("GetDeterministicHashCode(", StringComparison.Ordinal))
            .Select(l => $"{relativePath}:{l.Number}")
            .ToList();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The remaining derivations are the ALLOCATION sites, and they are exactly these. Pinned as a closed set
    /// so a new one has to be argued for in review rather than appearing: an allocation site is where identity
    /// is minted and made permanent, so an unnoticed extra one mints identities nothing else agrees with.
    /// </summary>
    [Fact]
    public void IdentityIsMintedInExactlyThreePlaces()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null, "repo root not found -- the source pin cannot run");

        var expected = new[]
        {
            /* The single fallback: MonitoredServer.ServerId. */
            "Darling/PerformanceMonitor.Darling.Service/DarlingConfig.cs",
            /* add_servers, which hashes a storage key string rather than a MonitoredServer. */
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.cs",
            /* The Viewer, which writes registry rows without ever building a MonitoredServer. */
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.MonitoredServers.cs",
        };

        var found = new[]
        {
            "Darling/PerformanceMonitor.Darling.Service",
            "Darling/PerformanceMonitor.Darling.Viewer",
            "Darling/PerformanceMonitor.Darling.Storage",
        }
            .SelectMany(dir => Directory.EnumerateFiles(Path.Combine(root!, dir), "*.cs", SearchOption.AllDirectories))
            .Where(path => !path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(path => File.ReadAllLines(path)
                .Any(line => !line.TrimStart().StartsWith("///", StringComparison.Ordinal)
                          && !line.TrimStart().StartsWith("*", StringComparison.Ordinal)
                          && line.Contains("GetDeterministicHashCode(", StringComparison.Ordinal)))
            .Select(path => Path.GetRelativePath(root!, path).Replace(Path.DirectorySeparatorChar, '/'))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected.OrderBy(p => p, StringComparer.Ordinal), found);
    }

    /* ---------------- live (DARLING_TEST_PG): the store's id is what the service uses ---------------- */

    /// <summary>
    /// The round-trip, against a row whose stored <c>server_id</c> is deliberately NOT the hash of its host —
    /// which is the only way to tell the new read from the old one, since production rows have the two equal.
    ///
    /// <para>It also asserts every other column the read projects, because <b>adding a column to a positional
    /// reader is exactly how a silent mis-map happens</b>: shift one ordinal and <c>auth</c> arrives in
    /// <c>username</c>, which no compiler and no id assertion would catch. Distinctive values per column, so a
    /// mis-map cannot coincide with a plausible default.</para>
    ///
    /// <para>Its own scratch database (<see cref="ScratchPostgres"/>) because <c>SeedIfEmptyAsync</c> no-ops on
    /// a store any earlier test already seeded.</para>
    /// </summary>
    [Fact]
    public async Task TheLoadedServerCarriesTheStoresOwnId_NotAFreshHash()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the server_id round-trip (the test mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new StoreConfigProvider(dataSource);

        var seeded = new MonitoredServer
        {
            Name = "identity-roundtrip",
            Host = "identity-host",
            Database = "identityDb",
            Auth = "sql",
            Username = "identity-user",
            EncryptedPassword = "not-a-real-blob",
            EncryptMode = "Strict",
            TrustServerCertificate = true,
            ReadOnlyIntent = true,
            MultiSubnetFailover = true,
            ExcludedDatabases = { "excluded-one", "excluded-two" },
            MonthlyCostUsd = 1234.56m,
            AlertDeliveryModeOverride = AlertNotificationMode.PerEvent,
            Engine = "postgres",
            Port = 6432,
        };

        var config = new DarlingConfig();
        config.Servers.Add(seeded);
        await provider.SeedIfEmptyAsync(config, ct);

        /* The seed writes the derivation, which is what makes every existing store migration-free. Assert it
           rather than assume it -- if this ever stops holding, the "no data moves" claim stops holding too. */
        /* #2218: derived from the SERVER'S OWN StorageName rather than from a re-statement of the rule here.
           This test previously hashed (host, database, readOnlyIntent) by hand, which silently stopped matching
           the moment the derivation grew the engine and port the seeded server actually carries — and the
           failure reads as "the seed wrote the wrong id" rather than "the test's copy of the rule is stale". */
        var seededId = ServerIdHelper.GetDeterministicHashCode(seeded.StorageName);
        Assert.Equal(seededId, await ReadServerIdByNameAsync(dataSource, "identity-roundtrip", ct));

        /* Now the thing production cannot produce yet: re-key the row so the stored id disagrees with the hash
           of its own host. Legal precisely because nothing references config_monitored_servers -- there is not
           one foreign key to it, which is separately why an edit orphans a server's config today (#2158). */
        const int Surrogate = 20260814;
        Assert.NotEqual(Surrogate, seededId);
        await using (var rekey = dataSource.CreateCommand(
            "UPDATE config_monitored_servers SET server_id = $1 WHERE name = $2"))
        {
            rekey.Parameters.AddWithValue(Surrogate);
            rekey.Parameters.AddWithValue("identity-roundtrip");
            Assert.Equal(1, await rekey.ExecuteNonQueryAsync(ct));
        }

        var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);
        var loaded = Assert.Single(view!.EnabledServers, s => s.Name == "identity-roundtrip");

        /* THE ASSERTION: the surrogate, not the hash. Old code returns the hash here. */
        Assert.Equal(Surrogate, loaded.StoredServerId);
        Assert.Equal(Surrogate, loaded.ServerId);
        Assert.NotEqual(Derived(loaded.Host, loaded.Database, loaded.ReadOnlyIntent), loaded.ServerId);

        /* Every other projected column, in the reader's own order, because that is what a shifted ordinal
           breaks. Each value is distinctive: a mis-map surfaces as a wrong value, not a plausible default. */
        Assert.Equal("identity-roundtrip", loaded.Name);
        Assert.Equal("identity-host", loaded.Host);
        Assert.Equal("identityDb", loaded.Database);
        Assert.Equal("sql", loaded.Auth);
        Assert.Equal("identity-user", loaded.Username);
        Assert.Equal("not-a-real-blob", loaded.EncryptedPassword);
        Assert.Equal("Strict", loaded.EncryptMode);
        Assert.True(loaded.TrustServerCertificate);
        Assert.True(loaded.ReadOnlyIntent);
        Assert.True(loaded.MultiSubnetFailover);
        Assert.Equal(new[] { "excluded-one", "excluded-two" }, loaded.ExcludedDatabases);
        Assert.Equal(1234.56m, loaded.MonthlyCostUsd);
        Assert.Equal(AlertNotificationMode.PerEvent, loaded.AlertDeliveryModeOverride);
        Assert.Equal("postgres", loaded.Engine);
        Assert.Equal(6432, loaded.Port);
    }

    private static async Task<int> ReadServerIdByNameAsync(
        NpgsqlDataSource dataSource, string name, System.Threading.CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT server_id FROM config_monitored_servers WHERE name = $1");
        command.Parameters.AddWithValue(name);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Walks up to the directory holding <c>PerformanceMonitor.sln</c> — the same idiom as
    /// <c>CollectorStateContractTests.FindRepoRoot</c>.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
