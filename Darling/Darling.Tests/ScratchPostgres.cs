using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Darling.Tests;

/// <summary>
/// Mints an isolated scratch DATABASE on the DARLING_TEST_PG server for tests that seed the
/// singleton config rows. <c>StoreConfigProvider.SeedIfEmptyAsync</c> deliberately no-ops on a
/// store any earlier test already seeded, so running these tests against the one shared CI
/// database made them order-dependent (caught live: the V20 round-trip read the V3 defaults an
/// earlier test seeded). Each such test gets its own database instead — created here, dropped on
/// dispose with <c>WITH (FORCE)</c> so a lingering connection can never wedge the drop.
/// <c>Pooling=false</c> on the scratch connection string keeps in-process pooled connections from
/// pinning the database in the first place.
/// </summary>
internal sealed class ScratchPostgres : IAsyncDisposable
{
    private readonly string _adminConnectionString;

    public string DatabaseName { get; }

    public string ConnectionString { get; }

    /// <summary>
    /// The same store WITHOUT the session time-zone pin, so a session opened through it reports the database's own
    /// default zone. It exists for tests that must observe that default (e.g. to prove the product pin overrides it).
    /// </summary>
    public string UnpinnedConnectionString { get; }

    private ScratchPostgres(string adminConnectionString, string databaseName, string connectionString, string unpinnedConnectionString)
    {
        UnpinnedConnectionString = unpinnedConnectionString;
        _adminConnectionString = adminConnectionString;
        DatabaseName = databaseName;
        ConnectionString = connectionString;
    }

    public static async Task<ScratchPostgres> CreateAsync(string baseConnectionString, CancellationToken cancellationToken)
    {
        /* Hex-only generated name: safe to interpolate as a quoted identifier. */
        var databaseName = "darling_scratch_" + Guid.NewGuid().ToString("N")[..12];

        await using (var admin = new NpgsqlConnection(baseConnectionString))
        {
            await admin.OpenAsync(cancellationToken);
            await using var create = new NpgsqlCommand($"CREATE DATABASE \"{databaseName}\"", admin);
            await create.ExecuteNonQueryAsync(cancellationToken);
        }

        var builder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = databaseName,
            Pooling = false,
        };

        /* The base string can already carry the session pin (LiveStoreSessionTimeZone adds it to DARLING_TEST_PG),
           so the unpinned string drops it. */
        builder.Remove("Timezone");

        /* Pinned to UTC the way every product store connection is, so a cluster whose default zone is behind UTC
           cannot skew timestamp round trips (a chunk's range_end read back through a ::timestamp cast). */
        return new ScratchPostgres(
            baseConnectionString,
            databaseName,
            PerformanceMonitor.Darling.Storage.DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString),
            builder.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var admin = new NpgsqlConnection(_adminConnectionString);
            await admin.OpenAsync();
            await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{DatabaseName}\" WITH (FORCE)", admin);
            await drop.ExecuteNonQueryAsync();
        }
        catch
        {
            /* Best-effort: a leaked scratch database on a throwaway test cluster is harmless,
               and failing a passing test in its cleanup would invert the signal. */
        }
    }
}
