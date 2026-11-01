using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7097: <see cref="ScratchPostgres.CreateAsync"/> clones a template this process migrated once. A clone must behave
/// like the store a test used to build for itself, and a test that needs an unmigrated database must still get one.
/// </summary>
public sealed class ScratchPostgresTemplateLiveTests
{
    private static string? BaseConnectionString()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live store-template tests (they mint their own scratch databases).");
        return baseConnectionString;
    }

    private static async Task<T> ScalarAsync<T>(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string AdminOf(ScratchPostgres scratch) =>
        new NpgsqlConnectionStringBuilder(scratch.UnpinnedConnectionString) { Database = "postgres" }.ConnectionString;

    /// <summary>
    /// The state the 1,800 MigrateAsync call sites rely on: nothing left to apply, this build's newest rung in the
    /// journal, the database-default search_path set by the no-op call just as a real migrate sets it, and
    /// bare table names resolving on the next connection.
    /// </summary>
    [Fact]
    public async Task ADefaultStore_IsMigrated_AndItsMigrateAsyncIsANoOp_ThatStillSetsTheSearchPath()
    {
        var baseConnectionString = BaseConnectionString();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
        }

        Assert.Equal(
            PgMigrations.Scripts[^1].Version,
            await ScalarAsync<int>(scratch.ConnectionString, "SELECT max(version) FROM darling_schema_version", ct));
        Assert.Equal(
            (long)PgMigrations.Scripts.Count,
            await ScalarAsync<long>(scratch.ConnectionString, "SELECT count(*) FROM darling_schema_version", ct));
    }

    /// <summary>
    /// TimescaleDB starts a scheduler on CREATE EXTENSION, which a clone never runs. Without one, a job the test
    /// schedules on its store would silently never run.
    /// </summary>
    [Fact]
    public async Task ADefaultStore_HasTheSchedulerAFreshStoreHas_SoItsJobsRun()
    {
        var baseConnectionString = BaseConnectionString();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not available on this PostgreSQL.");

        var admin = AdminOf(scratch);
        var sql = $"SELECT count(*) FROM pg_stat_activity WHERE datname = '{scratch.DatabaseName}' AND backend_type = 'TimescaleDB Background Worker Scheduler'";
        var deadline = DateTime.UtcNow.AddSeconds(30);
        long schedulers;
        do
        {
            schedulers = await ScalarAsync<long>(admin, sql, ct);
            if (schedulers == 1)
            {
                break;
            }

            await Task.Delay(100, ct);
        }
        while (DateTime.UtcNow < deadline);

        Assert.Equal(1L, schedulers);
    }

    /// <summary>The opt-out: an unmigrated store really applies the whole ladder, and the plain path stays empty too.</summary>
    [Fact]
    public async Task AnEmptyStore_AppliesTheWholeLadder_AndPlainPostgresIsEmptyToo()
    {
        var baseConnectionString = BaseConnectionString();
        var ct = TestContext.Current.CancellationToken;

        await using (var plain = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, plainPostgres: true))
        {
            Assert.False(plain.TimescaleEnabled);
            Assert.False(await ScalarAsync<bool>(plain.ConnectionString, "SELECT to_regnamespace('collect') IS NOT NULL", ct));
        }

        await using var empty = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(empty.ConnectionString);
        await connection.OpenAsync(ct);
        Assert.Equal(PgMigrations.Scripts.Count, await PgMigrations.MigrateAsync(connection, ct));
    }

    /// <summary>
    /// A template nobody can connect to: a session in the source fails every clone, and the launcher must not start a
    /// scheduler there. A sweep in another process can drop it, and this process recovers. The base string names another
    /// database on the same cluster, so this test has a template of its own: dropping the one the tests running beside it
    /// clone would prove nothing more and slow them down.
    /// </summary>
    [Fact]
    public async Task TheTemplate_IsClosedAndSessionFree_AndALostTemplateIsRebuilt()
    {
        var baseConnectionString = BaseConnectionString();
        var ct = TestContext.Current.CancellationToken;
        var ownBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString);
        Assert.SkipWhen(ownBuilder.Database == "postgres", "DARLING_TEST_PG names the database this test needs as a second base.");
        ownBuilder.Database = "postgres";
        var ownBase = ownBuilder.ConnectionString;

        await using var first = await ScratchPostgres.CreateAsync(ownBase, ct);
        var template = await ScratchPostgres.GetTemplateAsync(ownBase, ct);
        var admin = AdminOf(first);
        Assert.True(ScratchPostgres.IsRemembered(template.Name));
        Assert.True(ScratchDatabaseSweep.IsFactoryName(template.Name));

        Assert.False(await ScalarAsync<bool>(admin, $"SELECT datallowconn FROM pg_database WHERE datname = '{template.Name}'", ct));
        Assert.False(await ScalarAsync<bool>(admin, $"SELECT datistemplate FROM pg_database WHERE datname = '{template.Name}'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(admin, $"SELECT count(*) FROM pg_stat_activity WHERE datname = '{template.Name}'", ct));

        /* The plain drop the sweep uses. */
        await using (var connection = new NpgsqlConnection(admin))
        {
            await connection.OpenAsync(ct);
            await using var drop = new NpgsqlCommand($"DROP DATABASE \"{template.Name}\"", connection);
            await drop.ExecuteNonQueryAsync(ct);
        }

        await using var second = await ScratchPostgres.CreateAsync(ownBase, ct);
        await using var migrate = new NpgsqlConnection(second.ConnectionString);
        await migrate.OpenAsync(ct);
        Assert.Equal(0, await PgMigrations.MigrateAsync(migrate, ct));

        var rebuilt = await ScratchPostgres.GetTemplateAsync(ownBase, ct);
        Assert.NotEqual(template.Name, rebuilt.Name);
    }
}
