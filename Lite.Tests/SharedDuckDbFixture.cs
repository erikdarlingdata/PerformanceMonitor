using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Class fixture that creates one DuckDB database with the full Lite schema per test
/// CLASS instead of per test. DuckDbInitializer.InitializeAsync runs ~80 DDL statements
/// (36 collector tables plus indexes, the archive views, and the analysis schema) and
/// dominated the analysis-heavy suite at roughly 1.4s per test. Tests share the class's
/// database and get pristine DATA via <see cref="ResetData"/> from the test class
/// constructor — xUnit runs tests within a class serially, so the shared database is
/// never touched concurrently. The schema and the schema_version /
/// analysis_schema_version stamps persist for the life of the class, so the database
/// keeps reading as current rather than as a blank file needing migration.
///
/// Deliberately IClassFixture (one database per class), NOT a collection fixture: a
/// single shared collection would serialize these classes against each other and give
/// back most of the win. Each class owns its own database file, so the SCHEMA-BUILD cost
/// — the ~80 DDL statements above, which is what this fixture exists to amortise — runs
/// concurrently across classes.
///
/// <para><b>What that does NOT buy (#2376).</b> The file separation is real, but it does
/// not make the classes independent at RUNTIME: <c>DuckDbInitializer</c>'s lock is
/// <c>private static readonly</c>, so it is ONE lock for the whole process no matter which
/// database file a call targets.</para>
///
/// <para>Be precise about what that costs, because the obvious reading overstates it. The
/// lock is a <c>ReaderWriterLockSlim</c>, so readers do NOT queue behind each other — any
/// number of <c>AcquireReadLock</c> holders run concurrently across classes, and this suite
/// is overwhelmingly readers (~184 read call sites against ~20 write sites). The
/// serialization is writer-driven: a writer excludes everyone, and readers block only while
/// a writer holds the lock or is waiting for it. The contention is therefore bursty around
/// the write sites — archival, compaction, CHECKPOINT, the mute and alert-history stores —
/// rather than a flat tax on every database call.</para>
///
/// <para>That distinction decides what a fix could even look like, and it rules out the
/// obvious one. A shared collection over the classes that use this fixture serializes the
/// READERS as well, which is where the wall clock goes, and it still would not bound the
/// contention: the test methods that hold this lock EXCLUSIVELY for seconds at a time — the
/// ones asserting on how a writer, a status-bar read or a cancellable read behaves while a
/// writer holds it — build their own <c>DuckDbInitializer</c> and are not classes of this
/// fixture at all, so a collection they are not in cannot stop them running alongside it.</para>
///
/// <para>That lock is correct and must not be narrowed to fix this: production creates
/// several <c>DuckDbInitializer</c> instances over the same <c>App.DatabasePath</c>
/// (MainWindow, DatabaseStateOverridesWindow, and DuckDbAlertHistoryStore), and the static
/// lock is exactly what keeps those mutually exclusive. Per-instance would trade a slow
/// suite for a real data race.</para>
///
/// <para>The practical consequence is worth knowing when a test here fails oddly: a
/// scheduling-pressure window can starve <c>LocalDataService.OpenWriteConnectionAsync</c>'s
/// acquisition past a budget sized for a UI thread, which is #2374 —
/// <c>GetDatabaseStateDeviationsAsync</c>'s maintenance block is best-effort and simply SKIPS
/// on timeout, and a test that assumed the maintenance had run rather than waiting for it
/// failed a nightly. This host therefore states its own budget
/// (<c>LocalDataService.WriteLockBudgetConfigKey</c> in <c>Lite.Tests.csproj</c>): the wait a
/// dispatcher cannot afford is one there is nothing here to protect, so an acquisition queued
/// behind a neighbour's deliberate hold waits it out instead of expiring inside it.</para>
/// </summary>
public sealed class SharedDuckDbFixture : IAsyncLifetime
{
    private readonly string _tempDir;
    private string[] _dataTables = [];

    public DuckDbInitializer DuckDb { get; }

    public SharedDuckDbFixture()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        DuckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public async ValueTask InitializeAsync()
    {
        await DuckDb.InitializeAsync();

        /* Snapshot the base tables present after a fresh initialization. ResetData deletes rows
           from exactly this set; the two version-stamp tables are excluded so the schema keeps
           reading as current instead of triggering the fresh-database migration path if a test
           ever re-runs InitializeAsync. */
        var tables = new List<string>();
        using var readLock = DuckDb.AcquireReadLock();
        using var connection = DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
AND   table_type = 'BASE TABLE'
AND   table_name NOT IN ('schema_version', 'analysis_schema_version', 'store_identity')";
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tables.Add(reader.GetString(0));
        }
        _dataTables = [.. tables];
    }

    /// <summary>
    /// Returns every data table to its post-initialization state (empty) so each test starts
    /// from pristine data on the shared schema. Called from the test class constructor, which
    /// xUnit runs before every test.
    /// </summary>
    public void ResetData()
    {
        using var readLock = DuckDb.AcquireReadLock();
        using var connection = DuckDb.CreateConnection();
        connection.Open();
        foreach (var table in _dataTables)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {table}";
            cmd.ExecuteNonQuery();
        }
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
        return default;
    }
}

/// <summary>
/// One explicit transaction around a test's per-row seed loop (#5208). DuckDB commits every auto-commit
/// statement to the WAL, and on a hosted CI disk a commit costs on the order of 0.2 s, so a few hundred
/// single-row INSERTs turned into minutes. Inside this batch they share one commit.
///
/// <para>The batch changes WHEN the rows commit, nothing else: the seeding code keeps taking the read lock
/// around each statement exactly as before, and so do BEGIN and COMMIT here. The batch must be committed
/// (<see cref="Commit"/> or dispose) BEFORE the code under test reads, because the code under test opens its
/// own connection and sees only committed rows. Commit on dispose is best-effort for the same reason
/// <c>TestDataSeeder</c>'s own batch is: if the seed threw, the test is already failing, and a commit error
/// must not mask that exception.</para>
/// </summary>
internal sealed class SeedBatch : IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly DuckDB.NET.Data.DuckDBConnection _connection;
    private bool _open;

    public SeedBatch(DuckDbInitializer duckDb, DuckDB.NET.Data.DuckDBConnection connection)
    {
        _duckDb = duckDb;
        _connection = connection;
        Run("BEGIN TRANSACTION");
        _open = true;
    }

    /// <summary>Commits now, so the rows are visible to the code under test. Safe to call twice.</summary>
    public void Commit()
    {
        if (!_open) return;
        _open = false;
        Run("COMMIT");
    }

    public void Dispose()
    {
        try { Commit(); }
        catch { /* the test is already failing; don't mask its exception */ }
    }

    private void Run(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}

/// <summary>
/// Seeding for tests that call a one-statement helper many times between reads (#5208). Each helper call used to open
/// its own connection and auto-commit one INSERT, which is one WAL commit (about 0.2 s on a hosted CI disk) per call.
/// Here every <see cref="ExecuteAsync"/> joins one open <see cref="SeedBatch"/>, and <see cref="Flush"/> commits it.
///
/// <para>The owner must call <see cref="Flush"/> BEFORE anything reads: the code under test opens its own connection
/// and sees only committed rows. The tests do that by building every <c>LocalDataService</c> through a method that
/// flushes first, so a seed that follows an act (seed, act, seed again) still commits before the next act reads.</para>
/// </summary>
internal sealed class PendingSeedSession : IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDB.NET.Data.DuckDBConnection? _connection;
    private SeedBatch? _batch;

    public PendingSeedSession(DuckDbInitializer duckDb) => _duckDb = duckDb;

    /// <summary>The session's one open connection, inside its open transaction. Callers must not dispose it.</summary>
    public async Task<DuckDB.NET.Data.DuckDBConnection> ConnectionAsync()
    {
        if (_connection == null)
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            _connection = connection;
            _batch = new SeedBatch(_duckDb, connection);
        }

        return _connection;
    }

    public async Task ExecuteAsync(string sql, params object?[] values)
    {
        var connection = await ConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Commits what has been seeded and closes the connection. Safe to call when nothing is pending.</summary>
    public void Flush()
    {
        if (_connection == null) return;
        try { _batch?.Commit(); }
        finally
        {
            _batch?.Dispose();
            _batch = null;
            _connection.Dispose();
            _connection = null;
        }
    }

    public void Dispose()
    {
        try { Flush(); }
        catch { /* the test is already failing; don't mask its exception */ }
    }
}
