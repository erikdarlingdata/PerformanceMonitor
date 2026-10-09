using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A database file with the full Lite schema, built once per test process and copied to each test or test
/// class that needs a fresh one (#5208).
///
/// <para><b>Why.</b> <see cref="DuckDbInitializer.InitializeAsync"/> runs ~80 DDL statements while holding the
/// process-wide write lock (<c>DuckDbInitializer.s_dbLock</c> is static, one lock for every database file).
/// About 190 classes build one in a class fixture and about 100 more build one per test, so a suite run
/// queues every concurrent test's database read behind a hold of a third of a second on a fast laptop and
/// several seconds on a hosted runner. The tests that spend that wall time do almost no work: a one-test
/// class measured 62 s beside the others against 1.9 s alone, and every one of its per-row inserts took the
/// read lock behind the next init. Copying a file takes no lock, and <see cref="Adopt"/> takes the write
/// lock only to hand over a sentinel it opened before taking it.</para>
///
/// <para><b>What a copy is.</b> A byte copy of the file a real <c>InitializeAsync</c> built and closed,
/// with the empty <c>archive</c> folder next to it. The archive views it carries read the live table alone (no
/// Parquet file existed when they were built), so they hold no path and stay valid at any location. A test
/// that adds Parquet files rebuilds the views through the code under test, as before. The schema stamps are
/// copied too. The per-file <c>store_identity</c> row is copied with the file, then renewed by
/// <see cref="CopyTo"/> on the private copy, so every copy has its own identity as a freshly initialized store
/// does.</para>
/// </summary>
internal static class PreinitializedDuckDb
{
    private static readonly Lazy<string> s_templatePath = new(Build, LazyThreadSafetyMode.ExecutionAndPublication);

    private static string Build()
    {
        var dir = Path.Combine(Path.GetTempPath(), "LiteTests_template_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "test.duckdb");

        /* Closing the initializer checkpoints the file, so the copy below needs no WAL. */
        var initializer = new DuckDbInitializer(path);
        try { initializer.InitializeAsync().GetAwaiter().GetResult(); }
        finally { initializer.Dispose(); }

        /* The copy takes the main file only. A WAL left beside the template would hold committed work the
           copies would silently lack. */
        var wal = path + ".wal";
        if (File.Exists(wal))
        {
            throw new InvalidOperationException(
                $"The template database at {path} still has a WAL file after its initializer was disposed; "
                + "a copy of the main file alone would miss it.");
        }

        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            try { Directory.Delete(dir, recursive: true); }
            catch { /* best-effort cleanup */ }
        };
        return path;
    }

    /// <summary>
    /// Puts a copy of the initialized file at <paramref name="databasePath"/>, with its archive folder. The
    /// path must not hold a database yet. Takes no database lock.
    /// </summary>
    public static void CopyTo(string databasePath)
    {
        var template = s_templatePath.Value;
        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        File.Copy(template, databasePath, overwrite: false);
        RenewStoreIdentity(databasePath);
        Directory.CreateDirectory(Path.Combine(directory ?? ".", "archive"));
    }

    /// <summary>
    /// Gives the copy its own <c>store_identity</c> row, as a file a real <see cref="DuckDbInitializer.InitializeAsync"/>
    /// builds has: the template's row is copied with the file, so without this every copy would claim the same
    /// identity and a test that compares two stores (or the interrupted-reset recovery's marker check) would see
    /// them as one. The copy is a private file nothing else has open yet, so this needs no database lock. It used
    /// to run inside <see cref="DuckDbInitializer.AdoptInitializedFileForTests"/>'s write lock, about 40 ms of
    /// exclusive hold per copy (#5208). The connection closes before anything else opens the file, so the change
    /// is checkpointed into the main file.
    /// </summary>
    private static void RenewStoreIdentity(string databasePath)
    {
        using var connection = new DuckDBConnection($"Data Source={databasePath}");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "UPDATE store_identity SET id = CAST(uuid() AS VARCHAR)";
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// The copy of <see cref="DuckDbInitializer.InitializeAsync"/> a test uses when it wants a database with
    /// the current schema and nothing else: copies the initialized file to the initializer's own path and
    /// opens the initializer's sentinel on it.
    ///
    /// <para>It is exactly as safe to call again as <see cref="DuckDbInitializer.InitializeAsync"/> is. When
    /// the file is already there, the store exists and may hold the test's own rows, so this calls the real
    /// <c>InitializeAsync</c> on it (what every converted call site did before #5208): identity, migration and
    /// sentinel behavior on a repeat call stay what they were, and the identity is not renewed. Only a path
    /// with no file gets the copy. <see cref="CopyTo"/> still refuses an existing file, so any other caller
    /// fails loudly (#5612: a helper that initialized once per call threw "already exists" 38 times).</para>
    /// </summary>
    public static async Task InitializeFromTemplateAsync(this DuckDbInitializer duckDb)
    {
        if (File.Exists(duckDb.DatabasePath))
        {
            await duckDb.InitializeAsync();
            return;
        }

        CopyTo(duckDb.DatabasePath);
        Adopt(duckDb);
    }

    /// <summary>Opens <paramref name="duckDb"/>'s sentinel on a file <see cref="CopyTo"/> put at its path.</summary>
    public static void Adopt(DuckDbInitializer duckDb) => duckDb.AdoptInitializedFileForTests();
}
