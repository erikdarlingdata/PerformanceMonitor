using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
/// lock only to open the sentinel.</para>
///
/// <para><b>What a copy is.</b> A byte copy of the file a real <c>InitializeAsync</c> built and closed,
/// with the empty <c>archive</c> folder next to it. The archive views it carries read the live table alone (no
/// Parquet file existed when they were built), so they hold no path and stay valid at any location. A test
/// that adds Parquet files rebuilds the views through the code under test, as before. The schema stamps and
/// the per-file <c>store_identity</c> row are copied too; two copies share one identity, which only a test
/// that compares identities across files would notice, and none of the classes that use this does.</para>
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
        Directory.CreateDirectory(Path.Combine(directory ?? ".", "archive"));
    }

    /// <summary>
    /// The copy of <see cref="DuckDbInitializer.InitializeAsync"/> a test uses when it wants a database with
    /// the current schema and nothing else: copies the initialized file to the initializer's own path and
    /// opens the initializer's sentinel on it.
    /// </summary>
    public static Task InitializeFromTemplateAsync(this DuckDbInitializer duckDb)
    {
        CopyTo(duckDb.DatabasePath);
        Adopt(duckDb);
        return Task.CompletedTask;
    }

    /// <summary>Opens <paramref name="duckDb"/>'s sentinel on a file <see cref="CopyTo"/> put at its path.</summary>
    public static void Adopt(DuckDbInitializer duckDb) => duckDb.AdoptInitializedFileForTests();
}
