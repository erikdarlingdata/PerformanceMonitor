using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5208: a copy of the prebuilt template (<see cref="PreinitializedDuckDb"/>) stands in for a store a real
/// <see cref="DuckDbInitializer.InitializeAsync"/> built, so it has to look like one where a test can see it.
/// The one per-file value is the <c>store_identity</c> row: a real initialization writes a new GUID for each
/// new store, and the copy carries the template's until the adoption step renews it.
/// </summary>
public sealed class PreinitializedDuckDbTests : IDisposable
{
    private readonly string _tempDir;

    public PreinitializedDuckDbTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_tpl_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string ReadIdentity(DuckDbInitializer duckDb)
    {
        using var connection = new DuckDBConnection(duckDb.ConnectionString);
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT id FROM store_identity";
        return Assert.IsType<string>(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task TwoCopiesOfTheTemplate_EachHaveTheirOwnStoreIdentity()
    {
        using var first = new DuckDbInitializer(Path.Combine(_tempDir, "first", "test.duckdb"));
        using var second = new DuckDbInitializer(Path.Combine(_tempDir, "second", "test.duckdb"));
        using var real = new DuckDbInitializer(Path.Combine(_tempDir, "real", "test.duckdb"));

        await first.InitializeFromTemplateAsync();
        await second.InitializeFromTemplateAsync();
        await real.InitializeAsync();

        var firstId = ReadIdentity(first);
        var secondId = ReadIdentity(second);
        var realId = ReadIdentity(real);

        Assert.True(Guid.TryParse(firstId, out _), "the renewed identity is a GUID, like a real initialization writes");
        Assert.NotEqual(firstId, secondId);
        Assert.NotEqual(firstId, realId);
        Assert.NotEqual(secondId, realId);
    }

    [Fact]
    public async Task ACopy_KeepsExactlyOneStoreIdentityRow()
    {
        using var copy = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        await copy.InitializeFromTemplateAsync();

        using var connection = new DuckDBConnection(copy.ConnectionString);
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM store_identity";
        Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    /* #5208: several converted helpers (ArchiveReadLimitTests.ExecuteAsync) initialize the same store once per
       call, and InitializeAsync is safe to repeat on an existing file. The template form has to be exactly as
       safe: the file already there is the store, so it is not copied over and its identity is not renewed. */
    [Fact]
    public async Task InitializeFromTemplateAsync_CalledTwice_KeepsTheStoreAndItsIdentity()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));

        await duckDb.InitializeFromTemplateAsync();
        var identityAfterFirst = ReadIdentity(duckDb);

        using (var connection = new DuckDBConnection(duckDb.ConnectionString))
        {
            connection.Open();
            using var write = connection.CreateCommand();
            write.CommandText = "CREATE TABLE second_call_probe (v INTEGER); INSERT INTO second_call_probe VALUES (7)";
            write.ExecuteNonQuery();
        }

        await duckDb.InitializeFromTemplateAsync();

        Assert.Equal(identityAfterFirst, ReadIdentity(duckDb));

        using var after = new DuckDBConnection(duckDb.ConnectionString);
        after.Open();
        using var read = after.CreateCommand();
        read.CommandText = "SELECT v FROM second_call_probe";
        Assert.Equal(7, Convert.ToInt32(read.ExecuteScalar()));
        using var insert = after.CreateCommand();
        insert.CommandText = "INSERT INTO second_call_probe VALUES (8)";
        Assert.Equal(1, insert.ExecuteNonQuery());
    }
}
