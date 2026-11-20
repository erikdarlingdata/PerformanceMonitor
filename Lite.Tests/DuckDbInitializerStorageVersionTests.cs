using System;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6523: a store that DuckDB 2.0 creates must stay readable by DuckDB 1.5, so a user who rolls back to an earlier Lite
/// keeps their history. With default settings 2.0 writes storage version 999, which 1.5.6.1 refuses ("we can only read
/// versions between 64 and 68"). The initializer's connection string asks for the 1.5 format, which is version 68.
/// </summary>
public sealed class DuckDbInitializerStorageVersionTests : IDisposable
{
    /// <summary>The newest storage version DuckDB 1.5.6.1 reads.</summary>
    private const ulong NewestStorageVersionDuckDb15Reads = 68;

    private readonly string _dir;

    public DuckDbInitializerStorageVersionTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir))
                Directory.Delete(_dir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>The header's storage version: a little-endian uint64 at byte offset 12, after the 4-byte checksum and the DUCK magic.</summary>
    private static ulong ReadStorageVersion(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var header = new byte[20];
        stream.ReadExactly(header, 0, header.Length);
        return BitConverter.ToUInt64(header, 12);
    }

    [Fact]
    public async Task NewStore_IsWrittenInAFormatDuckDb15CanOpen()
    {
        var path = Path.Combine(_dir, "monitor.duckdb");
        using (var initializer = new DuckDbInitializer(path))
        {
            await initializer.InitializeAsync();
        }

        Assert.True(File.Exists(path));
        var version = ReadStorageVersion(path);
        Assert.InRange(version, 1UL, NewestStorageVersionDuckDb15Reads);
    }
}
