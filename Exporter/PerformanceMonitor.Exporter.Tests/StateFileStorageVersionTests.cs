using System;
using System.IO;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// #6523: the exporter's own state files must stay readable by DuckDB 1.5, so an older exporter can still read them. With
/// default settings DuckDB 2.0 writes storage version 999, which 1.5.6.1 refuses ("we can only read versions between 64 and
/// 68"). The state stores ask for the 1.5 format, which is version 68.
/// </summary>
public class StateFileStorageVersionTests
{
    /// <summary>The newest storage version DuckDB 1.5.6.1 reads.</summary>
    private const ulong NewestStorageVersionDuckDb15Reads = 68;

    /// <summary>The header's storage version: a little-endian uint64 at byte offset 12, after the 4-byte checksum and the DUCK magic.</summary>
    private static ulong ReadStorageVersion(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var header = new byte[20];
        stream.ReadExactly(header, 0, header.Length);
        return BitConverter.ToUInt64(header, 12);
    }

    [Fact]
    public void StateStore_WritesANewFileInAFormatDuckDb15CanOpen()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"darling-export-state-test-{Guid.NewGuid():N}");
        try
        {
            using (StateStore.Open(dir))
            {
            }

            var version = ReadStorageVersion(Path.Combine(dir, "exported_keys.duckdb"));
            Assert.InRange(version, 1UL, NewestStorageVersionDuckDb15Reads);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* Best-effort cleanup */ }
        }
    }

    [Fact]
    public void ChangeStateStore_WritesANewFileInAFormatDuckDb15CanOpen()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"darling-export-state-test-{Guid.NewGuid():N}");
        try
        {
            using (ChangeStateStore.Open(dir))
            {
            }

            var version = ReadStorageVersion(Path.Combine(dir, "change_state.duckdb"));
            Assert.InRange(version, 1UL, NewestStorageVersionDuckDb15Reads);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* Best-effort cleanup */ }
        }
    }
}
