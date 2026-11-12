using System.Text.Json;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Covers the M0 grains manifest fields (grain, source_table, read_from/read_to, daily_missing_servers,
/// change_key_columns, change_baseline, change_rows_suppressed, rollup_version, schema_fingerprint):
/// each is written only when set, following the existing policy_version pattern, so a core table's
/// entry (none of these set) is byte-identical to today's output.
/// </summary>
public class ManifestWriterM0FieldsTests
{
    private static ManifestTableEntry BaseEntry() => new()
    {
        Table = "some_table",
        OutputFile = "some_table/x.parquet",
        RowCount = 5,
        ByteSize = 100,
        Sha256 = "abc",
        Columns = [],
        StoreQueryElapsedMs = 1.0,
        WriteElapsedMs = 2.0,
        MissingContentCount = 0,
    };

    private static async Task<JsonElement> WriteAndReadTableEntry(ManifestTableEntry entry)
    {
        var manifest = new Manifest
        {
            Store = "test-store",
            FromUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            ToUtc = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc),
            ExporterVersion = "0.0.0-test",
            DarlingSchemaVersion = null,
            Tables = [entry],
        };

        var path = Path.Combine(Path.GetTempPath(), $"manifest-m0-{Guid.NewGuid()}.json");
        try
        {
            await ManifestWriter.WriteAsync(manifest, path, CancellationToken.None);
            var json = await File.ReadAllTextAsync(path);
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.GetProperty("tables")[0].Clone();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task AllFieldsSet_WritesAllKeysWithCorrectValues()
    {
        var readFrom = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var readTo = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);

        var entry = new ManifestTableEntry
        {
            Table = "some_table",
            OutputFile = "some_table/x.parquet",
            RowCount = 5,
            ByteSize = 100,
            Sha256 = "abc",
            Columns = [],
            StoreQueryElapsedMs = 1.0,
            WriteElapsedMs = 2.0,
            MissingContentCount = 0,
            Grain = "hourly",
            SourceTable = "raw_some_table",
            ReadFrom = readFrom,
            ReadTo = readTo,
            DailyMissingServers = 3,
            ChangeKeyColumns = ["id", "updated_at"],
            ChangeBaseline = true,
            ChangeRowsSuppressed = 42,
            RollupVersion = 7,
            SchemaFingerprint = "sha256:deadbeef",
        };

        var table = await WriteAndReadTableEntry(entry);

        Assert.Equal("hourly", table.GetProperty("grain").GetString());
        Assert.Equal("raw_some_table", table.GetProperty("source_table").GetString());
        Assert.Equal(readFrom.ToString("O"), table.GetProperty("read_from").GetString());
        Assert.Equal(readTo.ToString("O"), table.GetProperty("read_to").GetString());
        Assert.Equal(3, table.GetProperty("daily_missing_servers").GetInt64());
        Assert.Equal(new[] { "id", "updated_at" }, table.GetProperty("change_key_columns").EnumerateArray().Select(e => e.GetString()));
        Assert.True(table.GetProperty("change_baseline").GetBoolean());
        Assert.Equal(42, table.GetProperty("change_rows_suppressed").GetInt64());
        Assert.Equal(7, table.GetProperty("rollup_version").GetInt32());
        Assert.Equal("sha256:deadbeef", table.GetProperty("schema_fingerprint").GetString());
    }

    [Fact]
    public async Task NoFieldsSet_WritesNoneOfTheKeys()
    {
        var entry = BaseEntry();

        var table = await WriteAndReadTableEntry(entry);

        Assert.False(table.TryGetProperty("grain", out _));
        Assert.False(table.TryGetProperty("source_table", out _));
        Assert.False(table.TryGetProperty("read_from", out _));
        Assert.False(table.TryGetProperty("read_to", out _));
        Assert.False(table.TryGetProperty("daily_missing_servers", out _));
        Assert.False(table.TryGetProperty("change_key_columns", out _));
        Assert.False(table.TryGetProperty("change_baseline", out _));
        Assert.False(table.TryGetProperty("change_rows_suppressed", out _));
        Assert.False(table.TryGetProperty("rollup_version", out _));
        Assert.False(table.TryGetProperty("schema_fingerprint", out _));
        Assert.False(table.TryGetProperty("policy_version", out _));
    }

    [Fact]
    public async Task Manifest_carries_contract_version_as_its_first_key()
    {
        var manifest = new Manifest
        {
            Store = "test-store",
            FromUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            ToUtc = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc),
            ExporterVersion = "0.0.0-test",
            DarlingSchemaVersion = null,
            Tables = [BaseEntry()],
        };
        var optional = new Manifest
        {
            Store = manifest.Store, FromUtc = manifest.FromUtc, ToUtc = manifest.ToUtc, ExporterVersion = manifest.ExporterVersion,
            DarlingSchemaVersion = null, Tables = [BaseEntry()],
            SkippedOptional = [new SkippedOptionalTable { Table = "x", Reason = "absent" }],
            FailedOptional = [new FailedOptionalTable { Table = "y", Error = "boom" }],
        };

        foreach (var m in new[] { manifest, optional })
        {
            var path = Path.Combine(Path.GetTempPath(), $"manifest-cv-{Guid.NewGuid()}.json");
            try
            {
                await ManifestWriter.WriteAsync(m, path, CancellationToken.None);
                using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
                var first = doc.RootElement.EnumerateObject().First();
                Assert.Equal("contract_version", first.Name);
                Assert.Equal(1, first.Value.GetInt32());
                Assert.Equal(FileContract.Version, first.Value.GetInt32());
                Assert.True(doc.RootElement.TryGetProperty("store", out _));
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
