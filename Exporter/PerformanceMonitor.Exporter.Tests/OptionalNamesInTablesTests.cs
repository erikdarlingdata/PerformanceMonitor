using System.Text.Json.Nodes;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// An optional-registry name given to `--tables` takes the optional path: the table's column policy, its schema gate
/// and the optional manifest shape are the ones an `--also` run gets. Runs against a throwaway PostgreSQL (Docker)
/// laid out like a Darling store; with no Docker each test returns without asserting, like the other Docker-backed
/// suites.
/// </summary>
[Trait("Category", "Slow")]
public sealed class OptionalNamesInTablesTests : IClassFixture<OptionalNamesInTablesTests.StoreFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);

    public sealed class StoreFixture : IDisposable
    {
        public GenericRawTableEquivalenceTests.GenericPgFixture Pg { get; } = new();
        public string Root { get; } = Directory.CreateTempSubdirectory("darling-export-tables-opt-").FullName;
        private string PasswordFile => Path.Combine(Root, "pw.txt");

        public StoreFixture()
        {
            if (Pg.Builder is null) { return; }
            File.WriteAllText(PasswordFile, Pg.Builder.Password);
            // analysis_findings has a Drop policy (finding_id); pg_log_events has a keep-only policy and a schema gate
            // the store's version never reaches. The chunk guard reads the TimescaleDB catalog view, which a plain
            // PostgreSQL lacks, so an empty stand-in stands in for it.
            var result = Pg.Psql("""
                CREATE TABLE collect.darling_schema_version (version int);
                INSERT INTO collect.darling_schema_version VALUES (150);
                CREATE TABLE collect.analysis_findings (server_name text, collection_time timestamp, analysis_time timestamp, finding_id bigint, finding_type text, severity text);
                INSERT INTO collect.analysis_findings VALUES ('srv1', '2026-10-22 11:30:00', '2026-10-22 11:30:00', 1, 'blocking', 'high');
                CREATE TABLE collect.pg_log_events (server_name text, collection_time timestamp, occurred_at timestamp, family text, message text, detail text, user_name text);
                INSERT INTO collect.pg_log_events VALUES ('srv1', '2026-10-22 11:30:00', '2026-10-22 11:29:00', 'vacuum', 'secret text', 'secret detail', 'app_user');
                CREATE SCHEMA timescaledb_information;
                CREATE VIEW timescaledb_information.chunks AS
                  SELECT NULL::text AS chunk_name, NULL::boolean AS is_compressed, NULL::text AS hypertable_schema,
                         NULL::text AS hypertable_name, NULL::timestamp AS range_start, NULL::timestamp AS range_end
                  WHERE false;
                """);
            Assert.True(result.code == 0, result.output);
        }

        public ExportOptions Options(string outDir, bool dryRun, params string[] tables) => new()
        {
            Store = "store_a",
            FromUtc = From,
            ToUtc = To,
            OutDir = outDir,
            Tables = tables,
            PgHost = Pg.Builder!.Host!,
            PgPort = Pg.Builder.Port,
            PgDatabase = Pg.Builder.Database!,
            PgUser = Pg.Builder.Username!,
            Credential = new CredentialSpec(CredentialSources.File, Path: PasswordFile),
            StateDir = Path.Combine(Root, "state"),
            AllowOpenWindow = true,
            DryRun = dryRun,
        };

        public void Dispose()
        {
            Pg.Dispose();
            try { Directory.Delete(Root, recursive: true); } catch (IOException) { }
        }
    }

    private readonly StoreFixture _store;

    public OptionalNamesInTablesTests(StoreFixture store) => _store = store;

    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-tables-opt-out-").FullName;

    private static JsonObject ReadManifest(string outDir, params string[] tables)
    {
        var path = OutputPaths.ManifestFilePath(outDir, "store_a", From, To, tables);
        return (JsonObject)JsonNode.Parse(File.ReadAllText(path))!;
    }

    private static List<string> ParquetColumns(string absolutePath)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{absolutePath.Replace("'", "''")}'))";
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    [Fact]
    public async Task A_policy_table_named_in_tables_leaves_the_store_with_its_policy_applied()
    {
        if (_store.Pg.Builder is null) { return; }
        var outDir = NewOutDir();

        var exit = await ExportRunner.RunAsync(_store.Options(outDir, dryRun: false, "analysis_findings"), TextWriter.Null, CancellationToken.None);

        Assert.Equal(0, exit);
        var manifest = ReadManifest(outDir, "analysis_findings");
        var entry = Assert.Single(manifest["tables"]!.AsArray())!;
        Assert.Equal("analysis_findings", entry["table"]!.GetValue<string>());
        // The optional path stamps the policy version it applied; the raw core path has none to stamp.
        Assert.Equal(ColumnPolicies.For("analysis_findings")!.Version, entry["policy_version"]?.GetValue<int>());

        var file = Path.Combine(outDir, "store_a", entry["output_file"]!.GetValue<string>().Replace('/', Path.DirectorySeparatorChar));
        var columns = ParquetColumns(file);
        Assert.DoesNotContain("finding_id", columns);
        Assert.Contains("severity", columns);
    }

    [Fact]
    public async Task A_schema_gated_table_named_in_tables_is_skipped_and_never_exported_raw()
    {
        if (_store.Pg.Builder is null) { return; }
        var outDir = NewOutDir();

        var exit = await ExportRunner.RunAsync(_store.Options(outDir, dryRun: false, "pg_log_events"), TextWriter.Null, CancellationToken.None);

        Assert.Equal(0, exit);
        var tableDir = Path.Combine(outDir, "store_a", "pg_log_events");
        var written = Directory.Exists(tableDir) ? Directory.GetFiles(tableDir, "*.parquet", SearchOption.AllDirectories) : [];
        Assert.Empty(written);

        var manifest = ReadManifest(outDir, "pg_log_events");
        Assert.Empty(manifest["tables"]!.AsArray());
        var skipped = Assert.Single(manifest["skipped_optional"]!.AsArray())!;
        Assert.Equal("pg_log_events", skipped["table"]!.GetValue<string>());
        Assert.StartsWith("needs darling schema", skipped["reason"]!.GetValue<string>());
    }

    [Fact]
    public async Task Dry_run_applies_the_policy_to_a_registry_name_in_tables()
    {
        if (_store.Pg.Builder is null) { return; }
        var output = new StringWriter();

        var exit = await ExportRunner.RunAsync(_store.Options(NewOutDir(), dryRun: true, "pg_log_events"), output, CancellationToken.None);

        Assert.Equal(0, exit);
        var text = output.ToString();
        // family is on the keep-only list; message is not, and the printed statement must not select it.
        Assert.Contains("\"family\"", text);
        Assert.DoesNotContain("\"message\"", text);
    }
}
