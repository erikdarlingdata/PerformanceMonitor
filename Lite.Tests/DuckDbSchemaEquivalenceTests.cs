/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// THE data-safety proof for the catalog-driven collector schema: for every one of the 36 collector
/// tables, the DDL <see cref="DuckDbSchemaGenerator"/> now generates must produce a DuckDB table that
/// is byte-for-byte STORAGE-equivalent to the hand-written table it replaced — identical columns,
/// DuckDB types, column order, NOT NULL flags, DEFAULT values, and PRIMARY KEY, plus an identical
/// index. Equivalence is demonstrated empirically by executing BOTH the frozen pre-change DDL
/// (<see cref="GoldenCollectorSchema"/>) and the freshly generated DDL against DuckDB and comparing
/// the resulting <c>PRAGMA table_info</c> row-for-row.
///
/// <para>This is what guarantees an existing DuckDB store (whose tables were created by the old
/// hand-written DDL) stays compatible with the appender the app writes through, and that a fresh
/// store built by the generator is indistinguishable from one built the old way. A single divergence
/// — a dropped column, a widened type, a lost NOT NULL, a reordered column — fails the build with an
/// exact per-column diff. If it ever fails, the generator is wrong and must be fixed to match the
/// golden snapshot; the snapshot is the authority and must never be edited to accept a difference.</para>
/// </summary>
public class DuckDbSchemaEquivalenceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public DuckDbSchemaEquivalenceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteEquiv_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "equiv.duckdb");
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

    /// <summary>A single PRAGMA table_info row — everything DuckDB records about a column's storage shape.</summary>
    private readonly record struct ColumnInfo(int Cid, string Name, string Type, bool NotNull, string? Default, bool Pk)
    {
        public override string ToString() =>
            $"[{Cid}] {Name} {Type}{(NotNull ? " NOT NULL" : "")}{(Default is null ? "" : $" DEFAULT {Default}")}{(Pk ? " PK" : "")}";
    }

    /// <summary>Creates the table from <paramref name="ddl"/> in a clean slate and returns its table_info.</summary>
    private static List<ColumnInfo> TableInfo(DuckDBConnection conn, string ddl, string table)
    {
        using (var drop = conn.CreateCommand())
        {
            drop.CommandText = $"DROP TABLE IF EXISTS {table}";
            drop.ExecuteNonQuery();
        }

        using (var create = conn.CreateCommand())
        {
            create.CommandText = ddl;
            create.ExecuteNonQuery();
        }

        var rows = new List<ColumnInfo>();
        using (var info = conn.CreateCommand())
        {
            info.CommandText = $"PRAGMA table_info('{table}')";
            using var reader = info.ExecuteReader();
            while (reader.Read())
            {
                rows.Add(new ColumnInfo(
                    Cid: Convert.ToInt32(reader.GetValue(0)),
                    Name: reader.GetValue(1).ToString()!,
                    Type: reader.GetValue(2).ToString()!,
                    NotNull: Convert.ToBoolean(reader.GetValue(3)),
                    Default: reader.IsDBNull(4) ? null : reader.GetValue(4).ToString(),
                    Pk: Convert.ToBoolean(reader.GetValue(5))));
            }
        }

        return rows;
    }

    /// <summary>Collapses whitespace so two equivalent index statements compare equal regardless of formatting.</summary>
    private static string? NormalizeSql(string? sql) =>
        sql is null ? null : Regex.Replace(sql.Trim(), @"\s+", " ");

    [Fact]
    public void Golden_CoversExactlyTheCatalogCollectorTables()
    {
        /* The oracle describes the tables LITE stores, which is the SQL Server subset of the shared
           engine-mixed catalog — Lite has no PostgreSQL target and creates no table for those definitions. */
        var catalogTables = DuckDbSchemaGenerator.StoredCollectors
            .Select(c => c.TargetTable).OrderBy(t => t, StringComparer.Ordinal);
        var goldenTables = GoldenCollectorSchema.Tables.Keys.OrderBy(t => t, StringComparer.Ordinal);

        /* The frozen oracle must describe exactly those tables — no more, no fewer. 41 stays a literal here
           BECAUSE it is the frozen historical shape: if a SQL Server collector is added, this is supposed to
           fail until the oracle is extended by hand. That is the whole point of an oracle. */
        Assert.Equal(catalogTables, goldenTables);
        Assert.Equal(42, GoldenCollectorSchema.Tables.Count);

        /* Only server_config and database_config lack an index (matches DuckDbSchemaGenerator.CreateIndex). */
        var goldenIndexless = DuckDbSchemaGenerator.StoredCollectors
            .Select(c => c.TargetTable)
            .Where(t => !GoldenCollectorSchema.Indexes.ContainsKey(t))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[] { "database_config", "server_config" }, goldenIndexless);
    }

    /// <summary>
    /// Columns whose storage shape has INTENTIONALLY diverged from the pre-change hand-written
    /// schema since the extraction, each paired with the schema version that migrates existing
    /// databases. The golden snapshot stays frozen — it is the proof the extraction itself was
    /// lossless — so a deliberate change is recorded here rather than by rewriting history, and
    /// every entry has to be justified in review.
    ///
    /// <para>#1591 / schema v48: <c>cpu_count</c>, <c>hyperthread_ratio</c> and
    /// <c>physical_memory_mb</c> dropped NOT NULL. They are the only server_properties columns read
    /// from <c>sys.dm_os_sys_info</c>, which requires VIEW SERVER STATE (VIEW DATABASE STATE on
    /// Azure SQL DB). The collector now reads them inside TRY/CATCH so a login without that grant
    /// keeps every permission-free column instead of losing the entire row — which only works if the
    /// columns can hold NULL. <see cref="DuckDbInitializer"/>'s v48 migration drops the constraint on
    /// existing databases.</para>
    /// </summary>
    private static readonly HashSet<string> IntentionalStorageDivergences = new(StringComparer.Ordinal)
    {
        "server_properties.cpu_count",
        "server_properties.hyperthread_ratio",
        "server_properties.physical_memory_mb",
    };

    /// <summary>
    /// Columns ADDED to a collector table since the extraction, each paired with the schema version
    /// that adds them to existing databases. Same philosophy as
    /// <see cref="IntentionalStorageDivergences"/>: the golden snapshot stays frozen — it proves the
    /// extraction was lossless — so a deliberate addition is recorded here, and it is held to the
    /// append-only contract the stores rely on: the column must sit at the END of the generated table
    /// (every golden ordinal untouched) and must be nullable (existing rows are never backfilled).
    ///
    /// <para>#2012 stage 2 / schema v52: <c>query_stats.host_object_name</c> — the statement's hosting
    /// module (<c>schema.object</c>) captured at collection, so the hash-grouped top-queries reads can
    /// split INSERT...EXEC callers that share a <c>query_hash</c>. <see cref="DuckDbInitializer"/>'s
    /// v52 migration adds it to existing databases.</para>
    /// </summary>
    private static readonly HashSet<string> IntentionalAppendedColumns = new(StringComparer.Ordinal)
    {
        "query_stats.host_object_name",
    };

    [Fact]
    public void GeneratedCollectorTables_AreStorageEquivalentToPreChangeHandWritten()
    {
        var failures = new List<string>();

        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        conn.Open();

        foreach (var schema in DuckDbSchemaGenerator.StoredCollectors)
        {
            var table = schema.TargetTable;

            var goldenDdl = GoldenCollectorSchema.Tables[table];
            var generatedDdl = DuckDbSchemaGenerator.CreateTable(schema);

            var goldenInfo = TableInfo(conn, goldenDdl, table);
            var generatedInfo = TableInfo(conn, generatedDdl, table);

            /* Allow ONLY the recorded divergences, and only in the NOT NULL flag — a changed type,
               name, position, default or PK still fails even for a listed column. */
            var goldenComparable = goldenInfo
                .Select(c => IntentionalStorageDivergences.Contains($"{table}.{c.Name}") ? c with { NotNull = false } : c)
                .ToList();

            /* Recorded appended-since-extraction columns are allowed ONLY at the tail and nullable;
               strip them before the row-for-row comparison so the frozen golden keeps proving the
               original extraction was lossless while the appended column proves its own contract. */
            var generatedComparable = new List<ColumnInfo>();
            foreach (var column in generatedInfo)
            {
                if (!IntentionalAppendedColumns.Contains($"{table}.{column.Name}"))
                {
                    generatedComparable.Add(column);
                    continue;
                }

                if (column.NotNull || column.Cid < goldenInfo.Count)
                {
                    failures.Add(
                        $"{table}.{column.Name}: a recorded appended column must be nullable and sit " +
                        $"after every golden column (append-only contract), got: {column}");
                }
            }

            if (!goldenComparable.SequenceEqual(generatedComparable))
            {
                failures.Add(BuildTableDiff(table, goldenInfo, generatedInfo));
            }
        }

        Assert.True(
            failures.Count == 0,
            "Generated collector table(s) diverge in STORAGE SHAPE from the pre-change hand-written schema. " +
            "The generator is wrong — fix it to match the golden snapshot; do NOT edit the snapshot.\n\n" +
            string.Join("\n\n", failures));
    }

    [Fact]
    public void GeneratedCollectorIndexes_MatchPreChangeHandWritten()
    {
        var failures = new List<string>();

        foreach (var schema in DuckDbSchemaGenerator.StoredCollectors)
        {
            var table = schema.TargetTable;
            var generated = DuckDbSchemaGenerator.CreateIndex(schema);
            GoldenCollectorSchema.Indexes.TryGetValue(table, out var golden);

            if (NormalizeSql(golden) != NormalizeSql(generated))
            {
                failures.Add($"{table}: index DDL differs.\n    golden   : {golden ?? "(none)"}\n    generated: {generated ?? "(none)"}");
            }
        }

        Assert.True(
            failures.Count == 0,
            "Generated collector index(es) diverge from the pre-change hand-written schema:\n\n" +
            string.Join("\n", failures));
    }

    /// <summary>
    /// Belt-and-suspenders: executing every generated index against a real table must succeed (proves
    /// the reproduced irregular names / columns reference real columns and DuckDB accepts them).
    /// </summary>
    [Fact]
    public void GeneratedSchema_TablesAndIndexes_AllExecuteAgainstDuckDb()
    {
        using var conn = new DuckDBConnection($"Data Source={_dbPath}");
        conn.Open();

        foreach (var schema in DuckDbSchemaGenerator.StoredCollectors)
        {
            using (var t = conn.CreateCommand())
            {
                t.CommandText = DuckDbSchemaGenerator.CreateTable(schema);
                t.ExecuteNonQuery();
            }

            var index = DuckDbSchemaGenerator.CreateIndex(schema);
            if (index is not null)
            {
                using var i = conn.CreateCommand();
                i.CommandText = index;
                i.ExecuteNonQuery();
            }
        }

        /* Every generated table now exists in one database — the same end state InitializeAsync reaches. */
        using var count = conn.CreateCommand();
        count.CommandText =
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN (" +
            string.Join(",", DuckDbSchemaGenerator.StoredCollectors.Select(c => $"'{c.TargetTable}'")) + ")";
        /* The generated schema executes into DuckDB and must produce exactly the tables Lite stores.
           Derived rather than pinned at 41: the golden below stays a frozen literal on purpose (it is the
           historical shape), but this side tracks the generator, so adding a SQL Server collector updates it
           and adding a PostgreSQL one correctly does not. */
        Assert.Equal(DuckDbSchemaGenerator.StoredCollectors.Count(), Convert.ToInt32(count.ExecuteScalar()));
    }

    private static string BuildTableDiff(string table, List<ColumnInfo> golden, List<ColumnInfo> generated)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("TABLE ").Append(table).Append(" — golden(").Append(golden.Count)
          .Append(" cols) vs generated(").Append(generated.Count).Append(" cols):");

        var max = Math.Max(golden.Count, generated.Count);
        for (int i = 0; i < max; i++)
        {
            var g = i < golden.Count ? golden[i] : (ColumnInfo?)null;
            var n = i < generated.Count ? generated[i] : (ColumnInfo?)null;
            if (!Nullable.Equals(g, n))
            {
                sb.Append("\n    golden   : ").Append(g?.ToString() ?? "(missing)");
                sb.Append("\n    generated: ").Append(n?.ToString() ?? "(missing)");
            }
        }

        return sb.ToString();
    }
}
