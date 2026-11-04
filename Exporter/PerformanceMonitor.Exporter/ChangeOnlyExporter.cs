using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>One output Parquet part written for a change-only export.</summary>
public sealed record ChangeOnlyPart(string RelativePath, long RowCount, long ByteSize, string Sha256);

/// <summary>
/// The result of one <see cref="ChangeOnlyExporter.ExportAsync"/> call, shaped so <see cref="OptionalTableRunner"/>
/// can fold it into a <see cref="ManifestTableEntry"/>.
/// </summary>
public sealed class ChangeOnlyExportResult
{
    public required long RowCount { get; init; }
    public required List<ChangeOnlyPart> Parts { get; init; }
    public required double StoreQueryElapsedMs { get; init; }
    public required double WriteElapsedMs { get; init; }
    public required bool Baseline { get; init; }
    public required long RowsSuppressed { get; init; }

    /// <summary>The Change spec's key columns actually used, after dropping any not discovered on this store. Same
    /// order as <see cref="OptionalTable.Change"/>.Key, minus the dropped ones.</summary>
    public required IReadOnlyList<string> ResolvedKeyColumns { get; init; }

    /// <summary>Set only for a "Change + Rollup" table: the extra <c>*_hourly</c> Parquet part built from the SAME
    /// single store read as the Change output. Null for every plain Change-only table.</summary>
    public HourlyRollupPart? RollupPart { get; init; }

    /// <summary>The rolled-up row count for <see cref="RollupPart"/>. 0 when there is no rollup.</summary>
    public long RollupRowCount { get; init; }
}

/// <summary>
/// Exports one change-only ("&#916;") optional table for one window, in TWO passes so memory is bounded by bytes, not
/// rows -- the same lesson <see cref="OptionalTableExporter"/> already applies. The original one-pass design loaded
/// the whole window into a single DuckDB table and ran <c>to_json(struct_pack)</c> hashing, a
/// <c>lag</c>/<c>row_number</c> window and a join over the FULL wide rows. DuckDB processes 2,048-row vectors, so a
/// table with ~2 MB strings per row pins gigabytes at once; `plan_correction` (200 rows, ~2 MB each, ~400 MB total)
/// exhausted the 4 GB `memory_limit` ("3.7 GiB/3.7 GiB used"). Pass 1 streams the reader exactly once (unchanged SQL).
/// For each row it computes `key_hash`/`value_hash` in C# (streaming, never holding a wide struct in DuckDB -- see
/// <see cref="ComputeCanonicalHash"/>), appends the full wide row plus `_ord`/`_key_hash`/`_value_hash` into
/// byte-bounded chunk databases (reusing <see cref="OptionalTableExporter.BuildColumns"/>/<see
/// cref="OptionalTableExporter.AppendColumnValue"/> and <see cref="ChunkedParquetWriter"/>'s chunk bounds), and ALSO
/// appends a narrow `(_ord, _time, key_hash, value_hash)` row into one small `narrow` table -- narrow because it never
/// holds the tracked columns' text. Decide then runs the existing `lag`/`row_number`/baseline/EmitOnce emission SQL
/// over `narrow` ONLY, which is cheap because narrow rows are hashes, not wide text. That produces `emit(_ord,
/// change_kind)`. Pass 2 revisits each chunk database (one at a time, so memory is bounded by one chunk, not the whole
/// window), joins it against the slice of `emit` for that chunk's `_ord` range, and writes one sorted Parquet part per
/// chunk that has at least one emitted row. A chunk with none writes no part. Everything downstream of `emit` -- the
/// state write, `tracked_set_hash`, baseline forcing, `--ignore-state` and `Prune` -- is unchanged from the original
/// one-pass design; only where the per-row hashing and the wide columns live has moved.
/// </summary>
public static class ChangeOnlyExporter
{
    private const string PartParquetOptions = "FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000, ROW_GROUP_SIZE_BYTES '64MB'";

    /// <summary>
    /// Exports <paramref name="table"/> (which must have <see cref="OptionalTable.Grain"/> ==
    /// <see cref="ExportGrain.Change"/> and a non-null <see cref="OptionalTable.Change"/> spec) for the window
    /// [<paramref name="fromUtc"/>, <paramref name="toUtc"/>). <paramref name="readerFactory"/> is called exactly
    /// once. When <paramref name="changeStateStore"/> is null (the <c>--ignore-state</c> path), no previous-hash
    /// comparison happens and every row is emitted as 'new'; nothing is read from or written to state.
    /// </summary>
    public static async Task<ChangeOnlyExportResult> ExportAsync(
        OptionalTable table,
        List<DiscoveredColumn> columns,
        string store,
        string outDir,
        Func<CancellationToken, Task<DbDataReader>> readerFactory,
        DateTime fromUtc,
        DateTime toUtc,
        DateTime nowUtc,
        ChangeStateStore? changeStateStore,
        bool forceBaseline,
        CancellationToken ct)
    {
        if (table.Grain != ExportGrain.Change || table.Change is null)
        {
            throw new InvalidOperationException($"'{table.Name}' is not a Grain.Change table with a Change spec.");
        }

        var spec = table.Change;
        var tableDir = Path.Combine(outDir, store, table.Name);
        Directory.CreateDirectory(tableDir);

        var storeStopwatch = Stopwatch.StartNew();
        await using var reader = await readerFactory(ct).ConfigureAwait(false);
        var duckColumns = OptionalTableExporter.BuildColumns(reader, isSnapshot: false);

        // The tracked columns: every discovered column, minus the key, minus Ignore, minus the time column. This
        // drives both _value_hash and tracked_set_hash.
        var allColumnNames = duckColumns.Select(c => c.Name).ToList();
        var discoveredNameSet = new HashSet<string>(allColumnNames, StringComparer.Ordinal);

        // A key column not among the discovered columns (e.g. `group_id` on a store below the schema version that added it)
        // is dropped from the key -- never resolved against the reader, which would throw -- so an older
        // store still exports this table, just with a narrower key. Logged once per dropped column, not per row.
        var resolvedKey = new List<string>();
        foreach (var keyColumn in spec.Key)
        {
            if (discoveredNameSet.Contains(keyColumn))
            {
                resolvedKey.Add(keyColumn);
            }
            else
            {
                Console.Error.WriteLine($"  {table.Name}: change key column '{keyColumn}' not discovered on this store, dropped from the key");
            }
        }

        var keySet = new HashSet<string>(spec.Key, StringComparer.Ordinal);
        var ignoreSet = new HashSet<string>(spec.Ignore, StringComparer.Ordinal);
        var timeColumn = table.TimeColumn;
        var trackedColumns = allColumnNames
            .Where(c => !keySet.Contains(c) && !ignoreSet.Contains(c)
                && !string.Equals(c, timeColumn, StringComparison.Ordinal)
                && !string.Equals(c, "source_store", StringComparison.Ordinal))
            .ToList();

        var trackedSetHash = ComputeTrackedSetHash(trackedColumns);

        // Key/tracked column -> reader ordinal + DuckDB type, resolved once, in the SAME fixed order the old
        // struct_pack used (resolvedKey's own order for the key; discovery order, post-exclusion, for the value) --
        // "fixed column order" carries over from SQL to this streaming C# encoding.
        var keyPlan = resolvedKey.Select(name => ResolveColumn(name, reader, duckColumns)).ToList();
        var trackedPlan = trackedColumns.Select(name => ResolveColumn(name, reader, duckColumns)).ToList();
        var timePlan = timeColumn is null ? default((string Name, int Ordinal, string DuckType)?) : ResolveColumn(timeColumn, reader, duckColumns);

        // The narrow table: one row per store row, holding only _ord, the time value (if any) and the two hashes
        // -- never the wide tracked columns. It is what the Decide step's lag/row_number/join run over, so that
        // step's memory cost is bytes-of-hashes, not bytes-of-plan-XML.
        var narrowSpillDir = Path.Combine(Path.GetTempPath(), $"darling-export-change-narrow-{Guid.NewGuid():N}");
        using var narrowDb = DuckDbSession.OpenInMemory(narrowSpillDir);
        var narrowTimeColumnDef = timePlan is { } tp ? $", _time {tp.DuckType}" : string.Empty;
        Execute(narrowDb, $"CREATE TABLE narrow (_ord BIGINT{narrowTimeColumnDef}, key_hash VARCHAR, value_hash VARCHAR)");

        var rawSpillRoot = Path.Combine(Path.GetTempPath(), $"darling-export-change-raw-{Guid.NewGuid():N}");
        var chunkColumns = duckColumns.Concat([new ColumnShape("_ord", "BIGINT"), new ColumnShape("_key_hash", "VARCHAR"), new ColumnShape("_value_hash", "VARCHAR")]).ToList();

        long rowsRead = 0;
        List<ChunkInfo> chunks;
        using (var chunkWriter = new ChunkedRawWriter(chunkColumns, rawSpillRoot))
        {
            using var narrowAppender = narrowDb.CreateAppender("narrow");
            var readerColumnCount = reader.FieldCount;
            while (reader.Read())
            {
                var ord = rowsRead;
                var keyHash = ComputeCanonicalHash(reader, keyPlan);
                var valueHash = ComputeCanonicalHash(reader, trackedPlan);

                var chunkRow = chunkWriter.CreateRow();
                chunkRow.AppendValue(store);
                long rowBytes = 0;
                for (var i = 0; i < readerColumnCount; i++)
                {
                    rowBytes += OptionalTableExporter.AppendColumnValue(chunkRow, reader, i, duckColumns[i + 1].DuckDbType);
                }
                chunkRow.AppendValue(ord);
                chunkRow.AppendValue(keyHash);
                chunkRow.AppendValue(valueHash);
                chunkRow.EndRow();
                chunkWriter.RowAppended(rowBytes, ord);

                var narrowRow = narrowAppender.CreateRow();
                narrowRow.AppendValue(ord);
                if (timePlan is { } timeColumnPlan)
                {
                    AppendTimeValue(narrowRow, reader, timeColumnPlan.Ordinal, timeColumnPlan.DuckType);
                }
                narrowRow.AppendValue(keyHash);
                narrowRow.AppendValue(valueHash);
                narrowRow.EndRow();

                rowsRead++;
            }

            chunks = chunkWriter.Complete();
        }

        storeStopwatch.Stop();
        var writeStopwatch = Stopwatch.StartNew();

        // "Change + Rollup": build the *_hourly rollup from the SAME chunk databases Pass 1 already wrote, instead of
        // a second store read -- store-gentleness forbids two statements for the same table and window. These tables
        // are narrow (a few AGs times databases times replicas, at 1-minute samples), so re-reading every chunk's full
        // wide rows into one small `raw` table and running the rollup SQL over it is bytes, not gigabytes -- unlike
        // ChangeOnlyExporter's own two-pass split (which exists because plan_correction's ~2 MB text rows can't sit in
        // DuckDB's 2,048-row vectors all at once). No second pass is needed here.
        HourlyRollupPart? rollupPart = null;
        long rollupRowCount = 0;
        if (table.RollupName is not null)
        {
            (rollupPart, rollupRowCount) = BuildRollupFromChunks(table, chunks, duckColumns, store, outDir, fromUtc, toUtc);
        }

        var isBaselineWindow = IsUtcMidnightWindow(toUtc);
        var storedTrackedSetHash = changeStateStore?.GetTrackedSetHash(store, table.Name);
        var trackedSetChanged = storedTrackedSetHash is not null
            && !string.Equals(storedTrackedSetHash, trackedSetHash, StringComparison.Ordinal);
        var wasRecreated = changeStateStore?.WasRecreated ?? false;

        // --ignore-state (changeStateStore is null): no state read or write, and every row is emitted 'new', never
        // compared against anything, not even within-window.
        var ignoreState = changeStateStore is null;
        var baseline = !ignoreState && (forceBaseline || isBaselineWindow || trackedSetChanged || wasRecreated);

        // Prev is loaded whether or not this window is a baseline: it decides 'new' vs 'changed'/'baseline' for
        // every present key, and a baseline row is emitted regardless because _rn_last = 1 covers it.
        string prevTempTable;
        if (changeStateStore is not null)
        {
            prevTempTable = changeStateStore.LoadPrevAsOf(store, table.Name, fromUtc, narrowDb);
        }
        else
        {
            prevTempTable = "prev_empty";
            Execute(narrowDb, $"CREATE TEMP TABLE {prevTempTable} (key_hash VARCHAR, value_hash VARCHAR)");
        }

        var orderTimeCol = timePlan is not null ? "_time" : "_ord";
        Execute(narrowDb,
            "CREATE TABLE seq AS SELECT n.*, " +
            $"lag(value_hash) OVER (PARTITION BY key_hash ORDER BY {orderTimeCol}, _ord) AS _prev_in_win, " +
            $"row_number() OVER (PARTITION BY key_hash ORDER BY {orderTimeCol} DESC, _ord DESC) AS _rn_last " +
            "FROM narrow n");

        var emitOnceFilter = spec.EmitOnce
            ? " AND coalesce(s._prev_in_win, p.value_hash) IS NULL"
            : string.Empty;

        if (ignoreState)
        {
            // Every row is 'new', with no comparison at all -- not even within-window.
            Execute(narrowDb, "CREATE TABLE emit AS SELECT s._ord, 'new' AS change_kind FROM seq s");
        }
        else
        {
            Execute(narrowDb,
                "CREATE TABLE emit AS " +
                "SELECT s._ord, " +
                "CASE WHEN coalesce(s._prev_in_win, p.value_hash) IS NULL THEN 'new' " +
                "WHEN coalesce(s._prev_in_win, p.value_hash) <> s.value_hash THEN 'changed' " +
                "ELSE 'baseline' END AS change_kind " +
                "FROM seq s LEFT JOIN " + prevTempTable + " p ON p.key_hash = s.key_hash " +
                "WHERE (coalesce(s._prev_in_win, p.value_hash) IS DISTINCT FROM s.value_hash" +
                (baseline ? " OR s._rn_last = 1" : string.Empty) + ")" +
                emitOnceFilter);
        }

        var emittedOrds = new List<(long Ord, string ChangeKind)>();
        using (var command = narrowDb.CreateCommand())
        {
            command.CommandText = "SELECT _ord, change_kind FROM emit ORDER BY _ord";
            using var emitReader = command.ExecuteReader();
            while (emitReader.Read())
            {
                emittedOrds.Add((emitReader.GetInt64(0), emitReader.GetString(1)));
            }
        }

        var orderBy = timeColumn is not null ? $"server_name, {QuoteIdentifier(timeColumn)}" : "server_name";
        var parts = new List<ChangeOnlyPart>();
        try
        {
            var emitIndex = 0;
            var nonEmptyChunkWrites = new List<(int PartIndex, long RowCount)>();
            var pendingWrites = new List<(ChunkInfo Chunk, List<(long Ord, string ChangeKind)> Rows)>();

            foreach (var chunk in chunks)
            {
                var rows = new List<(long Ord, string ChangeKind)>();
                while (emitIndex < emittedOrds.Count && emittedOrds[emitIndex].Ord <= chunk.MaxOrd)
                {
                    rows.Add(emittedOrds[emitIndex]);
                    emitIndex++;
                }

                if (rows.Count > 0)
                {
                    pendingWrites.Add((chunk, rows));
                }
            }

            var multiPart = pendingWrites.Count > 1;
            for (var i = 0; i < pendingWrites.Count; i++)
            {
                var (chunk, rows) = pendingWrites[i];
                var relativePath = multiPart
                    ? OutputPaths.RelativeParquetPath(store, table.Name, fromUtc, toUtc, part: $"p{i:D5}")
                    : OutputPaths.RelativeParquetPath(store, table.Name, fromUtc, toUtc);
                var absolutePath = Path.Combine(outDir, store, relativePath.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

                using var chunkConnection = DuckDbSession.Open(chunk.Path);
                Execute(chunkConnection, "CREATE TEMP TABLE emit_here (_ord BIGINT, change_kind VARCHAR)");
                using (var appender = chunkConnection.CreateAppender("emit_here"))
                {
                    foreach (var (ord, changeKind) in rows)
                    {
                        var row = appender.CreateRow();
                        row.AppendValue(ord);
                        row.AppendValue(changeKind);
                        row.EndRow();
                    }
                }

                Execute(chunkConnection,
                    "COPY (SELECT c.* EXCLUDE (_ord, _key_hash, _value_hash), e.change_kind " +
                    "FROM chunk c JOIN emit_here e USING (_ord) " +
                    $"ORDER BY {orderBy}) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

                var byteSize = new FileInfo(absolutePath).Length;
                var sha256 = ComputeSha256(absolutePath);
                parts.Add(new ChangeOnlyPart(relativePath, rows.Count, byteSize, sha256));
            }

            // Every emitted row belongs to exactly one chunk, since chunk _ord ranges partition [0, rowsRead) in
            // read order; if nothing at all was emitted, still write one empty, schema-only part, the same "always
            // one file" convention OptionalTableExporter's writer follows, so a downstream loader never has to
            // special-case a change-only table with zero rows in the window.
            if (parts.Count == 0)
            {
                var relativePath = OutputPaths.RelativeParquetPath(store, table.Name, fromUtc, toUtc);
                var absolutePath = Path.Combine(outDir, store, relativePath.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

                var emptyColumnDefs = string.Join(", ", duckColumns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}") .Append("change_kind VARCHAR"));
                using var emptyConnection = DuckDbSession.OpenInMemory(Path.Combine(Path.GetTempPath(), $"darling-export-change-empty-{Guid.NewGuid():N}"));
                Execute(emptyConnection, $"CREATE TABLE empty_out ({emptyColumnDefs})");
                Execute(emptyConnection, $"COPY (SELECT * FROM empty_out) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

                var byteSize = new FileInfo(absolutePath).Length;
                var sha256 = ComputeSha256(absolutePath);
                parts.Add(new ChangeOnlyPart(relativePath, 0, byteSize, sha256));
            }
        }
        finally
        {
            try
            {
                if (Directory.Exists(rawSpillRoot))
                {
                    Directory.Delete(rawSpillRoot, recursive: true);
                }
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        var emittedRowCount = emittedOrds.Count;
        var rowsSuppressed = rowsRead - emittedRowCount;

        // The state write: one (key_hash, value_hash) row per key, from that key's LAST row in the window, but only
        // when it differs from prev or this is a baseline window.
        if (changeStateStore is not null)
        {
            changeStateStore.DeleteWindow(store, table.Name, toUtc);

            var stateRows = new List<(string KeyHash, string ValueHash)>();
            using (var command = narrowDb.CreateCommand())
            {
                command.CommandText =
                    "SELECT s.key_hash, s.value_hash FROM seq s LEFT JOIN " + prevTempTable + " p ON p.key_hash = s.key_hash " +
                    "WHERE s._rn_last = 1 AND (coalesce(s._prev_in_win, p.value_hash) IS DISTINCT FROM s.value_hash" +
                    (baseline ? " OR TRUE" : string.Empty) + ")";
                using var stateReader = command.ExecuteReader();
                while (stateReader.Read())
                {
                    stateRows.Add((stateReader.GetString(0), stateReader.GetString(1)));
                }
            }

            if (stateRows.Count > 0)
            {
                changeStateStore.Record(store, table.Name, stateRows, toUtc);
            }

            if (trackedSetHash != storedTrackedSetHash)
            {
                changeStateStore.SetTrackedSetHash(store, table.Name, trackedSetHash);
            }

            if (isBaselineWindow)
            {
                changeStateStore.Prune(nowUtc, keepDays: 7);
            }
        }

        writeStopwatch.Stop();

        return new ChangeOnlyExportResult
        {
            RowCount = emittedRowCount,
            Parts = parts,
            StoreQueryElapsedMs = storeStopwatch.Elapsed.TotalMilliseconds,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
            Baseline = baseline,
            RowsSuppressed = Math.Max(0, rowsSuppressed),
            ResolvedKeyColumns = resolvedKey,
            RollupPart = rollupPart,
            RollupRowCount = rollupRowCount,
        };
    }

    /// <summary>
    /// Builds the <c>*_hourly</c> rollup part for a "Change + Rollup" table, reading from Pass 1's already-written
    /// chunk databases -- never a second store statement. Concatenates every chunk's wide rows (minus the
    /// `_ord`/`_key_hash`/`_value_hash` bookkeeping columns) into one `raw` table, then reuses <see
    /// cref="RollupSql.Build"/> exactly as <see cref="HourlyRollupExporter"/> does.
    /// </summary>
    private static (HourlyRollupPart Part, long RowCount) BuildRollupFromChunks(
        OptionalTable table, List<ChunkInfo> chunks, List<ColumnShape> duckColumns,
        string store, string outDir, DateTime fromUtc, DateTime toUtc)
    {
        var spec = RollupSql.Specs[table.RollupName!];
        var spillDir = Path.Combine(Path.GetTempPath(), $"darling-export-change-rollup-{Guid.NewGuid():N}");
        using var connection = DuckDbSession.OpenInMemory(spillDir);
        var columnDefs = string.Join(", ", duckColumns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}"));
        Execute(connection, $"CREATE TABLE raw ({columnDefs})");

        var columnList = string.Join(", ", duckColumns.Select(c => QuoteIdentifier(c.Name)));
        for (var i = 0; i < chunks.Count; i++)
        {
            var alias = $"chunk_src_{i}";
            Execute(connection, $"ATTACH '{chunks[i].Path.Replace("'", "''")}' AS {alias} (READ_ONLY)");
            Execute(connection, $"INSERT INTO raw SELECT {columnList} FROM {alias}.chunk");
            Execute(connection, $"DETACH {alias}");
        }

        var rollupSql = RollupSql.Build(spec);
        Execute(connection, $"CREATE TABLE rolled AS {rollupSql}");
        // RollupSql emits source_store as a NULL placeholder; the store this export ran for fills it in.
        Execute(connection, $"UPDATE rolled SET source_store = '{store.Replace("'", "''")}'");

        var relativePath = OutputPaths.RelativeParquetPath(store, spec.Name, fromUtc, toUtc);
        var absolutePath = Path.Combine(outDir, store, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(absolutePath)!);

        Execute(connection, $"COPY (SELECT * FROM rolled ORDER BY server_name, hour) TO '{absolutePath.Replace("'", "''")}' ({PartParquetOptions})");

        var rolledRowCount = Convert.ToInt64(ExecuteScalar(connection, "SELECT count(*) FROM rolled"), CultureInfo.InvariantCulture);
        var byteSize = new FileInfo(absolutePath).Length;
        var sha256 = ComputeSha256(absolutePath);

        try
        {
            if (Directory.Exists(spillDir))
            {
                Directory.Delete(spillDir, recursive: true);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }

        return (new HourlyRollupPart(relativePath, rolledRowCount, byteSize, sha256), rolledRowCount);
    }

    private static object? ExecuteScalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    /// <summary>True when <paramref name="toUtc"/> is exactly a UTC midnight -- the daily safety-net baseline
    /// window.</summary>
    private static bool IsUtcMidnightWindow(DateTime toUtc) =>
        toUtc.Hour == 0 && toUtc.Minute == 0 && toUtc.Second == 0 && toUtc.Millisecond == 0;

    /// <summary>The SHA-256 of the sorted, comma-joined tracked column names.</summary>
    public static string ComputeTrackedSetHash(IEnumerable<string> trackedColumns)
    {
        var joined = string.Join(",", trackedColumns.OrderBy(c => c, StringComparer.Ordinal));
        var hash = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(joined));
        return Convert.ToHexStringLower(hash);
    }

    private static (string Name, int Ordinal, string DuckType) ResolveColumn(string name, DbDataReader reader, List<ColumnShape> duckColumns)
    {
        var ordinal = reader.GetOrdinal(name);
        var duckType = duckColumns.First(c => string.Equals(c.Name, name, StringComparison.Ordinal)).DuckDbType;
        return (name, ordinal, duckType);
    }

    /// <summary>
    /// The exporter's own canonical, streaming encoding for key_hash/value_hash: an ordered JSON object over the given
    /// columns, hashed with SHA-256. It replaces the original design's `sha256(to_json(struct_pack(...)))` run inside
    /// DuckDB, but byte-for-byte parity with that SQL expression is NOT required -- <see cref="ChangeStateStore"/>'s
    /// state file only ever compares hashes this exporter itself produced. What matters, and what this preserves: a
    /// fixed column order, NULL kept distinct from '' (a <c>JsonNode?</c> null is never the same JSON as an empty
    /// string), and a type-stable text form per DuckDB type (a decimal or timestamp always renders the same way for
    /// the same value). This one encoding is used for every row, streaming, and nothing here holds more than one row's
    /// columns at a time.
    /// </summary>
    private static string ComputeCanonicalHash(DbDataReader reader, List<(string Name, int Ordinal, string DuckType)> plan)
    {
        var obj = new JsonObject();
        foreach (var (name, ordinal, duckType) in plan)
        {
            obj[name] = CanonicalJsonValue(reader, ordinal, duckType);
        }

        var json = obj.ToJsonString();
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(json));
        return Convert.ToHexStringLower(hash);
    }

    private static JsonNode? CanonicalJsonValue(DbDataReader reader, int ordinal, string duckType)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        switch (duckType)
        {
            case "BOOLEAN":
                return JsonValue.Create(reader.GetBoolean(ordinal));
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
                return JsonValue.Create(Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture));
            case "REAL":
            case "DOUBLE":
                return JsonValue.Create(Convert.ToDouble(reader.GetValue(ordinal), CultureInfo.InvariantCulture).ToString("R", CultureInfo.InvariantCulture));
            case "VARCHAR":
                return JsonValue.Create(reader.GetValue(ordinal)?.ToString());
            case "BLOB":
                return JsonValue.Create(Convert.ToHexStringLower((byte[])reader.GetValue(ordinal)));
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return JsonValue.Create(reader.GetDateTime(ordinal).ToString("O", CultureInfo.InvariantCulture));
            case "DATE":
                return JsonValue.Create(DateOnly.FromDateTime(reader.GetDateTime(ordinal)).ToString("O", CultureInfo.InvariantCulture));
            case "UUID":
                return JsonValue.Create(reader.GetGuid(ordinal).ToString());
            case var decimalType when decimalType.StartsWith("DECIMAL(", StringComparison.Ordinal):
                return JsonValue.Create(reader.GetDecimal(ordinal).ToString(CultureInfo.InvariantCulture));
            default:
                throw new NotSupportedException($"No canonical-hash case for DuckDB type '{duckType}'.");
        }
    }

    /// <summary>Appends the time column's value into the narrow table's `_time` column, mirroring <see cref="OptionalTableExporter.AppendColumnValue"/>'s type cases for the small set of types a time column can have.</summary>
    private static void AppendTimeValue(IDuckDBAppenderRow row, DbDataReader reader, int ordinal, string duckType)
    {
        if (reader.IsDBNull(ordinal))
        {
            row.AppendNullValue();
            return;
        }

        switch (duckType)
        {
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                row.AppendValue(reader.GetDateTime(ordinal));
                return;
            case "DATE":
                row.AppendValue(DateOnly.FromDateTime(reader.GetDateTime(ordinal)));
                return;
            default:
                throw new NotSupportedException($"No narrow-table case for time column DuckDB type '{duckType}'.");
        }
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }

    private static string QuoteIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>One flushed Pass-1 chunk database: its file path (a real file, not :memory:, so it can be closed and reopened for Pass 2) and the inclusive `_ord` range it holds.</summary>
    private sealed record ChunkInfo(string Path, long MinOrd, long MaxOrd);

    /// <summary>
    /// Writes Pass 1's wide rows (plus `_ord`/`_key_hash`/`_value_hash`) into a series of byte-bounded chunk
    /// databases, exactly the memory shape <see cref="OptionalTableExporter"/>'s writer uses (a fresh DuckDB
    /// instance per chunk, bounded by <see cref="OptionalTableExporter.ChunkRows"/>/<see cref="ChunkedParquetWriter.ChunkBytes"/>).
    /// Unlike that writer, each chunk is written to a real on-disk DuckDB file (not :memory:) and its connection is
    /// closed once full, so Pass 2 can reopen and finish exactly one chunk's data at a time -- the whole window's
    /// raw rows are never open across two chunks at once.
    /// </summary>
    private sealed class ChunkedRawWriter : IDisposable
    {
        private readonly List<ColumnShape> _columns;
        private readonly string _spillRoot;
        private readonly string _createTableSql;
        private readonly List<ChunkInfo> _chunks = [];
        private DuckDBConnection? _connection;
        private DuckDBAppender? _appender;
        private string? _currentPath;
        private long? _currentMinOrd;
        private long _currentMaxOrd;
        private int _rowsInChunk;
        private long _bytesInChunk;

        public ChunkedRawWriter(List<ColumnShape> columns, string spillRoot)
        {
            _columns = columns;
            _spillRoot = spillRoot;
            Directory.CreateDirectory(_spillRoot);
            var columnDefs = string.Join(", ", columns.Select(c => $"{QuoteIdentifier(c.Name)} {c.DuckDbType}"));
            _createTableSql = $"CREATE TABLE chunk ({columnDefs})";
        }

        public IDuckDBAppenderRow CreateRow()
        {
            if (_appender is null)
            {
                _currentPath = Path.Combine(_spillRoot, $"chunk-{_chunks.Count:D6}.duckdb");
                _connection = DuckDbSession.Open(_currentPath);
                Execute(_connection, _createTableSql);
                _appender = _connection.CreateAppender("chunk");
            }

            return _appender.CreateRow();
        }

        public void RowAppended(long rowBytes, long ord)
        {
            _currentMinOrd ??= ord;
            _currentMaxOrd = ord;
            _rowsInChunk++;
            _bytesInChunk += Math.Max(0, rowBytes);
            if (_rowsInChunk >= OptionalTableExporter.ChunkRows || _bytesInChunk >= ChunkedParquetWriter.ChunkBytes)
            {
                FlushChunk();
            }
        }

        public List<ChunkInfo> Complete()
        {
            FlushChunk();
            return _chunks;
        }

        private void FlushChunk()
        {
            if (_appender is null || _connection is null)
            {
                return;
            }

            _appender.Close();
            _appender.Dispose();
            _appender = null;
            _connection.Dispose();
            _connection = null;

            if (_rowsInChunk > 0 && _currentPath is not null && _currentMinOrd is not null)
            {
                _chunks.Add(new ChunkInfo(_currentPath, _currentMinOrd.Value, _currentMaxOrd));
            }

            _currentPath = null;
            _currentMinOrd = null;
            _rowsInChunk = 0;
            _bytesInChunk = 0;
        }

        public void Dispose()
        {
            _appender?.Dispose();
            _connection?.Dispose();
        }
    }
}
