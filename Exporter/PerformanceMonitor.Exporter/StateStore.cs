using DuckDB.NET.Data;

namespace PerformanceMonitor.Exporter;

/// <summary>One plan key's totals for the current window, as read from the DuckDB window caches (before the plan-fetch gate).</summary>
public sealed record PlanFetchCandidate(
    string? ServerName, string QueryPlanHash, string Origin, long Executions, long CpuUs, DateTime FirstSeen, DateTime LastSeen);

/// <summary>
/// The plan-fetch gate's verdict for one key: totals accumulated over every window counted so far (including this
/// one), the merged first_seen / last_seen, whether the key now qualifies for a plan XML fetch, and whether it
/// already had a pending row (an earlier window counted for it).
/// </summary>
public sealed record PlanFetchDecision(
    string? ServerName, string QueryPlanHash, string Origin, long Executions, long CpuUs, DateTime FirstSeen, DateTime LastSeen,
    bool Qualifies, bool WasPending);

/// <summary>The plan-fetch threshold: fetch a plan's XML only when it has run enough.</summary>
public static class PlanFetchThreshold
{
    /// <summary>A plan qualifies when its accumulated executions reach this number...</summary>
    public const long MinExecutions = 5;

    /// <summary>...or when its accumulated CPU time reaches this many microseconds (1 second).</summary>
    public const long MinCpuUs = 1_000_000;

    /// <summary>Pending rows not updated for this many days are deleted.</summary>
    public const int PendingRetentionDays = 14;
}

/// <summary>
/// Tracks which dimension keys (plan_shape / query_text) have already been exported for a source_store, in a small
/// on-disk DuckDB file at `&lt;StateDir&gt;/exported_keys.duckdb`. The exporter is one small process run repeatedly
/// against a store that keeps generating a huge number of distinct plan/text keys per day, of which only a fraction
/// are new; without this, every run would re-fetch and re-write full plan XML / query text for keys already exported
/// in a previous run.: every `FilterUnknown*` call used to read every row for the store into a.NET HashSet, which does
/// not scale once the exporter runs hourly instead of daily (48 reads a day per box, and `procedure_digests` alone
/// gains about 110K rows a day on a large store). Each filter now loads only the candidate keys into a DuckDB temp
/// table (via the appender) and anti-joins it against the known-keys table inside DuckDB, so only the unknown rows
/// ever cross into managed memory. Method signatures and outputs are unchanged.
/// </summary>
public sealed class StateStore : IDisposable
{
    private readonly DuckDBConnection _connection;
    private int _tempTableSeq;

    private StateStore(DuckDBConnection connection)
    {
        _connection = connection;
    }

    /// <summary>Opens (creating if needed) the state file at `&lt;stateDir&gt;/exported_keys.duckdb` and ensures its tables exist.</summary>
    public static StateStore Open(string stateDir)
    {
        Directory.CreateDirectory(stateDir);
        var path = Path.Combine(stateDir, "exported_keys.duckdb");
        var connection = new DuckDBConnection($"Data Source={path}");
        connection.Open();

        using var createPlanKeys = connection.CreateCommand();
        createPlanKeys.CommandText =
            "CREATE TABLE IF NOT EXISTS plan_keys (source_store VARCHAR, server_name VARCHAR, query_plan_hash VARCHAR)";
        createPlanKeys.ExecuteNonQuery();

        using var createTextKeys = connection.CreateCommand();
        createTextKeys.CommandText =
            "CREATE TABLE IF NOT EXISTS text_keys (source_store VARCHAR, server_name VARCHAR, database_name VARCHAR, query_hash VARCHAR)";
        createTextKeys.ExecuteNonQuery();

        using var createFeatureDigests = connection.CreateCommand();
        createFeatureDigests.CommandText =
            "CREATE TABLE IF NOT EXISTS feature_digests (source_store VARCHAR, plan_digest VARCHAR, feature_version INTEGER)";
        createFeatureDigests.ExecuteNonQuery();

        using var createProcedureDigests = connection.CreateCommand();
        createProcedureDigests.CommandText =
            "CREATE TABLE IF NOT EXISTS procedure_digests (source_store VARCHAR, plan_digest VARCHAR, shape_hash VARCHAR, recorded_at TIMESTAMP)";
        createProcedureDigests.ExecuteNonQuery();

        using var createProcedurePlanKeys = connection.CreateCommand();
        createProcedurePlanKeys.CommandText =
            "CREATE TABLE IF NOT EXISTS procedure_plan_keys (source_store VARCHAR, server_name VARCHAR, database_name VARCHAR, " +
            "schema_name VARCHAR, object_name VARCHAR, shape_hash VARCHAR)";
        createProcedurePlanKeys.ExecuteNonQuery();

        // Procedure plan reuse: when each cached plan identity (plan_handle + cached_time) last had its plan XML
        // fetched, and which digest that was.
        using var createProcedurePlanIdentity = connection.CreateCommand();
        createProcedurePlanIdentity.CommandText =
            "CREATE TABLE IF NOT EXISTS procedure_plan_identity (source_store VARCHAR, server_name VARCHAR, database_name VARCHAR, " +
            "schema_name VARCHAR, object_name VARCHAR, plan_handle VARCHAR, cached_time TIMESTAMP, last_fetch_utc TIMESTAMP, last_digest VARCHAR)";
        createProcedurePlanIdentity.ExecuteNonQuery();

        using var createPgTextKeys = connection.CreateCommand();
        createPgTextKeys.CommandText =
            "CREATE TABLE IF NOT EXISTS pg_text_keys (source_store VARCHAR, server_name VARCHAR, queryid BIGINT)";
        createPgTextKeys.ExecuteNonQuery();

        // Plan keys held back by the plan-fetch gate: the running totals a key has accumulated over the windows it has
        // been seen in, until it is exported.
        using var createPlanFetchPending = connection.CreateCommand();
        createPlanFetchPending.CommandText =
            "CREATE TABLE IF NOT EXISTS plan_fetch_pending (source_store VARCHAR, server_name VARCHAR, query_plan_hash VARCHAR, " +
            "origin VARCHAR, executions BIGINT, cpu_us BIGINT, first_seen TIMESTAMP, last_seen TIMESTAMP, updated_utc TIMESTAMP)";
        createPlanFetchPending.ExecuteNonQuery();

        // The days whose daily snapshot (a Daily-grain table, or the servers snapshot) a run has produced, so a later
        // window of the same day knows whether the 00:00 window's part exists.
        using var createDailySnapshots = connection.CreateCommand();
        createDailySnapshots.CommandText =
            "CREATE TABLE IF NOT EXISTS daily_snapshots (source_store VARCHAR, table_name VARCHAR, snapshot_day TIMESTAMP)";
        createDailySnapshots.ExecuteNonQuery();

        return new StateStore(connection);
    }

    /// <summary>Makes a fresh temp table name for one filter call, so concurrent-looking reuse of the connection never collides.</summary>
    private string NextTempTableName(string prefix) => $"{prefix}_{Interlocked.Increment(ref _tempTableSeq)}";

    /// <summary>
    /// Returns the subset of (server_name, query_plan_hash) pairs in <paramref name="keys"/> that are NOT already
    /// recorded as exported for <paramref name="sourceStore"/>. The candidate keys are loaded into a DuckDB temp table
    /// and anti-joined against `plan_keys` inside DuckDB, so only unknown keys are ever materialized in managed
    /// memory.
    /// </summary>
    public HashSet<(string? ServerName, string QueryPlanHash)> FilterUnknownPlanKeys(
        string sourceStore, IEnumerable<(string? ServerName, string QueryPlanHash)> keys)
    {
        var distinctKeys = keys.ToHashSet();
        if (distinctKeys.Count == 0)
        {
            return [];
        }

        var tempTable = NextTempTableName("cand_plan");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, query_plan_hash VARCHAR)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var (server, hash) in distinctKeys)
            {
                var row = appender.CreateRow();
                if (server is null) row.AppendNullValue(); else row.AppendValue(server);
                row.AppendValue(hash);
                row.EndRow();
            }
        }

        var result = new HashSet<(string?, string)>();
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"SELECT cand.server_name, cand.query_plan_hash FROM {tempTable} cand " +
            "ANTI JOIN (SELECT server_name, query_plan_hash FROM plan_keys WHERE source_store = $store) known " +
            "ON cand.server_name IS NOT DISTINCT FROM known.server_name AND cand.query_plan_hash = known.query_plan_hash";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var server = reader.IsDBNull(0) ? null : reader.GetString(0);
                var hash = reader.GetString(1);
                result.Add((server, hash));
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Returns the subset of (server_name, database_name, query_hash) triples in <paramref name="keys"/> that are NOT
    /// already recorded as exported for <paramref name="sourceStore"/>. `database_name` compares NULL-safely: a NULL
    /// database is a valid key of its own, not a wildcard. anti-join in DuckDB, see <see
    /// cref="FilterUnknownPlanKeys"/>.
    /// </summary>
    public HashSet<(string? ServerName, string? DatabaseName, string? QueryHash)> FilterUnknownTextKeys(
        string sourceStore, IEnumerable<(string? ServerName, string? DatabaseName, string? QueryHash)> keys)
    {
        var distinctKeys = keys.ToHashSet();
        if (distinctKeys.Count == 0)
        {
            return [];
        }

        var tempTable = NextTempTableName("cand_text");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, database_name VARCHAR, query_hash VARCHAR)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var (server, database, hash) in distinctKeys)
            {
                var row = appender.CreateRow();
                if (server is null) row.AppendNullValue(); else row.AppendValue(server);
                if (database is null) row.AppendNullValue(); else row.AppendValue(database);
                if (hash is null) row.AppendNullValue(); else row.AppendValue(hash);
                row.EndRow();
            }
        }

        var result = new HashSet<(string?, string?, string?)>();
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"SELECT cand.server_name, cand.database_name, cand.query_hash FROM {tempTable} cand " +
            "ANTI JOIN (SELECT server_name, database_name, query_hash FROM text_keys WHERE source_store = $store) known " +
            "ON cand.server_name IS NOT DISTINCT FROM known.server_name " +
            "AND cand.database_name IS NOT DISTINCT FROM known.database_name " +
            "AND cand.query_hash IS NOT DISTINCT FROM known.query_hash";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var server = reader.IsDBNull(0) ? null : reader.GetString(0);
                var database = reader.IsDBNull(1) ? null : reader.GetString(1);
                var hash = reader.IsDBNull(2) ? null : reader.GetString(2);
                result.Add((server, database, hash));
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Returns the subset of plan digests in <paramref name="digestHex"/> that are NOT already recorded as parsed for
    /// <paramref name="sourceStore"/> at <paramref name="featureVersion"/>. A digest recorded under a different
    /// feature_version is treated as unknown, so bumping the version reparses everything. anti-join in DuckDB, see
    /// <see cref="FilterUnknownPlanKeys"/>.
    /// </summary>
    public HashSet<string> FilterUnknownFeatureDigests(string sourceStore, IEnumerable<string> digestHex, int featureVersion)
    {
        var distinctDigests = digestHex.ToHashSet(StringComparer.Ordinal);
        if (distinctDigests.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        var tempTable = NextTempTableName("cand_feature");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (plan_digest VARCHAR)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var digest in distinctDigests)
            {
                var row = appender.CreateRow();
                row.AppendValue(digest);
                row.EndRow();
            }
        }

        var result = new HashSet<string>(StringComparer.Ordinal);
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"SELECT cand.plan_digest FROM {tempTable} cand " +
            "ANTI JOIN (SELECT plan_digest FROM feature_digests WHERE source_store = $store AND feature_version = $version) known " +
            "ON cand.plan_digest = known.plan_digest";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        command.Parameters.Add(new DuckDBParameter("version", featureVersion));
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                result.Add(reader.GetString(0));
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Returns the subset of plan digests (hex) in <paramref name="digestHex"/> that are NOT already recorded in
    /// `procedure_digests` for <paramref name="sourceStore"/>. A known digest's shape is looked up too, so the caller
    /// can skip fetching its content for the shape-hash computation. anti-join in DuckDB for the unknown set, and a
    /// separate scoped lookup for the known shapes -- both scans are bounded by the candidate digest set, not by the
    /// whole table.
    /// </summary>
    public (HashSet<string> Unknown, Dictionary<string, string> KnownShapeByDigest) FilterUnknownProcedureDigests(
        string sourceStore, IEnumerable<string> digestHex)
    {
        var distinctDigests = digestHex.ToHashSet(StringComparer.Ordinal);
        var knownShapeByDigest = new Dictionary<string, string>(StringComparer.Ordinal);
        if (distinctDigests.Count == 0)
        {
            return (new HashSet<string>(StringComparer.Ordinal), knownShapeByDigest);
        }

        var tempTable = NextTempTableName("cand_procdigest");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (plan_digest VARCHAR)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var digest in distinctDigests)
            {
                var row = appender.CreateRow();
                row.AppendValue(digest);
                row.EndRow();
            }
        }

        using (var known = _connection.CreateCommand())
        {
            known.CommandText =
                $"SELECT p.plan_digest, p.shape_hash FROM procedure_digests p " +
                $"JOIN {tempTable} cand ON cand.plan_digest = p.plan_digest " +
                "WHERE p.source_store = $store";
            known.Parameters.Add(new DuckDBParameter("store", sourceStore));
            using var reader = known.ExecuteReader();
            while (reader.Read())
            {
                knownShapeByDigest[reader.GetString(0)] = reader.GetString(1);
            }
        }

        var unknown = new HashSet<string>(StringComparer.Ordinal);
        using (var command = _connection.CreateCommand())
        {
            command.CommandText =
                $"SELECT cand.plan_digest FROM {tempTable} cand " +
                "ANTI JOIN (SELECT plan_digest FROM procedure_digests WHERE source_store = $store) known " +
                "ON cand.plan_digest = known.plan_digest";
            command.Parameters.Add(new DuckDBParameter("store", sourceStore));
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                unknown.Add(reader.GetString(0));
            }
        }

        DropTempTable(tempTable);
        return (unknown, knownShapeByDigest);
    }

    /// <summary>
    /// Returns the subset of (server_name, database_name, schema_name, object_name, shape_hash) keys in <paramref
    /// name="keys"/> that are NOT already recorded as exported for <paramref name="sourceStore"/>. anti-join in
    /// DuckDB, see <see cref="FilterUnknownPlanKeys"/>.
    /// </summary>
    public HashSet<(string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)> FilterUnknownProcedurePlanKeys(
        string sourceStore,
        IEnumerable<(string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)> keys)
    {
        var distinctKeys = keys.ToHashSet();
        if (distinctKeys.Count == 0)
        {
            return [];
        }

        var tempTable = NextTempTableName("cand_procplan");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText =
            $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, shape_hash VARCHAR)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var (server, database, schema, objectName, shapeHash) in distinctKeys)
            {
                var row = appender.CreateRow();
                if (server is null) row.AppendNullValue(); else row.AppendValue(server);
                if (database is null) row.AppendNullValue(); else row.AppendValue(database);
                if (schema is null) row.AppendNullValue(); else row.AppendValue(schema);
                if (objectName is null) row.AppendNullValue(); else row.AppendValue(objectName);
                row.AppendValue(shapeHash);
                row.EndRow();
            }
        }

        var result = new HashSet<(string?, string?, string?, string?, string)>();
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"SELECT cand.server_name, cand.database_name, cand.schema_name, cand.object_name, cand.shape_hash FROM {tempTable} cand " +
            "ANTI JOIN (SELECT server_name, database_name, schema_name, object_name, shape_hash FROM procedure_plan_keys WHERE source_store = $store) known " +
            "ON cand.server_name IS NOT DISTINCT FROM known.server_name " +
            "AND cand.database_name IS NOT DISTINCT FROM known.database_name " +
            "AND cand.schema_name IS NOT DISTINCT FROM known.schema_name " +
            "AND cand.object_name IS NOT DISTINCT FROM known.object_name " +
            "AND cand.shape_hash = known.shape_hash";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var server = reader.IsDBNull(0) ? null : reader.GetString(0);
                var database = reader.IsDBNull(1) ? null : reader.GetString(1);
                var schema = reader.IsDBNull(2) ? null : reader.GetString(2);
                var objectName = reader.IsDBNull(3) ? null : reader.GetString(3);
                var shapeHash = reader.GetString(4);
                result.Add((server, database, schema, objectName, shapeHash));
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Returns when each of <paramref name="identities"/> that is recorded in `procedure_plan_identity` for
    /// <paramref name="sourceStore"/> last had its plan XML fetched. An identity that is not recorded is absent from
    /// the result. The name columns match NULL-safe (a NULL name is a value, not a wildcard). Anti-join style, in
    /// DuckDB: the candidates go into a temp table and only the matching state rows come back.
    /// </summary>
    public Dictionary<ProcedurePlanIdentity, DateTime> LookupProcedurePlanIdentities(
        string sourceStore, IEnumerable<ProcedurePlanIdentity> identities)
    {
        var distinct = identities.ToHashSet();
        var result = new Dictionary<ProcedurePlanIdentity, DateTime>();
        if (distinct.Count == 0)
        {
            return result;
        }

        var tempTable = NextTempTableName("cand_procident");
        using (var createTemp = _connection.CreateCommand())
        {
            createTemp.CommandText =
                $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, " +
                "plan_handle VARCHAR, cached_time TIMESTAMP)";
            createTemp.ExecuteNonQuery();
        }

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var identity in distinct)
            {
                var row = appender.CreateRow();
                AppendNullable(row, identity.ServerName);
                AppendNullable(row, identity.DatabaseName);
                AppendNullable(row, identity.SchemaName);
                AppendNullable(row, identity.ObjectName);
                row.AppendValue(identity.PlanHandle);
                row.AppendValue(identity.CachedTime);
                row.EndRow();
            }
        }

        using (var command = _connection.CreateCommand())
        {
            command.CommandText =
                $"SELECT cand.server_name, cand.database_name, cand.schema_name, cand.object_name, cand.plan_handle, cand.cached_time, max(k.last_fetch_utc) " +
                $"FROM {tempTable} cand JOIN procedure_plan_identity k " +
                "ON k.source_store = $store " +
                "AND cand.server_name IS NOT DISTINCT FROM k.server_name " +
                "AND cand.database_name IS NOT DISTINCT FROM k.database_name " +
                "AND cand.schema_name IS NOT DISTINCT FROM k.schema_name " +
                "AND cand.object_name IS NOT DISTINCT FROM k.object_name " +
                "AND cand.plan_handle = k.plan_handle AND cand.cached_time = k.cached_time " +
                "GROUP BY ALL";
            command.Parameters.Add(new DuckDBParameter("store", sourceStore));
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var identity = new ProcedurePlanIdentity(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetString(4),
                    reader.GetDateTime(5));
                result[identity] = reader.GetDateTime(6);
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// For each of <paramref name="identities"/> that is in `procedure_plan_identity`, the shape_hash that
    /// `procedure_digests` holds for the identity's `last_digest`, or null when that digest has no shape (pruned at
    /// 14 days, or its content was missing). An identity not in state is absent from the result. Reads the local
    /// state file only; one batched lookup, like <see cref="LookupProcedurePlanIdentities"/>.
    /// </summary>
    public Dictionary<ProcedurePlanIdentity, string?> LookupProcedurePlanPreviousShapes(
        string sourceStore, IEnumerable<ProcedurePlanIdentity> identities)
    {
        var distinct = identities.ToHashSet();
        var result = new Dictionary<ProcedurePlanIdentity, string?>();
        if (distinct.Count == 0)
        {
            return result;
        }

        var tempTable = NextTempTableName("cand_procprev");
        using (var createTemp = _connection.CreateCommand())
        {
            createTemp.CommandText =
                $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, " +
                "plan_handle VARCHAR, cached_time TIMESTAMP)";
            createTemp.ExecuteNonQuery();
        }

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var identity in distinct)
            {
                var row = appender.CreateRow();
                AppendNullable(row, identity.ServerName);
                AppendNullable(row, identity.DatabaseName);
                AppendNullable(row, identity.SchemaName);
                AppendNullable(row, identity.ObjectName);
                row.AppendValue(identity.PlanHandle);
                row.AppendValue(identity.CachedTime);
                row.EndRow();
            }
        }

        var latest = new Dictionary<ProcedurePlanIdentity, DateTime>();
        using (var command = _connection.CreateCommand())
        {
            command.CommandText =
                "SELECT cand.server_name, cand.database_name, cand.schema_name, cand.object_name, cand.plan_handle, cand.cached_time, " +
                "k.last_fetch_utc, (SELECT min(p.shape_hash) FROM procedure_digests p WHERE p.source_store = $store AND p.plan_digest = k.last_digest) " +
                $"FROM {tempTable} cand JOIN procedure_plan_identity k " +
                "ON k.source_store = $store " +
                "AND cand.server_name IS NOT DISTINCT FROM k.server_name " +
                "AND cand.database_name IS NOT DISTINCT FROM k.database_name " +
                "AND cand.schema_name IS NOT DISTINCT FROM k.schema_name " +
                "AND cand.object_name IS NOT DISTINCT FROM k.object_name " +
                "AND cand.plan_handle = k.plan_handle AND cand.cached_time = k.cached_time";
            command.Parameters.Add(new DuckDBParameter("store", sourceStore));
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var identity = new ProcedurePlanIdentity(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetString(4),
                    reader.GetDateTime(5));
                var fetchedAt = reader.GetDateTime(6);
                if (latest.TryGetValue(identity, out var seen) && seen >= fetchedAt) continue; // keep the most recent fetch's digest
                latest[identity] = fetchedAt;
                result[identity] = reader.IsDBNull(7) ? null : reader.GetString(7);
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Deletes `procedure_plan_identity` rows whose `last_fetch_utc` is more than <paramref name="retentionDays"/> days
    /// before <paramref name="nowUtc"/>, and returns the number deleted. Called once per run. A pruned identity is at
    /// worst fetched again as new. The clock is a parameter so tests do not depend on the real one.
    /// </summary>
    public long PruneOldProcedurePlanIdentities(DateTime nowUtc, int retentionDays = 14)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM procedure_plan_identity WHERE last_fetch_utc < $cutoff";
        command.Parameters.Add(new DuckDBParameter("cutoff", nowUtc.AddDays(-retentionDays)));
        return command.ExecuteNonQuery();
    }

    private static void AppendNullable(IDuckDBAppenderRow row, string? value)
    {
        if (value is null) row.AppendNullValue();
        else row.AppendValue(value);
    }

    /// <summary>Test-visible: the number of rows in `procedure_plan_identity` (all stores).</summary>
    public long ProcedurePlanIdentityRowCount()
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM procedure_plan_identity";
        return Convert.ToInt64(command.ExecuteScalar() ?? 0L);
    }

    /// <summary>
    /// Deletes `procedure_digests` rows whose `recorded_at` is more than 14 days old. That
    /// matches the store's plan retention window: a pruned digest is at worst shaped again on its next sighting, which
    /// only costs one re-fetch. Idempotent -- a run with nothing older than the cutoff deletes 0 rows. Returns the
    /// number of rows deleted, which callers log. `plan_keys`, `text_keys`, `feature_digests` and
    /// `procedure_plan_keys` do NOT get a TTL: a downstream loader treats plan_shape and query_text as insert-if-absent
    /// keyed by (server, hash) with no re-export path, so "known key never re-fetched" must hold forever for those
    /// tables. Only `procedure_digests` is a pure digest-to-shape cache with no export semantics of its own, so
    /// pruning it is safe.
    /// </summary>
    public long PruneOldProcedureDigests(DateTime nowUtc, int retentionDays = 14)
    {
        var cutoff = nowUtc.AddDays(-retentionDays);
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM procedure_digests WHERE recorded_at < $cutoff";
        command.Parameters.Add(new DuckDBParameter("cutoff", cutoff));
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// Deletes `plan_fetch_pending` rows not updated for <paramref name="retentionDays"/> days, in DuckDB, and
    /// returns the number deleted. Called once per run, before <see cref="EvaluatePlanFetchGate"/>, so the state
    /// stays bounded: a plan that has not shown up for two weeks is no longer running, and if it comes back its
    /// totals start again from zero. The clock is a parameter so tests do not depend on the real one.
    /// </summary>
    public long PrunePlanFetchPending(DateTime nowUtc, int retentionDays = PlanFetchThreshold.PendingRetentionDays)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "DELETE FROM plan_fetch_pending WHERE updated_utc < $cutoff";
        command.Parameters.Add(new DuckDBParameter("cutoff", nowUtc.AddDays(-retentionDays)));
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// The plan-fetch gate. Adds each candidate's totals for this window to its `plan_fetch_pending` row (no row yet
    /// counts as zero) and reports whether the new totals qualify: executions &gt;= <paramref name="minExecutions"/>
    /// OR cpu_us &gt;= <paramref name="minCpuUs"/>. The join is NULL-safe on server_name like every other key here,
    /// and runs in DuckDB over a temp table of the candidates. Read-only. Like every other key in this file, the new
    /// totals are written by <see cref="RecordExportedKeys"/> only after the whole run succeeded, so a failed run that
    /// is retried cannot count its window twice. first_seen is the earlier of the pending row's and this window's,
    /// last_seen the later.
    /// </summary>
    public List<PlanFetchDecision> EvaluatePlanFetchGate(
        string sourceStore, IEnumerable<PlanFetchCandidate> candidates, long minExecutions, long minCpuUs)
    {
        var distinct = candidates
            .GroupBy(c => (c.ServerName, c.QueryPlanHash))
            .Select(g => g.First())
            .ToList();
        var result = new List<PlanFetchDecision>(distinct.Count);
        if (distinct.Count == 0)
        {
            return result;
        }

        var candTable = NextTempTableName("cand_fetch");
        using (var createTemp = _connection.CreateCommand())
        {
            createTemp.CommandText =
                $"CREATE TEMP TABLE {candTable} (server_name VARCHAR, query_plan_hash VARCHAR, origin VARCHAR, " +
                "executions BIGINT, cpu_us BIGINT, first_seen TIMESTAMP, last_seen TIMESTAMP)";
            createTemp.ExecuteNonQuery();
        }

        using (var appender = _connection.CreateAppender(candTable))
        {
            foreach (var c in distinct)
            {
                var row = appender.CreateRow();
                if (c.ServerName is null) row.AppendNullValue(); else row.AppendValue(c.ServerName);
                row.AppendValue(c.QueryPlanHash);
                row.AppendValue(c.Origin);
                row.AppendValue(c.Executions);
                row.AppendValue(c.CpuUs);
                row.AppendValue(c.FirstSeen);
                row.AppendValue(c.LastSeen);
                row.EndRow();
            }
        }

        using (var select = _connection.CreateCommand())
        {
            select.CommandText =
                "SELECT server_name, query_plan_hash, origin, executions, cpu_us, first_seen, last_seen, " +
                "       (executions >= $minExec OR cpu_us >= $minCpu) AS qualifies, was_pending FROM (" +
                "  SELECT c.server_name, c.query_plan_hash, c.origin, " +
                "    CAST(least(coalesce(p.executions, 0)::HUGEINT + c.executions, 9223372036854775807) AS BIGINT) AS executions, " +
                "    CAST(least(coalesce(p.cpu_us, 0)::HUGEINT + c.cpu_us, 9223372036854775807) AS BIGINT) AS cpu_us, " +
                "    least(c.first_seen, coalesce(p.first_seen, c.first_seen)) AS first_seen, " +
                "    greatest(c.last_seen, coalesce(p.last_seen, c.last_seen)) AS last_seen, " +
                "    p.query_plan_hash IS NOT NULL AS was_pending " +
                $"  FROM {candTable} c " +
                "  LEFT JOIN (SELECT * FROM plan_fetch_pending WHERE source_store = $store) p " +
                "    ON c.server_name IS NOT DISTINCT FROM p.server_name AND c.query_plan_hash = p.query_plan_hash)";
            select.Parameters.Add(new DuckDBParameter("store", sourceStore));
            select.Parameters.Add(new DuckDBParameter("minExec", minExecutions));
            select.Parameters.Add(new DuckDBParameter("minCpu", minCpuUs));
            using var reader = select.ExecuteReader();
            while (reader.Read())
            {
                result.Add(new PlanFetchDecision(
                    reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetInt64(3), reader.GetInt64(4), reader.GetDateTime(5), reader.GetDateTime(6),
                    Qualifies: reader.GetBoolean(7), WasPending: reader.GetBoolean(8)));
            }
        }

        DropTempTable(candTable);
        return result;
    }

    /// <summary>Returns the pending rows for <paramref name="sourceStore"/>, ordered by key, with their stored updated_utc.</summary>
    public List<(PlanFetchDecision Row, DateTime UpdatedUtc)> ReadPlanFetchPending(string sourceStore)
    {
        var result = new List<(PlanFetchDecision, DateTime)>();
        using var command = _connection.CreateCommand();
        command.CommandText =
            "SELECT server_name, query_plan_hash, origin, executions, cpu_us, first_seen, last_seen, updated_utc FROM plan_fetch_pending " +
            "WHERE source_store = $store ORDER BY server_name NULLS FIRST, query_plan_hash";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            result.Add((new PlanFetchDecision(
                reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetString(2),
                reader.GetInt64(3), reader.GetInt64(4), reader.GetDateTime(5), reader.GetDateTime(6),
                Qualifies: false, WasPending: true), reader.GetDateTime(7)));
        }

        return result;
    }

    /// <summary>
    /// Records newly exported plan keys and text keys in one transaction, so a crash between
    /// the two inserts cannot leave the state half-updated. Callers must only call this after
    /// the whole run has succeeded (and, when --upload is given, after the upload succeeded):
    /// a failed run must record nothing, because its output was never delivered.
    /// </summary>
    public void RecordExportedKeys(
        string sourceStore,
        IEnumerable<(string? ServerName, string QueryPlanHash)> newPlanKeys,
        IEnumerable<(string? ServerName, string? DatabaseName, string? QueryHash)> newTextKeys,
        IEnumerable<(string PlanDigest, int FeatureVersion)>? newFeatureDigests = null,
        IEnumerable<(string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)>? newProcedurePlanKeys = null,
        IEnumerable<(string PlanDigest, string ShapeHash)>? newProcedureDigests = null,
        IEnumerable<PlanFetchDecision>? planFetchPendingUpserts = null,
        DateTime? nowUtc = null,
        IEnumerable<ProcedurePlanIdentityFetch>? newProcedurePlanIdentities = null)
    {
        // The identities a procedure_plan export fetched are written in this same transaction as the run's other keys.
        // Only ExportRunner passes them (from PendingProcedurePlanIdentities), after the whole run has succeeded.
        var identityUpserts = (newProcedurePlanIdentities ?? []).ToList();

        // The plan-fetch gate's held-back totals ride along with the run's other keys. ExportRunner calls this
        // without them, so they come from the per-store recorder when not passed explicitly.
        var pendingUpserts = (planFetchPendingUpserts ?? PendingKeyRecorder.TakePlanFetchPending(sourceStore)).ToList();

        using var transaction = _connection.BeginTransaction();

        using (var insertPlan = _connection.CreateCommand())
        {
            insertPlan.CommandText = "INSERT INTO plan_keys (source_store, server_name, query_plan_hash) VALUES ($store, $server, $hash)";
            var storeParam = new DuckDBParameter("store", sourceStore);
            var serverParam = new DuckDBParameter("server", DBNull.Value);
            var hashParam = new DuckDBParameter("hash", DBNull.Value);
            insertPlan.Parameters.Add(storeParam);
            insertPlan.Parameters.Add(serverParam);
            insertPlan.Parameters.Add(hashParam);

            foreach (var (server, hash) in newPlanKeys)
            {
                serverParam.Value = (object?)server ?? DBNull.Value;
                hashParam.Value = hash;
                insertPlan.ExecuteNonQuery();
            }
        }

        using (var insertText = _connection.CreateCommand())
        {
            insertText.CommandText =
                "INSERT INTO text_keys (source_store, server_name, database_name, query_hash) VALUES ($store, $server, $db, $hash)";
            var storeParam = new DuckDBParameter("store", sourceStore);
            var serverParam = new DuckDBParameter("server", DBNull.Value);
            var dbParam = new DuckDBParameter("db", DBNull.Value);
            var hashParam = new DuckDBParameter("hash", DBNull.Value);
            insertText.Parameters.Add(storeParam);
            insertText.Parameters.Add(serverParam);
            insertText.Parameters.Add(dbParam);
            insertText.Parameters.Add(hashParam);

            foreach (var (server, database, hash) in newTextKeys)
            {
                serverParam.Value = (object?)server ?? DBNull.Value;
                dbParam.Value = (object?)database ?? DBNull.Value;
                hashParam.Value = (object?)hash ?? DBNull.Value;
                insertText.ExecuteNonQuery();
            }
        }

        if (newFeatureDigests is not null)
        {
            using var insertFeature = _connection.CreateCommand();
            insertFeature.CommandText =
                "INSERT INTO feature_digests (source_store, plan_digest, feature_version) VALUES ($store, $digest, $version)";
            var storeParam = new DuckDBParameter("store", sourceStore);
            var digestParam = new DuckDBParameter("digest", "");
            var versionParam = new DuckDBParameter("version", 0);
            insertFeature.Parameters.Add(storeParam);
            insertFeature.Parameters.Add(digestParam);
            insertFeature.Parameters.Add(versionParam);

            foreach (var (digest, version) in newFeatureDigests)
            {
                digestParam.Value = digest;
                versionParam.Value = version;
                insertFeature.ExecuteNonQuery();
            }
        }

        if (newProcedurePlanKeys is not null)
        {
            using var insertProcedurePlanKey = _connection.CreateCommand();
            insertProcedurePlanKey.CommandText =
                "INSERT INTO procedure_plan_keys (source_store, server_name, database_name, schema_name, object_name, shape_hash) " +
                "VALUES ($store, $server, $db, $schema, $object, $shape)";
            var storeParam = new DuckDBParameter("store", sourceStore);
            var serverParam = new DuckDBParameter("server", DBNull.Value);
            var dbParam = new DuckDBParameter("db", DBNull.Value);
            var schemaParam = new DuckDBParameter("schema", DBNull.Value);
            var objectParam = new DuckDBParameter("object", DBNull.Value);
            var shapeParam = new DuckDBParameter("shape", "");
            insertProcedurePlanKey.Parameters.Add(storeParam);
            insertProcedurePlanKey.Parameters.Add(serverParam);
            insertProcedurePlanKey.Parameters.Add(dbParam);
            insertProcedurePlanKey.Parameters.Add(schemaParam);
            insertProcedurePlanKey.Parameters.Add(objectParam);
            insertProcedurePlanKey.Parameters.Add(shapeParam);

            foreach (var (server, database, schema, objectName, shapeHash) in newProcedurePlanKeys)
            {
                serverParam.Value = (object?)server ?? DBNull.Value;
                dbParam.Value = (object?)database ?? DBNull.Value;
                schemaParam.Value = (object?)schema ?? DBNull.Value;
                objectParam.Value = (object?)objectName ?? DBNull.Value;
                shapeParam.Value = shapeHash;
                insertProcedurePlanKey.ExecuteNonQuery();
            }
        }

        if (newProcedureDigests is not null)
        {
            using var insertProcedureDigest = _connection.CreateCommand();
            insertProcedureDigest.CommandText =
                "INSERT INTO procedure_digests (source_store, plan_digest, shape_hash, recorded_at) VALUES ($store, $digest, $shape, now())";
            var storeParam = new DuckDBParameter("store", sourceStore);
            var digestParam = new DuckDBParameter("digest", "");
            var shapeParam = new DuckDBParameter("shape", "");
            insertProcedureDigest.Parameters.Add(storeParam);
            insertProcedureDigest.Parameters.Add(digestParam);
            insertProcedureDigest.Parameters.Add(shapeParam);

            foreach (var (digest, shapeHash) in newProcedureDigests)
            {
                digestParam.Value = digest;
                shapeParam.Value = shapeHash;
                insertProcedureDigest.ExecuteNonQuery();
            }
        }

        if (identityUpserts.Count > 0)
        {
            UpsertProcedurePlanIdentities(sourceStore, identityUpserts);
        }

        // A key that was exported has nothing left to wait for, whichever way it got exported (gate, --plan-fetch-all
        // or --ignore-state): drop its pending row.
        // Then store the totals of the keys still held back (and of qualifying keys whose content was missing, so the
        // next run starts from their totals instead of from zero).
        var exportedPlanKeys = newPlanKeys as IList<(string?, string)> ?? newPlanKeys.ToList();
        if (exportedPlanKeys.Count > 0 || pendingUpserts.Count > 0)
        {
            ReplacePlanFetchPending(sourceStore, exportedPlanKeys, pendingUpserts, nowUtc ?? DateTime.UtcNow);
        }

        transaction.Commit();
    }

    /// <summary>
    /// Replaces the `procedure_plan_identity` row of each identity in <paramref name="fetches"/> (delete, then insert).
    /// When one identity appears twice, the entry with the smallest digest wins, so the result does not depend on
    /// the order of the input. Must run inside the caller's transaction.
    /// </summary>
    private void UpsertProcedurePlanIdentities(string sourceStore, List<ProcedurePlanIdentityFetch> fetches)
    {
        var table = NextTempTableName("ident_ups");
        using (var create = _connection.CreateCommand())
        {
            create.CommandText =
                $"CREATE TEMP TABLE {table} (server_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, object_name VARCHAR, " +
                "plan_handle VARCHAR, cached_time TIMESTAMP, last_fetch_utc TIMESTAMP, last_digest VARCHAR)";
            create.ExecuteNonQuery();
        }

        using (var appender = _connection.CreateAppender(table))
        {
            foreach (var group in fetches.GroupBy(f => f.Identity))
            {
                var pick = group.OrderBy(f => f.LastDigest, StringComparer.Ordinal).First();
                var row = appender.CreateRow();
                AppendNullable(row, pick.Identity.ServerName);
                AppendNullable(row, pick.Identity.DatabaseName);
                AppendNullable(row, pick.Identity.SchemaName);
                AppendNullable(row, pick.Identity.ObjectName);
                row.AppendValue(pick.Identity.PlanHandle);
                row.AppendValue(pick.Identity.CachedTime);
                row.AppendValue(pick.LastFetchUtc);
                row.AppendValue(pick.LastDigest);
                row.EndRow();
            }
        }

        using (var delete = _connection.CreateCommand())
        {
            delete.CommandText =
                $"DELETE FROM procedure_plan_identity k USING {table} u " +
                "WHERE k.source_store = $store " +
                "AND k.server_name IS NOT DISTINCT FROM u.server_name " +
                "AND k.database_name IS NOT DISTINCT FROM u.database_name " +
                "AND k.schema_name IS NOT DISTINCT FROM u.schema_name " +
                "AND k.object_name IS NOT DISTINCT FROM u.object_name " +
                "AND k.plan_handle = u.plan_handle AND k.cached_time = u.cached_time";
            delete.Parameters.Add(new DuckDBParameter("store", sourceStore));
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO procedure_plan_identity (source_store, server_name, database_name, schema_name, object_name, plan_handle, cached_time, last_fetch_utc, last_digest) " +
                $"SELECT $store, server_name, database_name, schema_name, object_name, plan_handle, cached_time, last_fetch_utc, last_digest FROM {table}";
            insert.Parameters.Add(new DuckDBParameter("store", sourceStore));
            insert.ExecuteNonQuery();
        }

        DropTempTable(table);
    }

    private void ReplacePlanFetchPending(
        string sourceStore, IList<(string? ServerName, string QueryPlanHash)> exportedKeys,
        List<PlanFetchDecision> upserts, DateTime nowUtc)
    {
        var deleteTable = NextTempTableName("pending_del");
        var upsertTable = NextTempTableName("pending_ups");
        using (var create = _connection.CreateCommand())
        {
            create.CommandText =
                $"CREATE TEMP TABLE {deleteTable} (server_name VARCHAR, query_plan_hash VARCHAR); " +
                $"CREATE TEMP TABLE {upsertTable} (server_name VARCHAR, query_plan_hash VARCHAR, origin VARCHAR, " +
                "executions BIGINT, cpu_us BIGINT, first_seen TIMESTAMP, last_seen TIMESTAMP)";
            create.ExecuteNonQuery();
        }

        // Every exported key and every upserted key loses its old row; the upserts are then inserted fresh.
        using (var appender = _connection.CreateAppender(deleteTable))
        {
            foreach (var (server, hash) in exportedKeys.Concat(upserts.Select(u => (u.ServerName, u.QueryPlanHash))))
            {
                var row = appender.CreateRow();
                if (server is null) row.AppendNullValue(); else row.AppendValue(server);
                row.AppendValue(hash);
                row.EndRow();
            }
        }

        using (var appender = _connection.CreateAppender(upsertTable))
        {
            foreach (var u in upserts)
            {
                var row = appender.CreateRow();
                if (u.ServerName is null) row.AppendNullValue(); else row.AppendValue(u.ServerName);
                row.AppendValue(u.QueryPlanHash);
                row.AppendValue(u.Origin);
                row.AppendValue(u.Executions);
                row.AppendValue(u.CpuUs);
                row.AppendValue(u.FirstSeen);
                row.AppendValue(u.LastSeen);
                row.EndRow();
            }
        }

        // An exported key is not also an upsert, so the order below is: drop old rows, then insert the upserts.
        using (var delete = _connection.CreateCommand())
        {
            delete.CommandText =
                $"DELETE FROM plan_fetch_pending p USING {deleteTable} d WHERE p.source_store = $store " +
                "AND p.server_name IS NOT DISTINCT FROM d.server_name AND p.query_plan_hash = d.query_plan_hash";
            delete.Parameters.Add(new DuckDBParameter("store", sourceStore));
            delete.ExecuteNonQuery();
        }

        using (var insert = _connection.CreateCommand())
        {
            insert.CommandText =
                "INSERT INTO plan_fetch_pending (source_store, server_name, query_plan_hash, origin, executions, cpu_us, first_seen, last_seen, updated_utc) " +
                $"SELECT $store, server_name, query_plan_hash, origin, executions, cpu_us, first_seen, last_seen, $now FROM {upsertTable}";
            insert.Parameters.Add(new DuckDBParameter("store", sourceStore));
            insert.Parameters.Add(new DuckDBParameter("now", nowUtc));
            insert.ExecuteNonQuery();
        }

        DropTempTable(deleteTable);
        DropTempTable(upsertTable);
    }

    /// <summary>
    /// Returns the subset of (server_name, queryid) pairs in <paramref name="keys"/> that are NOT already recorded as
    /// fetched for <paramref name="sourceStore"/>: the Postgres store's `pg_statement_text` dimension keys, computed
    /// in DuckDB from the hour's already-streamed `pg_statement_stats` rows, never re-fetched once known. anti-join in
    /// DuckDB, see <see cref="FilterUnknownPlanKeys"/>.
    /// </summary>
    public HashSet<(string? ServerName, long QueryId)> FilterUnknownPgTextKeys(
        string sourceStore, IEnumerable<(string? ServerName, long QueryId)> keys)
    {
        var distinctKeys = keys.ToHashSet();
        if (distinctKeys.Count == 0)
        {
            return [];
        }

        var tempTable = NextTempTableName("cand_pgtext");
        using var createTemp = _connection.CreateCommand();
        createTemp.CommandText = $"CREATE TEMP TABLE {tempTable} (server_name VARCHAR, queryid BIGINT)";
        createTemp.ExecuteNonQuery();

        using (var appender = _connection.CreateAppender(tempTable))
        {
            foreach (var (server, queryId) in distinctKeys)
            {
                var row = appender.CreateRow();
                if (server is null) row.AppendNullValue(); else row.AppendValue(server);
                row.AppendValue(queryId);
                row.EndRow();
            }
        }

        var result = new HashSet<(string?, long)>();
        using var command = _connection.CreateCommand();
        command.CommandText =
            $"SELECT cand.server_name, cand.queryid FROM {tempTable} cand " +
            "ANTI JOIN (SELECT server_name, queryid FROM pg_text_keys WHERE source_store = $store) known " +
            "ON cand.server_name IS NOT DISTINCT FROM known.server_name AND cand.queryid = known.queryid";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var server = reader.IsDBNull(0) ? null : reader.GetString(0);
                var queryId = reader.GetInt64(1);
                result.Add((server, queryId));
            }
        }

        DropTempTable(tempTable);
        return result;
    }

    /// <summary>
    /// Records newly fetched `pg_statement_text` keys for <paramref name="sourceStore"/>, in its own small transaction
    /// -- separate from <see cref="RecordExportedKeys"/> because this state is written by
    /// <c>PgStatementTextExporter</c>, not the core dimension path.
    /// </summary>
    public void RecordPgTextKeys(string sourceStore, IEnumerable<(string? ServerName, long QueryId)> newKeys)
    {
        using var transaction = _connection.BeginTransaction();
        using var insert = _connection.CreateCommand();
        insert.CommandText = "INSERT INTO pg_text_keys (source_store, server_name, queryid) VALUES ($store, $server, $qid)";
        var storeParam = new DuckDBParameter("store", sourceStore);
        var serverParam = new DuckDBParameter("server", DBNull.Value);
        var qidParam = new DuckDBParameter("qid", 0L);
        insert.Parameters.Add(storeParam);
        insert.Parameters.Add(serverParam);
        insert.Parameters.Add(qidParam);

        foreach (var (server, queryId) in newKeys)
        {
            serverParam.Value = (object?)server ?? DBNull.Value;
            qidParam.Value = queryId;
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    /// <summary>True when a run has recorded <paramref name="table"/>'s daily snapshot for the UTC date of <paramref name="dayUtc"/>.</summary>
    public bool HasDailySnapshot(string sourceStore, string table, DateTime dayUtc)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM daily_snapshots WHERE source_store = $store AND table_name = $table AND snapshot_day = $day";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        command.Parameters.Add(new DuckDBParameter("table", table));
        command.Parameters.Add(new DuckDBParameter("day", DateTime.SpecifyKind(dayUtc.Date, DateTimeKind.Unspecified)));
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) > 0;
    }

    /// <summary>Records that <paramref name="table"/>'s daily snapshot for the UTC date of <paramref name="dayUtc"/> is exported. Recording a day twice is a no-op.</summary>
    public void RecordDailySnapshot(string sourceStore, string table, DateTime dayUtc)
    {
        if (HasDailySnapshot(sourceStore, table, dayUtc))
        {
            return;
        }

        using var command = _connection.CreateCommand();
        command.CommandText = "INSERT INTO daily_snapshots (source_store, table_name, snapshot_day) VALUES ($store, $table, $day)";
        command.Parameters.Add(new DuckDBParameter("store", sourceStore));
        command.Parameters.Add(new DuckDBParameter("table", table));
        command.Parameters.Add(new DuckDBParameter("day", DateTime.SpecifyKind(dayUtc.Date, DateTimeKind.Unspecified)));
        command.ExecuteNonQuery();
    }

    private void DropTempTable(string tempTable)
    {
        using var drop = _connection.CreateCommand();
        drop.CommandText = $"DROP TABLE IF EXISTS {tempTable}";
        drop.ExecuteNonQuery();
    }

    public void Dispose() => _connection.Dispose();
}
