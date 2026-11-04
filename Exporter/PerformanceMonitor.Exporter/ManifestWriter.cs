using System.Text.Json;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Exporter;

/// <summary>Serializes a completed run's manifest to JSON on disk.</summary>
public static class ManifestWriter
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    public static async Task WriteAsync(Manifest manifest, string path, CancellationToken ct)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        var dto = new
        {
            contract_version = FileContract.Version,
            store = manifest.Store,
            window = new
            {
                from_utc = manifest.FromUtc.ToString("O"),
                to_utc = manifest.ToUtc.ToString("O"),
            },
            exporter_version = manifest.ExporterVersion,
            darling_schema_version = manifest.DarlingSchemaVersion,
            straggler_warning = manifest.StragglerWarning,
            straggler_wait_seconds = manifest.StragglerWaitSeconds,
            tables = manifest.Tables.Select(BuildTableEntry),
            skipped_optional = manifest.SkippedOptional?.Select(s => new { table = s.Table, reason = s.Reason }),
            failed_optional = manifest.FailedOptional?.Select(f => new { table = f.Table, error = f.Error }),
        };

        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, dto, SerializerOptions, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds one table's JSON entry. <c>policy_version</c> is added only when non-null, so a core table -- which
    /// never has a policy -- never gains the key, and the manifest of a core table
    /// stays byte-identical to what it was before the field existed.
    /// </summary>
    private static IDictionary<string, object?> BuildTableEntry(ManifestTableEntry t)
    {
        var entry = new Dictionary<string, object?>
        {
            ["table"] = t.Table,
            ["output_file"] = t.OutputFile,
            ["row_count"] = t.RowCount,
            ["byte_size"] = t.ByteSize,
            ["sha256"] = t.Sha256,
            ["columns"] = t.Columns.Select(c => new { name = c.Name, duckdb_type = c.DuckDbType }),
            ["store_query_elapsed_ms"] = t.StoreQueryElapsedMs,
            ["write_elapsed_ms"] = t.WriteElapsedMs,
            ["missing_content_count"] = t.MissingContentCount,
            ["known_keys_skipped"] = t.KnownKeysSkipped,
            ["feature_digests_parsed"] = t.FeatureDigestsParsed,
            ["feature_digests_skipped"] = t.FeatureDigestsSkipped,
            ["oversized_plan_rows"] = t.OversizedPlanRows,
            ["unparsed_plan_rows"] = t.UnparsedPlanRows,
            ["time_column"] = t.TimeColumn,
            ["kind"] = t.Kind,
            ["key_columns"] = t.KeyColumns,
            ["unresolved_keys"] = t.UnresolvedKeys,
        };

        // The split timing fields sit next to store_query_elapsed_ms. A null field is omitted, so a table whose
        // exporter does not measure them keeps its old JSON shape.
        if (t.Timings is { } timings)
        {
            AddIfPresent(entry, "store_read_ms", timings.StoreReadMs);
            AddIfPresent(entry, "store_connect_ms", timings.StoreConnectMs);
            AddIfPresent(entry, "window_cache_build_ms", timings.WindowCacheBuildMs);
            AddIfPresent(entry, "pace_sleep_ms", timings.PaceSleepMs);
            AddIfPresent(entry, "parse_ms", timings.ParseMs);
            AddIfPresent(entry, "content_fetch_calls", timings.ContentFetchCalls);
            AddIfPresent(entry, "content_rows_fetched", timings.ContentRowsFetched);
            AddIfPresent(entry, "content_bytes_fetched", timings.ContentBytesFetched);
        }

        // Present only on a procedure_plan entry whose reuse mode is shadow or on. identity_skip_bytes is null in `on`
        // (the skipped content was never fetched, so there is nothing to measure).
        if (t.ProcedurePlanReuse is { } reuse)
        {
            entry["procedure_plan_reuse_mode"] = reuse.Mode;
            entry["identity_new"] = reuse.IdentityNew;
            entry["identity_refetch"] = reuse.IdentityRefetch;
            entry["identity_skip"] = reuse.IdentitySkip;
            entry["identity_null"] = reuse.IdentityNull;
            entry["identity_skip_bytes"] = reuse.IdentitySkipBytes;
            entry["identity_skip_rows"] = reuse.IdentitySkipRows;
            entry["identity_refetch_rows"] = reuse.IdentityRefetchRows;
            entry["identity_new_rows"] = reuse.IdentityNewRows;
            entry["identity_null_rows"] = reuse.IdentityNullRows;
            entry["identity_refetch_shape_changed"] = reuse.IdentityRefetchShapeChanged;
            entry["identity_refetch_shape_same"] = reuse.IdentityRefetchShapeSame;
            entry["identity_refetch_prev_unknown"] = reuse.IdentityRefetchPrevUnknown;
        }

        // Present only on a query_store_stats entry read with --query-store-source wide or compare. Null fields are omitted
        // (a fallback reason only exists after a fallback, the compare counts only in compare mode).
        if (t.QueryStoreSource is { } qs)
        {
            entry["query_store_source_mode"] = qs.Mode;
            entry["query_store_source"] = qs.Source;
            if (qs.FallbackReason is { } fallbackReason) entry["query_store_fallback_reason"] = fallbackReason;
            if (qs.FallbackDetail is { } fallbackDetail) entry["query_store_fallback_detail"] = fallbackDetail;
            AddIfPresent(entry, "query_store_fallback_server_id", qs.FallbackServerId);
            AddIfPresent(entry, "wide_read_ms", qs.WideReadMs);
            AddIfPresent(entry, "raw_read_ms", qs.RawReadMs);
            AddIfPresent(entry, "compare_only_raw", qs.CompareOnlyRaw);
            AddIfPresent(entry, "compare_only_wide", qs.CompareOnlyWide);
            AddIfPresent(entry, "compare_diff_rows", qs.CompareDiffRows);
        }

        // Present only on a gated plan_shape entry, so every other table's JSON keeps its shape.
        if (t.BelowThresholdDeferred is { } belowThresholdDeferred)
        {
            entry["below_threshold_deferred"] = belowThresholdDeferred;
        }

        if (t.DeferredNowFetched is { } deferredNowFetched)
        {
            entry["deferred_now_fetched"] = deferredNowFetched;
        }

        if (t.PolicyVersion is { } policyVersion)
        {
            entry["policy_version"] = policyVersion;
        }

        if (t.BelowThresholdSkipped is { } belowThresholdSkipped)
        {
            entry["below_threshold_skipped"] = belowThresholdSkipped;
        }

        if (t.Grain is { } grain)
        {
            entry["grain"] = grain;
        }

        if (t.SourceTable is { } sourceTable)
        {
            entry["source_table"] = sourceTable;
        }

        if (t.ReadFrom is { } readFrom)
        {
            entry["read_from"] = readFrom.ToString("O");
        }

        if (t.ReadTo is { } readTo)
        {
            entry["read_to"] = readTo.ToString("O");
        }

        if (t.DailyMissingServers is { } dailyMissingServers)
        {
            entry["daily_missing_servers"] = dailyMissingServers;
        }

        if (t.ChangeKeyColumns is { } changeKeyColumns)
        {
            entry["change_key_columns"] = changeKeyColumns;
        }

        if (t.ChangeBaseline is { } changeBaseline)
        {
            entry["change_baseline"] = changeBaseline;
        }

        if (t.ChangeRowsSuppressed is { } changeRowsSuppressed)
        {
            entry["change_rows_suppressed"] = changeRowsSuppressed;
        }

        if (t.RollupVersion is { } rollupVersion)
        {
            entry["rollup_version"] = rollupVersion;
        }

        if (t.SchemaFingerprint is { } schemaFingerprint)
        {
            entry["schema_fingerprint"] = schemaFingerprint;
        }

        return entry;
    }

    private static void AddIfPresent<T>(Dictionary<string, object?> entry, string key, T? value) where T : struct
    {
        if (value is { } v)
        {
            entry[key] = v;
        }
    }
}
