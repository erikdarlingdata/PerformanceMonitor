/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One slow or failed read, ready to store in <c>collect.slow_reads</c> (#5097).</summary>
internal sealed record SlowReadRecord(
    DateTime ReadTimeUtc,
    string Surface,
    string Route,
    string Outcome,
    int TotalMs,
    int? ServerId,
    DateTime? WindowStart,
    DateTime? WindowEnd,
    string ArgumentsJson,
    bool ArgumentsTruncated,
    string? Source,
    string? SourceReason,
    string StatementsJson,
    int StatementCount,
    bool StatementsTruncated,
    string? ErrorClass,
    long? RowCount = null);

/// <summary>
/// The slow-read record's write path (#5097). The three recorders (the MCP tool filter, the <c>/api/read/*</c> loop
/// and the composed-panel runner) call <see cref="Offer"/> when a read finishes; it decides whether the read is worth
/// a row and hands it to a bounded channel (capacity 256, oldest dropped and counted). One background writer
/// (<see cref="RunAsync"/>) inserts each record and then purges, on the stall-probe store's shape: failure-isolated, at
/// Debug, never reaching a request. Retention and the row cap are paid by the insert, not by the daily sweep, because
/// that sweep enumerates the collector catalog and this table is deliberately not in it.
/// </summary>
public sealed class SlowReadLog
{
    /// <summary>The default for a read at or over this many milliseconds, which is recorded whatever its outcome.</summary>
    internal const int DefaultThresholdMs = 5_000;

    /// <summary>The age and row-cap purge runs at most this often...</summary>
    internal static readonly TimeSpan PurgeInterval = TimeSpan.FromSeconds(60);

    /// <summary>...or after this many inserts, whichever comes first.</summary>
    internal const int PurgeEveryInserts = 100;

    internal const int MaxStringLength = 128;
    internal const int MaxDepth = 8;
    internal const int MaxArrayElements = 50;
    internal const string OmittedMarker = "[omitted]";
    internal const string RedactedMarker = "[redacted]";

    internal const int Capacity = 256;
    internal const int RetentionDays = 30;
    internal const int RowCap = 10_000;
    internal const int MaxArgumentsBytes = 4_096;
    internal const int MaxStatementsStored = 50;

    internal const string InsertSql = @"
INSERT INTO collect.slow_reads
(read_time, surface, route, outcome, total_ms, server_id, window_start, window_end, arguments, arguments_truncated,
 source, source_reason, statements, statement_count, statements_truncated, error_class, row_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);";

    internal const string PurgeSql = @"
DELETE FROM collect.slow_reads WHERE read_time < $1;";

    /* Keeps the newest RowCap rows in the read's own total order (time, then id). */
    internal const string CapSql = @"
DELETE FROM collect.slow_reads
WHERE slow_read_id IN
(
    SELECT slow_read_id FROM collect.slow_reads ORDER BY read_time DESC, slow_read_id DESC OFFSET $1
);";

    private readonly Channel<SlowReadRecord> _channel;
    private long _dropped;
    private DateTime _lastPurgeUtc = DateTime.MinValue;
    private int _insertsSincePurge;

    public SlowReadLog()
        : this(DefaultThresholdMs)
    {
    }

    internal SlowReadLog(int thresholdMs)
    {
        ThresholdMs = thresholdMs;
        _channel = Channel.CreateBounded<SlowReadRecord>(
            new BoundedChannelOptions(Capacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false,
            },
            _ => Interlocked.Increment(ref _dropped));
    }

    /// <summary>A read at or over this many milliseconds is recorded whatever its outcome.</summary>
    internal int ThresholdMs { get; }

    /// <summary>Records dropped because the channel was full.</summary>
    internal long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Hands one record to the writer; false only when the writer has finished.</summary>
    internal bool TryEnqueue(SlowReadRecord record) => _channel.Writer.TryWrite(record);

    /// <summary>Reads what is queued without a writer (for tests).</summary>
    internal bool TryRead(out SlowReadRecord? record)
    {
        var ok = _channel.Reader.TryRead(out var item);
        record = item;
        return ok;
    }

    /// <summary>The recording rule: over the threshold, or ended in a timeout, error or limit. A cancelled read and
    /// the fallbacks are recorded only when they are also over the threshold; their counts live in read_latency.</summary>
    internal bool ShouldRecord(ReadOutcome outcome, long totalMs) =>
        totalMs >= ThresholdMs || outcome is ReadOutcome.Timeout or ReadOutcome.Error or ReadOutcome.Limit;

    /// <summary>Decides, builds and enqueues one record. Never throws.</summary>
    internal void Offer(
        ReadScope scope, ReadSurface surface, string route, ReadOutcome outcome, long totalMs,
        JsonObject? arguments, string? errorClass, ILogger? logger)
    {
        try
        {
            if (!ShouldRecord(outcome, totalMs))
            {
                return;
            }

            TryEnqueue(Build(scope, surface, route, outcome, totalMs, arguments, errorClass, DateTime.UtcNow));
        }
        catch (Exception ex)
        {
            logger?.LogDebug(ex, "Slow-read recording failed for {Route}.", route);
        }
    }

    internal static SlowReadRecord Build(
        ReadScope scope, ReadSurface surface, string route, ReadOutcome outcome, long totalMs,
        JsonObject? arguments, string? errorClass, DateTime nowUtc)
    {
        var (argumentsJson, argumentsTruncated, windowStart, windowEnd) = NormaliseArguments(arguments, scope.ServerId, scope.StartedUtc);

        var timings = scope.Snapshot().OrderBy(t => t.Ordinal).Take(MaxStatementsStored).ToList();
        var statementsTruncated = scope.StatementTotal > timings.Count;
        var statements = new JsonArray();
        foreach (var t in timings)
        {
            statements.Add(new JsonObject
            {
                ["ordinal"] = t.Ordinal,
                ["label"] = t.Label,
                ["hash"] = t.Hash,
                ["ms"] = Math.Round(t.DurationMs, 1, MidpointRounding.AwayFromZero),
                ["rows"] = t.Rows,
            });
        }

        return new SlowReadRecord(
            DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified),
            surface.ToString().ToLowerInvariant(),
            route,
            ReadLatencyAccumulator.OutcomeLabel(outcome),
            (int)Math.Min(int.MaxValue, Math.Max(0, totalMs)),
            scope.ServerId,
            windowStart,
            windowEnd,
            argumentsJson,
            argumentsTruncated,
            scope.Source,
            scope.SourceReason,
            statements.ToJsonString(),
            scope.StatementTotal,
            statementsTruncated,
            errorClass,
            scope.Rows);
    }

    /// <summary>The arguments as stored, one policy for every surface: the server name replaced by its id, the window
    /// (hours / hours_back ending at as_of or the read's start) lifted into start and end, every value passed through
    /// <see cref="Scrub"/>, and the whole object held to <see cref="MaxArgumentsBytes"/> (else a truncated object
    /// naming the keys and the flag set).</summary>
    internal static (string Json, bool Truncated, DateTime? WindowStart, DateTime? WindowEnd) NormaliseArguments(
        JsonObject? arguments, int? serverId, DateTime startedUtc)
    {
        var normal = new JsonObject();
        DateTime? windowStart = null;
        DateTime? windowEnd = null;

        if (arguments is not null)
        {
            foreach (var (key, value) in arguments)
            {
                if (key is "server_name" or "server")
                {
                    continue;
                }

                normal[key] = IsSecretKey(key) ? JsonValue.Create(RedactedMarker) : Scrub(value, 1);
            }

            var hours = NumberOf(arguments, "hours") ?? NumberOf(arguments, "hours_back");
            if (hours is > 0)
            {
                windowEnd = ParseUtc(arguments, "as_of") ?? startedUtc;
                windowStart = windowEnd.Value.AddHours(-hours.Value);
            }
        }

        if (serverId is not null)
        {
            normal["server_id"] = serverId;
        }

        var json = normal.ToJsonString();
        if (Encoding.UTF8.GetByteCount(json) <= MaxArgumentsBytes)
        {
            return (json, false, Unspecified(windowStart), Unspecified(windowEnd));
        }

        var names = new JsonArray();
        foreach (var key in normal.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal).Take(20))
        {
            names.Add(key.Length > 64 ? key[..64] : key);
        }

        return (new JsonObject { ["truncated"] = true, ["keys"] = names }.ToJsonString(), true, Unspecified(windowStart), Unspecified(windowEnd));
    }

    /// <summary>True when a key's lower-cased name carries a secret fragment.</summary>
    internal static bool IsSecretKey(string key) => SecretTextGuard.IsSecretKey(key);

    /// <summary>
    /// The one argument policy for the MCP, web and compose surfaces. Numbers, booleans and nulls are kept. A string is
    /// kept only when it is at most <see cref="MaxStringLength"/> characters and does not parse as a JSON object or
    /// array; anything else becomes <see cref="OmittedMarker"/>, so a JSON-bearing argument (a server batch, a settings
    /// document) never reaches the table. Objects and arrays are walked to <see cref="MaxDepth"/> levels (deeper is
    /// omitted) and <see cref="MaxArrayElements"/> elements; a key naming a secret is redacted at any depth.
    /// </summary>
    internal static JsonNode? Scrub(JsonNode? node, int depth)
    {
        switch (node)
        {
            case null:
                return null;
            case JsonObject obj:
            {
                if (depth > MaxDepth)
                {
                    return JsonValue.Create(OmittedMarker);
                }

                var copy = new JsonObject();
                foreach (var (key, value) in obj)
                {
                    copy[key] = IsSecretKey(key) ? JsonValue.Create(RedactedMarker) : Scrub(value, depth + 1);
                }

                return copy;
            }

            case JsonArray array:
            {
                if (depth > MaxDepth)
                {
                    return JsonValue.Create(OmittedMarker);
                }

                var copy = new JsonArray();
                var taken = 0;
                foreach (var item in array)
                {
                    if (taken == MaxArrayElements)
                    {
                        copy.Add(JsonValue.Create("[omitted " + (array.Count - MaxArrayElements).ToString(CultureInfo.InvariantCulture) + " more]"));
                        break;
                    }

                    copy.Add(Scrub(item, depth + 1));
                    taken++;
                }

                return copy;
            }

            case JsonValue value:
            {
                if (value.TryGetValue<string>(out var text))
                {
                    return KeepString(text) ? JsonValue.Create(text) : JsonValue.Create(OmittedMarker);
                }

                return value.DeepClone();
            }

            default:
                return JsonValue.Create(OmittedMarker);
        }
    }

    private static bool KeepString(string text)
    {
        if (text.Length > MaxStringLength)
        {
            return false;
        }

        /* JSON-looking text is never stored, parsed or not: a BOM, a trailing comma or a comment would defeat the parse. */
        var cleaned = text.Replace("\uFEFF", string.Empty, StringComparison.Ordinal).Trim();
        if (cleaned.Contains('{', StringComparison.Ordinal) || cleaned.Contains('[', StringComparison.Ordinal))
        {
            return false;
        }

        return !SecretTextGuard.ValueLooksSecret(cleaned);
    }

    private static DateTime? Unspecified(DateTime? value)
    {
        if (value.HasValue)
        {
            return DateTime.SpecifyKind(value.Value, DateTimeKind.Unspecified);
        }

        return null;
    }

    private static double? NumberOf(JsonObject arguments, string key)
    {
        if (arguments[key] is not JsonValue value)
        {
            return null;
        }

        if (value.TryGetValue<int>(out var whole))
        {
            return whole;
        }

        if (value.TryGetValue<long>(out var wide))
        {
            return wide;
        }

        if (value.TryGetValue<double>(out var number))
        {
            return number;
        }

        return value.TryGetValue<string>(out var text)
            && double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
    }

    private static DateTime? ParseUtc(JsonObject arguments, string key) =>
        arguments[key] is JsonValue value && value.TryGetValue<string>(out var text)
            && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed : null;

    /// <summary>The type name or SQLSTATE of a fault, never its message text.</summary>
    internal static string ErrorClassOf(Exception? ex)
    {
        if (ex is null)
        {
            return string.Empty;
        }

        if (ex is PostgresException postgres && !string.IsNullOrEmpty(postgres.SqlState))
        {
            return postgres.SqlState;
        }

        return ex.GetType().Name;
    }

    /// <summary>The class of an error that arrived as a tool's caught answer rather than an exception.</summary>
    internal static string? ErrorClassOf(ReadOutcome outcome) => outcome switch
    {
        ReadOutcome.Timeout => "57014",
        ReadOutcome.Limit => "53400",
        _ => null,
    };

    /// <summary>The one writer: drains the channel until <paramref name="cancellationToken"/> fires, storing each record.</summary>
    internal async Task RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var record in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await StoreAsync(postgres, record, logger).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown. */
        }
    }

    /// <summary>Whether this insert should be followed by the purge: the first one, then one per
    /// <see cref="PurgeInterval"/> or <see cref="PurgeEveryInserts"/> inserts, whichever comes first. The one writer
    /// calls it, so it takes no lock.</summary>
    internal bool PurgeDue(DateTime nowUtc)
    {
        _insertsSincePurge++;
        if (_insertsSincePurge < PurgeEveryInserts && nowUtc - _lastPurgeUtc < PurgeInterval)
        {
            return false;
        }

        _insertsSincePurge = 0;
        _lastPurgeUtc = nowUtc;
        return true;
    }

    /// <summary>Inserts one record, then purges by age and by row cap when the purge is due. A failure is logged at
    /// Debug and goes no further.</summary>
    internal async Task StoreAsync(NpgsqlDataSource postgres, SlowReadRecord record, ILogger? logger)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);

            await using (var insert = new NpgsqlCommand(InsertSql, connection))
            {
                insert.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, record.ReadTimeUtc);
                insert.Parameters.AddWithValue(record.Surface);
                insert.Parameters.AddWithValue(record.Route);
                insert.Parameters.AddWithValue(record.Outcome);
                insert.Parameters.AddWithValue(record.TotalMs);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)record.ServerId ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)record.WindowStart ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)record.WindowEnd ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = record.ArgumentsJson });
                insert.Parameters.AddWithValue(record.ArgumentsTruncated);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.Source ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.SourceReason ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = record.StatementsJson });
                insert.Parameters.AddWithValue(record.StatementCount);
                insert.Parameters.AddWithValue(record.StatementsTruncated);
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)record.ErrorClass ?? DBNull.Value });
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)record.RowCount ?? DBNull.Value });
                await insert.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }

            if (!PurgeDue(DateTime.UtcNow))
            {
                return;
            }

            await using (var purge = new NpgsqlCommand(PurgeSql, connection))
            {
                purge.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                purge.Parameters.AddWithValue(NpgsqlDbType.Timestamp, record.ReadTimeUtc.AddDays(-RetentionDays));
                await purge.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }

            await using (var cap = new NpgsqlCommand(CapSql, connection))
            {
                cap.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                cap.Parameters.AddWithValue(NpgsqlDbType.Bigint, (long)RowCap);
                await cap.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger?.LogDebug(ex, "A slow-read row for {Route} was not stored.", record.Route);
        }
    }
}
