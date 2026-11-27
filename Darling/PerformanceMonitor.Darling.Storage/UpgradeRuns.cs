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
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store's record of each upgrade run (#9432): one row in <c>collect.upgrade_runs</c> per run id, so a service start that
/// looks wrong can be read next to the upgrade that came before it.
///
/// <para><b>The upgrade script never opens a connection.</b> <c>upgrade-darling.ps1</c> has no PostgreSQL driver and the store
/// can be down in the middle of an upgrade. It writes <see cref="FileName"/> beside the install instead: at the start of the run
/// with state <c>in_progress</c>, again at the end with the result (<c>succeeded</c> or <c>failed</c>), and a later
/// <c>-MarkVerified</c> run sets <c>verified</c>. Each write replaces the whole file through a temp file and a move. The file
/// holds no secret.</para>
///
/// <para><b>The service records the file</b> (<see cref="UpgradeRunPickup"/>) at its start after the migrations and again on the
/// five-minute store-side pass. The upgrade restarts the service before the script ends, so the end state and the verified state
/// are only ever seen by a pass. One row per run id: <c>in_progress</c>, the result and <c>verified</c> land on the same row.</para>
///
/// <para><b>A rollback is a run of its own</b> (#9821). <c>upgrade-darling.sh --rollback</c> writes a file with a new run id into the
/// folder of the build it puts back, with the states and fields of an upgrade: <c>target_build</c> is the build that is back and
/// <c>rollback_folder</c> is the folder that holds the build it replaced. It is a second row, so the row of the upgrade it undid stays
/// as it was, whatever its state, and the newest row names the build that runs.</para>
/// </summary>
public static class UpgradeRuns
{
    /// <summary>The store migration that creates <c>collect.upgrade_runs</c>. One constant so a renumber is mechanical.</summary>
    public const int Version = 312;

    /// <summary>How long a row is kept: the same as the other store self-metrics series, <see cref="StoreSelfMetrics.RetentionDays"/>.</summary>
    public const int RetentionDays = StoreSelfMetrics.RetentionDays;

    /// <summary>The run has started and not ended, as far as the file says.</summary>
    public const string InProgress = "in_progress";

    /// <summary>The script reached its end and the service is Running again.</summary>
    public const string Succeeded = "succeeded";

    /// <summary>The script stopped on an error. The first error is in the row.</summary>
    public const string Failed = "failed";

    /// <summary>The operator's checks passed and <c>-MarkVerified</c> said so.</summary>
    public const string Verified = "verified";

    /// <summary>The states a file may carry, in the order a run moves through them.</summary>
    public static readonly IReadOnlyList<string> States = new[] { InProgress, Succeeded, Failed, Verified };

    /// <summary>The table, keyed on the run id. Timestamps are UTC <c>timestamp</c> columns like the other store tables.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.upgrade_runs
(
    run_id uuid NOT NULL,
    state text NOT NULL,
    tool_version text NULL,
    target_build text NULL,
    started_at timestamp NOT NULL,
    ended_at timestamp NULL,
    exit_code integer NULL,
    stop_check text NULL,
    rollback_folder text NULL,
    first_error text NULL,
    verified_at timestamp NULL,
    recorded_at timestamp NOT NULL,
    updated_at timestamp NOT NULL,
    CONSTRAINT pk_upgrade_runs PRIMARY KEY (run_id)
);

CREATE INDEX IF NOT EXISTS idx_upgrade_runs_started
    ON collect.upgrade_runs(started_at);";

    /// <summary>One row per run id: a second pickup of the same run updates the row, it never adds one. <c>recorded_at</c> keeps the
    /// first time the service saw the run. A row never steps back: the update runs only when the file's state is at least as far along
    /// as the stored one (in_progress, then succeeded or failed, then verified), so an older copy of the run's file, put back from a
    /// rollback backup, cannot move a failed or verified row to in_progress. A file that matches the stored row in every column the update
    /// sets writes nothing, so <c>updated_at</c> is the time the row last changed and a finished run's file that waits for its removal does
    /// not rewrite the row on each pass (#9852). The statement returns one word that keeps the three apart: <see cref="Written"/> for an
    /// insert or an update, <see cref="Unchanged"/> for a file that is current and equal to the row, <see cref="Stale"/> for a file behind
    /// the row. The skipped update returns no row of its own, so the word comes from reading the stored state as the statement began.</summary>
    public const string UpsertSql = @"
WITH written AS
(
    INSERT INTO collect.upgrade_runs AS r
        (run_id, state, tool_version, target_build, started_at, ended_at, exit_code, stop_check, rollback_folder, first_error, verified_at, recorded_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $12)
    ON CONFLICT (run_id) DO UPDATE SET
        state = EXCLUDED.state,
        tool_version = EXCLUDED.tool_version,
        target_build = EXCLUDED.target_build,
        started_at = EXCLUDED.started_at,
        ended_at = EXCLUDED.ended_at,
        exit_code = EXCLUDED.exit_code,
        stop_check = EXCLUDED.stop_check,
        rollback_folder = EXCLUDED.rollback_folder,
        first_error = EXCLUDED.first_error,
        verified_at = EXCLUDED.verified_at,
        updated_at = EXCLUDED.updated_at
    WHERE (CASE EXCLUDED.state WHEN 'verified' THEN 2 WHEN 'in_progress' THEN 0 ELSE 1 END)
        >= (CASE r.state WHEN 'verified' THEN 2 WHEN 'in_progress' THEN 0 ELSE 1 END)
      AND (r.state, r.tool_version, r.target_build, r.started_at, r.ended_at, r.exit_code, r.stop_check, r.rollback_folder, r.first_error, r.verified_at)
        IS DISTINCT FROM
          (EXCLUDED.state, EXCLUDED.tool_version, EXCLUDED.target_build, EXCLUDED.started_at, EXCLUDED.ended_at, EXCLUDED.exit_code, EXCLUDED.stop_check,
           EXCLUDED.rollback_folder, EXCLUDED.first_error, EXCLUDED.verified_at)
    RETURNING r.run_id
)
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM written) THEN 'written'
    WHEN (CASE $2 WHEN 'verified' THEN 2 WHEN 'in_progress' THEN 0 ELSE 1 END)
        >= (SELECT CASE s.state WHEN 'verified' THEN 2 WHEN 'in_progress' THEN 0 ELSE 1 END FROM collect.upgrade_runs s WHERE s.run_id = $1)
        THEN 'unchanged'
    ELSE 'stale'
END";

    /// <summary>The statement's word for an insert or an update of the row.</summary>
    public const string Written = "written";

    /// <summary>The statement's word for a file that is current and equal to the stored row: nothing was written (#9852).</summary>
    public const string Unchanged = "unchanged";

    /// <summary>The statement's word for a file behind the stored row: nothing was written, and the file is stale.</summary>
    public const string Stale = "stale";

    /// <summary>The bounded retention delete, run after each record.</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.upgrade_runs WHERE started_at < $1";
}

/// <summary>The run a file describes. Text fields are capped when the file is read.</summary>
public sealed record UpgradeRunFile(
    Guid RunId,
    string State,
    string? ToolVersion,
    string? TargetBuild,
    DateTime StartedUtc,
    DateTime? EndedUtc,
    int? ExitCode,
    string? StopCheck,
    string? RollbackFolder,
    string? FirstError,
    DateTime? VerifiedUtc);

/// <summary>What one pickup did.</summary>
public enum UpgradeRunPickupOutcome
{
    /// <summary>There was no file.</summary>
    NoFile,

    /// <summary>The file was recorded and left in place: the run is not finished or not yet verified.</summary>
    Recorded,

    /// <summary>The file was recorded and then removed: the run is verified, failed or older than a week, or the stored row is already further along than the file. A file that equals the stored row is recorded and handled by its state and age, as one that was written is (#9852).</summary>
    RecordedAndRemoved,

    /// <summary>The file was recorded but could not be removed. The next pass records it again and the row stays one row.</summary>
    RecordedNotRemoved,

    /// <summary>The file does not parse as a run. It stays where it is.</summary>
    Malformed,
}

/// <summary>The outcome of one pickup, with the run it read when the file parsed.</summary>
public readonly record struct UpgradeRunPickupResult(UpgradeRunPickupOutcome Outcome, Guid? RunId, string? State);

/// <summary>
/// Reads the upgrade run file and records it (#9432). One instance per service, because it remembers which problems it has already
/// warned about (a file that does not parse is warned about once per distinct content, not on every pass) and which run and state it
/// last said it recorded (#9841).
/// </summary>
public sealed class UpgradeRunPickup
{
    /// <summary>The file the upgrade script writes beside the install.</summary>
    public const string FileName = "upgrade-run.json";

    /// <summary>A file this large is not a run file. It is not read.</summary>
    public const int MaxFileBytes = 64 * 1024;

    /// <summary>A file whose run started longer ago than this is recorded and removed whatever its state.</summary>
    public static readonly TimeSpan MaxFileAge = TimeSpan.FromDays(7);

    private const int MaxTextChars = 2000;
    private const int MaxWarnedKeys = 100;

    private readonly ILogger _logger;
    private readonly HashSet<string> _warned = new(StringComparer.Ordinal);

    /* The run and state the last "recorded in the store" line named. A succeeded run keeps its file until it is verified, failed or a week
       old, and a pass records it again every five minutes, so the line is logged when this changes and not on every pass (#9841). */
    private (Guid RunId, string State)? _lastLoggedRecorded;

    /// <summary>Makes a pickup that logs through <paramref name="logger"/>.</summary>
    public UpgradeRunPickup(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Records the file in <paramref name="installDirectory"/> into <c>collect.upgrade_runs</c> and removes it when the run is
    /// done. A store failure propagates to the caller, which keeps the file for the next pass.
    /// </summary>
    public async Task<UpgradeRunPickupResult> PickUpAsync(
        NpgsqlConnection connection, string installDirectory, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var path = Path.Combine(installDirectory, UpgradeRunPickup.FileName);
        string text;
        try
        {
            var info = new FileInfo(path);
            if (!info.Exists)
            {
                return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.NoFile, null, null);
            }

            if (info.Length > MaxFileBytes)
            {
                WarnOnce("size:" + info.Length.ToString(CultureInfo.InvariantCulture) + ":" + info.LastWriteTimeUtc.Ticks.ToString(CultureInfo.InvariantCulture),
                    "The upgrade run file {Path} is {Bytes} bytes, more than the {Limit} a run file can be, so it was not read. It stays where it is.",
                    path, info.Length, MaxFileBytes);
                return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.Malformed, null, null);
            }

            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream, new UTF8Encoding(false), true);
            text = await reader.ReadToEndAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            WarnOnce("read:" + ex.GetType().Name, "The upgrade run file {Path} could not be read: {Message}", path, ex.Message);
            return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.NoFile, null, null);
        }

        if (!TryParse(text, out var run, out var problem))
        {
            WarnOnce("parse:" + Hash(text),
                "The upgrade run file {Path} was not recorded: {Problem}. It stays where it is, and this is logged once for this content.",
                path, problem);
            return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.Malformed, null, null);
        }

        var recorded = await RecordAsync(connection, run!, nowUtc, cancellationToken);

        /* A file behind the stored row (an older copy put back from a rollback backup) changed nothing and is stale: it is removed. A file
           equal to the stored row (#9852) is current and writes nothing: it is kept or removed by its state and age like one that was written. */
        var finished = recorded == UpgradeRuns.Stale || run!.State is UpgradeRuns.Verified or UpgradeRuns.Failed;
        var stale = nowUtc - run!.StartedUtc > MaxFileAge;
        if (!finished && !stale)
        {
            return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.Recorded, run.RunId, run.State);
        }

        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            WarnOnce("delete:" + run.RunId.ToString("N") + ":" + run.State,
                "The upgrade run {RunId} ({State}) was recorded, but {Path} could not be removed: {Message}. The next pass records it again and the store keeps one row for it.",
                run.RunId, run.State, path, ex.Message);
            return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.RecordedNotRemoved, run.RunId, run.State);
        }

        return new UpgradeRunPickupResult(UpgradeRunPickupOutcome.RecordedAndRemoved, run.RunId, run.State);
    }

    /// <summary>
    /// Logs that a pickup recorded its run, once for each run id and state (#9841). A pass that records the same run in the same state
    /// again, as it does every five minutes while a succeeded run's file waits to be verified, logs nothing. A new run id or a new state
    /// logs again. Only a pickup that recorded the file logs: the other outcomes carry their own warnings.
    /// </summary>
    public void LogRecordedOnce(UpgradeRunPickupResult result)
    {
        if (result.Outcome is not (UpgradeRunPickupOutcome.Recorded or UpgradeRunPickupOutcome.RecordedAndRemoved)
            || result.RunId is not { } runId
            || result.State is not { } state)
        {
            return;
        }

        if (_lastLoggedRecorded == (runId, state))
        {
            return;
        }

        _lastLoggedRecorded = (runId, state);
        _logger.LogInformation("Upgrade run {RunId} recorded in the store, state {State}", runId, state);
    }

    /// <summary>Parses a run file. False, with the reason, when it is not valid JSON or lacks a usable run id, state or start time.</summary>
    public static bool TryParse(string text, out UpgradeRunFile? run, out string problem)
    {
        run = null;
        try
        {
            using var document = JsonDocument.Parse(text);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                problem = "the file is not a JSON object";
                return false;
            }

            if (!TryGuid(root, "run_id", out var runId))
            {
                problem = "run_id is missing or is not a GUID";
                return false;
            }

            var state = Text(root, "state");
            if (state is null || !UpgradeRuns.States.Contains(state))
            {
                problem = "state is missing or is not one of in_progress, succeeded, failed, verified";
                return false;
            }

            if (!TryTime(root, "started_utc", out var started) || started is null)
            {
                problem = "started_utc is missing or is not a time";
                return false;
            }

            DateTime? ended = null;
            if (Has(root, "ended_utc"))
            {
                if (!TryTime(root, "ended_utc", out ended))
                {
                    problem = "ended_utc is not a time";
                    return false;
                }
            }

            DateTime? verified = null;
            if (Has(root, "verified_utc"))
            {
                if (!TryTime(root, "verified_utc", out verified))
                {
                    problem = "verified_utc is not a time";
                    return false;
                }
            }

            int? exitCode = null;
            if (Has(root, "exit_code"))
            {
                if (!root.GetProperty("exit_code").TryGetInt32(out var code))
                {
                    problem = "exit_code is not a number";
                    return false;
                }

                exitCode = code;
            }

            run = new UpgradeRunFile(
                runId, state, Text(root, "tool_version"), Text(root, "target_build"), started.Value, ended, exitCode,
                Text(root, "stop_check"), Text(root, "rollback_folder"), Text(root, "first_error"), verified);
            problem = "";
            return true;
        }
        catch (JsonException ex)
        {
            problem = "the file is not valid JSON (" + ex.Message + ")";
            return false;
        }
    }

    /// <summary><see cref="UpgradeRuns.Written"/> when the row was inserted or updated, <see cref="UpgradeRuns.Unchanged"/> when the file equals the
    /// stored row, <see cref="UpgradeRuns.Stale"/> when the stored row is further along than the file. The last two write nothing (#9852).</summary>
    private static async Task<string> RecordAsync(NpgsqlConnection connection, UpgradeRunFile run, DateTime nowUtc, CancellationToken cancellationToken)
    {
        /* The columns are timestamp without time zone, and Npgsql refuses a Kind=Utc value for one. The file's times are already Unspecified,
           so the caller's UtcNow gets the same Kind here. Without this the first pickup throws and the file never reaches the store (#9432). */
        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified);
        string recorded;
        await using (var upsert = new NpgsqlCommand(UpgradeRuns.UpsertSql, connection) { CommandTimeout = 30 })
        {
            upsert.Parameters.Add(new NpgsqlParameter { Value = run.RunId, NpgsqlDbType = NpgsqlDbType.Uuid });
            upsert.Parameters.Add(new NpgsqlParameter { Value = run.State, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.ToolVersion ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.TargetBuild ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = run.StartedUtc, NpgsqlDbType = NpgsqlDbType.Timestamp });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.EndedUtc ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.ExitCode ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.StopCheck ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.RollbackFolder ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.FirstError ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            upsert.Parameters.Add(new NpgsqlParameter { Value = (object?)run.VerifiedUtc ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
            upsert.Parameters.Add(new NpgsqlParameter { Value = now, NpgsqlDbType = NpgsqlDbType.Timestamp });
            recorded = (string?)await upsert.ExecuteScalarAsync(cancellationToken) ?? UpgradeRuns.Stale;
        }

        await using var retention = new NpgsqlCommand(UpgradeRuns.RetentionDeleteSql, connection) { CommandTimeout = 30 };
        retention.Parameters.Add(new NpgsqlParameter { Value = now.AddDays(-UpgradeRuns.RetentionDays), NpgsqlDbType = NpgsqlDbType.Timestamp });
        await retention.ExecuteNonQueryAsync(cancellationToken);
        return recorded;
    }

    private void WarnOnce(string key, string message, params object?[] args)
    {
        if (_warned.Count >= MaxWarnedKeys)
        {
            _warned.Clear();
        }

        if (_warned.Add(key))
        {
            _logger.LogWarning(message, args);
        }
    }

    private static string Hash(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    private static bool Has(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined);

    private static string? Text(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String)
        {
            return null;
        }

        var text = value.GetString();
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        return text.Length <= MaxTextChars ? text : text[..MaxTextChars];
    }

    private static bool TryGuid(JsonElement root, string name, out Guid value)
    {
        value = Guid.Empty;
        var text = Text(root, name);
        return text is not null && Guid.TryParse(text, out value) && value != Guid.Empty;
    }

    private static bool TryTime(JsonElement root, string name, out DateTime? value)
    {
        value = null;
        var text = Text(root, name);
        if (text is null
            || !DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
        {
            return false;
        }

        value = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
        return true;
    }
}
