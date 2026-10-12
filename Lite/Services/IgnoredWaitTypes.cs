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
using System.Text;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Single source for the configured "ignored" (benign/idle) wait types — the per-user editable
/// %LOCALAPPDATA%\PerformanceMonitorLite-Data\config\ignored_wait_types.json, falling back to the copy
/// bundled next to the exe. Used by BOTH the collector (skip at collection) and the wait-stats tab
/// queries (skip at display) so the two can't drift. Display-side filtering is what hides waits already
/// collected before the filter was active — copying the JSON only stops NEW collection, it can't remove
/// existing DuckDB rows (#1240).
/// </summary>
public static class IgnoredWaitTypes
{
    /// <summary>
    /// Loads the ignored wait-type set (case-insensitive): the per-user copy first, then the bundled
    /// copy next to the exe. Best-effort — returns an empty set if neither file is present or parsing
    /// fails (callers warn when the result is empty).
    /// </summary>
    public static HashSet<string> Load()
    {
        var waits = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var configPath = Path.Combine(App.ConfigDirectory, "ignored_wait_types.json");
        if (!File.Exists(configPath))
        {
            configPath = Path.Combine(AppContext.BaseDirectory, "config", "ignored_wait_types.json");
        }

        if (!File.Exists(configPath))
        {
            return waits;
        }

        try
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(configPath));
            if (doc.RootElement.TryGetProperty("ignored_waits", out var waitsArray))
            {
                foreach (var wait in waitsArray.EnumerateArray())
                {
                    var waitType = wait.GetString();
                    if (!string.IsNullOrEmpty(waitType))
                    {
                        waits.Add(waitType);
                    }
                }
            }
        }
        catch
        {
            /* Best-effort; callers warn when the resulting set is empty. */
        }

        return waits;
    }

    /// <summary>
    /// The 124 names v3.8.0 bundled, name for name and in order (git show v3.8.0:Lite/config/ignored_wait_types.json).
    /// A per-user file with no <c>seen_defaults</c> property was written by v3.8.0 or earlier, and every one
    /// of those releases shipped exactly this list, so that is what such a file has seen. Any later default
    /// differs from it, and from the <c>seen_defaults</c> every merged file then carries, with no further
    /// change here.
    /// </summary>
    internal static readonly string[] V380Defaults =
    [
        "AZURE_IMDS_VERSIONS", "BMPALLOCATION", "BMPBUILD", "BMPREPARTITION",
        "BROKER_EVENTHANDLER", "BROKER_RECEIVE_WAITFOR", "BROKER_TASK_STOP", "BROKER_TO_FLUSH",
        "BROKER_TRANSMITTER", "BUFFERPOOL_SCAN", "CHECKPOINT_QUEUE", "CHKPT",
        "CLR_AUTO_EVENT", "CLR_MANUAL_EVENT", "CLR_SEMAPHORE", "COLUMNSTORE_BUILD_THROTTLE",
        "DAC_INIT", "DBMIRROR_DBM_EVENT", "DBMIRROR_DBM_MUTEX", "DBMIRROR_EVENTS_QUEUE",
        "DBMIRROR_SEND", "DBMIRROR_WORKER_QUEUE", "DBMIRRORING_CMD", "DIRTY_PAGE_POLL",
        "DIRTY_PAGE_TABLE_LOCK", "DISPATCHER_QUEUE_SEMAPHORE", "FSAGENT", "FT_IFTS_SCHEDULER_IDLE_WAIT",
        "FT_IFTSHC_MUTEX", "HADR_CLUSAPI_CALL", "HADR_FABRIC_CALLBACK", "HADR_FILESTREAM_IOMGR_IOCOMPLETION",
        "HADR_LOGCAPTURE_WAIT", "HADR_NOTIFICATION_DEQUEUE", "HADR_TIMER_TASK", "HADR_WORK_QUEUE",
        "KSOURCE_WAKEUP", "LAZYWRITER_SLEEP", "LOGMGR_QUEUE", "MEMORY_ALLOCATION_EXT",
        "ONDEMAND_TASK_QUEUE", "PARALLEL_REDO_DRAIN_WORKER", "PARALLEL_REDO_FLOW_CONTROL", "PARALLEL_REDO_LOG_CACHE",
        "PARALLEL_REDO_TRAN_LIST", "PARALLEL_REDO_TRAN_TURN", "PARALLEL_REDO_WORKER_SYNC", "PARALLEL_REDO_WORKER_WAIT_WORK",
        "PERFORMANCE_COUNTERS_RWLOCK", "PREEMPTIVE_OS_FLUSHFILEBUFFERS", "PREEMPTIVE_XE_CALLBACKEXECUTE", "PREEMPTIVE_XE_DISPATCHER",
        "PREEMPTIVE_XE_GETTARGETSTATE", "PREEMPTIVE_XE_SESSIONCOMMIT", "PREEMPTIVE_XE_TARGETFINALIZE", "PREEMPTIVE_XE_TARGETINIT",
        "PRINT_ROLLBACK_PROGRESS", "PURVIEW_POLICY_SDK_PREEMPTIVE_SCHEDULING", "PVS_PREALLOCATE", "PWAIT_ALL_COMPONENTS_INITIALIZED",
        "PWAIT_DIRECTLOGCONSUMER_GETNEXT", "PWAIT_EXTENSIBILITY_CLEANUP_TASK", "PWAIT_HADR_ACTION_COMPLETED", "PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC",
        "PWAIT_HADR_CLUSTER_INTEGRATION", "PWAIT_HADR_FAILOVER_COMPLETED", "PWAIT_HADR_JOIN", "PWAIT_HADR_OFFLINE_COMPLETED",
        "PWAIT_HADR_ONLINE_COMPLETED", "PWAIT_HADR_POST_ONLINE_COMPLETED", "PWAIT_HADR_SERVER_READY_CONNECTIONS", "PWAIT_HADR_WORKITEM_COMPLETED",
        "PWAIT_HADRSIM", "PWAIT_MASTERDBREADY", "QDS_ASYNC_QUEUE", "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
        "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP", "QDS_SHUTDOWN_QUEUE", "QUERY_EXECUTION_INDEX_SORT_EVENT_OPEN", "QUERY_TASK_ENQUEUE_MUTEX",
        "REDO_THREAD_PENDING_WORK", "REQUEST_FOR_DEADLOCK_SEARCH", "RESOURCE_QUEUE", "RESOURCE_SEMAPHORE_MUTEX",
        "SECURITY_CNG_PROVIDER_MUTEX", "SERVER_IDLE_CHECK", "SLEEP_BUFFERPOOL_HELPLW", "SLEEP_DBSTARTUP",
        "SLEEP_DCOMSTARTUP", "SLEEP_MASTERDBREADY", "SLEEP_MASTERMDREADY", "SLEEP_MASTERUPGRADED",
        "SLEEP_MSDBSTARTUP", "SLEEP_PHYSMASTERDBREADY", "SLEEP_SYSTEMTASK", "SLEEP_TASK",
        "SLEEP_TEMPDBSTARTUP", "SNI_CRITICAL_SECTION", "SNI_HTTP_ACCEPT", "SOS_PROCESS_AFFINITY_MUTEX",
        "SOS_WORK_DISPATCHER", "SP_SERVER_DIAGNOSTICS_SLEEP", "SQLTRACE_BUFFER_FLUSH", "SQLTRACE_FILE_BUFFER",
        "SQLTRACE_FILE_READ_IO_COMPLETION", "SQLTRACE_FILE_WRITE_IO_COMPLETION", "SQLTRACE_INCREMENTAL_FLUSH_SLEEP", "SQLTRACE_WAIT_ENTRIES",
        "UCS_SESSION_REGISTRATION", "VDI_CLIENT_OTHER", "WAIT_FOR_RESULTS", "WAIT_XTP_CKPT_CLOSE",
        "WAIT_XTP_HOST_WAIT", "WAIT_XTP_OFFLINE_CKPT_NEW_LOG", "WAIT_XTP_RECOVERY", "WAITFOR",
        "WAITFOR_TASKSHUTDOWN", "WINDOW_AGGREGATES_MULTIPASS", "XE_BUFFERMGR_ALLPROCESSED_EVENT", "XE_DISPATCHER_JOIN",
        "XE_DISPATCHER_WAIT", "XE_FILE_TARGET_TVF", "XE_LIVE_TARGET_TVF", "XE_TIMER_EVENT",
    ];

    private static readonly JsonSerializerOptions CrlfOptions = WriterOptions("\r\n");
    private static readonly JsonSerializerOptions LfOptions = WriterOptions("\n");

    private static JsonSerializerOptions WriterOptions(string newLine) => new()
    {
        WriteIndented = true,
        NewLine = newLine,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private const string WaitsProperty = "ignored_waits";
    private const string SeenProperty = "seen_defaults";

    /// <summary>
    /// Merges the bundled defaults the per-user file has not seen yet into its <c>ignored_waits</c>, once.
    /// The user's file wins everywhere else: a default they removed stays removed, their own additions and
    /// ordering stay, and every other property survives. Writes the file only when something changed,
    /// through a temp file and a move so a crash can't leave it half-written. Best-effort: a missing,
    /// unreadable, locked or malformed file is logged and left exactly as it was. Returns true when the
    /// file was rewritten.
    /// </summary>
    public static bool MergeNewDefaults(string bundledPath, string userPath)
    {
        try
        {
            if (!File.Exists(userPath))
            {
                return false;
            }

            if (!File.Exists(bundledPath))
            {
                AppLogger.Warn("Config", $"Bundled ignored_wait_types.json not found at {bundledPath}; new default ignored waits were not merged");
                return false;
            }

            var bundled = ReadWaits(JsonNode.Parse(File.ReadAllText(bundledPath)) as JsonObject);
            if (bundled is null)
            {
                AppLogger.Warn("Config", "Bundled ignored_wait_types.json has no ignored_waits list; new default ignored waits were not merged");
                return false;
            }

            var original = File.ReadAllText(userPath);
            if (JsonNode.Parse(original) is not JsonObject user)
            {
                AppLogger.Warn("Config", "ignored_wait_types.json is not a JSON object; new default ignored waits were not merged");
                return false;
            }

            if (!TryMergeNewDefaults(user, bundled, out var merged, out var refusal))
            {
                if (refusal is not null)
                {
                    AppLogger.Warn("Config", $"ignored_wait_types.json {refusal}; new default ignored waits were not merged");
                }
                return false;
            }

            var newline = original.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
            var text = merged.ToJsonString(newline == "\r\n" ? CrlfOptions : LfOptions);
            if (original.EndsWith('\n'))
            {
                text += newline;
            }

            var tempPath = userPath + ".tmp";
            try
            {
                File.WriteAllText(tempPath, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                File.Move(tempPath, userPath, overwrite: true);
            }
            catch
            {
                try { File.Delete(tempPath); } catch { /* best-effort cleanup */ }
                throw;
            }

            AppLogger.Info("Config", "Merged new default ignored waits into ignored_wait_types.json");
            return true;
        }
        catch (Exception ex)
        {
            AppLogger.Warn("Config", $"Could not merge new default ignored waits into ignored_wait_types.json: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// The pure merge over the parsed per-user document. <paramref name="merged"/> is a copy with the new
    /// defaults appended to <c>ignored_waits</c> and <c>seen_defaults</c> set to what the file has now seen;
    /// it is the input itself when nothing changed. Returns false, with the input untouched, when nothing
    /// needs writing or when the document's shape is not the expected one;
    /// <paramref name="refusal"/> says why in the second case and is null when there was simply nothing new.
    /// </summary>
    internal static bool TryMergeNewDefaults(JsonObject user, IReadOnlyCollection<string> bundled, out JsonObject merged, out string? refusal)
    {
        merged = user;
        refusal = null;

        var current = ReadWaits(user);
        if (current is null)
        {
            refusal = "has no ignored_waits list of strings";
            return false;
        }

        List<string> seen;
        var seenPresent = user.TryGetPropertyValue(SeenProperty, out var seenNode);
        if (seenPresent)
        {
            var seenList = seenNode is JsonArray ? ReadWaits(user, SeenProperty) : null;
            if (seenList is null)
            {
                refusal = "has a seen_defaults that is not a list of strings";
                return false;
            }
            seen = seenList;
        }
        else
        {
            seen = [.. V380Defaults];
        }

        var seenSet = new HashSet<string>(seen, StringComparer.OrdinalIgnoreCase);
        var currentSet = new HashSet<string>(current, StringComparer.OrdinalIgnoreCase);

        var unseen = bundled.Where(w => !string.IsNullOrEmpty(w) && seenSet.Add(w)).ToList();
        var toAdd = unseen.Where(currentSet.Add).ToList();

        if (seenPresent && unseen.Count == 0)
        {
            return false;
        }

        var copy = (JsonObject)user.DeepClone();
        var waits = (JsonArray)copy[WaitsProperty]!;
        foreach (var w in toAdd)
        {
            waits.Add(JsonValue.Create(w));
        }

        var seenArray = new JsonArray();
        foreach (var w in seen.Concat(unseen))
        {
            seenArray.Add(JsonValue.Create(w));
        }
        copy[SeenProperty] = seenArray;

        merged = copy;
        return true;
    }

    /// <summary>Reads a string-array property of the document, or null when it is absent or not all strings.</summary>
    private static List<string>? ReadWaits(JsonObject? root, string property = WaitsProperty)
    {
        if (root is null || root[property] is not JsonArray array)
        {
            return null;
        }

        var list = new List<string>(array.Count);
        foreach (var item in array)
        {
            if (item is JsonValue value && value.TryGetValue<string>(out var s))
            {
                list.Add(s);
            }
            else
            {
                return null;
            }
        }
        return list;
    }

    /// <summary>
    /// Builds a SQL predicate <c>AND rtrim(wait_type) NOT IN ('A','B',...)</c> excluding the ignored types, or an
    /// empty string when there is nothing to exclude. Names are sanitized to [A-Za-z0-9_] before being
    /// inlined, so the literals are injection-safe (the source is controlled config and SQL Server
    /// wait_type names are always identifiers). Applied at display time so benign waits already in the
    /// DuckDB don't surface in the wait-stats tab/picker.
    /// <para>The stored name is compared without its trailing spaces: rows collected before the collectors
    /// trimmed wait names (<c>WaitTypeName.Trim</c>) keep the DMV's trailing space, for example
    /// <c>SQP_STATS_REPORTING </c>, until retention removes them, and the entry <c>SQP_STATS_REPORTING</c> has to
    /// hide those too. A read that groups or looks up by wait name keys on the same <c>rtrim(wait_type)</c>.</para>
    /// </summary>
    public static string BuildExclusionClause(IReadOnlyCollection<string>? ignored)
    {
        if (ignored == null || ignored.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        foreach (var wait in ignored)
        {
            if (string.IsNullOrEmpty(wait) || !IsSafeIdentifier(wait))
            {
                continue;
            }
            if (sb.Length > 0)
            {
                sb.Append(',');
            }
            sb.Append('\'').Append(wait).Append('\'');
        }

        return sb.Length == 0 ? string.Empty : $"AND rtrim(wait_type) NOT IN ({sb})";
    }

    private static bool IsSafeIdentifier(string value)
    {
        foreach (var c in value)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }
        return true;
    }
}
