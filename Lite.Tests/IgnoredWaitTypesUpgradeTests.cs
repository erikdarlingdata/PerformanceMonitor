using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// An install upgraded from v3.8.0 keeps its per-user <c>ignored_wait_types.json</c>, which the seeder never
/// touches once it exists, so defaults added after v3.8.0 (RBIO_COMM_RETRY, SQP_STATS_REPORTING) never reach
/// it. <see cref="IgnoredWaitTypes.MergeNewDefaults"/> adds each bundled default the file has not yet seen,
/// exactly once, and leaves the user's removals, additions and unknown properties alone.
/// </summary>
public class IgnoredWaitTypesUpgradeTests
{
    private static readonly string[] AddedAfter380 = ["RBIO_COMM_RETRY", "SQP_STATS_REPORTING"];

    /// <summary>The bundled list exactly as v3.8.0 shipped it (git show v3.8.0:Lite/config/ignored_wait_types.json).</summary>
    private static readonly string[] V380 =
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

    private static string[] Bundled => [.. V380, .. AddedAfter380];

    private static string Json(IEnumerable<string> waits, string? tail = null) =>
        "{\r\n  \"ignored_waits\": [" + string.Join(",", waits.Select(w => "\"" + w + "\"")) + "]" + (tail ?? "") + "\r\n}\r\n";

    private static string[] Waits(JsonObject o) =>
        o["ignored_waits"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray();

    private static string[] Seen(JsonObject o) =>
        o["seen_defaults"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray();

    private static JsonObject Parse(string json) => JsonNode.Parse(json)!.AsObject();

    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_ignwaits_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void V380ShapedFile_GainsExactlyTheTwoNewDefaults_AndRecordsSeenDefaults()
    {
        var user = Parse(Json(V380));

        var changed = IgnoredWaitTypes.TryMergeNewDefaults(user, Bundled, out var merged);

        Assert.True(changed);
        Assert.Equal(V380.Concat(AddedAfter380), Waits(merged));
        Assert.Equal(Bundled.OrderBy(w => w), Seen(merged).OrderBy(w => w));
    }

    [Fact]
    public void RemovedOldDefault_StaysRemoved_WhileTheNewDefaultsAreStillAdded()
    {
        var user = Parse(Json(V380.Where(w => w != "CHECKPOINT_QUEUE")));

        IgnoredWaitTypes.TryMergeNewDefaults(user, Bundled, out var merged);

        var waits = Waits(merged);
        Assert.DoesNotContain("CHECKPOINT_QUEUE", waits, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("RBIO_COMM_RETRY", waits);
        Assert.Contains("SQP_STATS_REPORTING", waits);
        Assert.Equal(V380.Length - 1 + 2, waits.Length);
    }

    [Fact]
    public void UserAddedWait_Stays_AndKeepsItsPosition()
    {
        var user = Parse(Json(V380.Prepend("MY_OWN_WAIT")));

        IgnoredWaitTypes.TryMergeNewDefaults(user, Bundled, out var merged);

        var waits = Waits(merged);
        Assert.Equal("MY_OWN_WAIT", waits[0]);
        Assert.Equal(V380.Length + 1 + 2, waits.Length);
    }

    [Fact]
    public void AlreadyPresentNewDefault_IsNotDuplicated_AnyCase()
    {
        var user = Parse(Json(V380.Append("rbio_comm_retry")));

        IgnoredWaitTypes.TryMergeNewDefaults(user, Bundled, out var merged);

        var waits = Waits(merged);
        Assert.Single(waits, w => string.Equals(w, "RBIO_COMM_RETRY", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("SQP_STATS_REPORTING", waits);
    }

    [Fact]
    public void NewDefaultRemovedAfterTheMerge_StaysRemoved_OnTheNextStart()
    {
        var merged1Source = Parse(Json(V380));
        IgnoredWaitTypes.TryMergeNewDefaults(merged1Source, Bundled, out var afterFirstStart);

        // The user deletes RBIO_COMM_RETRY from ignored_waits; seen_defaults still names it.
        var waits = Waits(afterFirstStart).Where(w => w != "RBIO_COMM_RETRY").ToArray();
        var edited = (JsonObject)afterFirstStart.DeepClone();
        edited["ignored_waits"] = new JsonArray(waits.Select(w => (JsonNode)JsonValue.Create(w)!).ToArray());

        var changed = IgnoredWaitTypes.TryMergeNewDefaults(edited, Bundled, out var afterSecondStart);

        Assert.False(changed);
        Assert.DoesNotContain("RBIO_COMM_RETRY", Waits(afterSecondStart));
        Assert.Contains("SQP_STATS_REPORTING", Waits(afterSecondStart));
    }

    [Fact]
    public void LaterDefault_IsAddedOnce_WhenSeenDefaultsIsPresent()
    {
        IgnoredWaitTypes.TryMergeNewDefaults(Parse(Json(V380)), Bundled, out var firstStart);

        var nextRelease = Bundled.Append("A_FUTURE_DEFAULT").ToArray();
        var changed = IgnoredWaitTypes.TryMergeNewDefaults(firstStart, nextRelease, out var next);

        Assert.True(changed);
        Assert.Equal("A_FUTURE_DEFAULT", Waits(next)[^1]);
        Assert.Contains("A_FUTURE_DEFAULT", Seen(next));
    }

    [Fact]
    public void UnknownProperties_Survive()
    {
        var user = Parse(Json(V380, ",\r\n  \"note\": \"keep me\",\r\n  \"nested\": { \"a\": [1, 2] }"));

        IgnoredWaitTypes.TryMergeNewDefaults(user, Bundled, out var merged);

        Assert.Equal("keep me", merged["note"]!.GetValue<string>());
        Assert.Equal(2, merged["nested"]!["a"]!.AsArray().Count);
    }

    [Fact]
    public void MergedFile_IsAWriteOnlyOnce_SecondStartLeavesBytesAndTimestampAlone()
    {
        var dir = NewTempDir();
        try
        {
            var bundledPath = Path.Combine(dir, "bundled.json");
            var userPath = Path.Combine(dir, "user.json");
            File.WriteAllText(bundledPath, Json(Bundled));
            File.WriteAllText(userPath, Json(V380));

            Assert.True(IgnoredWaitTypes.MergeNewDefaults(bundledPath, userPath));

            var merged = JsonNode.Parse(File.ReadAllText(userPath))!.AsObject();
            Assert.Equal(V380.Concat(AddedAfter380), Waits(merged));
            Assert.Contains("\r\n", File.ReadAllText(userPath));

            var stamp = new DateTime(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc);
            File.SetLastWriteTimeUtc(userPath, stamp);
            var bytes = File.ReadAllBytes(userPath);

            Assert.False(IgnoredWaitTypes.MergeNewDefaults(bundledPath, userPath));

            Assert.Equal(bytes, File.ReadAllBytes(userPath));
            Assert.Equal(stamp, File.GetLastWriteTimeUtc(userPath));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp"));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void MalformedUserFile_IsLeftByteIdentical()
    {
        var dir = NewTempDir();
        try
        {
            var bundledPath = Path.Combine(dir, "bundled.json");
            var userPath = Path.Combine(dir, "user.json");
            File.WriteAllText(bundledPath, Json(Bundled));
            var original = Encoding.UTF8.GetBytes("{ \"ignored_waits\": [\"CHKPT\", ");
            File.WriteAllBytes(userPath, original);

            var changed = IgnoredWaitTypes.MergeNewDefaults(bundledPath, userPath);

            Assert.False(changed);
            Assert.Equal(original, File.ReadAllBytes(userPath));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp"));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void MissingUserFile_IsNotCreatedByTheMerge()
    {
        var dir = NewTempDir();
        try
        {
            var bundledPath = Path.Combine(dir, "bundled.json");
            File.WriteAllText(bundledPath, Json(Bundled));

            Assert.False(IgnoredWaitTypes.MergeNewDefaults(bundledPath, Path.Combine(dir, "user.json")));
            Assert.False(File.Exists(Path.Combine(dir, "user.json")));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void FreshlySeededCopyOfTheBundle_KeepsItsWaits_AndGainsSeenDefaults()
    {
        var dir = NewTempDir();
        try
        {
            var bundledDir = Path.Combine(dir, "bundled");
            var userDir = Path.Combine(dir, "user");
            Directory.CreateDirectory(bundledDir);
            File.Copy(FindBundledFile(), Path.Combine(bundledDir, "ignored_wait_types.json"));

            ConfigSeeder.SeedMissing(bundledDir, userDir, ["ignored_wait_types.json"]);
            var userPath = Path.Combine(userDir, "ignored_wait_types.json");
            var bundledPath = Path.Combine(bundledDir, "ignored_wait_types.json");
            var before = Waits(JsonNode.Parse(File.ReadAllText(userPath))!.AsObject());

            IgnoredWaitTypes.MergeNewDefaults(bundledPath, userPath);

            var after = JsonNode.Parse(File.ReadAllText(userPath))!.AsObject();
            Assert.Equal(before, Waits(after));
            Assert.Equal(before.OrderBy(w => w), Seen(after).OrderBy(w => w));
            Assert.Contains("RBIO_COMM_RETRY", Waits(after));
            Assert.Contains("SQP_STATS_REPORTING", Waits(after));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void BundledList_StillCarriesEveryV380DefaultAndTheTwoAddedOnes()
    {
        var bundled = Waits(JsonNode.Parse(File.ReadAllText(FindBundledFile()))!.AsObject());

        Assert.All(V380.Concat(AddedAfter380), w => Assert.Contains(w, bundled));
        Assert.Equal(AddedAfter380.OrderBy(w => w), IgnoredWaitTypes.DefaultsAddedAfter380.OrderBy(w => w));
    }

    private static string FindBundledFile()
    {
        var relative = Path.Combine("Lite", "config", "ignored_wait_types.json");
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relative);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relative} walking up from {AppContext.BaseDirectory}");
    }
}
