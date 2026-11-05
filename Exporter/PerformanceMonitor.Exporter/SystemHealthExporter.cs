using System.Diagnostics;
using System.Security.Cryptography;
using System.Xml;
using System.Xml.Linq;
using DuckDB.NET.Data;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Exporter.Vendor.PerformanceMonitor;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Exports the nine system_health window tables in one pass: one store query streams
/// collect.system_health_events for the window, each row is parsed in C# (the vendored
/// PerformanceMonitor.Common.SystemHealthParser for every event except scheduler_monitor,
/// which never carries the fields that parser expects -- see SchedulerMonitorParser), and the
/// shredded records are appended straight to one DuckDB appender per requested output table.
/// Only tables actually requested via --tables are created and written; an event that maps to
/// a table not requested is parsed (it's one pass) but simply not appended anywhere.
/// </summary>
public static class SystemHealthExporter
{
    /// <summary>The single store query this exporter runs, shown as-is by --dry-run.</summary>
    public const string WindowSql =
        "/* darling-export:system_health_events.window */ SELECT server_name, collection_time, event_time, event_type, event_xml " +
        "FROM collect.system_health_events WHERE collection_time >= @from AND collection_time < @to";

    private static readonly XmlReaderSettings RootProbeSettings = new()
    {
        CheckCharacters = false,
        DtdProcessing = DtdProcessing.Prohibit,
    };

    public static async Task<List<ManifestTableEntry>> ExportAsync(
        NpgsqlConnectionStringBuilder connectionStringBuilder,
        ExportOptions options,
        string[] requestedTables,
        CancellationToken ct)
    {
        var requested = new HashSet<string>(requestedTables, StringComparer.Ordinal);
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-sh-{Guid.NewGuid():N}.duckdb");
        var missingCounts = SystemHealthTables.All.ToDictionary(t => t, _ => 0L);
        var rowCounts = SystemHealthTables.All.ToDictionary(t => t, _ => 0L);

        var storeStopwatch = Stopwatch.StartNew();

        try
        {
            using var duckConnection = DuckDbSession.Open(tempDbPath);
            foreach (var table in requested)
            {
                CreateTable(duckConnection, table);
            }

            var appenders = requested.ToDictionary(t => t, t => duckConnection.CreateAppender(t));

            try
            {
                await using var pgConnection = await StoreConnectionFactory.OpenAsync(connectionStringBuilder, ct).ConfigureAwait(false);
                await using var command = new NpgsqlCommand(WindowSql, pgConnection);
                command.Parameters.AddRange(SqlBuilder.WindowParameters(options.FromUtc, options.ToUtc));

                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var serverName = reader.GetString(0);
                    var collectionTime = reader.GetDateTime(1);
                    DateTime? eventTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
                    var eventType = reader.IsDBNull(3) ? null : reader.GetString(3);
                    var eventXml = reader.IsDBNull(4) ? null : reader.GetString(4);

                    RouteEvent(
                        options.Store, serverName, collectionTime, eventTime, eventType, eventXml,
                        requested, appenders, missingCounts, rowCounts);
                }
            }
            finally
            {
                foreach (var appender in appenders.Values)
                {
                    appender.Close();
                    appender.Dispose();
                }
            }

            storeStopwatch.Stop();

            var writeStopwatch = Stopwatch.StartNew();
            var entries = new List<ManifestTableEntry>();
            foreach (var table in requested.OrderBy(t => Array.IndexOf(SystemHealthTables.All, t)))
            {
                entries.Add(WriteParquetAndBuildEntry(
                    duckConnection, options, table, rowCounts[table], missingCounts[table],
                    storeStopwatch.Elapsed.TotalMilliseconds, writeStopwatch));
            }

            return entries;
        }
        finally
        {
            DuckDbSession.DeleteTempDatabase(tempDbPath);
        }
    }

    /// <summary>
    /// Dispatches one system_health row to whichever requested output table(s) its event_type
    /// maps to, exactly the way PerformanceMonitor.Common.SystemHealthParser.ParseInto
    /// dispatches: the four sp_server_diagnostics component shreds are all attempted per
    /// event, each a no-op unless its own component node is present. A parser returning null
    /// because the whole event XML failed to parse is counted as missing for every table that
    /// event_type could have produced; a component parser returning null because its node is
    /// simply absent (a different component's event) is normal routing, not a miss.
    /// </summary>
    private static void RouteEvent(
        string sourceStore, string serverName, DateTime collectionTime, DateTime? eventTime,
        string? eventType, string? eventXml,
        HashSet<string> requested,
        Dictionary<string, DuckDBAppender> appenders,
        Dictionary<string, long> missingCounts,
        Dictionary<string, long> rowCounts)
    {
        void Append(string table, Action<IDuckDBAppenderRow> writeFields)
        {
            if (!requested.Contains(table))
            {
                return;
            }

            var row = appenders[table].CreateRow();
            row.AppendValue(sourceStore);
            row.AppendValue(serverName);
            row.AppendValue(collectionTime);
            if (eventTime.HasValue)
            {
                row.AppendValue(eventTime.Value);
            }
            else
            {
                row.AppendNullValue();
            }

            writeFields(row);
            row.EndRow();
            rowCounts[table]++;
        }

        void Miss(string table)
        {
            if (requested.Contains(table))
            {
                missingCounts[table]++;
            }
        }

        switch (eventType)
        {
            case SchedulerMonitorParser.EventType:
            {
                var record = SchedulerMonitorParser.Parse(eventXml);
                if (record == null)
                {
                    Miss(SystemHealthTables.Cpu);
                    break;
                }

                Append(SystemHealthTables.Cpu, row =>
                {
                    AppendLong(row, record.RecordId);
                    AppendLong(row, record.ProcessUtilization);
                    AppendLong(row, record.SystemIdle);
                    AppendLong(row, record.MemoryUtilization);
                    AppendLong(row, record.UserModeTime);
                    AppendLong(row, record.KernelModeTime);
                    AppendLong(row, record.PageFaults);
                    AppendLong(row, record.WorkingSetDelta);
                });
                break;
            }

            case SystemHealthParser.SpServerDiagnosticsEvent:
            {
                // A whole-event parse failure (garbage XML) is a miss on every component table
                // this event type can feed; a missing component NODE (a different component's
                // event) is normal routing and not counted.
                if (!TryParseEventRoot(eventXml))
                {
                    Miss(SystemHealthTables.System);
                    Miss(SystemHealthTables.Memory);
                    Miss(SystemHealthTables.QueryProcessing);
                    Miss(SystemHealthTables.Io);
                    Miss(SystemHealthTables.IoPending);
                    break;
                }

                var system = SystemHealthParser.ParseSystemHealth(eventXml);
                if (system != null)
                {
                    Append(SystemHealthTables.System, row =>
                    {
                        row.AppendValue(system.State);
                        AppendLong(row, system.SpinlockBackoffs);
                        row.AppendValue(system.SickSpinlockType);
                        row.AppendValue(system.SickSpinlockTypeAfterAv);
                        AppendLong(row, system.LatchWarnings);
                        AppendLong(row, system.IsAccessViolationOccurred);
                        AppendLong(row, system.WriteAccessViolationCount);
                        AppendLong(row, system.TotalDumpRequests);
                        AppendLong(row, system.IntervalDumpRequests);
                        AppendLong(row, system.NonYieldingTasksReported);
                        AppendLong(row, system.PageFaults);
                        AppendLong(row, system.SystemCpuUtilization);
                        AppendLong(row, system.SqlCpuUtilization);
                        AppendLong(row, system.BadPagesDetected);
                        AppendLong(row, system.BadPagesFixed);
                    });
                }

                var memory = SystemHealthParser.ParseMemoryConditions(eventXml);
                if (memory != null)
                {
                    Append(SystemHealthTables.Memory, row =>
                    {
                        row.AppendValue(memory.LastNotification);
                        AppendLong(row, memory.OutOfMemoryExceptions);
                        AppendBool(row, memory.IsAnyPoolOutOfMemory);
                        AppendLong(row, memory.ProcessOutOfMemoryPeriod);
                        row.AppendValue(memory.Name);
                        AppendLong(row, memory.AvailablePhysicalMemoryGb);
                        AppendLong(row, memory.AvailableVirtualMemoryGb);
                        AppendLong(row, memory.AvailablePagingFileGb);
                        AppendLong(row, memory.WorkingSetGb);
                        AppendLong(row, memory.PercentOfCommittedMemoryInWs);
                        AppendLong(row, memory.PageFaults);
                        AppendLong(row, memory.SystemPhysicalMemoryHigh);
                        AppendLong(row, memory.SystemPhysicalMemoryLow);
                        AppendLong(row, memory.ProcessPhysicalMemoryLow);
                        AppendLong(row, memory.ProcessVirtualMemoryLow);
                        AppendLong(row, memory.VmReservedGb);
                        AppendLong(row, memory.VmCommittedGb);
                        AppendLong(row, memory.LockedPagesAllocated);
                        AppendLong(row, memory.LargePagesAllocated);
                        AppendLong(row, memory.EmergencyMemoryGb);
                        AppendLong(row, memory.EmergencyMemoryInUseGb);
                        AppendLong(row, memory.TargetCommittedGb);
                        AppendLong(row, memory.CurrentCommittedGb);
                        AppendLong(row, memory.PagesAllocated);
                        AppendLong(row, memory.PagesReserved);
                        AppendLong(row, memory.PagesFree);
                        AppendLong(row, memory.PagesInUse);
                        AppendLong(row, memory.PageAllocPotential);
                        AppendLong(row, memory.NumaGrowthPhase);
                        AppendLong(row, memory.LastOomFactor);
                        AppendLong(row, memory.LastOsError);
                    });
                }

                var cpuTasks = SystemHealthParser.ParseCpuTasks(eventXml);
                if (cpuTasks != null)
                {
                    Append(SystemHealthTables.QueryProcessing, row =>
                    {
                        row.AppendValue(cpuTasks.State);
                        AppendLong(row, cpuTasks.MaxWorkers);
                        AppendLong(row, cpuTasks.WorkersCreated);
                        AppendLong(row, cpuTasks.WorkersIdle);
                        AppendLong(row, cpuTasks.TasksCompletedWithinInterval);
                        AppendLong(row, cpuTasks.PendingTasks);
                        AppendLong(row, cpuTasks.OldestPendingTaskWaitingTime);
                        AppendBool(row, cpuTasks.HasUnresolvableDeadlockOccurred);
                        AppendBool(row, cpuTasks.HasDeadlockedSchedulersOccurred);
                        AppendBool(row, cpuTasks.DidBlockingOccur);
                    });
                }

                // One system_health_io row per IO_SUBSYSTEM event, with its interval counters. The vendored
                // ParseIoIssues follows sp_HealthParser and returns rows only for events with a pending request,
                // which would drop the counters of every event without one (all of them on a quiet server).
                var ioCounters = IoSubsystemParser.Parse(eventXml);
                if (ioCounters != null)
                {
                    Append(SystemHealthTables.Io, row =>
                    {
                        row.AppendValue(ioCounters.State);
                        AppendLong(row, ioCounters.IoLatchTimeouts);
                        AppendLong(row, ioCounters.IntervalLongIos);
                        AppendLong(row, ioCounters.TotalLongIos);
                    });
                }

                // The per-file pending-request detail, one row per file, from the vendored parser.
                foreach (var io in SystemHealthParser.ParseIoIssues(eventXml))
                {
                    Append(SystemHealthTables.IoPending, row =>
                    {
                        row.AppendValue(io.State);
                        AppendLong(row, io.LongestPendingRequestsDurationMs);
                        row.AppendValue(io.LongestPendingRequestsFilePath);
                    });
                }

                break;
            }

            case SystemHealthParser.ErrorReportedEvent:
            {
                var record = SystemHealthParser.ParseSevereError(eventXml);
                if (record == null)
                {
                    Miss(SystemHealthTables.Errors);
                    break;
                }

                Append(SystemHealthTables.Errors, row =>
                {
                    AppendInt(row, record.ErrorNumber);
                    AppendInt(row, record.Severity);
                    AppendInt(row, record.State);
                    row.AppendValue(record.Message);
                    row.AppendValue(record.DatabaseName);
                    AppendInt(row, record.DatabaseId);
                    row.AppendValue(eventXml);
                });
                break;
            }

            case SystemHealthParser.MemoryBrokerEvent:
            {
                var record = SystemHealthParser.ParseMemoryBroker(eventXml);
                if (record == null)
                {
                    Miss(SystemHealthTables.MemoryBroker);
                    break;
                }

                Append(SystemHealthTables.MemoryBroker, row =>
                {
                    AppendLong(row, record.BrokerId);
                    AppendLong(row, record.PoolMetadataId);
                    AppendLong(row, record.DeltaTime);
                    AppendLong(row, record.MemoryRatio);
                    AppendLong(row, record.NewTarget);
                    AppendLong(row, record.Overall);
                    AppendLong(row, record.Rate);
                    AppendLong(row, record.CurrentlyPredicated);
                    AppendLong(row, record.CurrentlyAllocated);
                    AppendLong(row, record.PreviouslyAllocated);
                    row.AppendValue(record.Broker);
                    row.AppendValue(record.Notification);
                });
                break;
            }

            case SystemHealthParser.MemoryNodeOomEvent:
            {
                var record = SystemHealthParser.ParseMemoryNodeOom(eventXml);
                if (record == null)
                {
                    Miss(SystemHealthTables.MemoryNodeOom);
                    break;
                }

                Append(SystemHealthTables.MemoryNodeOom, row =>
                {
                    AppendLong(row, record.NodeId);
                    AppendLong(row, record.MemoryNodeId);
                    AppendLong(row, record.MemoryUtilizationPct);
                    AppendLong(row, record.TotalPhysicalMemoryKb);
                    AppendLong(row, record.AvailablePhysicalMemoryKb);
                    AppendLong(row, record.TotalPageFileKb);
                    AppendLong(row, record.AvailablePageFileKb);
                    AppendLong(row, record.TotalVirtualAddressSpaceKb);
                    AppendLong(row, record.AvailableVirtualAddressSpaceKb);
                    AppendLong(row, record.TargetKb);
                    AppendLong(row, record.ReservedKb);
                    AppendLong(row, record.CommittedKb);
                    AppendDecimal(row, record.SharedCommittedKb);
                    AppendLong(row, record.AweKb);
                    AppendLong(row, record.PagesKb);
                    row.AppendValue(record.FailureType);
                    AppendLong(row, record.FailureValue);
                    AppendLong(row, record.Resources);
                    row.AppendValue(record.FactorText);
                    AppendLong(row, record.FactorValue);
                    AppendLong(row, record.LastError);
                    AppendLong(row, record.PoolMetadataId);
                    row.AppendValue(record.IsProcessInJob);
                    row.AppendValue(record.IsSystemPhysicalMemoryHigh);
                    row.AppendValue(record.IsSystemPhysicalMemoryLow);
                    row.AppendValue(record.IsProcessPhysicalMemoryLow);
                    row.AppendValue(record.IsProcessVirtualMemoryLow);
                });
                break;
            }

            case SystemHealthParser.WaitInfoEvent:
            {
                var record = SystemHealthParser.ParseSignificantWait(eventXml);
                if (record == null)
                {
                    Miss(SystemHealthTables.Waits);
                    break;
                }

                Append(SystemHealthTables.Waits, row =>
                {
                    row.AppendValue(record.WaitType);
                    AppendLong(row, record.DurationMs);
                    AppendLong(row, record.SignalDurationMs);
                    row.AppendValue(record.WaitResource);
                    row.AppendValue(record.QueryText);
                    AppendInt(row, record.SessionId);
                    row.AppendValue(eventXml);
                });
                break;
            }

            default:
                // Not one of the six feeders this exporter knows about; nothing to route.
                break;
        }
    }

    private static bool TryParseEventRoot(string? xml)
    {
        if (string.IsNullOrWhiteSpace(xml))
        {
            return false;
        }

        try
        {
            using var reader = XmlReader.Create(new StringReader(xml.TrimStart()), RootProbeSettings);
            XElement.Load(reader);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static void AppendLong(IDuckDBAppenderRow row, long? value)
    {
        if (value.HasValue)
        {
            row.AppendValue(value.Value);
        }
        else
        {
            row.AppendNullValue();
        }
    }

    private static void AppendInt(IDuckDBAppenderRow row, int? value)
    {
        if (value.HasValue)
        {
            row.AppendValue(value.Value);
        }
        else
        {
            row.AppendNullValue();
        }
    }

    private static void AppendBool(IDuckDBAppenderRow row, bool? value)
    {
        if (value.HasValue)
        {
            row.AppendValue(value.Value);
        }
        else
        {
            row.AppendNullValue();
        }
    }

    private static void AppendDecimal(IDuckDBAppenderRow row, decimal? value)
    {
        if (value.HasValue)
        {
            row.AppendValue((double)value.Value);
        }
        else
        {
            row.AppendNullValue();
        }
    }

    /// <summary>The output column list for a system_health table, after the shared (source_store, server_name, collection_time, event_time) prefix.</summary>
    private static (string Name, string DuckDbType)[] FieldColumns(string table) => table switch
    {
        SystemHealthTables.Cpu =>
        [
            ("record_id", "BIGINT"), ("process_utilization", "BIGINT"), ("system_idle", "BIGINT"),
            ("memory_utilization", "BIGINT"), ("user_mode_time", "BIGINT"), ("kernel_mode_time", "BIGINT"),
            ("page_faults", "BIGINT"), ("working_set_delta", "BIGINT"),
        ],
        SystemHealthTables.System =>
        [
            ("state", "VARCHAR"), ("spinlock_backoffs", "BIGINT"), ("sick_spinlock_type", "VARCHAR"),
            ("sick_spinlock_type_after_av", "VARCHAR"), ("latch_warnings", "BIGINT"),
            ("is_access_violation_occurred", "BIGINT"), ("write_access_violation_count", "BIGINT"),
            ("total_dump_requests", "BIGINT"), ("interval_dump_requests", "BIGINT"),
            ("non_yielding_tasks_reported", "BIGINT"), ("page_faults", "BIGINT"),
            ("system_cpu_utilization", "BIGINT"), ("sql_cpu_utilization", "BIGINT"),
            ("bad_pages_detected", "BIGINT"), ("bad_pages_fixed", "BIGINT"),
        ],
        SystemHealthTables.Memory =>
        [
            ("last_notification", "VARCHAR"), ("out_of_memory_exceptions", "BIGINT"),
            ("is_any_pool_out_of_memory", "BOOLEAN"), ("process_out_of_memory_period", "BIGINT"),
            ("name", "VARCHAR"), ("available_physical_memory_gb", "BIGINT"),
            ("available_virtual_memory_gb", "BIGINT"), ("available_paging_file_gb", "BIGINT"),
            ("working_set_gb", "BIGINT"), ("percent_of_committed_memory_in_ws", "BIGINT"),
            ("page_faults", "BIGINT"), ("system_physical_memory_high", "BIGINT"),
            ("system_physical_memory_low", "BIGINT"), ("process_physical_memory_low", "BIGINT"),
            ("process_virtual_memory_low", "BIGINT"), ("vm_reserved_gb", "BIGINT"),
            ("vm_committed_gb", "BIGINT"), ("locked_pages_allocated", "BIGINT"),
            ("large_pages_allocated", "BIGINT"), ("emergency_memory_gb", "BIGINT"),
            ("emergency_memory_in_use_gb", "BIGINT"), ("target_committed_gb", "BIGINT"),
            ("current_committed_gb", "BIGINT"), ("pages_allocated", "BIGINT"),
            ("pages_reserved", "BIGINT"), ("pages_free", "BIGINT"), ("pages_in_use", "BIGINT"),
            ("page_alloc_potential", "BIGINT"), ("numa_growth_phase", "BIGINT"),
            ("last_oom_factor", "BIGINT"), ("last_os_error", "BIGINT"),
        ],
        SystemHealthTables.QueryProcessing =>
        [
            ("state", "VARCHAR"), ("max_workers", "BIGINT"), ("workers_created", "BIGINT"),
            ("workers_idle", "BIGINT"), ("tasks_completed_within_interval", "BIGINT"),
            ("pending_tasks", "BIGINT"), ("oldest_pending_task_waiting_time", "BIGINT"),
            ("has_unresolvable_deadlock_occurred", "BOOLEAN"), ("has_deadlocked_schedulers_occurred", "BOOLEAN"),
            ("did_blocking_occur", "BOOLEAN"),
        ],
        SystemHealthTables.Io =>
        [
            ("state", "VARCHAR"), ("io_latch_timeouts", "BIGINT"), ("interval_long_ios", "BIGINT"),
            ("total_long_ios", "BIGINT"),
        ],
        SystemHealthTables.IoPending =>
        [
            ("state", "VARCHAR"), ("pending_request_duration_ms", "BIGINT"), ("file_path", "VARCHAR"),
        ],
        SystemHealthTables.Errors =>
        [
            ("error_number", "INTEGER"), ("severity", "INTEGER"), ("state", "INTEGER"),
            ("message", "VARCHAR"), ("database_name", "VARCHAR"), ("database_id", "INTEGER"),
            ("event_xml", "VARCHAR"),
        ],
        SystemHealthTables.Waits =>
        [
            ("wait_type", "VARCHAR"), ("duration_ms", "BIGINT"), ("signal_duration_ms", "BIGINT"),
            ("wait_resource", "VARCHAR"), ("query_text", "VARCHAR"), ("session_id", "INTEGER"),
            ("event_xml", "VARCHAR"),
        ],
        SystemHealthTables.MemoryBroker =>
        [
            ("broker_id", "BIGINT"), ("pool_metadata_id", "BIGINT"), ("delta_time", "BIGINT"),
            ("memory_ratio", "BIGINT"), ("new_target", "BIGINT"), ("overall", "BIGINT"),
            ("rate", "BIGINT"), ("currently_predicated", "BIGINT"), ("currently_allocated", "BIGINT"),
            ("previously_allocated", "BIGINT"), ("broker", "VARCHAR"), ("notification", "VARCHAR"),
        ],
        SystemHealthTables.MemoryNodeOom =>
        [
            ("node_id", "BIGINT"), ("memory_node_id", "BIGINT"), ("memory_utilization_pct", "BIGINT"),
            ("total_physical_memory_kb", "BIGINT"), ("available_physical_memory_kb", "BIGINT"),
            ("total_page_file_kb", "BIGINT"), ("available_page_file_kb", "BIGINT"),
            ("total_virtual_address_space_kb", "BIGINT"), ("available_virtual_address_space_kb", "BIGINT"),
            ("target_kb", "BIGINT"), ("reserved_kb", "BIGINT"), ("committed_kb", "BIGINT"),
            ("shared_committed_kb", "DOUBLE"), ("awe_kb", "BIGINT"), ("pages_kb", "BIGINT"),
            ("failure_type", "VARCHAR"), ("failure_value", "BIGINT"), ("resources", "BIGINT"),
            ("factor_text", "VARCHAR"), ("factor_value", "BIGINT"), ("last_error", "BIGINT"),
            ("pool_metadata_id", "BIGINT"), ("is_process_in_job", "VARCHAR"),
            ("is_system_physical_memory_high", "VARCHAR"), ("is_system_physical_memory_low", "VARCHAR"),
            ("is_process_physical_memory_low", "VARCHAR"), ("is_process_virtual_memory_low", "VARCHAR"),
        ],
        _ => throw new ArgumentException($"Unknown system_health table '{table}'."),
    };

    private static void CreateTable(DuckDBConnection connection, string table)
    {
        var columns = new List<string>
        {
            "source_store VARCHAR", "server_name VARCHAR", "collection_time TIMESTAMP", "event_time TIMESTAMP",
        };
        columns.AddRange(FieldColumns(table).Select(c => $"{QuoteDuckIdentifier(c.Name)} {c.DuckDbType}"));

        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TABLE {QuoteDuckIdentifier(table)} ({string.Join(", ", columns)})";
        command.ExecuteNonQuery();
    }

    private static ManifestTableEntry WriteParquetAndBuildEntry(
        DuckDBConnection duckConnection, ExportOptions options, string table, long rowCount, long missingCount,
        double storeQueryElapsedMs, Stopwatch writeStopwatch)
    {
        var outputPath = OutputPaths.ParquetFilePath(options.OutDir, options.Store, table, options.FromUtc, options.ToUtc);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);

        using (var copyCommand = duckConnection.CreateCommand())
        {
            copyCommand.CommandText =
                $"COPY (SELECT * FROM {QuoteDuckIdentifier(table)} ORDER BY server_name, collection_time) " +
                $"TO '{outputPath.Replace("'", "''")}' ({DuckDbSession.ParquetCopyOptions})";
            copyCommand.ExecuteNonQuery();
        }

        var columns = new List<ManifestColumn>();
        using (var describeCommand = duckConnection.CreateCommand())
        {
            describeCommand.CommandText =
                $"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{outputPath.Replace("'", "''")}'))";
            using var describeReader = describeCommand.ExecuteReader();
            while (describeReader.Read())
            {
                columns.Add(new ManifestColumn { Name = describeReader.GetString(0), DuckDbType = describeReader.GetString(1) });
            }
        }

        var byteSize = new FileInfo(outputPath).Length;
        var sha256 = ComputeSha256(outputPath);

        return new ManifestTableEntry
        {
            Table = table,
            OutputFile = OutputPaths.RelativeParquetPath(options.Store, table, options.FromUtc, options.ToUtc),
            RowCount = rowCount,
            ByteSize = byteSize,
            Sha256 = sha256,
            Columns = columns,
            StoreQueryElapsedMs = storeQueryElapsedMs,
            WriteElapsedMs = writeStopwatch.Elapsed.TotalMilliseconds,
            MissingContentCount = missingCount,
        };
    }

    private static string ComputeSha256(string path)
    {
        using var stream = File.OpenRead(path);
        var hash = SHA256.HashData(stream);
        return Convert.ToHexStringLower(hash);
    }

    private static string QuoteDuckIdentifier(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}
