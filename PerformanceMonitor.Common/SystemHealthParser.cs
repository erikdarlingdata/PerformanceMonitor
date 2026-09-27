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
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Shreds the raw <c>system_health</c> Extended Event XML captured by
    /// <c>SystemHealthEventsCollector</c> (Stage 1) into the eight unique warning-category record types — the
    /// monitor-side C# port of Erik's <c>sp_HealthParser</c> per-category xpath extraction. Pure and
    /// app-agnostic (Common), the same home and shape as <see cref="DeadlockGraphParser"/>: static, driven by
    /// <see cref="System.Xml.Linq"/>, and robust to malformed input (a bad event yields null / no record
    /// rather than throwing).
    ///
    /// <para>This is a <b>parse-on-read</b> shred: it reads stored raw event XML and produces typed records
    /// with NO target proc, NO persisted category tables, and NO viewer coupling — so events can be
    /// reprocessed without re-collecting. It faithfully extracts every column sp_HealthParser defines but
    /// deliberately does NOT apply sp_HealthParser's warnings_only-specific severity/duration/status WHERE
    /// predicates: those are an analysis/display concern (Stage 2b) and every field needed to re-apply them is
    /// preserved on the records. The ONE exception is <see cref="ParseSevereError"/>'s base filter (severity
    /// &lt; 16, and the two benign connection-reset error numbers) — sp_HealthParser applies that floor
    /// unconditionally, even in its unfiltered mode, so it is applied here too rather than duplicated in
    /// every consumer.</para>
    ///
    /// <para>Dispatch is by XE event name (<c>event_type</c>): the ring-buffer / error / wait feeders each map
    /// to exactly one category, while an <c>sp_server_diagnostics_component_result</c> event routes on its
    /// component — SYSTEM → system health (corruption + contention counters), RESOURCE → memory conditions,
    /// QUERY_PROCESSING → CPU tasks, IO_SUBSYSTEM → IO issues. Each component parser gates on the presence of
    /// its own node (<c>&lt;system&gt;</c> / <c>&lt;resource&gt;</c> / <c>&lt;queryProcessing&gt;</c> /
    /// <c>&lt;ioSubsystem&gt;</c>), so a single event yields a record for its component alone and nothing for
    /// the others (sp_HealthParser's remaining EVENTS component is out of scope). IO issues is the one
    /// many-per-event category: sp_HealthParser fans an IO_SUBSYSTEM event out to one row per pending-request
    /// file, so its parser returns a list.</para>
    /// </summary>
    public static class SystemHealthParser
    {
        // XE event names — the Stage 1 capture filter and this dispatcher agree on exactly these six.
        public const string SchedulerMonitorEvent = "scheduler_monitor_system_health_ring_buffer_recorded";
        public const string ErrorReportedEvent = "error_reported";
        public const string SpServerDiagnosticsEvent = "sp_server_diagnostics_component_result";
        public const string MemoryBrokerEvent = "memory_broker_ring_buffer_recorded";
        public const string MemoryNodeOomEvent = "memory_node_oom_ring_buffer_recorded";
        public const string WaitInfoEvent = "wait_info";

        /// <summary>
        /// Shreds a batch of captured events into the six per-category lists. <paramref name="events"/> is the
        /// (<c>event_type</c>, <c>event_xml</c>) pair per stored <c>system_health_events</c> row; a null/blank
        /// or unrecognized entry is skipped. Never throws.
        /// </summary>
        public static SystemHealthParseResult ParseEvents(IEnumerable<(string? EventType, string? EventXml)> events)
        {
            var result = new SystemHealthParseResult();
            if (events == null)
                return result;

            foreach (var (eventType, eventXml) in events)
                ParseInto(eventType, eventXml, result);

            return result;
        }

        /// <summary>
        /// Dispatches one event on its <paramref name="eventType"/> and appends the shredded record (if any)
        /// to the matching list on <paramref name="result"/>. When <paramref name="eventType"/> is null/blank
        /// it falls back to the event XML's own <c>@name</c> attribute. Never throws.
        /// </summary>
        public static void ParseInto(string? eventType, string? eventXml, SystemHealthParseResult result)
        {
            if (result == null || string.IsNullOrWhiteSpace(eventXml))
                return;

            var name = eventType;
            if (string.IsNullOrWhiteSpace(name))
                name = EventRoot(eventXml)?.Attribute("name")?.Value;
            if (string.IsNullOrWhiteSpace(name))
                return;

            switch (name)
            {
                case SchedulerMonitorEvent:
                    Add(result.SchedulerIssues, ParseSchedulerIssue(eventXml));
                    break;
                case ErrorReportedEvent:
                    Add(result.SevereErrors, ParseSevereError(eventXml));
                    break;
                case SpServerDiagnosticsEvent:
                    // One event carries one component; each parser is a no-op unless its component node is present.
                    Add(result.SystemHealth, ParseSystemHealth(eventXml));
                    Add(result.MemoryConditions, ParseMemoryConditions(eventXml));
                    Add(result.CpuTasks, ParseCpuTasks(eventXml));
                    result.IoIssues.AddRange(ParseIoIssues(eventXml));
                    break;
                case MemoryBrokerEvent:
                    Add(result.MemoryBroker, ParseMemoryBroker(eventXml));
                    break;
                case MemoryNodeOomEvent:
                    Add(result.MemoryNodeOom, ParseMemoryNodeOom(eventXml));
                    break;
                case WaitInfoEvent:
                    Add(result.SignificantWaits, ParseSignificantWait(eventXml));
                    break;
                default:
                    break; // Not one of the six feeders — ignore (parity with the Stage 1 capture filter).
            }
        }

        private static void Add<T>(List<T> list, T? record) where T : class
        {
            if (record != null)
                list.Add(record);
        }

        // ── per-category shredders (each independently testable with one raw event XML) ──

        /// <summary>
        /// Shreds a <c>scheduler_monitor_system_health_ring_buffer_recorded</c> event into a
        /// <see cref="SchedulerIssueRecord"/>. Returns null for null/blank or unparseable XML.
        /// </summary>
        public static SchedulerIssueRecord? ParseSchedulerIssue(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            if (ev == null)
                return null;

            return new SchedulerIssueRecord
            {
                EventTime = ParseTimestamp(ev),
                SchedulerId = ParseInt(DataValue(ev, "scheduler_id")),
                CpuId = ParseInt(DataValue(ev, "cpu_id")),
                Status = DataText(ev, "status"),
                IsOnline = ParseBool(DataValue(ev, "is_online")),
                IsRunnable = ParseBool(DataValue(ev, "is_runnable")),
                IsRunning = ParseBool(DataValue(ev, "is_running")),
                NonYieldingTimeMs = ParseLong(DataValue(ev, "non_yielding_time")),
                ThreadQuantumMs = ParseLong(DataValue(ev, "thread_quantum")),
            };
        }

        /// <summary>
        /// Shreds an <c>error_reported</c> event into a <see cref="SevereErrorRecord"/>. Returns null for
        /// null/blank or unparseable XML, and also null (dropped) for the two rows sp_HealthParser's own base
        /// filter never surfaces even under its unfiltered (<c>@warnings_only = 0</c>) mode: severity below 16,
        /// and the two benign connection-reset error numbers (17830, 18056) — sp_HealthParser.sql's
        /// <c>WHERE ... [. &gt;= 16]</c> plus its <c>#ignore_errors</c> anti-join. Applying that floor here, once,
        /// means every consumer (unfiltered or significant-only) already agrees with the proc's base population;
        /// <see cref="SystemHealthSignificance.IsSignificant(SevereErrorRecord)"/> only adds the warnings_only
        /// severity-19 raise on top. <see cref="SevereErrorRecord.DatabaseName"/> is intentionally left null
        /// (server-side <c>DB_NAME(database_id)</c> resolution is not available to a DB-free shred).
        /// </summary>
        public static SevereErrorRecord? ParseSevereError(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            if (ev == null)
                return null;

            var severity = ParseInt(DataValue(ev, "severity"));
            var errorNumber = ParseInt(DataValue(ev, "error_number"));

            // sp_HealthParser's always-on base filter (line ~4900): severity < 16 never surfaces, and
            // 17830/18056 never surface, whatever @warnings_only is set to.
            if (severity is null || severity < 16)
                return null;
            if (errorNumber is { } number && SystemHealthSignificance.IgnoredSevereErrorNumbers.Contains(number))
                return null;

            return new SevereErrorRecord
            {
                EventTime = ParseTimestamp(ev),
                ErrorNumber = errorNumber,
                Severity = severity,
                State = ParseInt(DataValue(ev, "state")),
                Message = DataValue(ev, "message"),
                DatabaseName = null, // sp_HealthParser: DB_NAME(database_id) — needs a live connection.
                // database_id rides on error_reported as an ACTION, not a data element (sp_HealthParser's own
                // comment: the data axis returns NULL for every event; session_id reads the action axis the
                // same way).
                DatabaseId = ParseInt(ActionValue(ev, "database_id")),
            };
        }

        /// <summary>
        /// Shreds the SYSTEM component of an <c>sp_server_diagnostics_component_result</c> event into a
        /// <see cref="SystemHealthRecord"/> — the corruption + contention counter set carried on the
        /// <c>&lt;system&gt;</c> node's attributes. Returns null for null/blank or unparseable XML, or when the
        /// event carries no <c>&lt;system&gt;</c> node (i.e. it is a non-SYSTEM component). Every counter is a
        /// raw bigint off the node; the two spinlock-type strings are kept verbatim (sp_HealthParser's
        /// <c>*_SystemHealth</c> column set).
        /// </summary>
        public static SystemHealthRecord? ParseSystemHealth(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            var system = ev?.Descendants("system").FirstOrDefault();
            if (ev == null || system == null)
                return null;

            return new SystemHealthRecord
            {
                EventTime = ParseTimestamp(ev),
                State = DataText(ev, "state"),
                SpinlockBackoffs = ParseLong((string?)system.Attribute("spinlockBackoffs")),
                SickSpinlockType = (string?)system.Attribute("sickSpinlockType"),
                SickSpinlockTypeAfterAv = (string?)system.Attribute("sickSpinlockTypeAfterAv"),
                LatchWarnings = ParseLong((string?)system.Attribute("latchWarnings")),
                IsAccessViolationOccurred = ParseLong((string?)system.Attribute("isAccessViolationOccurred")),
                WriteAccessViolationCount = ParseLong((string?)system.Attribute("writeAccessViolationCount")),
                TotalDumpRequests = ParseLong((string?)system.Attribute("totalDumpRequests")),
                IntervalDumpRequests = ParseLong((string?)system.Attribute("intervalDumpRequests")),
                NonYieldingTasksReported = ParseLong((string?)system.Attribute("nonYieldingTasksReported")),
                PageFaults = ParseLong((string?)system.Attribute("pageFaults")),
                SystemCpuUtilization = ParseLong((string?)system.Attribute("systemCpuUtilization")),
                SqlCpuUtilization = ParseLong((string?)system.Attribute("sqlCpuUtilization")),
                BadPagesDetected = ParseLong((string?)system.Attribute("BadPagesDetected")),
                BadPagesFixed = ParseLong((string?)system.Attribute("BadPagesFixed")),
            };
        }

        /// <summary>
        /// Shreds the RESOURCE component of an <c>sp_server_diagnostics_component_result</c> event into a
        /// <see cref="MemoryConditionsRecord"/>. Returns null for null/blank or unparseable XML, or when the
        /// event carries no <c>&lt;resource&gt;</c> node (i.e. it is a non-RESOURCE component — no memory
        /// conditions to report).
        /// </summary>
        public static MemoryConditionsRecord? ParseMemoryConditions(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            var resource = ev?.Descendants("resource").FirstOrDefault();
            if (ev == null || resource == null)
                return null;

            return new MemoryConditionsRecord
            {
                EventTime = ParseTimestamp(ev),
                LastNotification = (string?)resource.Attribute("lastNotification"),
                OutOfMemoryExceptions = ParseLong((string?)resource.Attribute("outOfMemoryExceptions")),
                IsAnyPoolOutOfMemory = ParseBool((string?)resource.Attribute("isAnyPoolOutOfMemory")),
                ProcessOutOfMemoryPeriod = ParseLong((string?)resource.Attribute("processOutOfMemoryPeriod")),
                Name = (string?)resource.Descendants("memoryReport").FirstOrDefault()?.Attribute("name"),
                AvailablePhysicalMemoryGb = GbFromBytes(EntryValue(resource, "Available Physical Memory")),
                AvailableVirtualMemoryGb = GbFromBytes(EntryValue(resource, "Available Virtual Memory")),
                AvailablePagingFileGb = GbFromBytes(EntryValue(resource, "Available Paging File")),
                WorkingSetGb = GbFromBytes(EntryValue(resource, "Working Set")),
                PercentOfCommittedMemoryInWs = ParseLong(EntryValue(resource, "Percent of Committed Memory in WS")),
                PageFaults = ParseLong(EntryValue(resource, "Page Faults")),
                SystemPhysicalMemoryHigh = ParseLong(EntryValue(resource, "System physical memory high")),
                SystemPhysicalMemoryLow = ParseLong(EntryValue(resource, "System physical memory low")),
                ProcessPhysicalMemoryLow = ParseLong(EntryValue(resource, "Process physical memory low")),
                ProcessVirtualMemoryLow = ParseLong(EntryValue(resource, "Process virtual memory low")),
                VmReservedGb = GbFromKb(EntryValue(resource, "VM Reserved")),
                VmCommittedGb = GbFromKb(EntryValue(resource, "VM Committed")),
                LockedPagesAllocated = ParseLong(EntryValue(resource, "Locked Pages Allocated")),
                LargePagesAllocated = ParseLong(EntryValue(resource, "Large Pages Allocated")),
                EmergencyMemoryGb = GbFromKb(EntryValue(resource, "Emergency Memory")),
                EmergencyMemoryInUseGb = GbFromKb(EntryValue(resource, "Emergency Memory In Use")),
                TargetCommittedGb = GbFromKb(EntryValue(resource, "Target Committed")),
                CurrentCommittedGb = GbFromKb(EntryValue(resource, "Current Committed")),
                PagesAllocated = ParseLong(EntryValue(resource, "Pages Allocated")),
                PagesReserved = ParseLong(EntryValue(resource, "Pages Reserved")),
                PagesFree = ParseLong(EntryValue(resource, "Pages Free")),
                PagesInUse = ParseLong(EntryValue(resource, "Pages In Use")),
                PageAllocPotential = ParseLong(EntryValue(resource, "Page Alloc Potential")),
                NumaGrowthPhase = ParseLong(EntryValue(resource, "NUMA Growth Phase")),
                LastOomFactor = ParseLong(EntryValue(resource, "Last OOM Factor")),
                LastOsError = ParseLong(EntryValue(resource, "Last OS Error")),
            };
        }

        /// <summary>
        /// Shreds the QUERY_PROCESSING component of an <c>sp_server_diagnostics_component_result</c> event into
        /// a <see cref="CpuTasksRecord"/>. Returns null for null/blank or unparseable XML, or when the event
        /// carries no <c>&lt;queryProcessing&gt;</c> node (i.e. it is a non-QUERY_PROCESSING component).
        /// </summary>
        public static CpuTasksRecord? ParseCpuTasks(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            var qp = ev?.Descendants("queryProcessing").FirstOrDefault();
            if (ev == null || qp == null)
                return null;

            return new CpuTasksRecord
            {
                EventTime = ParseTimestamp(ev),
                State = DataText(ev, "state"),
                MaxWorkers = ParseLong((string?)qp.Attribute("maxWorkers")),
                WorkersCreated = ParseLong((string?)qp.Attribute("workersCreated")),
                WorkersIdle = ParseLong((string?)qp.Attribute("workersIdle")),
                TasksCompletedWithinInterval = ParseLong((string?)qp.Attribute("tasksCompletedWithinInterval")),
                PendingTasks = ParseLong((string?)qp.Attribute("pendingTasks")),
                OldestPendingTaskWaitingTime = ParseLong((string?)qp.Attribute("oldestPendingTaskWaitingTime")),
                HasUnresolvableDeadlockOccurred = ParseBool((string?)qp.Attribute("hasUnresolvableDeadlockOccurred")),
                HasDeadlockedSchedulersOccurred = ParseBool((string?)qp.Attribute("hasDeadlockedSchedulersOccurred")),
                // sp_HealthParser: .exist('.../blockingTasks/blocked-process-report') — a definite bit, never null.
                DidBlockingOccur = qp.Descendants("blocked-process-report").Any(),
            };
        }

        /// <summary>
        /// Shreds the IO_SUBSYSTEM component of an <c>sp_server_diagnostics_component_result</c> event into
        /// zero or more <see cref="IoIssuesRecord"/> — one per distinct pending-request file, with that file's
        /// <c>@duration</c> values summed (sp_HealthParser's <c>#io</c> → <c>#i</c> per-event shred). Returns an
        /// empty list for null/blank or unparseable XML, a non-IO_SUBSYSTEM component (no
        /// <c>&lt;ioSubsystem&gt;</c> node), or an IO_SUBSYSTEM event with no pending request carrying a
        /// duration (sp_HealthParser's <c>WHERE longestPendingRequests_duration_ms IS NOT NULL</c>). Never throws.
        /// </summary>
        public static List<IoIssuesRecord> ParseIoIssues(string? eventXml)
        {
            var records = new List<IoIssuesRecord>();

            var ev = EventRoot(eventXml);
            var io = ev?.Descendants("ioSubsystem").FirstOrDefault();
            if (ev == null || io == null)
                return records;

            var eventTime = ParseTimestamp(ev);
            var state = DataText(ev, "state");
            var ioLatchTimeouts = ParseLong((string?)io.Attribute("ioLatchTimeouts"));
            var intervalLongIos = ParseLong((string?)io.Attribute("intervalLongIos"));
            var totalLongIos = ParseLong((string?)io.Attribute("totalLongIos"));

            // One row per pendingRequest with a duration, grouped by filePath (ISNULL -> "N/A") with the
            // durations summed — sp_HealthParser's WHERE duration IS NOT NULL + GROUP BY filePath, within one event.
            var groups = io.Descendants("pendingRequest")
                .Select(pr => (Duration: ParseLong((string?)pr.Attribute("duration")), FilePath: (string?)pr.Attribute("filePath")))
                .Where(pr => pr.Duration.HasValue)
                .GroupBy(pr => pr.FilePath ?? "N/A", StringComparer.Ordinal);

            foreach (var group in groups)
            {
                records.Add(new IoIssuesRecord
                {
                    EventTime = eventTime,
                    State = state,
                    IoLatchTimeouts = ioLatchTimeouts,
                    IntervalLongIos = intervalLongIos,
                    TotalLongIos = totalLongIos,
                    LongestPendingRequestsDurationMs = group.Sum(pr => pr.Duration!.Value),
                    LongestPendingRequestsFilePath = group.Key,
                });
            }

            return records;
        }

        /// <summary>
        /// Shreds a <c>memory_broker_ring_buffer_recorded</c> event into a <see cref="MemoryBrokerRecord"/>.
        /// Returns null for null/blank or unparseable XML.
        /// </summary>
        public static MemoryBrokerRecord? ParseMemoryBroker(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            if (ev == null)
                return null;

            return new MemoryBrokerRecord
            {
                EventTime = ParseTimestamp(ev),
                BrokerId = ParseLong(DataValue(ev, "id")),
                PoolMetadataId = ParseLong(DataValue(ev, "pool_metadata_id")),
                DeltaTime = ParseLong(DataValue(ev, "delta_time")),
                MemoryRatio = ParseLong(DataValue(ev, "memory_ratio")),
                NewTarget = ParseLong(DataValue(ev, "new_target")),
                Overall = ParseLong(DataValue(ev, "overall")),
                Rate = ParseLong(DataValue(ev, "rate")),
                CurrentlyPredicated = ParseLong(DataValue(ev, "currently_predicated")),
                CurrentlyAllocated = ParseLong(DataValue(ev, "currently_allocated")),
                PreviouslyAllocated = ParseLong(DataValue(ev, "previously_allocated")),
                Broker = DataValue(ev, "broker"),
                Notification = DataValue(ev, "notification"),
            };
        }

        /// <summary>
        /// Shreds a <c>memory_node_oom_ring_buffer_recorded</c> event into a <see cref="MemoryNodeOomRecord"/>.
        /// Returns null for null/blank or unparseable XML.
        /// </summary>
        public static MemoryNodeOomRecord? ParseMemoryNodeOom(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            if (ev == null)
                return null;

            return new MemoryNodeOomRecord
            {
                EventTime = ParseTimestamp(ev),
                NodeId = ParseLong(DataValue(ev, "id")),
                MemoryNodeId = ParseLong(DataValue(ev, "memory_node_id")),
                MemoryUtilizationPct = ParseLong(DataValue(ev, "memory_utilization_pct")),
                TotalPhysicalMemoryKb = ParseLong(DataValue(ev, "total_physical_memory_kb")),
                AvailablePhysicalMemoryKb = ParseLong(DataValue(ev, "available_physical_memory_kb")),
                TotalPageFileKb = ParseLong(DataValue(ev, "total_page_file_kb")),
                AvailablePageFileKb = ParseLong(DataValue(ev, "available_page_file_kb")),
                TotalVirtualAddressSpaceKb = ParseLong(DataValue(ev, "total_virtual_address_space_kb")),
                AvailableVirtualAddressSpaceKb = ParseLong(DataValue(ev, "available_virtual_address_space_kb")),
                TargetKb = ParseLong(DataValue(ev, "target_kb")),
                ReservedKb = ParseLong(DataValue(ev, "reserved_kb")),
                CommittedKb = ParseLong(DataValue(ev, "committed_kb")),
                SharedCommittedKb = ParseDecimal(DataValue(ev, "shared_committed_kb")),
                AweKb = ParseLong(DataValue(ev, "awe_kb")),
                PagesKb = ParseLong(DataValue(ev, "pages_kb")),
                FailureType = DataText(ev, "failure"),
                FailureValue = ParseLong(DataValue(ev, "failure")),
                Resources = ParseLong(DataValue(ev, "resources")),
                FactorText = DataText(ev, "factor"),
                FactorValue = ParseLong(DataValue(ev, "factor")),
                LastError = ParseLong(DataValue(ev, "last_error")),
                PoolMetadataId = ParseLong(DataValue(ev, "pool_metadata_id")),
                IsProcessInJob = DataValue(ev, "is_process_in_job"),
                IsSystemPhysicalMemoryHigh = DataValue(ev, "is_system_physical_memory_high"),
                IsSystemPhysicalMemoryLow = DataValue(ev, "is_system_physical_memory_low"),
                IsProcessPhysicalMemoryLow = DataValue(ev, "is_process_physical_memory_low"),
                IsProcessVirtualMemoryLow = DataValue(ev, "is_process_virtual_memory_low"),
            };
        }

        /// <summary>
        /// Shreds a <c>wait_info</c> event into a <see cref="SignificantWaitRecord"/>. Returns null for
        /// null/blank or unparseable XML.
        /// </summary>
        public static SignificantWaitRecord? ParseSignificantWait(string? eventXml)
        {
            var ev = EventRoot(eventXml);
            if (ev == null)
                return null;

            return new SignificantWaitRecord
            {
                EventTime = ParseTimestamp(ev),
                WaitType = DataText(ev, "wait_type"),
                DurationMs = ParseLong(DataValue(ev, "duration")),
                SignalDurationMs = ParseLong(DataValue(ev, "signal_duration")),
                WaitResource = DataValue(ev, "wait_resource"),
                QueryText = CleanSqlText(ActionValue(ev, "sql_text")),
                SessionId = ParseInt(ActionValue(ev, "session_id")),
            };
        }

        // ── XML navigation helpers ──

        /// <summary>
        /// Parses <paramref name="xml"/> and returns its <c>&lt;event&gt;</c> element (the root itself when it
        /// is the event, else the first event descendant). Null for null/blank or unparseable XML, mirroring
        /// <see cref="DeadlockGraphParser"/>'s never-throw contract.
        /// </summary>
        private static XElement? EventRoot(string? xml)
        {
            if (string.IsNullOrWhiteSpace(xml))
                return null;

            XElement root;
            try
            {
                // CheckCharacters=false: real wait_info sql_text carries control characters (the very reason
                // sp_HealthParser strips them). SQL Server serializes those as &#x01;-style references that a
                // default (XML 1.0) reader rejects — so tolerate them here and let CleanSqlText do the strip,
                // exactly as sp_HealthParser extracts-then-cleans. Ignorable whitespace stays significant so
                // element text round-trips faithfully.
                using var reader = XmlReader.Create(
                    new StringReader(xml.TrimStart()),
                    new XmlReaderSettings { CheckCharacters = false });
                root = XElement.Load(reader);
            }
            catch
            {
                return null;
            }

            return root.Name.LocalName == "event"
                ? root
                : root.DescendantsAndSelf("event").FirstOrDefault();
        }

        /// <summary>The <c>&lt;value&gt;</c> text of the <c>&lt;data name="name"&gt;</c> child of the event.</summary>
        private static string? DataValue(XElement ev, string name) =>
            DataNode(ev, name)?.Element("value")?.Value;

        /// <summary>The <c>&lt;text&gt;</c> text of the <c>&lt;data name="name"&gt;</c> child (friendly enum text).</summary>
        private static string? DataText(XElement ev, string name) =>
            DataNode(ev, name)?.Element("text")?.Value;

        private static XElement? DataNode(XElement ev, string name) =>
            ev.Elements("data").FirstOrDefault(d => string.Equals((string?)d.Attribute("name"), name, StringComparison.Ordinal));

        /// <summary>The <c>&lt;value&gt;</c> text of the <c>&lt;action name="name"&gt;</c> child of the event.</summary>
        private static string? ActionValue(XElement ev, string name) =>
            ev.Elements("action")
              .FirstOrDefault(a => string.Equals((string?)a.Attribute("name"), name, StringComparison.Ordinal))
              ?.Element("value")?.Value;

        /// <summary>
        /// The <c>@value</c> of the first <c>memoryReport/entry</c> whose <c>@description</c> matches, searched
        /// across all memoryReport groups under the resource — the C# form of sp_HealthParser's
        /// <c>(//memoryReport/entry[@description[.="X"]]/@value)[1]</c>.
        /// </summary>
        private static string? EntryValue(XElement resource, string description) =>
            (string?)resource.Descendants("entry")
                .FirstOrDefault(e => string.Equals((string?)e.Attribute("description"), description, StringComparison.Ordinal))
                ?.Attribute("value");

        // ── value conversions (mirror the sp_HealthParser .value() cast types) ──

        private static DateTime? ParseTimestamp(XElement ev)
        {
            var ts = (string?)ev.Attribute("timestamp");
            if (string.IsNullOrWhiteSpace(ts))
                return null;
            return DateTimeOffset.TryParse(ts, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto)
                ? dto.UtcDateTime
                : (DateTime?)null;
        }

        private static int? ParseInt(string? s) =>
            int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : (int?)null;

        private static long? ParseLong(string? s) =>
            long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : (long?)null;

        private static decimal? ParseDecimal(string? s) =>
            decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var v) ? v : (decimal?)null;

        /// <summary>SQL Server bit text: "1"/"true" -&gt; true, "0"/"false" -&gt; false, anything else -&gt; null.</summary>
        private static bool? ParseBool(string? s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return null;
            if (s == "1" || s.Equals("true", StringComparison.OrdinalIgnoreCase))
                return true;
            if (s == "0" || s.Equals("false", StringComparison.OrdinalIgnoreCase))
                return false;
            return null;
        }

        /// <summary>Bytes -&gt; GB by integer division (÷1024³), matching sp_HealthParser's bigint arithmetic (truncating).</summary>
        private static long? GbFromBytes(string? s) => ParseLong(s) is { } v ? v / 1024 / 1024 / 1024 : (long?)null;

        /// <summary>KB -&gt; GB by integer division (÷1024²), matching sp_HealthParser's bigint arithmetic (truncating).</summary>
        private static long? GbFromKb(string? s) => ParseLong(s) is { } v ? v / 1024 / 1024 : (long?)null;

        /// <summary>
        /// Replaces control characters — except tab (9), line feed (10) and carriage return (13) — with '?',
        /// the exact set sp_HealthParser strips when building its query_text column. Null in, null out.
        /// </summary>
        private static string? CleanSqlText(string? sql)
        {
            if (sql == null)
                return null;

            StringBuilder? sb = null;
            for (int i = 0; i < sql.Length; i++)
            {
                var c = sql[i];
                if (c < 32 && c != '\t' && c != '\n' && c != '\r')
                {
                    sb ??= new StringBuilder(sql, 0, i, sql.Length);
                    sb.Append('?');
                }
                else
                {
                    sb?.Append(c);
                }
            }
            return sb?.ToString() ?? sql;
        }
    }
}
