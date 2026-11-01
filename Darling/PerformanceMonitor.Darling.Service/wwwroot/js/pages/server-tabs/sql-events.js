/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { codeDisclosure } from "./shared.js";

export const HEALTH_CPU_SERIES = [
  { key: "sql_cpu_utilization", label: "SQL CPU %" },
  { key: "system_cpu_utilization", label: "System CPU %" },
];

export const HEALTH_ENTRY_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "sql_cpu_utilization", label: "SQL CPU %", format: "int" },
  { key: "system_cpu_utilization", label: "System CPU %", format: "int" },
  { key: "non_yielding_tasks_reported", label: "Non-yielding", format: "int" },
  { key: "latch_warnings", label: "Latch warnings", format: "int" },
  { key: "spinlock_backoffs", label: "Spinlock backoffs", format: "int" },
  { key: "sick_spinlock_type", label: "Sick spinlock" },
  { key: "bad_pages_detected", label: "Bad pages", format: "int" },
  { key: "bad_pages_fixed", label: "Bad pages fixed", format: "int" },
  { key: "is_access_violation_occurred", label: "AV", format: "int" },
  { key: "total_dump_requests", label: "Dumps", format: "int" },
  { key: "page_faults", label: "Page faults", format: "int" },
];

export const SEVERE_ERROR_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "error_number", label: "Error #", format: "int" },
  { key: "severity", label: "Severity", format: "int" },
  { key: "state", label: "State", format: "int" },
  { key: "database_name", label: "Database" },
  { key: "database_id", label: "DB ID", format: "int" },
  { key: "message", label: "Message", wrap: true },
];

export const SCHEDULER_ISSUE_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "sql_cpu_utilization", label: "SQL CPU %", format: "int" },
  { key: "other_process_cpu", label: "Other-Process CPU %", format: "int" },
  { key: "system_idle", label: "Idle %", format: "int" },
  { key: "memory_utilization", label: "Memory %", format: "int" },
  { key: "page_faults", label: "Page Faults", format: "int" },
  { key: "working_set_delta_mb", label: "Working Set Δ MB", format: "num1" },
];

export const IO_ISSUE_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "state", label: "State" },
  { key: "io_latch_timeouts", label: "Latch Timeouts", format: "int" },
  { key: "interval_long_ios", label: "Interval Long IOs", format: "int" },
  { key: "total_long_ios", label: "Total Long IOs", format: "int" },
  { key: "longest_pending_requests_duration_ms", label: "Pending Duration ms", format: "ms" },
  { key: "longest_pending_requests_file_path", label: "File Path", wrap: true },
];

export const CPU_TASK_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "state", label: "State" },
  { key: "max_workers", label: "Max Workers", format: "int" },
  { key: "workers_created", label: "Workers Created", format: "int" },
  { key: "workers_idle", label: "Workers Idle", format: "int" },
  { key: "tasks_completed_within_interval", label: "Tasks Completed", format: "int" },
  { key: "pending_tasks", label: "Pending Tasks", format: "int" },
  { key: "oldest_pending_task_waiting_time", label: "Oldest Pending ms", format: "int" },
  { key: "has_unresolvable_deadlock_occurred", label: "Unresolvable DL", format: "bool" },
  { key: "has_deadlocked_schedulers_occurred", label: "Sched Deadlock", format: "bool" },
  { key: "did_blocking_occur", label: "Blocking", format: "bool" },
];

/* The desktop grid's 32 columns in its order. */
export const MEMORY_CONDITION_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "last_notification", label: "Last Notification" },
  { key: "name", label: "Report" },
  { key: "out_of_memory_exceptions", label: "OOM Exceptions", format: "int" },
  { key: "is_any_pool_out_of_memory", label: "Pool OOM", format: "bool" },
  { key: "process_out_of_memory_period", label: "OOM Period", format: "int" },
  { key: "available_physical_memory_gb", label: "Avail Phys GB", format: "num1", group: "Process memory" },
  { key: "available_virtual_memory_gb", label: "Avail Virt GB", format: "num1", group: "Process memory" },
  { key: "available_paging_file_gb", label: "Avail Paging GB", format: "num1", group: "Process memory" },
  { key: "working_set_gb", label: "Working Set GB", format: "num1", group: "Process memory" },
  { key: "percent_of_committed_memory_in_ws", label: "% Committed in WS", format: "int", group: "Process memory" },
  { key: "page_faults", label: "Page Faults", format: "int", group: "Process memory" },
  { key: "system_physical_memory_high", label: "Sys Phys High", format: "int", group: "Pressure flags" },
  { key: "system_physical_memory_low", label: "Sys Phys Low", format: "int", group: "Pressure flags" },
  { key: "process_physical_memory_low", label: "Proc Phys Low", format: "int", group: "Pressure flags" },
  { key: "process_virtual_memory_low", label: "Proc Virt Low", format: "int", group: "Pressure flags" },
  { key: "vm_reserved_gb", label: "VM Reserved GB", format: "num1", group: "Allocation detail" },
  { key: "vm_committed_gb", label: "VM Committed GB", format: "num1", group: "Allocation detail" },
  { key: "locked_pages_allocated", label: "Locked Pages", format: "int", group: "Allocation detail" },
  { key: "large_pages_allocated", label: "Large Pages", format: "int", group: "Allocation detail" },
  { key: "emergency_memory_gb", label: "Emergency GB", format: "num1", group: "Allocation detail" },
  { key: "emergency_memory_in_use_gb", label: "Emerg In Use GB", format: "num1", group: "Allocation detail" },
  { key: "target_committed_gb", label: "Target Committed GB", format: "num1", group: "Allocation detail" },
  { key: "current_committed_gb", label: "Current Committed GB", format: "num1", group: "Allocation detail" },
  { key: "pages_allocated", label: "Pages Alloc", format: "int", group: "Allocation detail" },
  { key: "pages_reserved", label: "Pages Reserved", format: "int", group: "Allocation detail" },
  { key: "pages_free", label: "Pages Free", format: "int", group: "Allocation detail" },
  { key: "pages_in_use", label: "Pages In Use", format: "int", group: "Allocation detail" },
  { key: "page_alloc_potential", label: "Alloc Potential", format: "int", group: "Allocation detail" },
  { key: "numa_growth_phase", label: "NUMA Growth", format: "int", group: "Allocation detail" },
  { key: "last_oom_factor", label: "Last OOM Factor", group: "Allocation detail" },
  { key: "last_os_error", label: "Last OS Error", format: "int", group: "Allocation detail" },
];

/* Memory Conditions column groups: the report, the time and the OOM counters are always shown; the rest follow the
   desktop grid's order in three toggles, with the process memory figures on at first. */
export const MEMORY_CONDITION_GROUPS = { groups: ["Process memory", "Pressure flags", "Allocation detail"], defaultGroups: ["Process memory"] };

export const MEMORY_BROKER_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "broker_id", label: "Broker ID", format: "int" },
  { key: "broker", label: "Broker" },
  { key: "notification", label: "Notification" },
  { key: "pool_metadata_id", label: "Pool Meta ID", format: "int" },
  { key: "delta_time", label: "Delta Time", format: "int" },
  { key: "memory_ratio", label: "Mem Ratio", format: "num2" },
  { key: "new_target", label: "New Target", format: "int" },
  { key: "overall", label: "Overall", format: "int" },
  { key: "rate", label: "Rate", format: "num2" },
  { key: "currently_predicated", label: "Predicated", format: "int" },
  { key: "currently_allocated", label: "Allocated", format: "int" },
  { key: "previously_allocated", label: "Prev Allocated", format: "int" },
];

/* #2484: the Significant Waits grid. Signal duration sits beside the total on purpose -- both are
   milliseconds, so they share a column format honestly, and a signal close to the total is CPU pressure
   wearing a wait type's name. The statement goes through codeDisclosure like every other SQL column. */
export const SIGNIFICANT_WAIT_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "wait_type", label: "Wait Type" },
  { key: "duration_ms", label: "Duration", format: "ms" },
  { key: "signal_duration_ms", label: "Signal duration", format: "ms" },
  { key: "wait_resource", label: "Wait Resource", wrap: true },
  { key: "session_id", label: "Session", format: "int" },
  { key: "query_text", label: "Query", valueList: false, render: (r) => codeDisclosure(r.query_text) },
];

/* The desktop grid's 28 columns in its order. */
export const MEMORY_OOM_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "node_id", label: "Node ID", format: "int" },
  { key: "memory_node_id", label: "Mem Node ID", format: "int" },
  { key: "memory_utilization_pct", label: "Mem Util %", format: "pct" },
  { key: "total_physical_memory_kb", label: "Total Phys KB", format: "int", group: "Memory sizes" },
  { key: "available_physical_memory_kb", label: "Avail Phys KB", format: "int", group: "Memory sizes" },
  { key: "total_page_file_kb", label: "Total Page KB", format: "int", group: "Memory sizes" },
  { key: "available_page_file_kb", label: "Avail Page KB", format: "int", group: "Memory sizes" },
  { key: "total_virtual_address_space_kb", label: "Total VAS KB", format: "int", group: "Memory sizes" },
  { key: "available_virtual_address_space_kb", label: "Avail VAS KB", format: "int", group: "Memory sizes" },
  { key: "target_kb", label: "Target KB", format: "int", group: "Pool detail" },
  { key: "reserved_kb", label: "Reserved KB", format: "int", group: "Pool detail" },
  { key: "committed_kb", label: "Committed KB", format: "int", group: "Pool detail" },
  { key: "shared_committed_kb", label: "Shared Committed KB", format: "int", group: "Pool detail" },
  { key: "awe_kb", label: "AWE KB", format: "int", group: "Pool detail" },
  { key: "pages_kb", label: "Pages KB", format: "int", group: "Pool detail" },
  { key: "failure_type", label: "Failure", group: "Failure" },
  { key: "failure_value", label: "Failure Val", format: "int", group: "Failure" },
  { key: "resources", label: "Resources", wrap: true, group: "Failure" },
  { key: "factor_text", label: "Factor", group: "Failure" },
  { key: "factor_value", label: "Factor Val", format: "int", group: "Failure" },
  { key: "last_error", label: "Last Error", group: "Failure" },
  { key: "pool_metadata_id", label: "Pool Meta ID", format: "int", group: "Flags" },
  { key: "is_process_in_job", label: "In Job", group: "Flags" },
  { key: "is_system_physical_memory_high", label: "Sys Phys High", group: "Flags" },
  { key: "is_system_physical_memory_low", label: "Sys Phys Low", group: "Flags" },
  { key: "is_process_physical_memory_low", label: "Proc Phys Low", group: "Flags" },
  { key: "is_process_virtual_memory_low", label: "Proc Virt Low", group: "Flags" },
];

/* Node OOM column groups: the event time, node ids and utilisation are always shown; the failure columns are on at
   first and the size, pool and flag columns follow the desktop grid's order behind their toggles. */
export const MEMORY_OOM_GROUPS = { groups: ["Memory sizes", "Pool detail", "Failure", "Flags"], defaultGroups: ["Failure"] };

export const DEFAULT_TRACE_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "category", label: "Category" },
  { key: "event_name", label: "Event" },
  { key: "database_name", label: "Database" },
  { key: "object_name", label: "Object" },
  { key: "login_name", label: "Login" },
  { key: "host_name", label: "Host" },
  { key: "application_name", label: "Application" },
  { key: "spid", label: "SPID", format: "int" },
  { key: "duration_ms", label: "Duration ms", format: "ms" },
  { key: "growth_mb", label: "Growth MB", format: "mb" },
  { key: "severity", label: "Severity", format: "int" },
  { key: "error_number", label: "Error #", format: "int" },
  { key: "text_data", label: "Details", wrap: true },
];

