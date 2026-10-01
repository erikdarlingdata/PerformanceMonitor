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
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// Whose hardware the collected <c>server_properties</c> hardware columns describe.
///
/// <para>On an Azure SQL Database (<c>SERVERPROPERTY('EngineEdition')</c> 5) <c>sys.dm_os_sys_info</c> mixes two kinds of
/// figure. <c>cpu_count</c> and <c>max_workers_count</c> are the database's own: <c>cpu_count</c> is the number of
/// schedulers the database can see, which can be higher than its vCores (a 1-vCore General Purpose database reads 2, a
/// 2-vCore Hyperscale database reads 2), and the worker ceiling is the database's own too. Four figures are the HOST's:
/// <c>physical_memory_mb</c> (about 912 GB), <c>socket_count</c>, <c>cores_per_socket</c> and <c>hyperthread_ratio</c>.
/// What the database is GIVEN is its service objective (<c>DATABASEPROPERTYEX('ServiceObjective')</c>, for example
/// <c>GP_S_Gen5_1</c>) and the <c>vcore_count</c> the collector parses out of it.</para>
///
/// <para>Every surface that SHOWS <c>socket_count</c>, <c>cores_per_socket</c>, <c>hyperthread_ratio</c> or
/// <c>physical_memory_mb</c> asks this class first, so the rule and the words live in one place for both apps. The
/// collector keeps storing the values as read. <c>cpu_count</c> is shown as stored, as the database's own scheduler count.</para>
///
/// <para>Every calculation that would DIVIDE BY a CPU count asks this class too: the attributed-CPU denominator
/// (<see cref="CpuAttribution"/>) and the FinOps utilization card's CPU count. On an Azure SQL Database CPU percent is
/// measured against the vCores the service objective gives the database, not against the schedulers it can see, so those use
/// the database's <c>vcore_count</c> and are NOT APPLICABLE where the objective names none (a DTU-model objective or an
/// elastic pool). Neither falls back to <c>cpu_count</c>, because a percentage spread over a count above the vCores reads low.</para>
///
/// <para><b>The host's memory is only what <c>server_properties</c> holds.</b> <c>memory_stats</c> is a different table: on an
/// Azure SQL Database its <c>total_physical_memory_mb</c> is filled from <c>committed_target_kb</c>, the database's own memory
/// limit (1,838 MB on a 1-vCore General Purpose database whose <c>server_properties</c> row holds about 912 GB), and its buffer
/// pool and server-memory counters are the database's too. So what reads <c>memory_stats</c> (the FinOps utilization card's
/// Physical Memory and Buffer Pool %, its verdict sentences and the health score's memory term) is shown and scored on every
/// edition alike. Only the words change: on an Azure SQL Database the figure is the database's memory limit, not physical RAM.
/// <c>get_memory_stats</c> keeps its key names, so on an Azure SQL Database it adds a <c>memory_note</c> (see <see cref="McpMemoryNote"/>).</para>
///
/// <para><b>Two <c>memory_stats</c> facts have no source at all on an Azure SQL Database.</b> Its memory collector stores 0 for
/// both page-file columns and the constant "Available" for the memory state. Neither is a reading, so every surface that shows
/// them reads n/a there, and <c>get_memory_stats</c> publishes the state as null with <see cref="MemoryStateNote"/> beside it
/// (<see cref="MemoryStateOrNull"/>). No analysis fact, alert rule, FinOps figure or health score reads either column.</para>
/// </summary>
public static class ServerHardwareScope
{
    /// <summary><c>SERVERPROPERTY('EngineEdition')</c> for an Azure SQL Database.</summary>
    public const int AzureSqlDatabaseEngineEdition = 5;

    /// <summary>True when the stored hardware columns are the host's and must not be presented as the server's own.</summary>
    public static bool HardwareIsTheHosts(int? engineEdition) => engineEdition == AzureSqlDatabaseEngineEdition;

    /// <summary>
    /// The <c>hardware_note</c> <c>get_server_properties</c> returns on an Azure SQL Database, word for word in both
    /// apps (the web Server Properties list reads the same payload).
    /// </summary>
    public const string McpHardwareNote =
        "hyperthread_ratio, socket_count, cores_per_socket and physical_memory_mb are null on an Azure SQL Database: " +
        "they describe the host machine, not this database. cpu_count is the database's own scheduler count and can be higher " +
        "than its vCores. Read service_objective and vcore_count for what the database is given.";

    /// <summary>The four columns that describe the host on an Azure SQL Database, by their <c>get_server_properties</c> key.</summary>
    public static readonly IReadOnlyList<string> HostHardwareKeys =
        ["hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    /// <summary>
    /// Rewrites a <c>get_server_properties</c> payload for an Azure SQL Database: the four host-hardware keys become
    /// null where they stand, <c>cpu_count</c> passes through (it is the database's own scheduler count), <c>vcore_count</c>
    /// is inserted right after <c>service_objective</c> (null for a DTU-model objective or an elastic pool, which name no
    /// vCores), and <c>hardware_note</c> is appended. Both apps call it, so the shape and the words cannot drift apart. A
    /// payload of any other edition never reaches it.
    /// </summary>
    public static JsonObject ScopeServerPropertiesPayload(JsonObject payload, int? vcoreCount)
    {
        foreach (var key in HostHardwareKeys)
            payload[key] = null;

        var at = payload.IndexOf("service_objective");
        payload.Insert(at < 0 ? payload.Count : at + 1, "vcore_count", vcoreCount.HasValue ? JsonValue.Create(vcoreCount.Value) : null);
        payload["hardware_note"] = McpHardwareNote;
        return payload;
    }

    /// <summary>
    /// The <c>memory_note</c> <c>get_memory_stats</c> returns on an Azure SQL Database, word for word in both apps. The tool keeps
    /// the key names <c>total_physical_memory_mb</c> and <c>available_physical_memory_mb</c> on every edition, so on an Azure SQL
    /// Database this note is what says they are the database's memory limit and the room left under it, and that a utilization near
    /// 100% is the normal state of a database that has grown to its limit. Without it a client reads that figure as OS memory pressure.
    /// </summary>
    public const string McpMemoryNote =
        "On an Azure SQL Database total_physical_memory_mb is the database's memory limit (its committed target), not the host's " +
        "memory. available_physical_memory_mb is what is left under that limit. memory_utilization_pct is the share of that limit " +
        "in use: a value near 100% is normal once the database has grown to its limit, and is not memory pressure by itself.";

    /// <summary>
    /// Appends <c>memory_note</c> to a <c>get_memory_stats</c> payload for an Azure SQL Database. Every key the payload already
    /// has stays as it is, and the note comes last, as <c>hardware_note</c> does on <c>get_server_properties</c>. Both apps call it,
    /// so the key and the words cannot drift apart. A payload of any other edition never reaches it and carries no note.
    /// </summary>
    public static JsonObject WithMemoryNote(JsonObject payload)
    {
        payload["memory_note"] = McpMemoryNote;
        return payload;
    }

    /// <summary>The Hardware Note a FinOps Server Inventory row carries for an Azure SQL Database whose collector read gave no reason of its own.</summary>
    public const string InventoryHardwareNote =
        "Azure SQL Database: memory, sockets, cores per socket and hyperthread ratio are the host's, not this database's. " +
        "Logical CPUs is the database's own scheduler count; see its service objective for its vCores.";

    /// <summary>What a figure that is not applicable shows: the FinOps utilization card's CPU count (see <see cref="CpuCountText"/>),
    /// and the Memory Overview's page-file figures and memory state on an Azure SQL Database, whose memory collector stores 0
    /// and the constant "Available" for them.</summary>
    public const string NotApplicable = "n/a";

    /// <summary>
    /// The stored <c>edition</c> of a logical server's <c>master</c> database. <c>ServerPropertiesCollector</c> writes
    /// <c>N'Azure SQL Database' + N' (' + &lt;DATABASEPROPERTYEX(DB_NAME(), 'Edition')&gt; + N')'</c>, and master's database
    /// edition is <c>System</c> (its service objective, e.g. <c>GP_SYSTEM_4</c>, varies and is not matched).
    /// </summary>
    public const string AzureSqlDatabaseSystemEdition = "Azure SQL Database (System)";

    /// <summary>
    /// True for a logical server's <c>master</c> database (engine edition 5 and the stored edition
    /// <see cref="AzureSqlDatabaseSystemEdition"/>): it has nothing to resize, so no right-sizing advice or provisioning
    /// verdict applies. Ordinal compare: the collector writes that literal, so a different case is a different string.
    /// </summary>
    public static bool IsLogicalServerMaster(int? engineEdition, string? edition) =>
        engineEdition == AzureSqlDatabaseEngineEdition
        && string.Equals(edition, AzureSqlDatabaseSystemEdition, StringComparison.Ordinal);

    /// <summary>
    /// The <c>system_memory_state_note</c> <c>get_memory_stats</c> returns on an Azure SQL Database, word for word in both apps.
    /// The web Memory tiles show it where the state is null, so it reads as a value and still says why there is none.
    /// </summary>
    public const string MemoryStateNote = NotApplicable + " (no memory-state source on Azure SQL Database)";

    /// <summary>
    /// The memory state a reader may publish. On an Azure SQL Database the memory collector has no memory-state source and
    /// stores the constant "Available", which is not a reading, so the answer there is <c>null</c> and the reader publishes
    /// <see cref="MemoryStateNoteFor"/> beside it. Anywhere else it is the stored state, as it always was. The page-file
    /// columns follow the same rule on every surface that shows them; no MCP read publishes them.
    /// </summary>
    public static string? MemoryStateOrNull(int? engineEdition, string? storedState) =>
        engineEdition == AzureSqlDatabaseEngineEdition ? null : storedState;

    /// <summary>The note published beside <see cref="MemoryStateOrNull"/>: <see cref="MemoryStateNote"/> on an Azure SQL
    /// Database, where the state is null, and <c>null</c> anywhere else.</summary>
    public static string? MemoryStateNoteFor(int? engineEdition) =>
        engineEdition == AzureSqlDatabaseEngineEdition ? MemoryStateNote : null;

    /// <summary>
    /// The CPU count a calculation that divides CPU percent by it may use. Off an Azure SQL Database it is the stored
    /// <paramref name="cpuCount"/>, as it always was. On one the stored count is the number of schedulers the database can see,
    /// which can be higher than its vCores (a 1-vCore General Purpose database reads 2), and CPU percent there is measured
    /// against the vCores. So the answer is the <paramref name="vcoreCount"/> the collector parsed from the service objective,
    /// and <c>null</c> (not applicable) when the objective names none (a DTU-model objective, or an elastic pool). It is never
    /// the scheduler count.
    /// </summary>
    public static int? OwnCpuCount(int? engineEdition, int? cpuCount, int? vcoreCount) =>
        HardwareIsTheHosts(engineEdition)
            ? (vcoreCount > 0 ? vcoreCount : null)
            : cpuCount;

    /// <summary>
    /// The FinOps utilization card's CPU Count text. The FinOps read already resolves the count through
    /// <see cref="OwnCpuCount"/> in SQL and hands over 0 where there is none, so a 0 on an Azure SQL Database reads
    /// <see cref="NotApplicable"/> here. Anywhere else the text is the count with thousands separators, as it always was.
    /// </summary>
    public static string CpuCountText(int? engineEdition, int cpuCount) =>
        HardwareIsTheHosts(engineEdition) && cpuCount <= 0
            ? NotApplicable
            : cpuCount.ToString("N0", CultureInfo.CurrentCulture);

    /// <summary>
    /// The unit that follows the FinOps utilization card's CPU count: " vCores," on an Azure SQL Database, where the count is the
    /// vCores its service objective gives it (see <see cref="OwnCpuCount"/>) and so is not a count of CPUs, and " CPUs,"
    /// everywhere else. The leading space and trailing comma are part of the text, as the card draws it between two other figures.
    /// </summary>
    public static string CpuCountUnit(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? " vCores," : " CPUs,";

    /// <summary>
    /// What a sentence calls the CPU count it prints beside a number: "vCores" on an Azure SQL Database, where that count is the
    /// vCores the service objective gives the database (the same rule as <see cref="CpuCountUnit"/>), and "cores" everywhere else,
    /// which is the word the FinOps CPU right-sizing recommendation has always used. The recommendation takes the count from the
    /// FinOps utilization read, so it names it the way the utilization card does.
    /// </summary>
    public static string CpuCoreNoun(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? "vCores" : "cores";

    /// <summary>
    /// What the FinOps utilization card measures the buffer pool against: physical RAM on SQL Server and Managed Instance, and
    /// on an Azure SQL Database the database's memory limit. Both are <c>memory_stats.total_physical_memory_mb</c>, which on an
    /// Azure SQL Database is <c>committed_target_kb</c>, so the figure is shown on every edition and only its name changes.
    /// </summary>
    private static string MemoryBasis(bool azureSqlDatabase) => azureSqlDatabase ? "the database's memory limit" : "physical RAM";

    private const string MemoryLimitLabel = "Memory limit";

    /// <summary>
    /// The caption beside the utilization card's memory figure: "Physical: " on SQL Server and Managed Instance, "Memory limit: "
    /// on an Azure SQL Database, where the figure is the database's own limit and not the host's RAM.
    /// </summary>
    public static string PhysicalMemoryCaption(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? MemoryLimitLabel + ": " : "Physical: ";

    /// <summary>
    /// The label over the Memory tab's first figure (<c>memory_stats.total_physical_memory_mb</c>): "Physical Memory" on SQL Server
    /// and Managed Instance, "Memory limit" on an Azure SQL Database, where the collector fills the column from the database's
    /// own committed target and not from the host's RAM. The same word <see cref="PhysicalMemoryCaption"/> uses.
    /// </summary>
    public static string MemoryTabTotalLabel(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? MemoryLimitLabel : "Physical Memory";

    /// <summary>
    /// The label over the Memory tab's second figure (<c>memory_stats.available_physical_memory_mb</c>): "Available Physical" on SQL
    /// Server and Managed Instance, where it is the operating system's available physical memory, and "Available under limit" on
    /// an Azure SQL Database, where the collector computes it as the committed target minus what is committed, which is the room
    /// left under the database's memory limit and not a count of free RAM.
    /// </summary>
    public static string MemoryTabAvailableLabel(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? "Available under limit" : "Available Physical";

    /// <summary>
    /// The FinOps utilization verdict sentence for a server whose provisioning is RIGHT_SIZED. Off an Azure SQL Database it is
    /// the long-standing sentence, byte for byte. On one it says the same, with the share named against the database's memory
    /// limit instead of physical RAM.
    /// </summary>
    public static string RightSizedExplanation(decimal avgCpuPct, decimal p95CpuPct, double bufferPoolPct, bool azureSqlDatabase) =>
        string.Create(CultureInfo.CurrentCulture, $"CPU is moderately loaded (avg {avgCpuPct:N1}%, p95 {p95CpuPct:N1}%) and memory is well-utilized (buffer pool uses {bufferPoolPct:N0}% of {MemoryBasis(azureSqlDatabase)}). No action needed.");

    /// <summary>
    /// The FinOps utilization verdict sentence for a server whose provisioning is OVER_PROVISIONED. Same rule as
    /// <see cref="RightSizedExplanation"/>: the long-standing sentence off an Azure SQL Database, and on one the same share named
    /// against the database's memory limit, about "this database".
    /// </summary>
    public static string OverProvisionedExplanation(decimal avgCpuPct, int maxCpuPct, double bufferPoolPct, bool azureSqlDatabase) =>
        string.Create(CultureInfo.CurrentCulture, $"CPU is lightly loaded (avg {avgCpuPct:N1}%, max {maxCpuPct}%) and buffer pool uses only {bufferPoolPct:N0}% of {MemoryBasis(azureSqlDatabase)}. This {(azureSqlDatabase ? "database" : "server")} may have more resources than it needs.");

    /// <summary>
    /// The physical memory (<c>server_properties.physical_memory_mb</c>) a calculation may treat as the server's OWN. Off an
    /// Azure SQL Database it is the stored figure, as it always was. On one the stored figure is the HOST's and no
    /// database-scoped figure is collected to stand in for it, so the answer is <c>null</c> (not applicable). This is about the
    /// <c>server_properties</c> column only: the <c>memory_stats</c> memory figures come from the engine's own memory counters and
    /// its committed target, which describe the database, and they are not scoped here.
    /// </summary>
    public static long? OwnPhysicalMemoryMb(int? engineEdition, long? physicalMemoryMb) =>
        HardwareIsTheHosts(engineEdition) ? null : physicalMemoryMb;

    /// <summary>
    /// The FinOps utilization card's worker-thread text: "in use / maximum". The in-use count is NULL where the collector cannot
    /// read it (an Azure SQL Database: its query hard-codes NULL), and then it reads <see cref="NotApplicable"/>, never 0. The
    /// maximum is the engine's own figure on every edition and is shown as stored.
    /// </summary>
    public static string WorkerThreadsText(int? currentWorkers, int maxWorkers) =>
        string.Create(CultureInfo.CurrentCulture, $"{(currentWorkers is int inUse ? inUse.ToString("N0", CultureInfo.CurrentCulture) : NotApplicable)} / {maxWorkers:N0}");

    /// <summary>
    /// The tooltip on the FinOps health score when it has no CPU term: the 24-hour window held no CPU sample, so CPU is left
    /// out, not scored as zero and not scored as a default. It is not specific to an Azure SQL Database, so both apps read the same
    /// words from here.
    /// </summary>
    public const string HealthScoreWithoutCpuNote =
        "CPU is not part of this score: the last 24 hours hold no CPU sample.";
}
