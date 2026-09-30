/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// Whose hardware the collected <c>server_properties</c> hardware columns describe.
///
/// <para>On an Azure SQL Database (<c>SERVERPROPERTY('EngineEdition')</c> 5) <c>sys.dm_os_sys_info</c> reports the
/// HOST the database runs on, not what the database is given: a 1-vCore serverless General Purpose database showed
/// 2 logical CPUs, 0 sockets, 32 cores per socket, a hyperthread ratio of 64 and 911.9 GB of physical memory. What
/// does describe the database is the service objective (<c>DATABASEPROPERTYEX('ServiceObjective')</c>, for example
/// <c>GP_S_Gen5_1</c>) and the <c>vcore_count</c> the collector parses out of it.</para>
///
/// <para>Every surface that SHOWS <c>cpu_count</c>, <c>socket_count</c>, <c>cores_per_socket</c>,
/// <c>hyperthread_ratio</c> or <c>physical_memory_mb</c> asks this class first, so the rule and the words live in
/// one place for both apps. The collector keeps storing the values as read.</para>
///
/// <para>Every calculation that would DIVIDE BY one of the host's CPU figures asks this class too: the attributed-CPU
/// denominator (<see cref="CpuAttribution"/>) and the FinOps utilization card's CPU count. On an Azure SQL Database each of
/// those uses the database's own figure where one is collected (<c>vcore_count</c>) and is otherwise NOT APPLICABLE. Neither
/// falls back to the host's count, because a number built from the host reads as the database's and is wrong. A DTU-model
/// objective or an elastic pool names no vCores, so its CPU count is not applicable.</para>
///
/// <para><b>The host's memory is only what <c>server_properties</c> holds.</b> <c>memory_stats</c> is a different table: on an
/// Azure SQL Database its <c>total_physical_memory_mb</c> is filled from <c>committed_target_kb</c>, the database's own memory
/// limit (1,838 MB on a 1-vCore General Purpose database whose <c>server_properties</c> row holds 911.9 GB), and its buffer
/// pool and server-memory counters are the database's too. So what reads <c>memory_stats</c> (the FinOps utilization card's
/// Physical Memory and Buffer Pool %, its verdict sentences and the health score's memory term) is shown and scored on every
/// edition alike. Only the words change: on an Azure SQL Database the figure is the database's memory limit, not physical RAM.</para>
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
        "cpu_count, hyperthread_ratio, socket_count, cores_per_socket and physical_memory_mb are null on an Azure SQL Database: " +
        "the host's hardware is not this database's allocation. Read service_objective and vcore_count for what the database is given.";

    /// <summary>The five columns that describe the host on an Azure SQL Database, by their <c>get_server_properties</c> key.</summary>
    public static readonly IReadOnlyList<string> HostHardwareKeys =
        ["cpu_count", "hyperthread_ratio", "socket_count", "cores_per_socket", "physical_memory_mb"];

    /// <summary>
    /// Rewrites a <c>get_server_properties</c> payload for an Azure SQL Database: the five host-hardware keys become
    /// null where they stand, <c>vcore_count</c> is inserted right after <c>service_objective</c> (null for a DTU-model
    /// objective or an elastic pool, which name no vCores), and <c>hardware_note</c> is appended. Both apps call it, so the shape and the
    /// words cannot drift apart. A payload of any other edition never reaches it.
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

    /// <summary>The Hardware Note a FinOps Server Inventory row carries for an Azure SQL Database whose collector read gave no reason of its own.</summary>
    public const string InventoryHardwareNote =
        "Azure SQL Database: the host's hardware is not this database's allocation; see its service objective.";

    /// <summary>What the FinOps utilization card shows for a CPU count that is not applicable (see <see cref="CpuCountText"/>).</summary>
    public const string NotApplicable = "n/a";

    /// <summary>
    /// The CPU count a calculation may treat as the server's OWN. Off an Azure SQL Database it is the stored
    /// <paramref name="cpuCount"/>, as it always was. On one the stored count is the HOST's, so the answer is the
    /// <paramref name="vcoreCount"/> the collector parsed from the service objective, and <c>null</c> (not applicable)
    /// when the objective names none (a DTU-model objective, or an elastic pool). It is never the host's count.
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
    /// What the FinOps utilization card measures the buffer pool against: physical RAM on SQL Server and Managed Instance, and
    /// on an Azure SQL Database the database's memory limit. Both are <c>memory_stats.total_physical_memory_mb</c>, which on an
    /// Azure SQL Database is <c>committed_target_kb</c>, so the figure is shown on every edition and only its name changes.
    /// </summary>
    private static string MemoryBasis(bool azureSqlDatabase) => azureSqlDatabase ? "the database's memory limit" : "physical RAM";

    /// <summary>
    /// The caption beside the utilization card's memory figure: "Physical: " on SQL Server and Managed Instance, "Memory limit: "
    /// on an Azure SQL Database, where the figure is the database's own limit and not the host's RAM.
    /// </summary>
    public static string PhysicalMemoryCaption(int? engineEdition) =>
        HardwareIsTheHosts(engineEdition) ? "Memory limit: " : "Physical: ";

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
    /// The worker-thread maximum a calculation may treat as the server's OWN. The stored figure is
    /// <c>sys.dm_os_sys_info.max_workers_count</c>, which the engine derives from the same DMV's CPU count, so on an Azure SQL
    /// Database it follows the host's CPUs and is not what the database is given. No database-scoped maximum is collected, so
    /// there the answer is 0, which the provisioning verdict already reads as "unknown" and which never implies saturation.
    /// Anywhere else it is the stored figure.
    /// </summary>
    public static int OwnMaxWorkersCount(int? engineEdition, int maxWorkersCount) =>
        HardwareIsTheHosts(engineEdition) ? 0 : maxWorkersCount;

    /// <summary>
    /// The FinOps utilization card's worker-thread text. Off an Azure SQL Database it is "in use / maximum", as it always was. On
    /// one the maximum is not the database's and the in-use count is not collected (<c>current_workers_count</c> is NULL there), so
    /// the card reads <see cref="NotApplicable"/> instead of a count against a host-derived maximum.
    /// </summary>
    public static string WorkerThreadsText(int? engineEdition, int currentWorkers, int maxWorkers) =>
        HardwareIsTheHosts(engineEdition)
            ? NotApplicable
            : string.Create(CultureInfo.CurrentCulture, $"{currentWorkers:N0} / {maxWorkers:N0}");

    /// <summary>
    /// The tooltip on the FinOps health score when it has no CPU term: the 24-hour window held no CPU sample, so CPU is left
    /// out, not scored as zero and not scored as a default. It is not specific to an Azure SQL Database, so both apps read the same
    /// words from here.
    /// </summary>
    public const string HealthScoreWithoutCpuNote =
        "CPU is not part of this score: the last 24 hours hold no CPU sample.";
}
