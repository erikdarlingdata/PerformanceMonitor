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
/// one place for both apps. The collector and every sizing or verdict calculation keep reading the stored
/// values; this decides only what is presented as the database's.</para>
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
    /// objective that names no vCores), and <c>hardware_note</c> is appended. Both apps call it, so the shape and the
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

    /// <summary>What the FinOps utilization card shows where it would have shown the host's physical memory.</summary>
    public const string NotApplicable = "n/a";

    /// <summary>
    /// The FinOps utilization verdict sentence for a server whose provisioning is RIGHT_SIZED. Off an Azure SQL
    /// Database it is the long-standing sentence, byte for byte; on one it drops the clause that compares the buffer
    /// pool with the HOST's physical memory.
    /// </summary>
    public static string RightSizedExplanation(decimal avgCpuPct, decimal p95CpuPct, double bufferPoolPctOfPhysical, bool azureSqlDatabase) =>
        azureSqlDatabase
            ? string.Create(CultureInfo.CurrentCulture, $"CPU is moderately loaded (avg {avgCpuPct:N1}%, p95 {p95CpuPct:N1}%). No action needed.")
            : string.Create(CultureInfo.CurrentCulture, $"CPU is moderately loaded (avg {avgCpuPct:N1}%, p95 {p95CpuPct:N1}%) and memory is well-utilized (buffer pool uses {bufferPoolPctOfPhysical:N0}% of physical RAM). No action needed.");

    /// <summary>
    /// The FinOps utilization verdict sentence for a server whose provisioning is OVER_PROVISIONED. Same rule as
    /// <see cref="RightSizedExplanation"/>: an Azure SQL Database's sentence cites no buffer pool share of the host's RAM.
    /// </summary>
    public static string OverProvisionedExplanation(decimal avgCpuPct, int maxCpuPct, double bufferPoolPctOfPhysical, bool azureSqlDatabase) =>
        azureSqlDatabase
            ? string.Create(CultureInfo.CurrentCulture, $"CPU is lightly loaded (avg {avgCpuPct:N1}%, max {maxCpuPct}%). This database may have more resources than it needs.")
            : string.Create(CultureInfo.CurrentCulture, $"CPU is lightly loaded (avg {avgCpuPct:N1}%, max {maxCpuPct}%) and buffer pool uses only {bufferPoolPctOfPhysical:N0}% of physical RAM. This server may have more resources than it needs.");
}
