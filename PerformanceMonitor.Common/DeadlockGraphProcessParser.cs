/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared sp_BlitzLock-style per-process deadlock-graph walk — previously duplicated byte-for-byte as
/// <c>DeadlockProcessDetail.ParseFromRows</c> in both Lite (<c>LocalDataService.Blocking.cs</c>) and the
/// Darling viewer (<c>ViewerDataService.Deadlock.cs</c>). Parallel-vs-regular classification, victim-id
/// detection, per-process owner/waiter modes, contended object names, and proc-name resolution off the
/// execution stack all live here once. Each host passes its grid rows in as <see cref="DeadlockGraphInput"/>
/// and gets back its own <typeparamref name="T"/> (a <see cref="DeadlockProcessInfo"/> subclass carrying the
/// host-specific display getters). The walk is CPU-bound XML work, so callers run it off the UI thread.
/// </summary>
public static class DeadlockGraphProcessParser
{
    /// <summary>
    /// The host-neutral inputs the walk needs from one deadlock grid row: the graph XML, the event time
    /// stamped onto every parsed process, the victim SQL used for the malformed-XML fallback row, and the
    /// best-effort victim plan (#1368 / V7) threaded onto every process (NULL under Lite, often NULL otherwise).
    /// </summary>
    public readonly record struct DeadlockGraphInput(
        string? DeadlockGraphXml,
        DateTime? DeadlockTime,
        string? VictimSqlText,
        string? VictimQueryPlanXml);

    /// <summary>
    /// Parses a list of deadlock rows into per-process detail rows of the caller's <typeparamref name="T"/>.
    /// A row whose graph is absent contributes nothing; a row whose graph does not parse contributes the
    /// single victim-only fallback row described on <see cref="TryParseGraph{T}"/>.
    /// </summary>
    public static List<T> Parse<T>(IEnumerable<DeadlockGraphInput> rows)
        where T : DeadlockProcessInfo, new()
    {
        var details = new List<T>();
        foreach (var row in rows)
        {
            if (string.IsNullOrEmpty(row.DeadlockGraphXml))
                continue;

            if (TryParseGraph<T>(row, out var processes))
            {
                details.AddRange(processes);
                continue;
            }

            /* If XML parsing fails, add a single fallback row */
            details.Add(new T
            {
                DeadlockTime = row.DeadlockTime,
                SqlText = row.VictimSqlText ?? "",
                IsVictim = true,
                DeadlockGraphXml = row.DeadlockGraphXml,
                VictimQueryPlanXml = row.VictimQueryPlanXml
            });
        }
        return details;
    }

    /// <summary>
    /// Parses ONE graph into its per-process rows, or returns false when the graph is absent or does not
    /// parse — the seam a caller needs when "this graph told us nothing" and "this graph has one process"
    /// must stay distinguishable.
    ///
    /// <para><see cref="Parse{T}"/> answers a grid, where a graph it cannot read still has to occupy a row,
    /// so it substitutes a victim-only fallback carrying the stored victim SQL and nothing else — including
    /// <see cref="DeadlockProcessInfo.Spid"/> left at its default 0. A caller rendering text cannot use that:
    /// a fabricated SPID 0 reads as a session id. Returning false hands the decision to the caller instead of
    /// making it for them, and <see cref="Parse{T}"/> supplies the fallback itself so its own contract is
    /// unchanged.</para>
    ///
    /// <para><paramref name="processes"/> is empty rather than null on false, so a caller that ignores the
    /// return value iterates nothing instead of throwing.</para>
    /// </summary>
    public static bool TryParseGraph<T>(DeadlockGraphInput row, out List<T> processes)
        where T : DeadlockProcessInfo, new()
    {
        processes = new List<T>();

        if (string.IsNullOrEmpty(row.DeadlockGraphXml))
            return false;

        try
        {
            var doc = System.Xml.Linq.XElement.Parse(row.DeadlockGraphXml);

            /* Detect parallel deadlock */
            var resourceList = doc.Descendants("resource-list").FirstOrDefault();
            var isParallel = resourceList != null &&
                (resourceList.Elements("exchangeEvent").Any() || resourceList.Elements("SyncPoint").Any());
            var deadlockType = isParallel ? "Parallel" : "Regular";

            /* Get victim IDs from victim-list */
            var victimIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var vp in doc.Descendants("victimProcess"))
            {
                var id = vp.Attribute("id")?.Value;
                if (id != null) victimIds.Add(id);
            }

            /* Parse lock resources to build per-process owner/waiter modes and object names */
            var processOwnerModes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var processWaiterModes = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var processObjectNames = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            if (resourceList != null)
            {
                var lockTypes = new[] { "objectlock", "pagelock", "keylock", "ridlock", "rowgrouplock" };
                foreach (var lockType in lockTypes)
                {
                    foreach (var lockNode in resourceList.Elements(lockType))
                    {
                        var objectName = lockNode.Attribute("objectname")?.Value ?? "";

                        /* Parse owners */
                        foreach (var owner in lockNode.Descendants("owner"))
                        {
                            var ownerId = owner.Attribute("id")?.Value ?? "";
                            var ownerMode = owner.Attribute("mode")?.Value ?? "";
                            if (!string.IsNullOrEmpty(ownerId) && !string.IsNullOrEmpty(ownerMode))
                            {
                                if (!processOwnerModes.ContainsKey(ownerId))
                                    processOwnerModes[ownerId] = new HashSet<string>();
                                processOwnerModes[ownerId].Add(ownerMode);
                            }
                            if (!string.IsNullOrEmpty(ownerId) && !string.IsNullOrEmpty(objectName))
                            {
                                if (!processObjectNames.ContainsKey(ownerId))
                                    processObjectNames[ownerId] = new HashSet<string>();
                                processObjectNames[ownerId].Add(objectName);
                            }
                        }

                        /* Parse waiters */
                        foreach (var waiter in lockNode.Descendants("waiter"))
                        {
                            var waiterId = waiter.Attribute("id")?.Value ?? "";
                            var waiterMode = waiter.Attribute("mode")?.Value ?? "";
                            if (!string.IsNullOrEmpty(waiterId) && !string.IsNullOrEmpty(waiterMode))
                            {
                                if (!processWaiterModes.ContainsKey(waiterId))
                                    processWaiterModes[waiterId] = new HashSet<string>();
                                processWaiterModes[waiterId].Add(waiterMode);
                            }
                            if (!string.IsNullOrEmpty(waiterId) && !string.IsNullOrEmpty(objectName))
                            {
                                if (!processObjectNames.ContainsKey(waiterId))
                                    processObjectNames[waiterId] = new HashSet<string>();
                                processObjectNames[waiterId].Add(objectName);
                            }
                        }
                    }
                }
            }

            /* Parse each process */
            foreach (var proc in doc.Descendants("process"))
            {
                var id = proc.Attribute("id")?.Value ?? "";

                /* Get proc name from execution stack */
                var procName = "";
                foreach (var frame in proc.Descendants("frame"))
                {
                    var frameProcName = frame.Attribute("procname")?.Value ?? "";
                    if (!string.IsNullOrEmpty(frameProcName) && frameProcName != "adhoc" && frameProcName != "unknown")
                    {
                        procName = frameProcName;
                        break;
                    }
                }

                processes.Add(new T
                {
                    DeadlockTime = row.DeadlockTime,
                    ProcessId = id,
                    IsVictim = victimIds.Contains(id),
                    Spid = int.TryParse(proc.Attribute("spid")?.Value, out var spid) ? spid : 0,
                    DatabaseName = proc.Attribute("currentdbname")?.Value ?? "",
                    SqlText = proc.Element("inputbuf")?.Value?.Trim() ?? "",
                    WaitResource = proc.Attribute("waitresource")?.Value ?? "",
                    WaitTime = long.TryParse(proc.Attribute("waittime")?.Value, out var wt) ? wt : 0,
                    LockMode = proc.Attribute("lockMode")?.Value ?? "",
                    IsolationLevel = proc.Attribute("isolationlevel")?.Value ?? "",
                    LogUsed = long.TryParse(proc.Attribute("logused")?.Value, out var lu) ? lu : 0,
                    TransactionCount = int.TryParse(proc.Attribute("trancount")?.Value, out var tc) ? tc : 0,
                    ClientApp = proc.Attribute("clientapp")?.Value ?? "",
                    HostName = proc.Attribute("hostname")?.Value ?? "",
                    LoginName = proc.Attribute("loginname")?.Value ?? "",
                    Status = proc.Attribute("status")?.Value ?? "",
                    DeadlockGraphXml = row.DeadlockGraphXml,
                    VictimQueryPlanXml = row.VictimQueryPlanXml,
                    DeadlockType = deadlockType,
                    ProcName = procName,
                    TransactionName = proc.Attribute("transactionname")?.Value ?? "",
                    Priority = int.TryParse(proc.Attribute("priority")?.Value, out var pri) ? pri : 0,
                    LastTranStarted = DateTime.TryParse(proc.Attribute("lasttranstarted")?.Value, out var lts) ? lts : null,
                    LastBatchStarted = DateTime.TryParse(proc.Attribute("lastbatchstarted")?.Value, out var lbs) ? lbs : null,
                    LastBatchCompleted = DateTime.TryParse(proc.Attribute("lastbatchcompleted")?.Value, out var lbc) ? lbc : null,
                    OwnerMode = processOwnerModes.TryGetValue(id, out var om) ? string.Join(", ", om) : "",
                    WaiterMode = processWaiterModes.TryGetValue(id, out var wm) ? string.Join(", ", wm) : "",
                    ObjectNames = processObjectNames.TryGetValue(id, out var on) ? string.Join(", ", on) : ""
                });
            }
        }
        catch
        {
            processes = new List<T>();
            return false;
        }

        return true;
    }
}
