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
using System.Linq;
using System.Xml.Linq;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Parses a SQL Server deadlock graph XML into a <see cref="DeadlockGraphModel"/> — the waits-for graph
    /// among processes. Pure and app-agnostic (Common). Robust to:
    /// <list type="bullet">
    /// <item>both the bare <c>&lt;deadlock&gt;</c> element the apps store and a full
    ///   <c>&lt;event name="xml_deadlock_report"&gt;</c> / Azure <c>database_xml_deadlock_report</c> wrapper
    ///   (anchors on the first <c>deadlock</c> descendant);</item>
    /// <item>multiple victims (marks them all);</item>
    /// <item>a resource with several owners/waiters (emits the cross product of waiter -&gt; owner edges);</item>
    /// <item>parallel deadlocks (<c>exchangeEvent</c>/<c>SyncPoint</c>), including an owner == waiter self-edge;</item>
    /// <item>missing object names (falls back to a kind + id label);</item>
    /// <item>malformed XML (returns an empty model rather than throwing).</item>
    /// </list>
    /// Reuses the XPath knowledge of the apps' existing per-process parse, but keeps per-resource identity
    /// and per-edge modes the flat parse discards.
    /// </summary>
    public static class DeadlockGraphParser
    {
        // Lock resource elements that carry owner/waiter lists. exchangeEvent/SyncPoint mark a parallel
        // (intra-query) deadlock and may have neither object names nor modes.
        private static readonly HashSet<string> LockElementNames = new(StringComparer.Ordinal)
        {
            "objectlock", "keylock", "pagelock", "ridlock", "rowgrouplock",
            "hobtlock", "allocunitlock", "metadatalock", "applicationlock", "filegrouplock",
            "exchangeEvent", "SyncPoint"
        };

        private static readonly HashSet<string> ParallelElementNames = new(StringComparer.Ordinal)
        {
            "exchangeEvent", "SyncPoint"
        };

        /// <summary>
        /// Parses <paramref name="xml"/> into a deadlock graph model. Never throws — unparseable or
        /// non-deadlock input yields an empty model (<see cref="DeadlockGraphModel.IsEmpty"/>).
        /// </summary>
        public static DeadlockGraphModel Parse(string? xml)
        {
            if (string.IsNullOrWhiteSpace(xml))
                return Empty();

            XElement root;
            try
            {
                root = ParseRoot(xml);
            }
            catch
            {
                return Empty();
            }

            try
            {
                // Anchor on the first <deadlock> (self or descendant) so any event wrapper is transparent.
                var deadlock = root.Name.LocalName == "deadlock"
                    ? root
                    : root.Descendants("deadlock").FirstOrDefault();
                if (deadlock == null)
                    return Empty();

                var victimIds = new HashSet<string>(StringComparer.Ordinal);
                foreach (var vp in deadlock.Descendants("victimProcess"))
                {
                    var id = vp.Attribute("id")?.Value;
                    if (!string.IsNullOrEmpty(id))
                        victimIds.Add(id);
                }

                var processes = ParseProcesses(deadlock, victimIds);
                if (processes.Count == 0)
                    return Empty();

                var (edges, isParallel) = ParseEdges(deadlock);

                // Surface the resolved contended resource on each waiting process (parity with the block-chain
                // viewer). SQL Server already resolved the objectname in the resource-list, so this is a pure
                // projection of the edges — no DMV/object lookup needed (the whole reason deadlocks are easier).
                AssignContendedObjects(processes, edges);

                return new DeadlockGraphModel
                {
                    Processes = processes,
                    Edges = edges,
                    IsParallel = isParallel,
                    EventTime = ParseEventTime(root)
                };
            }
            catch
            {
                return Empty();
            }
        }

        private static XElement ParseRoot(string xml)
        {
            var trimmed = xml.TrimStart();
            // CONVERT(nvarchar(max), xml) yields a declaration-free string, but tolerate a stray prolog.
            if (trimmed.StartsWith("<?xml", StringComparison.OrdinalIgnoreCase))
                return XDocument.Parse(trimmed).Root ?? throw new FormatException("empty document");
            return XElement.Parse(trimmed);
        }

        private static List<DeadlockProcessNode> ParseProcesses(XElement deadlock, HashSet<string> victimIds)
        {
            var processes = new List<DeadlockProcessNode>();

            foreach (var proc in deadlock.Descendants("process"))
            {
                var id = proc.Attribute("id")?.Value ?? string.Empty;
                if (string.IsNullOrEmpty(id))
                    continue;

                processes.Add(new DeadlockProcessNode
                {
                    Id = id,
                    Spid = ParseInt(proc.Attribute("spid")?.Value),
                    Ecid = ParseInt(proc.Attribute("ecid")?.Value),
                    IsVictim = victimIds.Contains(id),
                    SqlText = ExtractSqlText(proc),
                    LockMode = proc.Attribute("lockMode")?.Value ?? string.Empty,
                    WaitResource = (proc.Attribute("waitresource")?.Value ?? string.Empty).Trim(),
                    WaitTimeMs = ParseLong(proc.Attribute("waittime")?.Value),
                    Priority = ParseInt(proc.Attribute("priority")?.Value),
                    // currentdbname is the DB NAME; currentdb is the numeric dbid — never substitute it.
                    DatabaseName = proc.Attribute("currentdbname")?.Value ?? string.Empty,
                    LoginName = proc.Attribute("loginname")?.Value ?? string.Empty,
                    ClientApp = proc.Attribute("clientapp")?.Value ?? string.Empty,
                    HostName = proc.Attribute("hostname")?.Value ?? string.Empty,
                    IsolationLevel = proc.Attribute("isolationlevel")?.Value ?? string.Empty,
                    ProcName = ExtractProcName(proc),
                    Status = proc.Attribute("status")?.Value ?? string.Empty
                });
            }

            // Deterministic order so the layout is stable regardless of XML element order.
            processes.Sort(static (a, b) =>
            {
                var c = a.Spid.CompareTo(b.Spid);
                if (c != 0) return c;
                c = a.Ecid.CompareTo(b.Ecid);
                if (c != 0) return c;
                return string.CompareOrdinal(a.Id, b.Id);
            });

            return processes;
        }

        private static (List<DeadlockWaitEdge> Edges, bool IsParallel) ParseEdges(XElement deadlock)
        {
            var edges = new List<DeadlockWaitEdge>();
            var isParallel = false;

            var resourceList = deadlock.Descendants("resource-list").FirstOrDefault();
            if (resourceList == null)
                return (edges, isParallel);

            foreach (var lockEl in resourceList.Elements())
            {
                var kind = lockEl.Name.LocalName;
                if (!LockElementNames.Contains(kind))
                    continue;

                if (ParallelElementNames.Contains(kind))
                    isParallel = true;

                var label = BuildResourceLabel(lockEl, kind);

                // owner-list/owner + waiter-list/waiter (Descendants tolerates a missing list wrapper).
                var owners = lockEl.Descendants("owner")
                    .Select(o => (Id: o.Attribute("id")?.Value, Mode: o.Attribute("mode")?.Value ?? string.Empty))
                    .Where(o => !string.IsNullOrEmpty(o.Id))
                    .ToList();
                var waiters = lockEl.Descendants("waiter")
                    .Select(w => (Id: w.Attribute("id")?.Value, Mode: w.Attribute("mode")?.Value ?? string.Empty))
                    .Where(w => !string.IsNullOrEmpty(w.Id))
                    .ToList();

                // One edge per (waiter, owner) pair on this resource: "waiter waits for what owner holds."
                // N owners × M waiters => N·M edges — how real >2-process cycles form.
                foreach (var waiter in waiters)
                {
                    foreach (var owner in owners)
                    {
                        edges.Add(new DeadlockWaitEdge
                        {
                            WaiterProcessId = waiter.Id!,
                            OwnerProcessId = owner.Id!,
                            ResourceKind = kind,
                            ResourceLabel = label,
                            RequestMode = waiter.Mode,
                            OwnerMode = owner.Mode
                        });
                    }
                }
            }

            return (edges, isParallel);
        }

        /// <summary>
        /// Sets <see cref="DeadlockProcessNode.ContentiousObject"/> on each process to the resolved label of
        /// the first lock resource it waits on (its first non-self lock edge). Pure projection of the edges
        /// SQL Server already resolved — no object lookup. Empty for a process that only owns (no outgoing
        /// wait), waits solely on a parallelism exchange (which is coordination, not a contended object), or
        /// whose wait resource carried no object name.
        /// </summary>
        private static void AssignContendedObjects(
            IReadOnlyList<DeadlockProcessNode> processes,
            IReadOnlyList<DeadlockWaitEdge> edges)
        {
            foreach (var node in processes)
            {
                foreach (var edge in edges)
                {
                    if (!string.Equals(edge.WaiterProcessId, node.Id, StringComparison.Ordinal)) continue;
                    if (edge.IsSelfEdge) continue;
                    if (ParallelElementNames.Contains(edge.ResourceKind)) continue; // the exchange is not a contended object
                    if (string.IsNullOrEmpty(edge.ResourceLabel)) continue;
                    node.ContentiousObject = edge.ResourceLabel;
                    break;
                }
            }
        }

        /// <summary>The statement to lead the card with: the process input buffer, falling back to the
        /// first execution-stack frame's text if the input buffer is blank.</summary>
        private static string ExtractSqlText(XElement proc)
        {
            var inputBuf = proc.Element("inputbuf")?.Value?.Trim();
            if (!string.IsNullOrEmpty(inputBuf))
                return inputBuf;

            var frame = proc.Element("executionStack")?.Elements("frame")
                .Select(f => f.Value?.Trim())
                .FirstOrDefault(v => !string.IsNullOrEmpty(v));
            return frame ?? string.Empty;
        }

        /// <summary>The first real procedure on the execution stack (skipping adhoc/unknown); blank for ad hoc.</summary>
        private static string ExtractProcName(XElement proc)
        {
            var stack = proc.Element("executionStack");
            if (stack == null)
                return string.Empty;

            foreach (var frame in stack.Elements("frame"))
            {
                var name = frame.Attribute("procname")?.Value;
                if (!string.IsNullOrEmpty(name) &&
                    !name.Equals("adhoc", StringComparison.OrdinalIgnoreCase) &&
                    !name.Equals("unknown", StringComparison.OrdinalIgnoreCase))
                {
                    return name;
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// Builds a human label for a contended resource: a friendly kind word + the object name when
        /// present ("PAGE hammerdb_tpcc.dbo.new_order"), else the kind + whatever id the element carries
        /// (page/hobt/object/application) so a nameless Azure/system resource still reads.
        /// </summary>
        private static string BuildResourceLabel(XElement lockEl, string kind)
        {
            var friendly = FriendlyKind(kind);

            var objectName = lockEl.Attribute("objectname")?.Value;
            if (!string.IsNullOrWhiteSpace(objectName))
                return $"{friendly} {objectName}";

            // exchangeEvent/SyncPoint never name an object — they ARE the parallelism resource.
            if (ParallelElementNames.Contains(kind))
                return friendly;

            var pageId = lockEl.Attribute("pageid")?.Value;
            if (!string.IsNullOrWhiteSpace(pageId))
            {
                var dbid = lockEl.Attribute("dbid")?.Value;
                var fileId = lockEl.Attribute("fileid")?.Value;
                return $"{friendly} {dbid}:{fileId}:{pageId}";
            }

            var hobtId = lockEl.Attribute("hobtid")?.Value;
            if (!string.IsNullOrWhiteSpace(hobtId))
                return $"{friendly} hobt {hobtId}";

            var objId = lockEl.Attribute("objid")?.Value;
            if (!string.IsNullOrWhiteSpace(objId))
                return $"{friendly} objid {objId}";

            var appResource = lockEl.Attribute("resource")?.Value;
            if (!string.IsNullOrWhiteSpace(appResource))
                return $"{friendly} {appResource}";

            return friendly;
        }

        private static string FriendlyKind(string kind) => kind switch
        {
            "pagelock" => "PAGE",
            "keylock" => "KEY",
            "objectlock" => "OBJECT",
            "ridlock" => "RID",
            "rowgrouplock" => "ROWGROUP",
            "hobtlock" => "HOBT",
            "allocunitlock" => "ALLOCUNIT",
            "metadatalock" => "METADATA",
            "applicationlock" => "APPLICATION",
            "filegrouplock" => "FILEGROUP",
            "exchangeEvent" => "Parallelism",
            "SyncPoint" => "Parallelism",
            _ => kind.ToUpperInvariant()
        };

        private static DateTime? ParseEventTime(XElement root)
        {
            var eventEl = root.Name.LocalName == "event"
                ? root
                : root.DescendantsAndSelf("event").FirstOrDefault();
            var ts = eventEl?.Attribute("timestamp")?.Value;
            if (string.IsNullOrWhiteSpace(ts))
                return null;
            return DateTimeOffset.TryParse(ts, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dto)
                ? dto.UtcDateTime
                : (DateTime?)null;
        }

        private static int ParseInt(string? s) =>
            int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

        private static long ParseLong(string? s) =>
            long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

        private static DeadlockGraphModel Empty() => new();
    }
}
