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
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #6005 (#5632 Phase C, D12): the per-graph signature an agent gate and the daily rollup key on. It is the object set, the
/// procedure names and the lock modes of one deadlock graph, with no line numbers, statement offsets, session ids or handles,
/// so editing a procedure keeps the signature. It is a second key, separate from <see cref="AlertFingerprint"/>, which keys the
/// human page and does not change.
///
/// <para>A pure function of the graph XML: no store, no clock. Two patterns on one table that differ in procedure or lock mode
/// get different signatures; #6006 merges what this still splits.</para>
/// </summary>
public static class DeadlockSignature
{
    /// <summary>The prefix of every signature, so the format can change without two formats colliding.</summary>
    public const string Prefix = "dl1-";

    /// <summary>
    /// The signature of the graph in <paramref name="graphXml"/>, or null when it is blank, unparseable or names no object (a
    /// graph with no object has no identity to count). Never throws.
    /// </summary>
    public static string? FromGraphXml(string? graphXml)
    {
        var objects = DeadlockObjectExtractor.FromGraphXml(graphXml);
        if (objects.Count == 0)
        {
            return null;
        }

        var procedures = new SortedSet<string>(StringComparer.Ordinal);
        var modes = new SortedSet<string>(StringComparer.Ordinal);
        try
        {
            var doc = XElement.Parse(graphXml!);
            foreach (var frame in doc.Descendants("frame"))
            {
                var name = frame.Attribute("procname")?.Value?.Trim();
                if (!string.IsNullOrEmpty(name))
                {
                    procedures.Add(name.ToLowerInvariant());
                }
            }

            var resources = doc.Descendants("resource-list").FirstOrDefault();
            if (resources is not null)
            {
                foreach (var node in resources.Descendants().Where(e => e.Name.LocalName is "owner" or "waiter"))
                {
                    var mode = node.Attribute("mode")?.Value?.Trim();
                    if (!string.IsNullOrEmpty(mode))
                    {
                        modes.Add(mode.ToUpperInvariant());
                    }
                }
            }
        }
        catch (System.Xml.XmlException)
        {
            return null;
        }

        var text = string.Join('\n',
            "o:" + string.Join('|', objects.Select(o => o.ToLowerInvariant()).Distinct(StringComparer.Ordinal).OrderBy(o => o, StringComparer.Ordinal)),
            "p:" + string.Join('|', procedures),
            "m:" + string.Join('|', modes));
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Prefix + Convert.ToHexString(hash, 0, 8).ToLower(CultureInfo.InvariantCulture);
    }
}
