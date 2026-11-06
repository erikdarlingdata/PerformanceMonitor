/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Common;

/// <summary>
/// The Host OS cell (#7699 item 3). SQL Server 2016 has no <c>sys.dm_os_host_info</c>, so the collector keeps the text after
/// <c>on</c> in <c>@@VERSION</c>: <c>Windows Server 2016 Datacenter 10.0 &lt;X64&gt; (Build 14393: ) (Hypervisor)</c>. Every other
/// version reports only the product name. The collector now cuts the text before it is stored, and every read passes the stored
/// value through <see cref="Clean"/> so rows stored earlier print the same way.
/// </summary>
public static class HostOsText
{
    private static readonly Regex TrailingVersion = new(@"\s+\d+\.\d+$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The value trimmed and, when it carries the <c>@@VERSION</c> tail, cut at the architecture mark with the kernel version
    /// number before it dropped. A <c>Windows NT 6.3</c> value keeps its number, which is all the name there is.</summary>
    public static string Clean(string? raw)
    {
        var text = raw?.Trim() ?? "";
        var arch = text.IndexOf(" <", StringComparison.Ordinal);
        if (arch < 0)
            return text;

        var head = text[..arch].TrimEnd();
        if (head.StartsWith("Windows NT", StringComparison.Ordinal))
            return head;

        var version = TrailingVersion.Match(head);
        return version.Success && version.Index > 0 ? head[..version.Index] : head;
    }
}
