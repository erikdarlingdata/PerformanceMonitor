/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The cross-tool reading guides <c>get_tool_guide</c> serves by name (#3898 D1). A topic is guidance that
/// belongs to several tools at once, stated once here instead of once per description. The texts are
/// <c>const</c> so a converted tool's tail can include one verbatim (attribute arguments must be constants),
/// which saves the caller a second call without a second copy of the sentence. Both SKUs serve the same set
/// (D6 lockstep).
///
/// <para><b>#3898 step 0: a partial class, one file per lane.</b> Each family converted under #3898 gets its own
/// <c>McpToolGuideTopics.&lt;Family&gt;.cs</c>, declaring that family's own <c>private static readonly
/// McpToolGuideTopic[]</c> field (<see cref="s_healthParser"/> is the pilot's). A lane fills in its own,
/// already-declared, already-empty array; it never edits this file or any other family's, so two families'
/// topic work never conflicts.</para>
///
/// <para><b>Why <see cref="All"/> is built in a static constructor.</b> Static field initializers in different
/// partial-class files run in an unspecified order, so an initializer here that read another part's array could
/// observe it before that part's own initializer ran. A static constructor in the SAME type runs only after
/// every part's field initializers have run, regardless of their relative order, so it is the one place that
/// may safely read across parts.</para>
/// </summary>
public static partial class McpToolGuideTopics
{
    /// <summary>Every topic, in the order the index lists them: every family's array, family arrays in a fixed
    /// order, each family's own topics in its own declared order.</summary>
    public static IReadOnlyList<McpToolGuideTopic> All { get; }

    static McpToolGuideTopics()
    {
        All = new[] { s_healthParser, s_data, s_sqlCore, s_sqlTail, s_alerting, s_platform, s_pgA, s_pgB }
            .SelectMany(topics => topics)
            .ToList();
    }
}
