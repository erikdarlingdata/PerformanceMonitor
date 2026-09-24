/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>The sqlTail family's topics (#3898).</summary>
public static partial class McpToolGuideTopics
{
    /// <summary>The <see cref="MemoryGrantWindowReadOrder"/> topic's name.</summary>
    public const string MemoryGrantWindowReadOrderName = "memory_grant_window_read_order";

    /// <summary>
    /// The read order for <c>get_resource_semaphore</c> and <c>get_memory_grants</c>: <c>window[]</c> answers
    /// whether there was pressure anywhere in the window, <c>grants[]</c> whether there is pressure right now.
    /// The same sentence, word for word, closed both tools' descriptions on both products (four copies); moved
    /// here so each tail carries it once instead of four verbatim copies of the same guidance.
    /// </summary>
    public const string MemoryGrantWindowReadOrder =
        "A calm grants[] beside a window[] with waiters or timeouts is a grant storm that has passed; read window[] first for 'was there pressure', grants[] for 'is there pressure now'.";

    private static readonly McpToolGuideTopic[] s_sqlTail =
    [
        new(
            MemoryGrantWindowReadOrderName,
            "Read order for get_resource_semaphore/get_memory_grants: window[] for whether there was pressure, grants[] for whether there is pressure now.",
            MemoryGrantWindowReadOrder),
    ];
}
