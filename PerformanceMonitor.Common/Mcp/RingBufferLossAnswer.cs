/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// #8146: how an MCP answer carries the ring-buffer loss of its window. A data answer gets a top-level
/// <c>ring_buffer_loss</c> key, an empty answer gets it inside <c>hints</c> beside the #4966 window keys, and a window that
/// lost nothing gets no key at all (absent, never null). Shared by Lite and Darling so the two answers match.
/// </summary>
public static class RingBufferLossAnswer
{
    public const string Key = "ring_buffer_loss";

    /// <summary>
    /// The clause an empty answer gives instead of its all-clear when the window lost events (REVIEW-TRAPS:39): collector runs that saw
    /// nothing prove nothing about the events the ring buffer evicted or dropped before those runs read it. It follows "but ", and
    /// both apps use this one text so their answers match.
    /// </summary>
    public const string LostEventsClaim =
        "the ring buffer lost events before they were collected (see hints.ring_buffer_loss), so the window is incomplete and finding nothing does not mean nothing happened";

    /// <summary>
    /// The note a database-filtered answer adds to its <c>ring_buffer_loss</c> (#8146): the XE session and its ring buffer are the
    /// server's, so the loss is counted over every database whatever the filter was. One text for Lite and Darling.
    /// </summary>
    public const string DatabaseFilterNote =
        "This answer is filtered by database, but the ring-buffer loss is not: the session's buffer holds events of every database, so these counts are for the whole session and the filter does not narrow them.";

    /// <summary>
    /// The serialized data answer with <c>ring_buffer_loss</c> appended at the top level, or the answer as is when nothing was lost.
    /// <paramref name="databaseFiltered"/> adds <see cref="DatabaseFilterNote"/> to the loss.
    /// </summary>
    public static string Attach(string json, RingBufferLossNote.LossWindow? loss, bool databaseFiltered = false)
    {
        if (loss is not { } window)
        {
            return json;
        }

        var node = JsonNode.Parse(json)!.AsObject();
        node[Key] = JsonSerializer.SerializeToNode(window.ToPayload(databaseFiltered), McpHelpers.JsonOptions);
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The hints of an empty answer with <c>ring_buffer_loss</c> added. <paramref name="hints"/> is the window notice's hints
    /// (null when its probe failed); the loss stands on its own then. Returns <paramref name="hints"/> untouched when nothing was lost.
    /// </summary>
    public static object? WithLoss(object? hints, RingBufferLossNote.LossWindow? loss, bool databaseFiltered = false)
    {
        if (loss is not { } window)
        {
            return hints;
        }

        var node = hints is null
            ? new JsonObject()
            : JsonSerializer.SerializeToNode(hints, McpHelpers.JsonOptions)!.AsObject();
        node[Key] = JsonSerializer.SerializeToNode(window.ToPayload(databaseFiltered), McpHelpers.JsonOptions);
        return node;
    }

    /// <summary>
    /// The loss of two collectors read for one answer (get_blocking_stats reads the blocked-process reports and the deadlocks), summed:
    /// counts add, the span widens, and the note is the newer run's. Null when neither lost anything.
    /// </summary>
    public static RingBufferLossNote.LossWindow? Combine(RingBufferLossNote.LossWindow? first, RingBufferLossNote.LossWindow? second)
    {
        if (first is not { } a)
        {
            return second;
        }

        if (second is not { } b)
        {
            return a;
        }

        var newer = b.LastLossUtc > a.LastLossUtc || (b.LastLossUtc == a.LastLossUtc && string.CompareOrdinal(b.NewestNote, a.NewestNote) > 0) ? b : a;
        return new RingBufferLossNote.LossWindow(
            a.Evicted + b.Evicted,
            a.Dropped + b.Dropped,
            a.UnknownRuns + b.UnknownRuns,
            a.FirstLossUtc < b.FirstLossUtc ? a.FirstLossUtc : b.FirstLossUtc,
            newer.LastLossUtc,
            newer.NewestNote);
    }

    /// <summary>
    /// An already-serialized status answer with <c>ring_buffer_loss</c> inside its <c>hints</c>: only an <c>empty</c> answer (or an
    /// <c>unavailable</c> one that already carries hints) takes it; <c>not_collected</c>, <c>precondition</c>, an offline answer and any
    /// data answer stay as they were. Lite writes its empty answers as strings, so this is its twin of <see cref="WithLoss"/>.
    /// </summary>
    public static string AttachToHints(string answer, RingBufferLossNote.LossWindow? loss, bool databaseFiltered = false)
    {
        if (loss is not { } window || JsonNode.Parse(answer) is not JsonObject node)
        {
            return answer;
        }

        var status = node["status"] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
        var hints = node["hints"] as JsonObject;
        if (status != "empty" && !(status == "unavailable" && hints is not null))
        {
            return answer;
        }

        hints ??= new JsonObject();
        node["hints"] = hints;
        hints[Key] = JsonSerializer.SerializeToNode(window.ToPayload(databaseFiltered), McpHelpers.JsonOptions);
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
