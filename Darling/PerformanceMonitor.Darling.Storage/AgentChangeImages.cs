/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>How the undo reads and writes one image field's column (#5917).</summary>
public enum ImageColumnType { Text, Bool, Whole, Real, Time, Json }

/// <summary>
/// Marks a property of an image record as a column of the changed table, with its type. The column's name is the property's
/// <c>[JsonPropertyName]</c>. A property without it (an id, a version, a summary) is on the wire but is not what an undo
/// compares or restores.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ImageColumnAttribute(ImageColumnType type) : Attribute
{
    public ImageColumnType Type { get; } = type;
}

/// <summary>
/// The <c>detail.kind</c> the gate journals for each Level 2 tool, shared by the tools and the undo (#5917, #5915, #5916). Below it, one
/// image record per kind is the single definition of the before and after image: the tools write it into their <c>action_called</c>
/// and <c>action_done</c> rows and the undo reads it back from the same rows, so the two cannot drift (#3684).
/// </summary>
public static class AgentChangeKinds
{
    public const string Mute = "mute";
    public const string CustomRule = "custom_rule";
    public const string ThresholdOverride = "threshold_override";
    public const string SignatureMerge = "signature_merge";

    /// <summary>A Query Store plan forced on a monitored SQL Server (#5632, step 7). A person undoes it from the Agent changes list (#5632, step 9): <see cref="AgentChangeUndo.UndoPlanChangeAsync"/>.</summary>
    public const string PlanForce = "plan_force";

    /// <summary>A Query Store plan unforced on a monitored SQL Server (#5632, step 7). A person undoes it from the Agent changes list (step 9).</summary>
    public const string PlanUnforce = "plan_unforce";

    /// <summary>One plan handle evicted from a monitored SQL Server's plan cache (#5632, step 10). It has no undo: an evicted plan cannot be put back, so the kind is not in <see cref="AgentChangeUndo.UndoableKinds"/>.</summary>
    public const string PlanEvict = "plan_evict";

    /// <summary>One session killed on a monitored SQL Server (#5632, step 12). It has no undo: a killed session cannot be brought back, so the kind is not in <see cref="AgentChangeUndo.UndoableKinds"/>.</summary>
    public const string SessionKill = "session_kill";

    /// <summary>One procedure, function or trigger recompiled on a monitored SQL Server with <c>sys.sp_recompile</c> (#6569). It has no undo: the next run compiles a new plan, so the kind is not in <see cref="AgentChangeUndo.UndoableKinds"/>.</summary>
    public const string ModuleRecompile = "module_recompile";

    /// <summary>One plan guide created on a monitored SQL Server with <c>sys.sp_create_plan_guide</c> (#6569, slice 2a). A person undoes it from the Agent changes list: the undo drops the guide by name, so the kind is read by the plan undo (<see cref="AgentChangeUndo.PlanUndoTool"/>), not by <see cref="AgentChangeUndo.UndoableKinds"/>.</summary>
    public const string PlanGuideCreate = "plan_guide_create";

    /// <summary>One plan guide dropped on a monitored SQL Server with <c>sys.sp_control_plan_guide N'DROP'</c> (#6569, slice 2b). The call journals the whole definition it read (name, type, statement, module or batch, params, hints, is_disabled). A person undoes it from the Agent changes list: the undo re-creates the guide from that definition, so the kind is read by the plan undo (<see cref="AgentChangeUndo.UndoPlanChangeAsync"/>) and is not in <see cref="AgentChangeUndo.UndoableKinds"/>.</summary>
    public const string PlanGuideDrop = "plan_guide_drop";

    /// <summary>A person's attempt to undo a plan change whose write was sent and whose reply was lost (#5632). Journaled as an <c>action_called</c> row with no result, so the list reads it as "result unknown". It has no undo.</summary>
    public const string PlanUndo = "plan_undo";
}

/// <summary>A mute rule as <c>config.config_mute_rules</c> holds it. Also the wire shape of the mute tools' <c>mute_rule</c> object.</summary>
public sealed record MuteRuleImage(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("enabled"), ImageColumn(ImageColumnType.Bool)] bool Enabled,
    [property: JsonPropertyName("created_at_utc"), ImageColumn(ImageColumnType.Time)] string? CreatedAtUtc,
    [property: JsonPropertyName("expires_at_utc"), ImageColumn(ImageColumnType.Time)] string? ExpiresAtUtc,
    [property: JsonPropertyName("starts_at_utc"), ImageColumn(ImageColumnType.Time)] string? StartsAtUtc,
    [property: JsonPropertyName("reason"), ImageColumn(ImageColumnType.Text)] string? Reason,
    [property: JsonPropertyName("server_name"), ImageColumn(ImageColumnType.Text)] string? ServerName,
    [property: JsonPropertyName("server_id"), ImageColumn(ImageColumnType.Whole)] int? ServerId,
    [property: JsonPropertyName("metric_name"), ImageColumn(ImageColumnType.Text)] string? MetricName,
    [property: JsonPropertyName("external_source"), ImageColumn(ImageColumnType.Text)] string? ExternalSource,
    [property: JsonPropertyName("database_pattern"), ImageColumn(ImageColumnType.Text)] string? DatabasePattern,
    [property: JsonPropertyName("query_text_pattern"), ImageColumn(ImageColumnType.Text)] string? QueryTextPattern,
    [property: JsonPropertyName("wait_type_pattern"), ImageColumn(ImageColumnType.Text)] string? WaitTypePattern,
    [property: JsonPropertyName("job_name_pattern"), ImageColumn(ImageColumnType.Text)] string? JobNamePattern,
    [property: JsonPropertyName("client_app_pattern"), ImageColumn(ImageColumnType.Text)] string? ClientAppPattern,
    [property: JsonPropertyName("match_summary")] string? MatchSummary)
{
    public const string Kind = AgentChangeKinds.Mute;
    public const string Table = "config.config_mute_rules";
}

/// <summary>A custom alert rule as <c>config.custom_alert_rules</c> holds it. Also the single-rule wire shape (<c>definition</c> embedded as JSON).</summary>
public sealed record CustomAlertRuleImage(
    [property: JsonPropertyName("id")] long Id,
    [property: JsonPropertyName("name"), ImageColumn(ImageColumnType.Text)] string Name,
    [property: JsonPropertyName("description"), ImageColumn(ImageColumnType.Text)] string? Description,
    [property: JsonPropertyName("definition"), ImageColumn(ImageColumnType.Json)] JsonNode? Definition,
    [property: JsonPropertyName("enabled"), ImageColumn(ImageColumnType.Bool)] bool Enabled,
    [property: JsonPropertyName("version")] int Version,
    [property: JsonPropertyName("created_at")] DateTime CreatedAt,
    [property: JsonPropertyName("updated_at")] DateTime UpdatedAt,
    [property: JsonPropertyName("updated_by")] string? UpdatedBy,
    [property: JsonPropertyName("expires_at_utc"), ImageColumn(ImageColumnType.Time)] string? ExpiresAtUtc = null,
    [property: JsonPropertyName("expired")] bool Expired = false)
{
    public const string Kind = AgentChangeKinds.CustomRule;
    public const string Table = "config.custom_alert_rules";
}

/// <summary>A threshold override as <c>config.config_threshold_overrides</c> holds it. Its key (server, database, threshold) is the journal's target.</summary>
public sealed record ThresholdOverrideImage(
    [property: JsonPropertyName("value"), ImageColumn(ImageColumnType.Real)] double Value)
{
    public const string Kind = AgentChangeKinds.ThresholdOverride;
    public const string Table = "config.config_threshold_overrides";
}

/// <summary>
/// A deadlock signature merge as <c>config.investigation_signature_merges</c> holds it (#6006). Its key (server, merged
/// signature) is the journal's target; an undo stamps <c>undone_at</c> and <c>undone_by</c> and keeps the row.
/// </summary>
public sealed record SignatureMergeImage(
    [property: JsonPropertyName("merge_id")] long MergeId,
    [property: JsonPropertyName("signature")] string Signature,
    [property: JsonPropertyName("merged_into"), ImageColumn(ImageColumnType.Text)] string MergedInto)
{
    public const string Kind = AgentChangeKinds.SignatureMerge;
    public const string Table = "config.investigation_signature_merges";
}

/// <summary>Reads an image record's column list from its attributes, so the undo's SQL names the columns the record names and no list is kept twice.</summary>
internal static class ImageColumns
{
    internal static (string Name, ImageColumnType Type)[] Of<T>() =>
        typeof(T).GetProperties()
            .Select(p => (Property: p, Column: (ImageColumnAttribute?)Attribute.GetCustomAttribute(p, typeof(ImageColumnAttribute))))
            .Where(x => x.Column is not null)
            .Select(x => (((JsonPropertyNameAttribute)Attribute.GetCustomAttribute(x.Property, typeof(JsonPropertyNameAttribute))!).Name, x.Column!.Type))
            .ToArray();

    /// <summary>
    /// The image as a flat object of column names (the shape the undo compares and writes), or null for no image. A name the
    /// journaled image does not carry is left out, so a partial image compares only what it names.
    /// </summary>
    internal static JsonObject? ToObject<T>(JsonNode? image)
    {
        if (image is not JsonObject named)
            return null;
        var typed = JsonSerializer.SerializeToNode(image.Deserialize<T>())!.AsObject();
        foreach (var key in typed.Select(p => p.Key).ToList())
        {
            if (!named.ContainsKey(key))
                typed.Remove(key);
        }

        return typed;
    }
}
