using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
namespace PerformanceMonitor.Common;

/// <summary>
/// The one masking rule for investigation fields at <c>mcp.dataDetail</c> (#5632 R3), shared by the brief composer and
/// <c>get_investigation_reports</c> (#5632 review round 2, M1): structured fields are masked by key through the field
/// table, so category, times, names, verdicts, hashes and counts stay at every level; text a person or an agent wrote, or
/// engine prose that can quote collected text, is masked as free text (its literals replaced under <c>maskLiterals</c>,
/// withheld under <c>metadataOnly</c>).
/// </summary>
internal static class InvestigationFieldMasking
{
    /// <summary>The free-text keys of each item in a stored brief's sections, as the composer fills them.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> BriefFreeTextKeys = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        ["findings"] = ["story"],
        ["notes"] = ["text"],
        ["environment_profile"] = ["text"],
        ["earlier_reports"] = ["root_cause", "summary"],
        ["recent_changes"] = ["text"],
        ["prior_links"] = ["url", "key"],
    };

    /// <summary>
    /// Masks <paramref name="node"/>'s structured fields by key, then sets each of <paramref name="freeText"/>, masked as
    /// free text, in place of its <c>null</c> placeholder. A node the masker cannot read becomes
    /// <c>{"withheld": Refusal}</c>. Under <c>full</c> the node comes back as built.
    /// </summary>
    internal static JsonObject Mask(JsonObject node, McpDataDetailMasker masker, params (string Key, string? Text)[] freeText)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(masker);
        JsonObject result = node;
        var json = node.ToJsonString();
        var masked = masker.Json(json);
        if (!ReferenceEquals(masked, json))
        {
            result = !string.Equals(masked, McpDataDetailMasker.Refusal, StringComparison.Ordinal)
                && JsonNode.Parse(masked) is JsonObject parsed
                    ? parsed
                    : new JsonObject { ["withheld"] = McpDataDetailMasker.Refusal };
        }

        if (!result.ContainsKey("withheld"))
        {
            foreach (var (key, text) in freeText)
                result[key] = masker.FreeText(text);
        }

        return result;
    }

    /// <summary>
    /// A journal actor at <paramref name="masker"/>'s level: the login part of a person's action (<c>person:&lt;login&gt;</c>)
    /// is masked as a login, as the brief masks a note's author; <c>agent:</c> and <c>service</c> are Darling's own words
    /// and an agent id is limited to <c>[A-Za-z0-9._:-]</c>, so they are kept.
    /// </summary>
    internal static string? Actor(string? actor, McpDataDetailMasker masker)
    {
        const string Person = "person:";
        return actor is not null && actor.StartsWith(Person, StringComparison.Ordinal)
            ? Person + masker.Field("login_name", actor[Person.Length..])
            : actor;
    }

    /// <summary>
    /// A stored brief re-masked at the current level when it was composed at a weaker one (an operator moved from
    /// <c>full</c> to <c>metadataOnly</c> after the brief was stored): each section item's free text through
    /// <see cref="McpDataDetailMasker.FreeText"/>, the rest by key. A brief composed at the current level or a stronger
    /// one comes back as stored. Its <c>data_detail</c> then names the level it is served at.
    /// </summary>
    internal static JsonNode? StoredBrief(string brief, McpDataDetail mode, McpDataDetailMasker masker)
    {
        var root = JsonNode.Parse(brief);
        if (mode == McpDataDetail.Full || root is not JsonObject briefObject)
            return root;

        var storedWord = briefObject["data_detail"] is JsonValue word && word.TryGetValue(out string? text) ? text : null;
        var stored = McpDataDetailModes.TryParse(storedWord, out var parsed) ? parsed : McpDataDetail.Full;
        if (stored >= mode)
            return root;

        foreach (var (section, keys) in BriefFreeTextKeys)
        {
            if (briefObject[section] is not JsonArray items)
                continue;
            for (var i = 0; i < items.Count; i++)
            {
                if (items[i] is not JsonObject item)
                    continue;
                var freeText = keys.Where(item.ContainsKey)
                    .Select(k => (Key: k, Text: item[k] is JsonValue v && v.TryGetValue(out string? s) ? s : item[k]?.ToJsonString()))
                    .ToArray();
                var copy = (JsonObject)item.DeepClone();
                foreach (var (key, _) in freeText)
                    copy[key] = null;
                items[i] = Mask(copy, masker, freeText);
            }
        }

        if (briefObject["alerts"] is JsonArray alerts)
        {
            for (var i = 0; i < alerts.Count; i++)
            {
                if (alerts[i] is JsonObject alert)
                    alerts[i] = Mask((JsonObject)alert.DeepClone(), masker);
            }
        }

        briefObject["data_detail"] = McpDataDetailModes.Word(mode);
        return briefObject;
    }
}
