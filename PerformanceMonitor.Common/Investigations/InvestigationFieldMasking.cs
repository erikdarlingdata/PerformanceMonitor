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

        /* #6001: the evidence section's items hold collected rows (query text, logins, hosts) in `content`, masked by key as
           a whole item, like an alert. */
        if (briefObject["evidence"] is JsonArray evidenceItems)
        {
            for (var i = 0; i < evidenceItems.Count; i++)
            {
                if (evidenceItems[i] is JsonObject evidenceItem)
                    evidenceItems[i] = Mask((JsonObject)evidenceItem.DeepClone(), masker);
            }
        }

        /* #5632: a brief stored at full carries no parameter-values line; one served at a weaker level needs it. */
        if (briefObject["read_rules"] is JsonArray rules
            && !rules.Any(r => r is JsonValue v && v.TryGetValue(out string? line) && string.Equals(line, InvestigationBriefComposer.ParameterValuesHiddenRule, StringComparison.Ordinal)))
            rules.Add(InvestigationBriefComposer.ParameterValuesHiddenRule);

        briefObject["data_detail"] = McpDataDetailModes.Word(mode);
        return briefObject;
    }

    /// <summary>
    /// Evidence items masked as a report's are (#5632 review round 2, M1): each item's <c>key_numbers</c> as free text, the
    /// rest by key. Shared by the report read and the journal (#5912), so a Level 2 change's evidence is masked exactly as
    /// the report's is.
    /// </summary>
    internal static JsonArray Evidence(JsonNode? evidence, McpDataDetailMasker masker)
    {
        var masked = new JsonArray();
        if (evidence is not JsonArray items)
            return masked;
        foreach (var item in items)
        {
            if (item is not JsonObject entry)
            {
                masked.Add(Mask(new JsonObject { ["item"] = item?.DeepClone() }, masker));
                continue;
            }

            var keyNumbers = entry["key_numbers"] is JsonValue v && v.TryGetValue(out string? text)
                ? text : entry["key_numbers"]?.ToJsonString();
            var copy = (JsonObject)entry.DeepClone();
            copy["key_numbers"] = null;
            masked.Add(Mask(copy, masker, ("key_numbers", keyNumbers)));
        }

        return masked;
    }

    /// <summary>The image keys that are free text (#5912): a mute's reason, a rule's name, description and definition, and
    /// an action_failed row's error. Every key that ends in <c>_pattern</c> is free text too.</summary>
    internal static readonly IReadOnlySet<string> ImageFreeTextKeys =
        new HashSet<string>(StringComparer.Ordinal) { "reason", "name", "description", "definition", "error" };

    /// <summary>
    /// A journal row's <c>detail</c> at <paramref name="masker"/>'s level: the one helper the MCP read and the web journal
    /// endpoint share (#5912). <c>evidence</c> goes through <see cref="Evidence"/>; the <c>before</c> and <c>after</c> images
    /// and an <c>error</c> have their free text (<see cref="ImageFreeTextKeys"/> and every <c>*_pattern</c>) through
    /// <see cref="McpDataDetailMasker.FreeText"/>, so it is masked under <c>maskLiterals</c> and withheld under
    /// <c>metadataOnly</c>, and the rest by key. Under <c>full</c> (the Viewer's and the web's level) it comes back as stored.
    /// </summary>
    internal static JsonNode? JournalDetail(string? detailJson, McpDataDetailMasker masker)
    {
        ArgumentNullException.ThrowIfNull(masker);
        if (detailJson is null)
            return null;
        var root = JsonNode.Parse(detailJson);
        if (root is not JsonObject detail)
            return root is null ? null : Mask(new JsonObject { ["detail"] = root.DeepClone() }, masker)["detail"]?.DeepClone();

        var evidence = detail["evidence"];
        var images = new List<(string Key, JsonNode? Node)>();
        foreach (var key in new[] { "before", "after" })
        {
            if (detail.ContainsKey(key))
                images.Add((key, detail[key]?.DeepClone()));
        }

        var error = detail["error"] is JsonValue ev && ev.TryGetValue(out string? errorText) ? errorText : detail["error"]?.ToJsonString();
        var copy = (JsonObject)detail.DeepClone();
        copy.Remove("evidence");
        foreach (var (key, _) in images)
            copy.Remove(key);
        var hasError = copy.Remove("error");
        if (hasError)
            copy["error"] = null;

        var result = hasError ? Mask(copy, masker, ("error", error)) : Mask(copy, masker);
        if (result.ContainsKey("withheld"))
            return result;
        if (evidence is not null)
            result["evidence"] = Evidence(evidence, masker);
        foreach (var (key, node) in images)
            result[key] = Image(node, masker);
        return result;
    }

    /// <summary>A before or after image: its free-text keys through <see cref="McpDataDetailMasker.FreeText"/> (an object's
    /// text as its JSON), the rest by key. A non-object image is masked whole as free text.</summary>
    private static JsonNode? Image(JsonNode? image, McpDataDetailMasker masker)
    {
        if (image is not JsonObject obj)
            return image is null ? null : JsonValue.Create(masker.FreeText(image.ToJsonString()));

        var freeText = obj.Where(p => ImageFreeTextKeys.Contains(p.Key) || p.Key.EndsWith("_pattern", StringComparison.Ordinal))
            .Select(p => (Key: p.Key, Text: p.Value is JsonValue v && v.TryGetValue(out string? s) ? s : p.Value?.ToJsonString()))
            .ToArray();
        var copy = (JsonObject)obj.DeepClone();
        foreach (var (key, _) in freeText)
            copy[key] = null;
        return Mask(copy, masker, freeText);
    }
}
