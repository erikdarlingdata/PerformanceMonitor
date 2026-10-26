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
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One line of the Custom Views manager's list (#6390 S5): a stored view's summary, shaped for the grid.</summary>
public sealed class CustomViewRow
{
    public CustomViewRow(CustomViewSummary summary)
    {
        Summary = summary ?? throw new ArgumentNullException(nameof(summary));
    }

    public CustomViewSummary Summary { get; }

    public long Id => Summary.Id;

    public string Name => Summary.Name;

    public int Version => Summary.Version;

    /// <summary>The folder the list groups under; the top level reads <see cref="CustomViewsEditorModel.TopLevelLabel"/>.</summary>
    public string FolderDisplay => string.IsNullOrEmpty(Summary.Folder) ? CustomViewsEditorModel.TopLevelLabel : Summary.Folder;

    public string KindDisplay => string.Equals(Summary.Kind, "notebook", StringComparison.Ordinal) ? "Notebook" : "Dashboard";

    public string Description => Summary.Description ?? string.Empty;

    /// <summary>The save time in UTC, the column's sort key (the displayed text would sort as text).</summary>
    public DateTime UpdatedUtc => DateTime.SpecifyKind(Summary.UpdatedAt, DateTimeKind.Utc);

    /// <summary>The save time and who saved it. A view belongs to the whole fleet, so the time is fleet-wide text in the display mode
    /// (this machine's zone for Local, UTC otherwise) with its zone label, never the active server tab's clock.</summary>
    public string UpdatedDisplay => UpdatedText(ViewerTimeHelper.CurrentDisplayMode);

    /// <summary><see cref="UpdatedDisplay"/> for an explicit display mode.</summary>
    public string UpdatedText(TimeDisplayMode mode) =>
        ViewerTimeHelper.FormatFleetWide(UpdatedUtc, "yyyy-MM-dd HH:mm", mode)
        + (string.IsNullOrEmpty(Summary.UpdatedBy) ? string.Empty : " by " + Summary.UpdatedBy);
}

/// <summary>A view read from an import file: what the web's Import panel would create.</summary>
public sealed record ImportedCustomView(string Name, string? Description, string DefinitionJson);

/// <summary>
/// The pure parts of the Custom Views manager (#6390 S5), apart from the window so they run without WPF: the list order, the import
/// file's shape (the web's own: <c>{name, description?, definition}</c>, or a bare definition with a top-level <c>name</c>) and the export text.
/// </summary>
public static class CustomViewsEditorModel
{
    public const string TopLevelLabel = "(top level)";

    /// <summary>A starting definition for a new dashboard that passes the validator as it stands.</summary>
    public const string NewViewTemplate = "{\n  \"panels\": [\n    { \"read\": \"get_wait_stats\", \"viz\": \"table\" }\n  ]\n}";

    /// <summary>The list in the web sidebar's order: top-level views first, then each folder in name order, views by name inside it.</summary>
    public static IReadOnlyList<CustomViewRow> Rows(IEnumerable<CustomViewSummary> views) =>
        views.Select(v => new CustomViewRow(v))
            .OrderBy(r => string.IsNullOrEmpty(r.Summary.Folder) ? 0 : 1)
            .ThenBy(r => r.Summary.Folder ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.Id)
            .ToList();

    /// <summary>The definition as the web sends it: parsed and written back compact. Text that is not JSON is returned as it is, so the
    /// validator gives its own refusal for it.</summary>
    public static string CompactDefinition(string definitionJson)
    {
        try
        {
            return JsonNode.Parse(definitionJson)?.ToJsonString() ?? definitionJson;
        }
        catch (JsonException)
        {
            return definitionJson;
        }
    }

    /// <summary>The definition indented for the editor; unchanged when it is not JSON.</summary>
    public static string IndentDefinition(string definitionJson)
    {
        try
        {
            return JsonNode.Parse(definitionJson)?.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) ?? definitionJson;
        }
        catch (JsonException)
        {
            return definitionJson;
        }
    }

    /// <summary>Reads an import file as the web's Import panel does (<c>wwwroot/js/pages/views.js</c>), with its sentences.</summary>
    public static (ImportedCustomView? View, string? Error) ParseImport(string? text)
    {
        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(text ?? string.Empty);
        }
        catch (JsonException ex)
        {
            return (null, "That is not valid JSON: " + ex.Message);
        }

        if (parsed is not JsonObject root)
        {
            return (null, "The JSON must contain a 'definition' (or be a definition with 'panels' or 'cells').");
        }

        var definition = root["definition"] is JsonObject wrapped
            ? wrapped
            : root.ContainsKey("panels") || root.ContainsKey("cells") ? root : null;
        if (definition is null)
        {
            return (null, "The JSON must contain a 'definition' (or be a definition with 'panels' or 'cells').");
        }

        var name = root["name"] is JsonValue nameValue && nameValue.TryGetValue<string>(out var n) ? n.Trim() : string.Empty;
        if (name.Length == 0)
        {
            return (null, "The imported JSON has no \"name\". Add a top-level \"name\" field and try again.");
        }

        var description = root["description"] is JsonValue descriptionValue && descriptionValue.TryGetValue<string>(out var d) && d.Length > 0 ? d : null;
        return (new ImportedCustomView(name, description, definition.ToJsonString()), null);
    }

    /// <summary>The export file's text: <c>{name, description, definition}</c>, indented, re-importable by this window and by the web.</summary>
    public static string ExportText(CustomView view)
    {
        var payload = new JsonObject
        {
            ["name"] = view.Name,
            ["description"] = view.Description,
            ["definition"] = JsonNode.Parse(view.DefinitionJson),
        };
        return payload.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>A file name for an export, the web's rule: anything outside letters, digits, dot, underscore and hyphen becomes an underscore.</summary>
    public static string SafeFileName(string? name)
    {
        var cleaned = System.Text.RegularExpressions.Regex
            .Replace(name ?? string.Empty, "[^A-Za-z0-9._-]+", "_", System.Text.RegularExpressions.RegexOptions.CultureInvariant)
            .Trim('_');
        return (cleaned.Length == 0 ? "view" : cleaned) + ".json";
    }
}
