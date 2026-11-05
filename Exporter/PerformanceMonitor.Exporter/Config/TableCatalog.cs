using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Exporter;

/// <summary>One row of `describe-tables`: what a table is, how it is windowed and what identifies a row.</summary>
public sealed record TableDescription(
    string Name,
    string Origin,
    string Kind,
    string? TimeColumn,
    IReadOnlyList<string>? KeyColumns,
    string Grain,
    IReadOnlyList<string>? ChangeKeyColumns);

/// <summary>Builds `describe-tables` from <see cref="CoreTableMetadata"/> (core) and the <see cref="OptionalTables"/> registry (optional).</summary>
public static class TableCatalog
{
    public static IReadOnlyList<TableDescription> Build()
    {
        var rows = new List<TableDescription>();
        foreach (var name in ExportOptions.AllTables.Concat(PlanFeatureTables.All))
        {
            var grain = HourlyTables.All.Contains(name) ? "hourly" : "event";
            rows.Add(new TableDescription(
                name, "core", CoreTableMetadata.Kind(name), CoreTableMetadata.TimeColumn(name),
                CoreTableMetadata.KeyColumns(name), grain, null));
        }

        foreach (var t in OptionalTables.All)
        {
            var snapshot = t.Kind == OptionalTableKind.Snapshot;
            var time = t.Grain == ExportGrain.Hourly ? "hour" : snapshot ? "snapshot_time" : t.TimeColumn;
            rows.Add(new TableDescription(
                t.Name, "optional", t.Kind.ToString().ToLowerInvariant(), time,
                t.KeyColumns.Count > 0 ? [.. t.KeyColumns] : null,
                t.Grain.ToString().ToLowerInvariant(),
                t.Change is { } change ? [.. change.Key] : null));
        }

        return rows;
    }

    public static string RenderJson(IReadOnlyList<TableDescription> rows)
    {
        var array = new JsonArray();
        foreach (var r in rows)
        {
            array.Add(new JsonObject
            {
                ["table"] = r.Name,
                ["origin"] = r.Origin,
                ["kind"] = r.Kind,
                ["time_column"] = r.TimeColumn,
                ["key_columns"] = r.KeyColumns is { } k ? new JsonArray([.. k.Select(x => (JsonNode?)JsonValue.Create(x))]) : null,
                ["grain"] = r.Grain,
                ["change_key_columns"] = r.ChangeKeyColumns is { } c ? new JsonArray([.. c.Select(x => (JsonNode?)JsonValue.Create(x))]) : null,
            });
        }

        return array.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    public static string RenderText(IReadOnlyList<TableDescription> rows)
    {
        var w = new System.Text.StringBuilder();
        w.AppendLine("table | origin | kind | time_column | grain | keys");
        foreach (var r in rows)
        {
            var keys = r.KeyColumns is { } k ? string.Join(",", k) : r.ChangeKeyColumns is { } c ? "change:" + string.Join(",", c) : "-";
            w.AppendLine($"{r.Name} | {r.Origin} | {r.Kind} | {r.TimeColumn ?? "-"} | {r.Grain} | {keys}");
        }

        return w.ToString();
    }
}
