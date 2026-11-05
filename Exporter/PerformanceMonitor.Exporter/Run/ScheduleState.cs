using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// `export-schedule.json` (schema 2) as a JsonObject, so keys this code does not know (`cutover_utc`, `last_exported_day`,
/// `store_login_error`, ...) survive every rewrite (R15). The file is internal to the exporter, and other tools on
/// the same host may read it, so key names and value shapes stay stable.
/// </summary>
public sealed class ScheduleState
{
    public const int Schema = 2;
    private static readonly JsonSerializerOptions WriteOptions = new() { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public JsonObject Root { get; }

    public ScheduleState(JsonObject root) => Root = root;

    public static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    public static DateTime? ParseUtc(JsonNode? node)
    {
        if (node is not JsonValue v || !v.TryGetValue<string>(out var s) || string.IsNullOrWhiteSpace(s)) return null;
        return DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
    }

    /// <summary>The state to start from when no file exists (a cold start). `startUtc` seeds both families' last_to.</summary>
    public static ScheduleState NewCold(string store, bool hasQueryStore, DateTime? startUtc, DateTime realNowUtc)
    {
        JsonObject Family() => new()
        {
            ["last_to"] = startUtc is { } s ? Stamp(s) : null,
            ["last_success_utc"] = null, ["last_failure_utc"] = null, ["last_error"] = null,
        };
        var root = new JsonObject
        {
            ["schema"] = Schema,
            ["store"] = store,
            ["fresh"] = Family(),
        };
        if (hasQueryStore) root["query_store"] = Family();
        root["gaps"] = new JsonArray();
        root["last_attempt_utc"] = null;
        root["last_exit_code"] = null;
        root["last_run"] = null;
        root["exporter_version"] = null;
        // The instant of this cold start: a reader of this file never measures staleness from before it.
        root["cutover_utc"] = Stamp(realNowUtc);
        return new ScheduleState(root);
    }

    /// <summary>
    /// Reads the file. Null state with a null error means no file (a cold start). A non-null error is a refusal (exit 2):
    /// unreadable JSON, not an object, or not `"schema": 2`. A UTF-8 BOM is accepted.
    /// </summary>
    public static (ScheduleState? State, string? Error) Load(string path)
    {
        if (!File.Exists(path)) return (null, null);
        byte[] bytes;
        try { bytes = File.ReadAllBytes(path); }
        catch (IOException ex) { return (null, $"cannot read {path}: {ex.Message}"); }
        var start = bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF ? 3 : 0;
        JsonNode? node;
        try { node = JsonNode.Parse(Encoding.UTF8.GetString(bytes, start, bytes.Length - start)); }
        catch (JsonException ex) { return (null, $"{path} is not valid JSON ({ex.Message}); nothing exported"); }
        if (node is not JsonObject root) return (null, $"{path} is not a JSON object; nothing exported");
        if (root["schema"] is not JsonValue sv || !sv.TryGetValue<int>(out var schema) || schema != Schema)
            return (null, $"{path} is not \"schema\": 2 (the daily format is no longer supported); nothing exported");
        var state = new ScheduleState(root);
        try
        {
            foreach (var family in new[] { Planner.Fresh, Planner.FreshFast, Planner.QueryStore })
            {
                if (root[family] is JsonObject block) ParseUtc(block["last_to"]);
            }
        }
        catch (FormatException ex)
        {
            return (null, $"{path} has a last_to that is not a date ({ex.Message}); nothing exported");
        }
        return (state, null);
    }

    public bool Has(string key) => Root.ContainsKey(key) && Root[key] is not null;

    public DateTime? LastTo(string family) => Root[family] is JsonObject b ? ParseUtc(b["last_to"]) : null;

    /// <summary>The family's block, created (empty last_to) when it is missing or null.</summary>
    public JsonObject Block(string family)
    {
        if (Root[family] is JsonObject existing) return existing;
        var block = NewBlock(null);
        Root[family] = block;
        return block;
    }

    public static JsonObject NewBlock(DateTime? lastTo) => new()
    {
        ["last_to"] = lastTo is { } t ? Stamp(t) : null,
        ["last_success_utc"] = null, ["last_failure_utc"] = null, ["last_error"] = null,
    };

    public JsonArray Array(string key)
    {
        if (Root[key] is JsonArray a) return a;
        var created = new JsonArray();
        Root[key] = created;
        return created;
    }

    public void Remove(string key) => Root.Remove(key);

    /// <summary>Atomic: a temp file in the same folder, then a move over the real path. The file is always the old or the new version.</summary>
    public void Save(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var tmp = path + ".tmp-" + Guid.NewGuid().ToString("N");
        try
        {
            File.WriteAllText(tmp, Root.ToJsonString(WriteOptions), new UTF8Encoding(false));
            File.Move(tmp, path, overwrite: true);
        }
        catch
        {
            try { File.Delete(tmp); } catch (IOException) { }
            throw;
        }
    }
}
