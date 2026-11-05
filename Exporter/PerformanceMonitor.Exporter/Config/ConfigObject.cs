using System.Text.Json;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// A JSON object being read key by key. Tracks which keys were asked for, so <see cref="Finish"/> can reject any
/// key left over (a typo). A JSON null counts as an absent key. Every error names the full dotted key.
/// </summary>
internal sealed class ConfigObject
{
    private static readonly JsonElement EmptyObject = JsonDocument.Parse("{}").RootElement;

    private readonly JsonElement _element;
    private readonly string _path;
    private readonly HashSet<string> _used = new(StringComparer.Ordinal);

    public ConfigObject(JsonElement element, string path)
    {
        _element = element;
        _path = path;
    }

    public static ConfigObject Empty(string path) => new(EmptyObject, path);

    private string KeyPath(string key) => _path.Length == 0 ? key : $"{_path}.{key}";

    private JsonElement? Get(string key)
    {
        _used.Add(key);
        return _element.TryGetProperty(key, out var value) && value.ValueKind != JsonValueKind.Null ? value : null;
    }

    public bool Has(string key) => Get(key) is not null;

    public JsonElement? Raw(string key) => Get(key);

    public string? String(string key, string? defaultValue, bool required = false)
    {
        if (Get(key) is not { } v)
        {
            return required ? throw new ConfigException($"{KeyPath(key)}: required") : defaultValue;
        }

        if (v.ValueKind != JsonValueKind.String)
        {
            throw new ConfigException($"{KeyPath(key)}: must be a string");
        }

        var s = v.GetString()!;
        return s.Length == 0 ? throw new ConfigException($"{KeyPath(key)}: must not be empty") : s;
    }

    public int? Int(string key, int? defaultValue, int min, int max, bool required = false)
    {
        if (Get(key) is not { } v)
        {
            return required ? throw new ConfigException($"{KeyPath(key)}: required") : defaultValue;
        }

        if (v.ValueKind != JsonValueKind.Number || !v.TryGetInt32(out var n))
        {
            throw new ConfigException($"{KeyPath(key)}: must be an integer");
        }

        if (n < min || n > max)
        {
            var range = max == int.MaxValue ? $">= {min}" : $"from {min} to {max}";
            throw new ConfigException($"{KeyPath(key)}: must be {range}, got {n}");
        }

        return n;
    }

    public bool Bool(string key, bool defaultValue)
    {
        if (Get(key) is not { } v)
        {
            return defaultValue;
        }

        return v.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => throw new ConfigException($"{KeyPath(key)}: must be true or false"),
        };
    }

    public List<string>? StringList(string key)
    {
        if (Get(key) is not { } v)
        {
            return null;
        }

        if (v.ValueKind != JsonValueKind.Array)
        {
            throw new ConfigException($"{KeyPath(key)}: must be an array of strings");
        }

        var list = new List<string>();
        foreach (var item in v.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(item.GetString()))
            {
                throw new ConfigException($"{KeyPath(key)}: must be an array of non-empty strings");
            }

            list.Add(item.GetString()!);
        }

        return list;
    }

    public ConfigObject? Object(string key, bool required = false)
    {
        if (Get(key) is not { } v)
        {
            return required ? throw new ConfigException($"{KeyPath(key)}: required") : null;
        }

        return v.ValueKind == JsonValueKind.Object
            ? new ConfigObject(v, KeyPath(key))
            : throw new ConfigException($"{KeyPath(key)}: must be an object");
    }

    /// <summary>Rejects the first key nobody asked for.</summary>
    public void Finish()
    {
        foreach (var property in _element.EnumerateObject())
        {
            if (!_used.Contains(property.Name))
            {
                throw new ConfigException($"{KeyPath(property.Name)}: unknown key");
            }
        }
    }
}
