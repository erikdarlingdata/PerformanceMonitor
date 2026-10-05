/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The parsed command line of <c>--diagnostics-bundle</c>.</summary>
internal sealed record DiagnosticsBundleOptions(
    string OutputPath, int Hours, string? ServerName, string? LogDirectory, string? ConfigPath, bool Force, string? AliasMapPath);

/// <summary>One section as read, before aliasing.</summary>
internal sealed record BundleSection(string Name, JsonNode? Node, bool Failed);

/// <summary>
/// The diagnostics bundle (#5097): one aliased JSON file a user can attach to a bug report. Each section is read through
/// the MCP reader that serves the same data, so the bundle shows what the tools show. Every name the verb can learn
/// (the registry, the config, the store connection, the local machine, and any identifier-keyed field in a section) is
/// replaced by an ordinal alias, secrets are redacted, and <see cref="BundleAliaser.Finish"/> proves it before one byte
/// is written: a leak means exit 5 and no file.
/// </summary>
internal static class DiagnosticsBundle
{
    /// <summary>The format version in the manifest.</summary>
    internal const int FormatVersion = 1;

    internal const int DefaultHours = 24;
    internal const int MaxHours = 168;
    internal const int TotalCapBytes = 4 * 1024 * 1024;
    internal const int DefaultSectionBudgetBytes = 128 * 1024;
    internal const int HealthFanOutBudgetBytes = 768 * 1024;
    internal const int SectionTimeoutSeconds = 60;

    /// <summary>The line the alias-map file starts with.</summary>
    internal const string AliasMapWarning = "DO NOT ATTACH: this file maps the aliases in a diagnostics bundle back to real names.";

    internal static readonly JsonSerializerOptions WriteOptions = new() { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>The byte budget of each named section; any other section gets <see cref="DefaultSectionBudgetBytes"/>.</summary>
    internal static readonly IReadOnlyDictionary<string, int> SectionBudgets = new Dictionary<string, int>(StringComparer.Ordinal)
    {
        ["collection"] = 1024 * 1024,
        ["slow_reads"] = 1024 * 1024,
        ["service_log"] = 768 * 1024,
        ["store_statements"] = 512 * 1024,
        ["store_log"] = 256 * 1024,
    };

    /// <summary>
    /// The DarlingConfig properties the <c>config_shape</c> section reads, as <c>Type.Property</c>. A property in
    /// neither this list nor <see cref="ConfigShapeExcluded"/> fails a reflection test, so a new setting is classified
    /// before it can reach a bundle.
    /// </summary>
    internal static readonly string[] ConfigShapeProjected =
    {
        "DarlingConfig.Postgres", "DarlingConfig.Servers", "DarlingConfig.Analyzer", "DarlingConfig.CapturePlans",
        "DarlingConfig.Mcp", "DarlingConfig.Web", "PostgresConfig.Managed", "PostgresConfig.ConnectionString",
        "PostgresConfig.Network", "McpConfig.Enabled", "McpConfig.Network", "WebConfig.Enabled", "WebConfig.Network",
        "MonitoredServer.Engine", "MonitoredServer.AlertDeliveryModeOverride",
    };

    /// <summary>The properties deliberately not projected: tuning numbers, free text, hosts, names and secrets.</summary>
    internal static readonly string[] ConfigShapeExcluded =
    {
        "DarlingConfig.QueryStoreBackfillEnabled",
        "DarlingConfig.QueryStoreTextBudgetMb",
        "DarlingConfig.PlanContentRetentionDays",
        "DarlingConfig.ComposeStatementTimeoutSeconds",
        "DarlingConfig.PlanXmlCompression",
        "DarlingConfig.MaxConcurrentSweeps",
        "DarlingConfig.CollectSchemaChangeEvents",
        "DarlingConfig.RawChunkIntervalReconcileEnabled",
        "DarlingConfig.ProcedureStatsPlanCycleInterval",
        "DarlingConfig.Alerts",
        "DarlingConfig.Smtp",
        "DarlingConfig.Webhooks",
        "DarlingConfig.NotificationRoutes",
        "DarlingConfig.Analysis",
        "DarlingConfig.ForcePlanBot",
        "DarlingConfig.Peers",
        "PostgresConfig.WebConnectionString",
        "PostgresConfig.McpConnectionString",
        "PostgresConfig.Port",
        "PostgresConfig.DataDirectory",
        "PostgresConfig.ConnectAs",
        "McpConfig.Port",
        "WebConfig.Port",
        "WebConfig.PublicBaseUrl",
        "MonitoredServer.Name",
        "MonitoredServer.Host",
        "MonitoredServer.Port",
        "MonitoredServer.Database",
        "MonitoredServer.Auth",
        "MonitoredServer.Username",
        "MonitoredServer.EncryptedPassword",
        "MonitoredServer.Password",
        "MonitoredServer.ReadOnlyIntent",
        "MonitoredServer.TrustServerCertificate",
        "MonitoredServer.EncryptMode",
        "MonitoredServer.MultiSubnetFailover",
        "MonitoredServer.ExcludedDatabases",
        "MonitoredServer.MonthlyCostUsd",
        "MonitoredServer.StoredServerId",
        "MonitoredServer.PlanForceBotEnabled",
        "MonitoredServer.RemediationUsername",
        "MonitoredServer.RemediationEncryptedPassword",
    };

    internal static JsonObject BuildConfigShape(DarlingConfig config)
    {
        var byEngine = new JsonObject();
        foreach (var group in config.Servers.GroupBy(s => s.Engine ?? "sqlserver", StringComparer.OrdinalIgnoreCase).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            byEngine[group.Key.ToLowerInvariant()] = group.Count();
        }

        var modes = new JsonArray();
        foreach (var mode in config.Servers.Select(s => s.AlertDeliveryModeOverride?.ToString()).Where(m => m is not null).Distinct(StringComparer.Ordinal).OrderBy(m => m, StringComparer.Ordinal))
        {
            modes.Add(mode);
        }

        return new JsonObject
        {
            ["postgres_managed"] = config.Postgres?.Managed ?? false,
            ["postgres_uses_connection_string"] = !string.IsNullOrWhiteSpace(config.Postgres?.ConnectionString),
            ["postgres_network_exposed"] = config.Postgres?.Network is not null,
            ["server_count"] = config.Servers.Count,
            ["servers_by_engine"] = byEngine,
            ["mcp_enabled"] = config.Mcp?.Enabled ?? false,
            ["mcp_network_exposed"] = config.Mcp?.Network is not null,
            ["web_enabled"] = config.Web?.Enabled ?? false,
            ["web_network_exposed"] = config.Web?.Network is not null,
            ["analyzer_present"] = config.Analyzer is not null,
            ["capture_plans"] = config.CapturePlans,
            ["alert_delivery_override_modes"] = modes,
        };
    }

    /// <summary>Parses the arguments after the verb. Returns the options, or the error sentence.</summary>
    internal static (DiagnosticsBundleOptions? Options, string? Error) ParseArgs(string[] args)
    {
        string? path = null, server = null, logDir = null, config = null, aliasMap = null;
        var hours = DefaultHours;
        var force = false;
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            string? Next() => i + 1 < args.Length ? args[++i] : null;
            if (string.Equals(arg, "--force", StringComparison.OrdinalIgnoreCase))
            {
                force = true;
            }
            else if (string.Equals(arg, "--hours", StringComparison.OrdinalIgnoreCase))
            {
                var value = Next();
                if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out hours) || hours < 1 || hours > MaxHours)
                {
                    return (null, $"--hours needs a whole number from 1 to {MaxHours} (got '{value}').");
                }
            }
            else if (string.Equals(arg, "--server", StringComparison.OrdinalIgnoreCase))
            {
                server = Next();
                if (string.IsNullOrWhiteSpace(server))
                {
                    return (null, "--server needs a server name.");
                }
            }
            else if (string.Equals(arg, "--log-dir", StringComparison.OrdinalIgnoreCase))
            {
                logDir = Next();
                if (string.IsNullOrWhiteSpace(logDir))
                {
                    return (null, "--log-dir needs a directory.");
                }
            }
            else if (string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase))
            {
                config = Next();
                if (string.IsNullOrWhiteSpace(config))
                {
                    return (null, "--config needs a path.");
                }
            }
            else if (string.Equals(arg, "--alias-map", StringComparison.OrdinalIgnoreCase))
            {
                aliasMap = Next();
                if (string.IsNullOrWhiteSpace(aliasMap))
                {
                    return (null, "--alias-map needs a path.");
                }
            }
            else if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                return (null, $"Unknown option '{arg}'.");
            }
            else if (path is null)
            {
                path = arg;
            }
            else
            {
                return (null, $"Only one output path is allowed (got a second: '{arg}').");
            }
        }

        if (string.IsNullOrWhiteSpace(path))
        {
            return (null, "An output path is required.");
        }

        if (string.IsNullOrEmpty(Path.GetExtension(path)))
        {
            path += ".json";
        }

        return (new DiagnosticsBundleOptions(path, hours, server, logDir, config, force, aliasMap), null);
    }

    /// <summary>Checks the output location before any work: an existing file without --force, or a missing or unwritable directory, is exit 4.</summary>
    internal static string? CheckOutput(DiagnosticsBundleOptions options)
    {
        if (File.Exists(options.OutputPath) && !options.Force)
        {
            return "The output file already exists; pass --force to replace it.";
        }

        var directory = Path.GetDirectoryName(Path.GetFullPath(options.OutputPath));
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory))
        {
            return "The output directory does not exist.";
        }

        try
        {
            var probe = Path.Combine(directory, ".diag-bundle-probe-" + Guid.NewGuid().ToString("N"));
            File.WriteAllText(probe, string.Empty);
            File.Delete(probe);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return "The output directory is not writable (" + ex.GetType().Name + ").";
        }

        return null;
    }

    /// <summary>
    /// Seeds the name set from darling.json and the local machine: every configured server's name, host and database,
    /// the store connection string's hosts, database and login, the machine and domain names, and the secrets (every
    /// string in the config under a secret-named key, and the store password).
    /// </summary>
    internal static void SeedFromConfig(BundleAliaser aliaser, DarlingConfig config, string? storeConnectionString)
    {
        foreach (var server in config.Servers)
        {
            aliaser.AddName(AliasKind.Server, server.Name);
            aliaser.AddName(AliasKind.Host, server.Host);
            aliaser.AddName(AliasKind.Database, server.Database);
            aliaser.AddName(AliasKind.Login, server.Username);
            aliaser.AddName(AliasKind.Login, server.RemediationUsername);
            foreach (var excluded in server.ExcludedDatabases ?? new List<string>())
            {
                aliaser.AddName(AliasKind.Database, excluded);
            }
        }

        try
        {
            if (JsonSerializer.SerializeToNode(config) is { } tree)
            {
                AddSecretsFrom(aliaser, tree, secretKey: false);
            }
        }
        catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException or JsonException)
        {
            /* A config shape the serializer cannot walk adds no secrets here; the connection-string password below
               and the guard on every string still apply. */
        }

        foreach (var cs in new[] { storeConnectionString, config.Postgres?.ConnectionString, config.Postgres?.WebConnectionString, config.Postgres?.McpConnectionString })
        {
            SeedFromConnectionString(aliaser, cs);
        }

        AddLocalIdentity(aliaser);
    }

    /// <summary>Adds a connection string's hosts, database and login as names and its password as a secret.</summary>
    internal static void SeedFromConnectionString(BundleAliaser aliaser, string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            aliaser.AddNameList(AliasKind.Host, builder.Host);
            aliaser.AddName(AliasKind.Database, builder.Database);
            aliaser.AddName(AliasKind.Login, builder.Username);
            aliaser.AddSecret(builder.Password);
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or KeyNotFoundException)
        {
            /* An unparseable string adds nothing; the text guard still redacts its password shape wherever it appears. */
        }
    }

    private static void AddSecretsFrom(BundleAliaser aliaser, JsonNode? node, bool secretKey)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    AddSecretsFrom(aliaser, value, SecretTextGuard.IsSecretKey(key));
                }

                break;
            case JsonArray array:
                foreach (var item in array)
                {
                    AddSecretsFrom(aliaser, item, secretKey);
                }

                break;
            case JsonValue value when secretKey && value.TryGetValue<string>(out var text) && text.Length >= SecretMinimumLength:
                aliaser.AddSecret(text);
                break;
        }
    }

    /// <summary>Secrets shorter than this are not tracked: they would match inside ordinary words.</summary>
    internal const int SecretMinimumLength = 4;

    private static readonly string[] s_sharedAccounts = { "system", "administrator", "root", "localsystem", "service", "networkservice", "admin" };

    private static void AddLocalIdentity(BundleAliaser aliaser)
    {
        aliaser.AddName(AliasKind.Host, Environment.MachineName);
        try
        {
            aliaser.AddName(AliasKind.Host, System.Net.Dns.GetHostName());
        }
        catch (System.Net.Sockets.SocketException)
        {
            /* No resolver on this host; the machine name above is the name that matters. */
        }

        try
        {
            var domain = IPGlobalProperties.GetIPGlobalProperties().DomainName;
            if (!string.IsNullOrWhiteSpace(domain))
            {
                aliaser.AddName(AliasKind.Domain, domain);
            }
        }
        catch (Exception ex) when (ex is NetworkInformationException or PlatformNotSupportedException or InvalidOperationException)
        {
            /* The platform has no domain to report. */
        }

        var user = Environment.UserName;
        if (!string.IsNullOrWhiteSpace(user) && !s_sharedAccounts.Contains(user, StringComparer.OrdinalIgnoreCase))
        {
            aliaser.AddName(AliasKind.Login, user);
        }
    }

    /// <summary>Runs one reader under its own timeout and try/catch; a failure becomes an <c>error_class</c>, never an exception.</summary>
    internal static async Task<BundleSection> RunSectionAsync(string name, Func<CancellationToken, Task<JsonNode?>> read, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(SectionTimeoutSeconds));
        try
        {
            var node = await read(timeout.Token);
            return new BundleSection(name, node, StatusOf(node) == "error");
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return new BundleSection(name, new JsonObject { ["status"] = "error", ["error_class"] = SlowReadLog.ErrorClassOf(ex) }, true);
        }
    }

    /// <summary>A reader's JSON answer as a tree; text that is not JSON is kept as a string so nothing is lost.</summary>
    internal static JsonNode? ParseReader(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return new JsonObject { ["status"] = "empty" };
        }

        try
        {
            return JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return JsonValue.Create(json);
        }
    }

    internal static string StatusOf(JsonNode? node) =>
        node is JsonObject obj && obj["status"] is JsonValue v && v.TryGetValue<string>(out var status) ? status : "ok";

    /// <summary>
    /// Cuts a section to its budget by dropping rows from the end of its largest array, then marks it
    /// <c>truncated</c>. A section with nothing left to drop is replaced by a status.
    /// </summary>
    internal static JsonNode? FitToBudget(JsonNode? node, int budgetBytes, out bool truncated)
    {
        truncated = false;
        if (node is null || Size(node) <= budgetBytes)
        {
            return node;
        }

        truncated = true;
        for (var guard = 0; guard < 400 && Size(node) > budgetBytes; guard++)
        {
            var largest = LargestArray(node);
            if (largest is null || largest.Count == 0)
            {
                return new JsonObject { ["status"] = "truncated", ["reason"] = "The section exceeded its size budget and had no rows to drop." };
            }

            var drop = Math.Max(1, largest.Count / 8);
            for (var i = 0; i < drop && largest.Count > 0; i++)
            {
                largest.RemoveAt(largest.Count - 1);
            }
        }

        if (node is JsonObject obj)
        {
            obj["truncated"] = true;
        }

        return node;
    }

    private static int Size(JsonNode node) => Encoding.UTF8.GetByteCount(node.ToJsonString(WriteOptions));

    private static JsonArray? LargestArray(JsonNode? node)
    {
        JsonArray? best = null;
        var bestSize = -1;

        void Walk(JsonNode? n)
        {
            switch (n)
            {
                case JsonArray a:
                {
                    var size = a.Count == 0 ? 0 : Size(a);
                    if (a.Count > 0 && size > bestSize)
                    {
                        best = a;
                        bestSize = size;
                    }

                    foreach (var item in a)
                    {
                        Walk(item);
                    }

                    break;
                }

                case JsonObject o:
                    foreach (var (_, value) in o)
                    {
                        Walk(value);
                    }

                    break;
            }
        }

        Walk(node);
        return best;
    }

    /// <summary>
    /// Shapes the sections into the bundle: harvests names from every section first (so the name set is complete
    /// before any alias is written), aliases every tree, fits each to its budget, holds the whole to the total cap,
    /// writes the manifest, and runs the verifier. Returns the final text, or the leaks that blocked it.
    /// </summary>
    internal static (string? Text, IReadOnlyList<BundleLeak> Leaks, bool OverCap) Assemble(
        IReadOnlyList<BundleSection> sections, BundleAliaser aliaser, JsonObject manifest)
    {
        foreach (var section in sections)
        {
            aliaser.Harvest(section.Node);
        }

        var aliased = new List<(string Name, JsonNode? Node, bool Truncated)>();
        foreach (var section in sections)
        {
            var shaped = aliaser.AliasTree(section.Node);
            var budget = SectionBudgets.TryGetValue(section.Name, out var b) ? b : DefaultSectionBudgetBytes;
            shaped = FitToBudget(shaped, budget, out var truncated);
            aliased.Add((section.Name, shaped, truncated));
        }

        JsonObject Build()
        {
            var sectionsNode = new JsonObject();
            var listing = new JsonArray();
            foreach (var (name, node, truncated) in aliased)
            {
                sectionsNode[name] = node?.DeepClone();
                listing.Add(new JsonObject
                {
                    ["name"] = name,
                    ["bytes"] = node is null ? 0 : Size(node),
                    ["truncated"] = truncated,
                    ["status"] = StatusOf(node),
                });
            }

            var m = (JsonObject)manifest.DeepClone();
            m["sections"] = listing;
            var counts = new JsonObject();
            foreach (var (kind, count) in aliaser.Counts.OrderBy(p => p.Key))
            {
                counts[kind.ToString().ToLowerInvariant() + "s"] = count;
            }

            m["alias_counts"] = counts;
            return new JsonObject { ["manifest"] = aliaser.AliasTree(m), ["sections"] = sectionsNode };
        }

        var root = Build();
        for (var guard = 0; guard < 40 && Size(root) > TotalCapBytes; guard++)
        {
            var index = aliased.Select((s, i) => (s, i)).OrderByDescending(x => x.s.Node is null ? 0 : Size(x.s.Node)).First().i;
            var current = aliased[index];
            var target = Math.Max(1024, (current.Node is null ? 0 : Size(current.Node)) * 3 / 4);
            aliased[index] = (current.Name, FitToBudget(current.Node, target, out _), true);
            root = Build();
        }

        if (Size(root) > TotalCapBytes)
        {
            return (null, Array.Empty<BundleLeak>(), true);
        }

        var (leaks, text) = aliaser.Finish(root, WriteOptions);
        return (text, leaks, false);
    }

    /// <summary>The manifest fields that do not depend on a section.</summary>
    internal static JsonObject BuildManifest(int hours, string scope, int exitCode)
    {
        var assembly = typeof(DarlingCliCommands).Assembly;
        var informational = assembly.GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .OfType<System.Reflection.AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion;
        return new JsonObject
        {
            ["bundle_format"] = FormatVersion,
            ["generated_at_utc"] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            ["product_version"] = DarlingCliCommands.ProductVersion(),
            ["informational_version"] = informational,
            ["os"] = new JsonObject
            {
                ["description"] = RuntimeInformation.OSDescription,
                ["architecture"] = RuntimeInformation.OSArchitecture.ToString(),
                ["framework"] = RuntimeInformation.FrameworkDescription,
                ["containerized"] = string.Equals(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase),
            },
            ["local_utc_offset"] = TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).ToString("c", CultureInfo.InvariantCulture),
            ["hours"] = hours,
            ["scope"] = scope,
            ["exit_code"] = exitCode,
        };
    }

    /// <summary>Writes the text to a temp file beside the target, then moves it over. Nothing is left behind on failure.</summary>
    internal static string? WriteAtomically(string path, string text, bool force)
    {
        var temp = path + ".tmp";
        try
        {
            File.WriteAllText(temp, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            File.Move(temp, path, overwrite: force);
            return null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            TryDelete(temp);
            return "Could not write the bundle (" + ex.GetType().Name + ").";
        }
    }

    internal static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A temp file we cannot delete is no worse than the failure that brought us here. */
        }
    }
}
