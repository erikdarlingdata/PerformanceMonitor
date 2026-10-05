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
    string OutputPath, int Hours, string? ServerName, string? LogDirectory, string? ConfigPath, bool Force, string? AliasMapPath,
    bool IncludeLogText = false);

/// <summary>One section as read, before aliasing.</summary>
internal sealed record BundleSection(string Name, JsonNode? Node, bool Failed, Func<JsonNode?, JsonNode?>? AfterAlias = null);

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
        /* Keyed by the parsed engine, never by the free text of the config: an unrecognized Engine string is
           user-typed text, and a key is never aliased. */
        foreach (var group in config.Servers.GroupBy(s => s.TargetEngine).OrderBy(g => g.Key.ToString(), StringComparer.Ordinal))
        {
            byEngine[group.Key.ToString().ToLowerInvariant()] = group.Count();
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
        var includeLogText = false;
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            string? Next() => i + 1 < args.Length ? args[++i] : null;
            if (string.Equals(arg, "--force", StringComparison.OrdinalIgnoreCase))
            {
                force = true;
            }
            else if (string.Equals(arg, "--include-log-text", StringComparison.OrdinalIgnoreCase))
            {
                includeLogText = true;
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
            /* "out." has no extension either; appending would give "out..json". */
            path = path.TrimEnd('.') + ".json";
        }

        return (new DiagnosticsBundleOptions(path, hours, server, logDir, config, force, aliasMap, includeLogText), null);
    }

    /// <summary>
    /// Checks the alias-map location before any work: the map must never share a path with the bundle (the map would
    /// replace the attachable file with the real-name map), and an existing map file needs <c>--force</c>. Returns the
    /// refusal sentence, or null.
    /// </summary>
    internal static string? CheckAliasMap(DiagnosticsBundleOptions options)
    {
        if (options.AliasMapPath is null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(options.AliasMapPath))
        {
            return "--alias-map needs a path.";
        }

        var comparison = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        string map, bundle;
        try
        {
            map = ResolveLinks(options.AliasMapPath);
            bundle = ResolveLinks(options.OutputPath);
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return "--alias-map is not a usable path (" + ex.GetType().Name + ").";
        }

        if (string.Equals(map, bundle, comparison))
        {
            return "--alias-map must be a different file from the bundle: the map holds the real names.";
        }

        if (File.Exists(options.AliasMapPath) && !options.Force)
        {
            return "The alias map file already exists; pass --force to replace it.";
        }

        return null;
    }

    /// <summary>
    /// The full path of <paramref name="path"/> with every symbolic link on it followed: each existing component is
    /// resolved in turn, so a link to a file that does not exist yet still compares equal to that file's own path, and
    /// a path inside a linked directory compares by the directory's real location plus the file name.
    /// </summary>
    internal static string ResolveLinks(string path, int depth = 0)
    {
        var full = Path.GetFullPath(path);
        if (depth > 16)
        {
            return full;
        }

        var root = Path.GetPathRoot(full) ?? string.Empty;
        var current = root;
        foreach (var part in full[root.Length..].Split(new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, part);
            try
            {
                FileSystemInfo info = Directory.Exists(current) ? new DirectoryInfo(current) : new FileInfo(current);
                var target = info.ResolveLinkTarget(returnFinalTarget: true);
                if (target is not null)
                {
                    current = ResolveLinks(target.FullName, depth + 1);
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* A component that cannot be inspected is compared as written. */
            }
        }

        return current;
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
    internal static IReadOnlyList<string> SeedFromConfig(BundleAliaser aliaser, DarlingConfig config, string? storeConnectionString)
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

        SeedNotificationIdentifiers(aliaser, config);
        return AddLocalIdentity(aliaser);
    }

    /// <summary>
    /// The other places a config names a host: the SMTP host, the domains of the From and To addresses, the dashboard's
    /// public base URL, and every webhook URL and proxy. A webhook URL carries its secret in the path, so each is also
    /// a secret.
    /// </summary>
    internal static void SeedNotificationIdentifiers(BundleAliaser aliaser, DarlingConfig config)
    {
        if (config.Smtp is { } smtp)
        {
            aliaser.AddName(AliasKind.Host, smtp.Host);
            AddAddressDomains(aliaser, smtp.From);
            AddAddressDomains(aliaser, smtp.To);

            /* The SMTP login is a name (and often an address): its domain half too. */
            aliaser.AddName(AliasKind.Login, smtp.Username);
            AddAddressDomains(aliaser, smtp.Username);
        }

        /* A route's destinations: each webhook URL is a host and a secret, the routing key is a secret, and the
           recipient list names mail domains. */
        foreach (var route in config.NotificationRoutes ?? Array.Empty<PerformanceMonitor.Notifications.NotificationRoute>())
        {
            foreach (var url in new[] { route.TeamsUrl, route.SlackUrl, route.GenericUrl })
            {
                AddUrlHost(aliaser, url);
                aliaser.AddSecret(url);
            }

            aliaser.AddSecret(route.PagerDutyRoutingKey);
            AddAddressDomains(aliaser, route.SmtpRecipients);
        }

        AddUrlHost(aliaser, config.Web?.PublicBaseUrl);
        if (config.Webhooks is { } hooks)
        {
            foreach (var url in new[] { hooks.TeamsUrl, hooks.SlackUrl, hooks.GenericUrl })
            {
                AddUrlHost(aliaser, url);
                aliaser.AddSecret(url);
            }

            foreach (var proxy in new[] { hooks.TeamsProxy, hooks.SlackProxy, hooks.GenericProxy })
            {
                AddUrlHost(aliaser, proxy);
            }
        }
    }

    /// <summary>Registers the domain of every address in a comma- or semicolon-separated list.</summary>
    private static void AddAddressDomains(BundleAliaser aliaser, string? list)
    {
        foreach (var address in (list ?? string.Empty).Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var at = address.LastIndexOf('@');
            if (at >= 0 && at < address.Length - 1)
            {
                aliaser.AddDomain(address[(at + 1)..]);
            }
        }
    }

    private static void AddUrlHost(BundleAliaser aliaser, string? url)
    {
        if (!string.IsNullOrWhiteSpace(url) && Uri.TryCreate(url.Trim(), UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host))
        {
            aliaser.AddName(AliasKind.Host, uri.Host);
        }
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
            case JsonValue value when secretKey && value.TryGetValue<string>(out var text):
                /* No length floor: a short secret is replaced on word boundaries (see BundleAliaser.AddSecret), and only
                   one of four or more characters is also substring-verified. */
                aliaser.AddSecret(text);
                break;
        }
    }

    private static readonly string[] s_sharedAccounts = { "system", "administrator", "root", "localsystem", "service", "networkservice", "admin" };

    /// <summary>
    /// Splits a service logon account (<c>CORP\svc_darling</c>, <c>.\svc</c>, <c>svc@corp.example.test</c>) into its
    /// domain and login. The built-in accounts (<c>LocalSystem</c>, <c>NT AUTHORITY\...</c>, <c>NT SERVICE\...</c>) name
    /// nothing and return null parts.
    /// </summary>
    internal static (string? Domain, string? Login) ParseServiceAccount(string? objectName)
    {
        var account = objectName?.Trim();
        if (string.IsNullOrEmpty(account) || string.Equals(account, "LocalSystem", StringComparison.OrdinalIgnoreCase))
        {
            return (null, null);
        }

        var slash = account.IndexOf('\\', StringComparison.Ordinal);
        if (slash > 0)
        {
            var domain = account[..slash];
            var login = account[(slash + 1)..];
            if (domain is "." or "NT AUTHORITY" or "NT SERVICE" || string.Equals(domain, "NT AUTHORITY", StringComparison.OrdinalIgnoreCase)
                || string.Equals(domain, "NT SERVICE", StringComparison.OrdinalIgnoreCase))
            {
                return (null, domain == "." ? login : null);
            }

            return (domain, login);
        }

        var at = account.IndexOf('@', StringComparison.Ordinal);
        return at > 0 ? (account[(at + 1)..], account[..at]) : (null, account);
    }

    /// <summary>
    /// What the service registration said about the logon account: the parsed parts, and a manifest note for each path
    /// that names nothing (the key is missing, the value is not text, or the account is a built-in one).
    /// </summary>
    internal static (string? Domain, string? Login, string? Note) ClassifyServiceAccount(bool keyFound, object? value)
    {
        if (!keyFound)
        {
            return (null, null, "The service's logon account was not read: the service registration was not found (the service may not be installed under its usual name).");
        }

        if (value is not string text)
        {
            return (null, null, "The service's logon account was not read: the registration holds no text value for it.");
        }

        var (domain, login) = ParseServiceAccount(text);
        if (domain is null && login is null)
        {
            return (null, null, "The service runs as a built-in account, which names nothing; no service account was added to the name set.");
        }

        return (domain, login, null);
    }

    private static (bool KeyFound, object? Value) ReadServiceObjectName()
    {
        if (!OperatingSystem.IsWindows())
        {
            return (false, null);
        }

        using var key = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(
            @"SYSTEM\CurrentControlSet\Services\" + DarlingCliCommands.ServiceName);
        return key is null ? (false, null) : (true, key.GetValue("ObjectName"));
    }

    private static List<string> AddLocalIdentity(BundleAliaser aliaser)
    {
        var notes = new List<string>();
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
            var machineDomain = IPGlobalProperties.GetIPGlobalProperties().DomainName;
            if (!string.IsNullOrWhiteSpace(machineDomain))
            {
                aliaser.AddName(AliasKind.Domain, machineDomain);
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

        var userDomain = Environment.UserDomainName;
        if (!string.IsNullOrWhiteSpace(userDomain) && !s_sharedAccounts.Contains(userDomain, StringComparer.OrdinalIgnoreCase))
        {
            aliaser.AddName(AliasKind.Domain, userDomain);
        }

        if (!OperatingSystem.IsWindows())
        {
            notes.Add("The service's logon account was not read: this host is not Windows. A service account named only in log text is aliased by its quoted DOMAIN\\login form.");
            return notes;
        }

        try
        {
            var (found, value) = ReadServiceObjectName();
            var (domain, login, note) = ClassifyServiceAccount(found, value);
            aliaser.AddName(AliasKind.Domain, domain);
            aliaser.AddName(AliasKind.Login, login);
            if (note is not null)
            {
                notes.Add(note);
            }
        }
        catch (Exception ex) when (ex is System.Security.SecurityException or UnauthorizedAccessException or IOException or InvalidOperationException)
        {
            notes.Add("The service's logon account could not be read from the service registration (" + ex.GetType().Name + ").");
        }

        return notes;
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

    /// <summary>The reason codes an unreachable-store section may carry. Nothing else ever reaches the file.</summary>
    internal static readonly string[] StoreReasonCodes =
    {
        "missing_store_credential", "first_run", "bootstrap_stopped", "connect_failed", "auth_failed", "timeout", "other",
    };

    /// <summary>The caller's reason when it is one of <see cref="StoreReasonCodes"/>, else the code the error classifies to.</summary>
    internal static string StoreReasonCode(string? requested, Exception? error) =>
        requested is not null && StoreReasonCodes.Contains(requested, StringComparer.Ordinal) ? requested : ClassifyStoreError(error);

    /// <summary>The fixed reason code when the config gives no store connection string at all.</summary>
    internal static string MissingConnectionReason(PostgresConfig postgres)
    {
        if (!postgres.Managed || !OperatingSystem.IsWindows())
        {
            return "other";
        }

        try
        {
            var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(postgres);
            var evidence = DarlingStoreBootstrapEvidence.FindBootstrapEvidence(dataDirectory);
            return evidence is null ? "first_run" : "bootstrap_stopped";
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or InvalidOperationException or IOException or UnauthorizedAccessException)
        {
            return "missing_store_credential";
        }
    }

    /// <summary>
    /// The fixed reason code for an unreachable store: <c>timeout</c>, <c>auth_failed</c> (a SQLSTATE of class 28),
    /// <c>connect_failed</c> (a socket error or any other driver error before a session), or <c>other</c>.
    /// </summary>
    internal static string ClassifyStoreError(Exception? error)
    {
        for (var ex = error; ex is not null; ex = ex.InnerException)
        {
            if (ex is TimeoutException or OperationCanceledException)
            {
                return "timeout";
            }

            if (ex is PostgresException pg)
            {
                return pg.SqlState.StartsWith("28", StringComparison.Ordinal) ? "auth_failed" : "other";
            }

            if (ex is System.Net.Sockets.SocketException)
            {
                return "connect_failed";
            }
        }

        return error is NpgsqlException ? "connect_failed" : "other";
    }

    /// <summary>The SQLSTATE of a PostgresException anywhere on the chain, or null.</summary>
    internal static string? SqlStateOf(Exception? error)
    {
        for (var ex = error; ex is not null; ex = ex.InnerException)
        {
            if (ex is PostgresException pg && !string.IsNullOrEmpty(pg.SqlState))
            {
                return pg.SqlState;
            }
        }

        return null;
    }

    /// <summary>The name of the <see cref="System.Net.Sockets.SocketError"/> of a SocketException anywhere on the chain, or null.</summary>
    internal static string? SocketErrorOf(Exception? error)
    {
        for (var ex = error; ex is not null; ex = ex.InnerException)
        {
            if (ex is System.Net.Sockets.SocketException socket)
            {
                return socket.SocketErrorCode.ToString();
            }
        }

        return null;
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
        aliaser.NoteProductValues(sections.Select(s => s.Name));
        foreach (var section in sections)
        {
            aliaser.NoteKeys(section.Node);
        }

        foreach (var section in sections)
        {
            aliaser.Harvest(section.Node);
        }

        var aliased = new List<(string Name, JsonNode? Node, bool Truncated)>();
        foreach (var section in sections)
        {
            var shaped = aliaser.AliasTree(section.Node);

            /* Text is cut only AFTER it is aliased: a name cut to a prefix by an earlier truncation would match no token. */
            if (section.AfterAlias is not null)
            {
                shaped = section.AfterAlias(shaped);
            }

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

    /// <summary>
    /// Writes the text to a temp file beside the target, then moves it over. The temp name is random and the file is
    /// created with <see cref="FileMode.CreateNew"/>, so a path someone planted in a shared directory is never written
    /// through. The temp file is deleted on every path that does not end in the move.
    /// </summary>
    internal static string? WriteAtomically(string path, string text, bool force)
    {
        string temp;
        try
        {
            var full = Path.GetFullPath(path);
            temp = Path.Combine(Path.GetDirectoryName(full)!, "." + Path.GetFileName(full) + "." + Guid.NewGuid().ToString("N") + ".tmp");
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return "Could not write the bundle (" + ex.GetType().Name + ").";
        }

        var moved = false;
        try
        {
            using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                var bytes = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false).GetBytes(text);
                stream.Write(bytes, 0, bytes.Length);
            }

            File.Move(temp, path, overwrite: force);
            moved = true;
            return null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return "Could not write the bundle (" + ex.GetType().Name + ").";
        }
        finally
        {
            if (!moved)
            {
                TryDelete(temp);
            }
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
