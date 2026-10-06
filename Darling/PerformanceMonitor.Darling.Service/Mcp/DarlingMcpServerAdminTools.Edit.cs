/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>edit_server</c> (#5240): changes one monitored server's settings IN PLACE. The row keeps its
/// <c>server_id</c> (the #2158 rule the desktop Edit already follows), so tags, collected history and every
/// per-id setting stay attached; only a fixed list of columns is ever written, and never <c>server_id</c>,
/// <c>engine</c>, <c>is_enabled</c>, <c>excluded_databases</c>, <c>capture_plans</c>,
/// <c>alert_delivery_mode_override</c>, <c>plan_force_bot_enabled</c> or the <c>remediation_*</c> columns.
///
/// <para><b>Credentials.</b> The stored secret cannot be read back (the <c>mcp</c> role has no SELECT on
/// <c>encrypted_password</c>), so the edit cannot probe with "keep the stored password". A change to how a SQL or
/// service-principal row is reached therefore has to carry the secret again and is probed before it is saved; a
/// change that does not touch the connection (name, cost) and a row that stores no secret need none. A secret
/// is checked by the same <see cref="ValidateSecret"/> add uses, and no secret value is put in an answer or a log
/// line.</para>
///
/// <para><b>Concurrency.</b> The row is read without a lock, probed, then written inside one transaction by
/// <c>config.edit_monitored_server</c>, which locks the row and compares <c>modified_at</c> to the token the edit read,
/// so a write that landed in between is answered <c>conflict</c> and nothing is written. A caller may also send the <c>modified_at</c> it read as
/// <c>expected_modified_at</c>. That transaction takes <see cref="IdentityLockSql"/> first, the lock the add's INSERT
/// takes too, and checks that the new address is still free under it: the address check before the probe is only a
/// fast refusal, and without the lock an add or another edit could claim the address during the probe (#5240).</para>
/// </summary>
public sealed partial class DarlingMcpServerAdminTools
{
    /// <summary>The <c>status</c> vocabulary of an edit answer.</summary>
    internal static class EditStatus
    {
        public const string Updated = "updated";
        public const string Unchanged = "unchanged";
        public const string Invalid = "invalid";
        public const string NotFound = "not_found";
        public const string Conflict = "conflict";
        public const string Collides = "collides";
        public const string ConnectionFailed = "connection_failed";
    }

    /// <summary>The keys <c>changes_json</c> may carry, in the order the refusal names them.</summary>
    internal static readonly string[] EditableKeys =
    [
        "display_name", "host", "port", "database", "read_only_intent", "auth", "username", "password",
        "encrypt_mode", "trust_server_certificate", "multi_subnet_failover", "monthly_cost_usd", "expected_modified_at",
    ];

    /// <summary>Keys that name a column this surface never writes. Named in the refusal, so a caller learns that it
    /// is a rule and not a typo.</summary>
    private static readonly string[] EditRefusedKeys =
    [
        "server_id", "engine", "is_enabled", "excluded_databases", "capture_plans", "alert_delivery_mode_override",
        "plan_force_bot_enabled",
    ];

    /// <summary>The columns an edit may write, by the field that names them. The SET list is built ONLY from this
    /// table, never from a caller's key.</summary>
    internal static readonly IReadOnlyDictionary<string, string> EditColumnOfField = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["display_name"] = "name",
        ["host"] = "host",
        ["port"] = "port",
        ["database"] = "database",
        ["read_only_intent"] = "read_only_intent",
        ["auth"] = "auth",
        ["username"] = "username",
        ["password"] = "encrypted_password",
        ["encrypt_mode"] = "encrypt_mode",
        ["trust_server_certificate"] = "trust_server_certificate",
        ["multi_subnet_failover"] = "multi_subnet_failover",
        ["monthly_cost_usd"] = "monthly_cost_usd",
    };

    private const string EditNoteSweep = "The service applies this within one sweep.";

    private const string EditNoteAddress =
        "This server keeps its id and history. The old address cannot be added as a new server under the same spelling.";

    internal const string EditPasswordNeededText =
        "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";

    /// <summary>The answer when the store refuses a password that is a reference (env: or file:): the store takes the
    /// password itself from the viewer, admin and MCP roles. The same words in the store's own trigger and edit function.</summary>
    internal const string EditReferenceRefusedText =
        "Enter the password itself. References (env: or file:) can only be set in the configuration file.";

    /// <summary>The fixed answer when the store has no edit function the calling role may run (#5240): a self-managed store
    /// whose roles were provisioned before the function existed. Never PostgreSQL's own text.</summary>
    internal const string EditStoreNeedsRolesText =
        "This store's roles predate server edits: re-run provision-roles.sql against the store, then try again.";

    /* ─────────────────────────────── the tool ─────────────────────────────── */

    [McpServerTool(Name = "edit_server"), Description(
        "Edits one monitored server's settings in place, no confirm step: name, address, database, authentication, " +
        "TLS posture or monthly cost. The server keeps its id, tags and history. A change to how it is reached is " +
        "probed first and saved only if the server answers; the service applies it within one sweep. Engine, " +
        "enabled state and excluded databases cannot change. Returns updated, unchanged, or " +
        "invalid/not_found/ambiguous/conflict/collides/connection_failed with nothing written." +
        "<<GUIDE>>" +
        "Changes ONLY the fields named in changes_json, a JSON object. Keys: display_name, host, port (PostgreSQL), " +
        "database, read_only_intent, auth (\"Windows\", \"SQL\", \"ServicePrincipal\", \"ManagedIdentity\"), username, " +
        "password, encrypt_mode, trust_server_certificate, multi_subnet_failover, monthly_cost_usd, " +
        "expected_modified_at. The values mean what they mean in add_servers. Naming engine, is_enabled, " +
        "excluded_databases or any other key is status \"invalid\". The server keeps its id: tags, history and " +
        "settings stay attached, even when the address changes; the old address then cannot be added as a new " +
        "server under the same spelling. PASSWORD: omit it, or leave it blank, to keep the stored one; a typed one replaces it and is the password itself (a value starting with env: or file: is refused, references are set in the configuration file). The stored secret cannot be " +
        "read back, so changing the host, port, database, read_only_intent, auth, username, encrypt_mode, " +
        "trust_server_certificate or multi_subnet_failover of a SQL or ServicePrincipal server REQUIRES password " +
        "again, and so does switching into either mode; a name or cost change, and a Windows or ManagedIdentity " +
        "server, need none. The secret is checked as add_servers checks it and is never returned or logged. " +
        "expected_modified_at is the modified_at an earlier answer returned; if the row changed since, the answer " +
        "is \"conflict\" with the current non-secret values and nothing is written. Omit it for last write wins. " +
        "\"collides\": the new address belongs to another monitored server, or the connection lands in a database " +
        "another one covers. \"connection_failed\": the probe could not connect, nothing was saved. \"unchanged\": " +
        "the request equals the stored values, nothing was written. updated returns {status, server, display_name, " +
        "server_id, changed:[field names], reconnects, tested, modified_at, note}; reconnects says whether the " +
        "service drops and re-opens its connection to the server. server_name is resolved as remove_server resolves it.")]
    public static Task<string> EditServer(
        NpgsqlDataSource postgres,
        [Description("The monitored server to edit: its display name or storage name / address. A partial name is accepted only when it matches exactly one definition.")] string server_name,
        [Description("A JSON object with ONLY the fields to change (e.g. {\"display_name\":\"Orders\",\"monthly_cost_usd\":120}). See the tool guide for the keys.")] string changes_json,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        EditServerByNameAsync(postgres, server_name, changes_json, DefaultProbeAsync, OperatingSystem.IsWindows(), logger, cancellationToken);

    /// <summary>The tool's body with the probe and the platform injected, so a test drives the product's own path
    /// (parse, resolve, core, store) without a reachable server.</summary>
    internal static async Task<string> EditServerByNameAsync(
        NpgsqlDataSource postgres, string server_name, string changes_json, ServerProbe probe, bool isWindows, ILogger? logger, CancellationToken cancellationToken)
    {
        string? submittedSecret = null;
        try
        {
            if (string.IsNullOrWhiteSpace(server_name))
            {
                return Outcome(EditStatus.Invalid, "server_name is required.");
            }

            /* Validate the body BEFORE any store access: a malformed request never reads the table. */
            var (changes, parseError) = ParseEditChanges(changes_json);
            if (parseError != null)
            {
                return Outcome(EditStatus.Invalid, parseError);
            }

            submittedSecret = changes!.Password;
            var definitions = await LoadDefinitionsForRemovalAsync(postgres);
            var target = ResolveForRemoval(definitions.Select(d => d.Server).ToList(), server_name);
            if (target.Candidates.Count == 0)
            {
                if (definitions.Count == 0)
                {
                    return Outcome(EditStatus.NotFound, $"Could not resolve server '{server_name}': no monitored-server definitions exist in the central store (servers defined in darling.json are not editable through this tool).");
                }

                var (_, missMessage) = DarlingServerResolver.ResolveOrError(definitions.Select(d => d.Server).ToList(), server_name);
                return Outcome(EditStatus.NotFound, missMessage is null ? $"Could not resolve server '{server_name}'." : McpHelpers.ErrorMessageOf(missMessage));
            }

            if (target.Candidates.Count > 1)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "ambiguous",
                    message = $"'{server_name}' matches {target.Candidates.Count} defined servers " +
                              $"({(target.MatchedBy == "exact" ? "the same name on more than one definition" : "as a partial name")}); " +
                              "nothing was changed. Re-issue edit_server with ONE candidate's full name.",
                    matched_by = target.MatchedBy,
                    matched_in = "config_monitored_servers",
                    candidates = target.Candidates.Select(c => new { server = c.ServerName, display_name = c.DisplayName }),
                }, McpHelpers.JsonOptions);
            }

            return await EditServerCoreAsync(
                new PostgresServerEditStore(postgres), target.Candidates[0].ServerId, changes!, probe,
                isWindows, logger, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A probe or driver that throws can carry the submitted secret in its message: redact the whole envelope. */
            ex = new InvalidOperationException(RedactEditSecret(ex.Message, submittedSecret));
            return McpHelpers.FormatError("edit_server", ex);
        }
    }

    /* ─────────────────────────────── the core ─────────────────────────────── */

    /// <summary>
    /// The web edit route's entry (#5240): the core over a store and the real probe, for a row already resolved to
    /// <paramref name="serverId"/>. Pass <c>null</c> for <paramref name="logger"/> when the caller writes its own audit
    /// line (the web route does, with the signed-in principal). A fault that escapes the core becomes the error
    /// envelope with the submitted secret redacted, because a driver's message can quote it.
    /// </summary>
    internal static async Task<string> EditServerByIdAsync(
        NpgsqlDataSource postgres, int serverId, string changesJson, ILogger? logger, CancellationToken cancellationToken)
    {
        string? submittedSecret = null;
        try
        {
            var (changes, parseError) = ParseEditChanges(changesJson);
            if (parseError != null)
            {
                return Outcome(EditStatus.Invalid, parseError);
            }

            submittedSecret = changes!.Password;
            return await EditServerCoreAsync(
                new PostgresServerEditStore(postgres), serverId, changes, DefaultProbeAsync, OperatingSystem.IsWindows(), logger, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ex = new InvalidOperationException(RedactEditSecret(ex.Message, submittedSecret));
            return McpHelpers.FormatError("edit_server", ex);
        }
    }

    /// <summary>The core over a changes string: parses it, then runs <see cref="EditServerCoreAsync(IServerEditStore, int, ServerEditChanges, ServerProbe, bool, ILogger?, CancellationToken)"/>.
    /// An unparseable body is <c>invalid</c> without a store read.</summary>
    internal static async Task<string> EditServerCoreAsync(
        IServerEditStore store, int serverId, string changesJson, ServerProbe probe, bool isWindows, ILogger? logger, CancellationToken cancellationToken)
    {
        var (changes, error) = ParseEditChanges(changesJson);
        return error != null
            ? Outcome(EditStatus.Invalid, error)
            : await EditServerCoreAsync(store, serverId, changes!, probe, isWindows, logger, cancellationToken);
    }

    /// <summary>
    /// The edit: read, check the token, plan, occupancy, probe, then ONE transaction that re-checks and writes. Every
    /// outcome is an answer; only an unexpected store fault throws (the caller turns it into the error envelope).
    /// </summary>
    internal static async Task<string> EditServerCoreAsync(
        IServerEditStore store, int serverId, ServerEditChanges changes, ServerProbe probe, bool isWindows, ILogger? logger, CancellationToken cancellationToken)
    {
        var row = await store.ReadRowAsync(serverId, cancellationToken);
        if (row is null)
        {
            return Outcome(EditStatus.NotFound, "This server's definition no longer exists (removed since it was looked up); nothing was changed.");
        }

        if (changes.ExpectedModifiedAt is not null
            && !string.Equals(changes.ExpectedModifiedAt, ModifiedAtToken(row.ModifiedAt), StringComparison.Ordinal))
        {
            return ConflictAnswer(row);
        }

        var (plan, planError) = PlanEdit(row, changes, isWindows);
        if (planError != null)
        {
            return Outcome(EditStatus.Invalid, planError);
        }

        if (plan!.Sets.Count == 0)
        {
            return JsonSerializer.Serialize(new
            {
                status = EditStatus.Unchanged,
                server = plan.NewStorageKey,
                display_name = row.Name,
                server_id = row.ServerId,
                modified_at = ModifiedAtToken(row.ModifiedAt),
                message = "The request equals the stored values; nothing was written.",
            }, McpHelpers.JsonOptions);
        }

        var otherKeys = new HashSet<string>(await store.LoadOtherStorageKeysAsync(serverId, cancellationToken), StringComparer.OrdinalIgnoreCase);
        if (plan.AddressChanged && otherKeys.Contains(plan.NewStorageKey))
        {
            return OccupiedAnswer(plan.NewStorageKey);
        }

        var tested = false;
        ParsedServerEntry? probedEntry = null;
        string? connectedDatabase = null;
        string? actualStorageKey = null;
        if (plan.NeedsProbe)
        {
            var probeResult = await probe(plan.ProbeConfig, cancellationToken);
            if (!probeResult.Success)
            {
                var detail = string.IsNullOrWhiteSpace(probeResult.Error) ? "Could not connect to the server." : $"Could not connect: {probeResult.Error}";
                return Outcome(EditStatus.ConnectionFailed, RedactEditSecret(detail, plan.PlaintextSecret) + " Nothing was saved.");
            }

            tested = true;
            probedEntry = new ParsedServerEntry(0, plan.ProbeConfig.Name, plan.NewStorageKey, plan.ProbeConfig, null);
            connectedDatabase = probeResult.ConnectedDatabase;
            var collision = ActualIdentityCollision(probedEntry, connectedDatabase, otherKeys);
            if (collision is not null)
            {
                return ActualCollisionOutcome(collision, plan.PlaintextSecret);
            }

            /* #5240: the key the check above compared, handed to the write so the write compares it again under the
               identity lock: another definition may have taken it during the probe, after otherKeys was read. Null when
               the probe named nothing new (a connected database equal to the declared one, or none reported). */
            actualStorageKey = ActualStorageKeyOf(probedEntry, connectedDatabase);
        }

        var sets = plan.Sets;
        if (plan.PlaintextSecret is not null)
        {
            sets = sets.Select(s => s.Column == "encrypted_password"
                ? s with { Value = ProtectPasswordForStorage(plan.PlaintextSecret) }
                : s).ToList();
        }

        var write = await store.WriteAsync(serverId, row.ModifiedAt, sets, plan.AddressChanged ? plan.NewStorageKey : null, actualStorageKey, cancellationToken);
        switch (write.Kind)
        {
            case ServerEditWriteKind.NotFound:
                return Outcome(EditStatus.NotFound, "This server's definition was removed while the edit ran; nothing was changed.");
            case ServerEditWriteKind.Occupied:
                return OccupiedAnswer(plan.NewStorageKey);
            case ServerEditWriteKind.ActualOccupied:
                /* The same refusal the check after the probe gives, reached under the lock: the database the probe reached
                   was claimed by another definition while the probe ran. Only a write handed an actual key answers this. */
                if (probedEntry is null || connectedDatabase is null)
                {
                    throw new InvalidOperationException("The store refused an actual database key that this edit never handed it.");
                }

                return ActualCollisionOutcome(ActualIdentityCollisionText(probedEntry, connectedDatabase), plan.PlaintextSecret);
            case ServerEditWriteKind.PasswordNeeded:
                /* The store's own check: a move of host or port that keeps the stored secret. The plan refuses it first, so
                   this is the answer when the two ever read the row differently, and it is the same sentence. */
                return Outcome(EditStatus.Invalid, EditPasswordNeededText);
            case ServerEditWriteKind.ReferenceRefused:
                /* The store's own check of a new password: it never takes a reference from this role. */
                return Outcome(EditStatus.Invalid, EditReferenceRefusedText);
            case ServerEditWriteKind.Conflict:
                /* Same shape as the pre-probe conflict: the current non-secret values, so the caller can retry. */
                if (await store.ReadRowAsync(serverId, cancellationToken) is { } currentRow)
                {
                    return ConflictAnswer(currentRow);
                }

                return Outcome(EditStatus.NotFound, "This server's definition was removed while the edit ran; nothing was changed.");
        }

        /* One line per edit: the id and the field NAMES. Never a value, never the secret. */
        logger?.LogInformation("Server edited through edit_server: id {ServerId}, fields {Fields}", serverId, string.Join(",", plan.ChangedFields));

        return JsonSerializer.Serialize(new
        {
            status = EditStatus.Updated,
            server = plan.NewStorageKey,
            display_name = plan.NewName,
            server_id = serverId,
            changed = plan.ChangedFields,
            reconnects = plan.Reconnects,
            tested,
            modified_at = ModifiedAtToken(write.ModifiedAt),
            note = plan.AddressChanged ? EditNoteSweep + " " + EditNoteAddress : EditNoteSweep,
        }, McpHelpers.JsonOptions);
    }

    /// <summary>The answer for an edit whose connected database another definition already covers: the add refusal's text
    /// with "Not added" turned into "Not saved", the secret redacted. One shape for the check before the write and the
    /// check under the lock.</summary>
    private static string ActualCollisionOutcome(string collisionText, string? plaintextSecret) =>
        Outcome(EditStatus.Collides, RedactEditSecret(collisionText.Replace("Not added:", "Not saved:", StringComparison.Ordinal), plaintextSecret));

    private static string OccupiedAnswer(string storageKey) =>
        JsonSerializer.Serialize(new
        {
            status = EditStatus.Collides,
            reason = "occupied",
            message = $"Not saved: another monitored server already uses the address '{storageKey}'. Nothing was changed.",
        }, McpHelpers.JsonOptions);

    private static string ConflictAnswer(ServerEditRow row) =>
        JsonSerializer.Serialize(new
        {
            status = EditStatus.Conflict,
            message = "This server was changed since you read it; nothing was saved. The current values are in current.",
            current = CurrentValuesOf(row),
        }, McpHelpers.JsonOptions);

    /// <summary>The non-secret values of a row, as the conflict answer's <c>current</c> and the web edit form's read
    /// return them. No password and no <c>encrypted_password</c>: this surface cannot read either. <c>modified_at</c> is
    /// the opaque token (<see cref="ModifiedAtToken"/>).</summary>
    internal static object CurrentValuesOf(ServerEditRow row) => new
    {
        server_id = row.ServerId,
        display_name = row.Name,
        engine = row.Engine,
        host = row.Host,
        port = row.Port,
        database = row.Database,
        read_only_intent = row.ReadOnlyIntent,
        auth = AuthWord(row.Auth),
        username = row.Username,
        encrypt_mode = row.EncryptMode,
        trust_server_certificate = row.TrustServerCertificate,
        multi_subnet_failover = row.MultiSubnetFailover,
        monthly_cost_usd = row.MonthlyCostUsd,
        modified_at = ModifiedAtToken(row.ModifiedAt),
    };

    internal static string AuthWord(string storeAuth) => storeAuth.ToLowerInvariant() switch
    {
        ServerStoreAuth.Sql => "SQL",
        ServerStoreAuth.ServicePrincipal => "ServicePrincipal",
        ServerStoreAuth.ManagedIdentity => "ManagedIdentity",
        _ => "Windows",
    };

    /// <summary>The opaque optimistic token: <c>modified_at</c> in the round-trip ("o") form, so the microseconds
    /// survive a trip through a client that cannot hold them in a date type.</summary>
    internal static string ModifiedAtToken(DateTime modifiedAt) => modifiedAt.ToString("o", CultureInfo.InvariantCulture);

    private static string RedactEditSecret(string text, string? secret) =>
        string.IsNullOrEmpty(secret) ? text : text.Replace(secret, "[redacted]", StringComparison.Ordinal);

    /* ─────────────────────────────── parse (pure) ─────────────────────────────── */

    /// <summary>The fields an edit names. A field that is not named is not touched.</summary>
    internal sealed class ServerEditChanges
    {
        public string? DisplayName { get; set; }
        public string? Host { get; set; }
        public int? Port { get; set; }
        public bool HasDatabase { get; set; }
        public string? Database { get; set; }
        public bool? ReadOnlyIntent { get; set; }
        public string? StoreAuth { get; set; }
        public bool HasUsername { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? EncryptMode { get; set; }
        public bool? TrustServerCertificate { get; set; }
        public bool? MultiSubnetFailover { get; set; }
        public decimal? MonthlyCostUsd { get; set; }
        public string? ExpectedModifiedAt { get; set; }
    }

    /// <summary>
    /// Parses <c>changes_json</c> the way <c>update_server_tag</c> parses its body: a whitelist, the first error wins,
    /// and nothing is written when any field is bad. A key that names a column this surface never writes is refused
    /// by name; any other unknown key is refused with the editable keys listed. Every message is a fixed sentence
    /// that never repeats a value, so a password sent under a wrong key is not echoed.
    /// </summary>
    internal static (ServerEditChanges? Changes, string? Error) ParseEditChanges(string? changesJson)
    {
        if (string.IsNullOrWhiteSpace(changesJson))
        {
            return (null, "changes_json is required: a JSON object with the fields to change, e.g. {\"display_name\":\"Orders\"}.");
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(changesJson);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            return (null, "changes_json is not valid JSON.");
        }

        if (root is not JsonObject body)
        {
            return (null, "changes_json must be a JSON object holding the fields to change.");
        }

        try
        {
            _ = body.Count; /* a duplicate property name throws on the first enumeration */
        }
        catch (ArgumentException)
        {
            return (null, "changes_json has a duplicate field.");
        }

        var keys = body.Select(p => p.Key).ToList();
        if (keys.FirstOrDefault(k => EditRefusedKeys.Contains(k, StringComparer.Ordinal)
                                  || k.StartsWith("remediation_", StringComparison.Ordinal)) is { } refused)
        {
            var refusedName = refused.StartsWith("remediation_", StringComparison.Ordinal) ? "remediation_*" : TruncateKey(refused);
            return (null, $"'{refusedName}' cannot be changed here: it is the server's identity or a setting kept in the desktop app. Nothing was changed.");
        }

        if (keys.FirstOrDefault(k => !EditableKeys.Contains(k, StringComparer.Ordinal)) is { } unknown)
        {
            return (null, $"'{TruncateKey(unknown)}' is not an editable field. The editable fields are: {string.Join(", ", EditableKeys)}.");
        }

        if (keys.Count == 0)
        {
            return (null, "changes_json names no field to change.");
        }

        var changes = new ServerEditChanges();
        foreach (var key in keys)
        {
            var node = body[key];
            switch (key)
            {
                case "display_name":
                    if (!TryEditString(node, out var displayName) || displayName is null)
                    {
                        return (null, "display_name must be text.");
                    }

                    changes.DisplayName = displayName.Trim();
                    break;
                case "host":
                    if (!TryEditString(node, out var host) || string.IsNullOrWhiteSpace(host))
                    {
                        return (null, "host must be non-blank text.");
                    }

                    changes.Host = host.Trim();
                    break;
                case "port":
                    var (port, portError) = ResolvePort(body);
                    if (portError != null)
                    {
                        return (null, portError);
                    }

                    changes.Port = port;
                    break;
                case "database":
                    if (node is not null && !TryEditString(node, out _))
                    {
                        return (null, "database must be text, or null to clear it.");
                    }

                    changes.HasDatabase = true;
                    changes.Database = node is null || string.IsNullOrWhiteSpace(TryGetString(body, "database")) ? null : TryGetString(body, "database")!.Trim();
                    break;
                case "read_only_intent":
                    var (ro, roError) = ResolveBool(body, "read_only_intent", false);
                    if (roError != null || node is null)
                    {
                        return (null, "read_only_intent must be true or false.");
                    }

                    changes.ReadOnlyIntent = ro;
                    break;
                case "trust_server_certificate":
                    var (trust, trustError) = ResolveBool(body, "trust_server_certificate", false);
                    if (trustError != null || node is null)
                    {
                        return (null, "trust_server_certificate must be true or false.");
                    }

                    changes.TrustServerCertificate = trust;
                    break;
                case "multi_subnet_failover":
                    var (multi, multiError) = ResolveBool(body, "multi_subnet_failover", false);
                    if (multiError != null || node is null)
                    {
                        return (null, "multi_subnet_failover must be true or false.");
                    }

                    changes.MultiSubnetFailover = multi;
                    break;
                case "auth":
                    var (storeAuth, authError) = ResolveAuth(TryGetString(body, "auth"));
                    if (authError != null)
                    {
                        return (null, authError);
                    }

                    if (string.IsNullOrWhiteSpace(TryGetString(body, "auth")))
                    {
                        return (null, "auth must be \"Windows\", \"SQL\", \"ServicePrincipal\", or \"ManagedIdentity\".");
                    }

                    changes.StoreAuth = storeAuth;
                    break;
                case "username":
                    if (node is not null && !TryEditString(node, out _))
                    {
                        return (null, "username must be text, or null to clear it.");
                    }

                    changes.HasUsername = true;
                    changes.Username = node is null || string.IsNullOrWhiteSpace(TryGetString(body, "username")) ? null : TryGetString(body, "username")!.Trim();
                    break;
                case "password":
                    if (!TryEditString(node, out var password))
                    {
                        return (null, "password must be text. Omit password, or leave it blank, to keep the stored one.");
                    }

                    /* A blank field keeps the stored secret, exactly as an absent one does (a form sends the field blank
                       when nothing was typed). Whatever is stored, a reference included, is left byte for byte as it is:
                       the secret column is not in the write unless a password was typed. */
                    if (!string.IsNullOrEmpty(password))
                    {
                        changes.Password = password;
                    }

                    break;
                case "encrypt_mode":
                    var (encryptMode, encryptError) = ResolveEncryptMode(TryGetString(body, "encrypt_mode"));
                    if (encryptError != null || !TryEditString(node, out _))
                    {
                        return (null, "encrypt_mode must be \"Optional\", \"Mandatory\", or \"Strict\".");
                    }

                    changes.EncryptMode = encryptMode;
                    break;
                case "monthly_cost_usd":
                    if (!TryEditDecimal(node, out var cost) || cost < 0)
                    {
                        return (null, "monthly_cost_usd must be a number, zero or more.");
                    }

                    changes.MonthlyCostUsd = cost;
                    break;
                case "expected_modified_at":
                    if (!TryEditString(node, out var token) || string.IsNullOrWhiteSpace(token))
                    {
                        return (null, "expected_modified_at must be the modified_at text an earlier answer returned.");
                    }

                    changes.ExpectedModifiedAt = token.Trim();
                    break;
            }
        }

        return (changes, null);
    }

    private static string TruncateKey(string key) => key.Length <= 40 ? key : key[..40] + "...";

    private static bool TryEditString(JsonNode? node, out string? value)
    {
        value = null;
        if (node is JsonValue v && v.TryGetValue<string>(out var s))
        {
            value = s;
            return true;
        }

        return false;
    }

    private static bool TryEditDecimal(JsonNode? node, out decimal value)
    {
        value = 0m;
        if (node is not JsonValue v)
        {
            return false;
        }

        if (v.TryGetValue<decimal>(out value))
        {
            return true;
        }

        return v.TryGetValue<string>(out var text)
            && decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }

    /* ─────────────────────────────── plan (pure) ─────────────────────────────── */

    /// <summary>One column an edit writes. <see cref="Field"/> is the field name an answer reports.</summary>
    internal sealed record EditColumnValue(string Field, string Column, NpgsqlDbType DbType, object? Value);

    /// <summary>What an edit would do, computed from the stored row and the request alone.</summary>
    internal sealed class ServerEditPlan
    {
        public List<EditColumnValue> Sets { get; } = [];
        public List<string> ChangedFields { get; } = [];
        public string NewStorageKey { get; set; } = "";
        public string NewName { get; set; } = "";
        public bool AddressChanged { get; set; }
        public bool NeedsProbe { get; set; }
        public bool Reconnects { get; set; }
        public string? PlaintextSecret { get; set; }
        public MonitoredServer ProbeConfig { get; set; } = new();
    }

    private static bool IsSecretAuth(string storeAuth) =>
        string.Equals(storeAuth, ServerStoreAuth.Sql, StringComparison.OrdinalIgnoreCase)
        || string.Equals(storeAuth, ServerStoreAuth.ServicePrincipal, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The edit core's own rule for "the server is at the same address": the host text is equal ordinally (a request's host
    /// is trimmed when it is read; the text is compared exactly as stored, and case counts) and the port is equal. An edit that fails it
    /// has moved the server, so the stored password is not reused. <c>StoreConfigProvider</c> applies the same rule to
    /// decide whether a row still sits at the address darling.json declares.
    /// </summary>
    internal static bool SameAddress(string host, int port, string otherHost, int otherPort) =>
        string.Equals(host, otherHost, StringComparison.Ordinal) && port == otherPort;

    /// <summary>Every setting that decides where a server is reached and how it is trusted: what an edit and a
    /// <c>test_connect</c> compare against the stored row before a stored password may be used.</summary>
    internal readonly record struct ServerConnectionSettings(
        string Host, int Port, string Engine, string? Database, bool ReadOnlyIntent, string Auth, string? Username,
        string EncryptMode, bool TrustServerCertificate, bool MultiSubnetFailover);

    /// <summary>
    /// The ONE rule for "this definition connects differently from that one", shared by the edit core and
    /// <c>test_connect</c> so neither lets a stored password go anywhere the other would refuse. Host, username and
    /// database compare ordinally (<see cref="SameAddress"/> for host and port); auth, encrypt mode and engine ignore
    /// case; the rest compare as stored. The store function in <c>provision-roles.sql</c> (<c>password_needed</c>) names the same set.
    /// </summary>
    internal static bool ConnectionSettingsDiffer(ServerConnectionSettings a, ServerConnectionSettings b) =>
        !SameAddress(a.Host, a.Port, b.Host, b.Port)
        || !string.Equals(a.Engine, b.Engine, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Database, b.Database, StringComparison.Ordinal)
        || a.ReadOnlyIntent != b.ReadOnlyIntent
        || !string.Equals(a.Auth, b.Auth, StringComparison.OrdinalIgnoreCase)
        || !string.Equals(a.Username, b.Username, StringComparison.Ordinal)
        || !string.Equals(a.EncryptMode, b.EncryptMode, StringComparison.OrdinalIgnoreCase)
        || a.TrustServerCertificate != b.TrustServerCertificate
        || a.MultiSubnetFailover != b.MultiSubnetFailover;


    /// <summary>
    /// The merged definition (the stored row plus the request) checked as add checks an entry, and the columns that
    /// differ. The credential rules: a password is required when a SQL or service-principal row's connection changes
    /// (probe rule b) or when the row switches INTO one of those modes, and it is never reused across modes; a
    /// switch out of them clears the stored secret; a row that stores no secret needs none.
    /// </summary>
    internal static (ServerEditPlan? Plan, string? Error) PlanEdit(ServerEditRow row, ServerEditChanges c, bool isWindows)
    {
        var host = c.Host ?? row.Host;
        var port = c.Port ?? row.Port;
        var database = c.HasDatabase ? c.Database : row.Database;
        var readOnly = c.ReadOnlyIntent ?? row.ReadOnlyIntent;
        var auth = c.StoreAuth ?? row.Auth;
        var encryptMode = c.EncryptMode ?? row.EncryptMode;
        var trust = c.TrustServerCertificate ?? row.TrustServerCertificate;
        var multi = c.MultiSubnetFailover ?? row.MultiSubnetFailover;
        var cost = c.MonthlyCostUsd ?? row.MonthlyCostUsd;
        var authSwitched = !string.Equals(auth, row.Auth, StringComparison.OrdinalIgnoreCase);
        var secretMode = IsSecretAuth(auth);
        var isSp = string.Equals(auth, ServerStoreAuth.ServicePrincipal, StringComparison.OrdinalIgnoreCase);

        var username = c.HasUsername ? c.Username : (authSwitched ? null : row.Username);
        var name = c.DisplayName is null ? row.Name : (c.DisplayName.Length == 0 ? host : c.DisplayName);

        var isPostgres = !string.Equals(row.Engine, EngineSqlServer, StringComparison.OrdinalIgnoreCase);
        if (isPostgres && !string.Equals(auth, ServerStoreAuth.Sql, StringComparison.OrdinalIgnoreCase))
        {
            return (null, "a PostgreSQL target requires auth \"SQL\" with a username and password " +
                          "(integrated/Kerberos auth is not supported for PostgreSQL targets).");
        }

        if (!isPostgres && c.Port is not null)
        {
            return (null, "A SQL Server port goes in host, as host,port.");
        }

        if (secretMode)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                return (null, isSp
                    ? "username is required for ServicePrincipal authentication (the Entra application/client id)."
                    : "username is required for SQL authentication.");
            }
        }
        else
        {
            /* Windows and managed identity store no secret; add ignores a password sent with them, and so does an edit. */
            c = CloneWithoutPassword(c);
            if (!string.Equals(auth, ServerStoreAuth.ManagedIdentity, StringComparison.OrdinalIgnoreCase))
            {
                username = null;
            }
        }

        if (secretMode && c.Password is not null && ValidateSecret(c.Password, isWindows, isSp) is { } secretRefusal)
        {
            return (null, secretRefusal);
        }

        var connectionChanged = ConnectionSettingsDiffer(
            new ServerConnectionSettings(host, port, row.Engine, database, readOnly, auth, username, encryptMode, trust, multi),
            new ServerConnectionSettings(row.Host, row.Port, row.Engine, row.Database, row.ReadOnlyIntent, row.Auth, row.Username,
                row.EncryptMode, row.TrustServerCertificate, row.MultiSubnetFailover));

        if (secretMode && c.Password is null && (authSwitched || connectionChanged))
        {
            return (null, authSwitched
                ? (isSp
                    ? "Switching to ServicePrincipal authentication needs the client secret as password."
                    : "Switching to SQL authentication needs the password.")
                : EditPasswordNeededText);
        }

        var plan = new ServerEditPlan
        {
            NewStorageKey = ServerIdHelper.BuildStorageName(host, database, readOnly, row.Engine, port),
            NewName = name,
            PlaintextSecret = secretMode ? c.Password : null,
        };
        var oldKey = ServerIdHelper.BuildStorageName(row.Host, row.Database, row.ReadOnlyIntent, row.Engine, row.Port);
        plan.AddressChanged = !string.Equals(plan.NewStorageKey, oldKey, StringComparison.Ordinal);

        void Set(string field, NpgsqlDbType type, object? value, bool changed)
        {
            if (!changed)
            {
                return;
            }

            plan.Sets.Add(new EditColumnValue(field, EditColumnOfField[field], type, value));
            plan.ChangedFields.Add(field);
        }

        Set("display_name", NpgsqlDbType.Text, name, !string.Equals(name, row.Name, StringComparison.Ordinal));
        Set("host", NpgsqlDbType.Text, host, !string.Equals(host, row.Host, StringComparison.Ordinal));
        Set("port", NpgsqlDbType.Integer, port, port != row.Port);
        Set("database", NpgsqlDbType.Text, database, !string.Equals(database, row.Database, StringComparison.Ordinal));
        Set("read_only_intent", NpgsqlDbType.Boolean, readOnly, readOnly != row.ReadOnlyIntent);
        Set("auth", NpgsqlDbType.Text, auth, authSwitched);
        Set("username", NpgsqlDbType.Text, username, !string.Equals(username, row.Username, StringComparison.Ordinal));
        Set("encrypt_mode", NpgsqlDbType.Text, encryptMode, !string.Equals(encryptMode, row.EncryptMode, StringComparison.OrdinalIgnoreCase));
        Set("trust_server_certificate", NpgsqlDbType.Boolean, trust, trust != row.TrustServerCertificate);
        Set("multi_subnet_failover", NpgsqlDbType.Boolean, multi, multi != row.MultiSubnetFailover);
        Set("monthly_cost_usd", NpgsqlDbType.Numeric, cost, cost != row.MonthlyCostUsd);

        /* The secret column: a new secret is written (the core encrypts it after the probe); a switch OUT of a secret
           mode clears the old one so it can never be reused as another mode's secret; otherwise it is not in the SET
           list at all, which is what "keep" means. */
        if (secretMode && c.Password is not null)
        {
            Set("password", NpgsqlDbType.Text, null, true);
        }
        else if (!secretMode && IsSecretAuth(row.Auth))
        {
            Set("password", NpgsqlDbType.Text, null, true);
        }

        plan.NeedsProbe = plan.Sets.Count > 0 && (connectionChanged || plan.PlaintextSecret is not null);
        plan.ProbeConfig = new MonitoredServer
        {
            StoredServerId = row.ServerId,
            Name = name,
            Host = host,
            Database = database,
            Auth = auth,
            Username = username,
            Password = plan.PlaintextSecret,
            EncryptMode = encryptMode,
            TrustServerCertificate = trust,
            ReadOnlyIntent = readOnly,
            MultiSubnetFailover = multi,
            Engine = row.Engine,
            Port = port,
        };

        /* Whether the running service drops and re-opens its connection: the worker's own comparison, over the held
           definition and the edited one. The secret differs by a marker, since the stored blob is not readable. */
        var before = new MonitoredServer
        {
            StoredServerId = row.ServerId, Name = row.Name, Host = row.Host, Database = row.Database, Auth = row.Auth,
            Username = row.Username, EncryptedPassword = IsSecretAuth(row.Auth) ? "stored" : null, EncryptMode = row.EncryptMode,
            TrustServerCertificate = row.TrustServerCertificate, ReadOnlyIntent = row.ReadOnlyIntent,
            MultiSubnetFailover = row.MultiSubnetFailover, Engine = row.Engine, Port = row.Port,
        };
        var after = new MonitoredServer
        {
            StoredServerId = row.ServerId, Name = name, Host = host, Database = database, Auth = auth,
            Username = username,
            EncryptedPassword = !secretMode ? null : (c.Password is not null ? "changed" : "stored"),
            EncryptMode = encryptMode, TrustServerCertificate = trust, ReadOnlyIntent = readOnly,
            MultiSubnetFailover = multi, Engine = row.Engine, Port = port,
        };
        plan.Reconnects = plan.Sets.Count > 0 && !DarlingWorker.ServerDefinitionEquals(before, after);
        return (plan, null);
    }

    private static ServerEditChanges CloneWithoutPassword(ServerEditChanges c) => new()
    {
        DisplayName = c.DisplayName, Host = c.Host, Port = c.Port, HasDatabase = c.HasDatabase, Database = c.Database,
        ReadOnlyIntent = c.ReadOnlyIntent, StoreAuth = c.StoreAuth, HasUsername = c.HasUsername, Username = c.Username,
        Password = null, EncryptMode = c.EncryptMode, TrustServerCertificate = c.TrustServerCertificate,
        MultiSubnetFailover = c.MultiSubnetFailover, MonthlyCostUsd = c.MonthlyCostUsd, ExpectedModifiedAt = c.ExpectedModifiedAt,
    };

    /* ─────────────────────────────── store seam ─────────────────────────────── */

    /// <summary>One <c>config_monitored_servers</c> row, the NON-SECRET columns an edit reads.</summary>
    internal sealed record ServerEditRow(
        int ServerId, string Name, string Host, int Port, string? Database, bool ReadOnlyIntent, string Engine, string Auth,
        string? Username, string EncryptMode, bool TrustServerCertificate, bool MultiSubnetFailover, decimal MonthlyCostUsd,
        DateTime ModifiedAt);

    /// <summary><c>Occupied</c>: another definition holds the address the edit moves to. <c>ActualOccupied</c>: another
    /// definition holds the storage key the probe's connected database gives (#5240), the refusal
    /// <see cref="ActualIdentityCollision"/> gives before the write. Neither commits.</summary>
    internal enum ServerEditWriteKind { Written, NotFound, Conflict, Occupied, PasswordNeeded, ActualOccupied, ReferenceRefused }

    internal sealed record ServerEditWrite(ServerEditWriteKind Kind, DateTime ModifiedAt);

    /// <summary>The storage operations an edit performs, as a seam: the real one is
    /// <see cref="PostgresServerEditStore"/>; a test stands in one that swallows a row, moves <c>modified_at</c> or
    /// records the columns written.</summary>
    internal interface IServerEditStore
    {
        /// <summary>The row, or null when no definition holds <paramref name="serverId"/>. No lock.</summary>
        Task<ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken);

        /// <summary>The storage key of every OTHER definition.</summary>
        Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken);

        /// <summary>Re-checks and writes in one transaction: the identity lock (<see cref="IdentityLockSql"/>), then
        /// <c>config.edit_monitored_server</c>, which locks the row, compares <c>modified_at</c> with
        /// <paramref name="expectedModifiedAt"/>, refuses a move of host or port that keeps the stored secret, and writes
        /// the listed columns; then, when <paramref name="newStorageKey"/> or <paramref name="actualStorageKey"/> is given,
        /// the other rows' keys, read under the lock. An address another row holds undoes the write: the transaction is
        /// never committed. <paramref name="actualStorageKey"/> is the key the probe's connected database gives when it
        /// differs from the edit's own key (<see cref="ActualStorageKeyOf"/>), else null; a row holding it answers
        /// <see cref="ServerEditWriteKind.ActualOccupied"/>, judged after <paramref name="newStorageKey"/>.</summary>
        Task<ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, CancellationToken cancellationToken);
    }

    /// <summary>The columns <see cref="ReadEditRowSql"/> reads, and the only ones an edit may name in a SET list.</summary>
    internal const string ReadEditRowSql = @"
SELECT server_id, name, host, port, database, read_only_intent, engine, auth, username, encrypt_mode,
       trust_server_certificate, multi_subnet_failover, monthly_cost_usd, modified_at
FROM config_monitored_servers WHERE server_id = $1";

    internal const string OtherServersSql =
        "SELECT host, database, read_only_intent, engine, port FROM config_monitored_servers WHERE server_id <> $1";

    /// <summary>
    /// The one write of an edit (#5240): <c>config.edit_monitored_server</c>, the owner-owned SECURITY DEFINER function
    /// (<see cref="DarlingManagedRoles.BuildEditMonitoredServerFunctionSql"/>). The web route's viewer role and the MCP
    /// tool's mcp role both run it, and neither holds a column UPDATE on the table. <c>$1</c> is the id, <c>$2</c> the
    /// expected <c>modified_at</c>, <c>$3</c> the names of the columns to write, then one value per editable column in
    /// <see cref="EditFunctionColumns"/> order (null for a column not listed): the new secret rides at <c>$11</c>.
    /// </summary>
    internal const string EditFunctionSql =
        "SELECT outcome, new_modified_at FROM config.edit_monitored_server($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";

    /// <summary>The editable columns in the order the function takes their values (<c>$4</c> onwards).</summary>
    internal static readonly (string Column, NpgsqlDbType DbType)[] EditFunctionColumns =
    [
        ("name", NpgsqlDbType.Text), ("host", NpgsqlDbType.Text), ("port", NpgsqlDbType.Integer), ("database", NpgsqlDbType.Text),
        ("read_only_intent", NpgsqlDbType.Boolean), ("auth", NpgsqlDbType.Text), ("username", NpgsqlDbType.Text),
        ("encrypted_password", NpgsqlDbType.Text), ("encrypt_mode", NpgsqlDbType.Text),
        ("trust_server_certificate", NpgsqlDbType.Boolean), ("multi_subnet_failover", NpgsqlDbType.Boolean),
        ("monthly_cost_usd", NpgsqlDbType.Numeric),
    ];

    /// <summary>
    /// The arguments of <see cref="EditFunctionSql"/> for a set of columns. Every column is checked against
    /// <see cref="EditColumnOfField"/>, so a set list cannot name another one. The expected <c>modified_at</c> is passed as
    /// the <see cref="DateTime"/> the edit read (a microsecond value), never rounded, because the function compares it exactly.
    /// </summary>
    internal static List<NpgsqlParameter> BuildEditFunctionParameters(
        int serverId, DateTime expectedModifiedAt, IReadOnlyList<EditColumnValue> sets)
    {
        var allowed = new HashSet<string>(EditColumnOfField.Values, StringComparer.Ordinal);
        foreach (var set in sets)
        {
            if (!allowed.Contains(set.Column))
            {
                throw new InvalidOperationException("An edit may not write column '" + set.Column + "'.");
            }
        }

        var parameters = new List<NpgsqlParameter>
        {
            new NpgsqlParameter<int> { TypedValue = serverId }, // $1
            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = expectedModifiedAt }, // $2
            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = sets.Select(s => s.Column).ToArray() }, // $3
        };
        foreach (var (column, dbType) in EditFunctionColumns)
        {
            var set = sets.FirstOrDefault(s => string.Equals(s.Column, column, StringComparison.Ordinal));
            parameters.Add(new NpgsqlParameter { NpgsqlDbType = dbType, Value = set?.Value ?? DBNull.Value });
        }

        return parameters;
    }

    internal sealed class PostgresServerEditStore : IServerEditStore
    {
        private readonly NpgsqlDataSource _postgres;

        public PostgresServerEditStore(NpgsqlDataSource postgres) => _postgres = postgres;

        public async Task<ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken)
        {
            var postgres = _postgres;
            await using var command = postgres.CreateCommand(ReadEditRowSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId }); // $1
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return new ServerEditRow(
                reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), !reader.IsDBNull(5) && reader.GetBoolean(5),
                reader.IsDBNull(6) ? EngineSqlServer : reader.GetString(6), reader.IsDBNull(7) ? ServerStoreAuth.Integrated : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8), reader.IsDBNull(9) ? "Mandatory" : reader.GetString(9),
                !reader.IsDBNull(10) && reader.GetBoolean(10), !reader.IsDBNull(11) && reader.GetBoolean(11),
                reader.IsDBNull(12) ? 0m : reader.GetDecimal(12), reader.GetDateTime(13));
        }

        public async Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken)
        {
            var keys = new List<string>();
            var postgres = _postgres;
            await using var command = postgres.CreateCommand(OtherServersSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId }); // $1
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                keys.Add(StorageKeyOf(reader));
            }

            return keys;
        }

        public async Task<ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<EditColumnValue> sets, string? newStorageKey, string? actualStorageKey, CancellationToken cancellationToken)
        {
            var parameters = BuildEditFunctionParameters(serverId, expectedModifiedAt, sets);
            var postgres = _postgres;
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            /* #5240: the identity lock, FIRST, before the function locks the row and the occupancy check below runs, so that
               check sees a table no other identity write (an add's INSERT, another edit) is in the middle of changing.
               The probe that preceded this write ran outside it; the lock covers this transaction only.

               Every command here is two-arg on the store connection with the transaction set on it: the shape
               McpReadCommandTimeoutTests recognises as a store command (see remove_server). */
            await using (var identityLock = new NpgsqlCommand(IdentityLockSql, connection) { Transaction = transaction })
            {
                identityLock.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                await identityLock.ExecuteNonQueryAsync(cancellationToken);
            }

            /* The write is the function, so the calling role needs no UPDATE on the table. A store whose roles predate it
               (or a role that may not run it) answers one of these two errors; both become the fixed sentence that says to
               re-run provision-roles.sql, and the driver's own text goes no further. */
            string outcome;
            var written = DateTime.MinValue;
            try
            {
                await using var call = new NpgsqlCommand(EditFunctionSql, connection) { Transaction = transaction };
                call.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                foreach (var parameter in parameters)
                {
                    call.Parameters.Add(parameter);
                }

                await using var reader = await call.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                {
                    throw new InvalidOperationException("The edit function returned no outcome.");
                }

                outcome = reader.GetString(0);
                if (!reader.IsDBNull(1))
                {
                    written = reader.GetDateTime(1);
                }
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.UndefinedFunction or PostgresErrorCodes.InsufficientPrivilege)
            {
                throw new InvalidOperationException(EditStoreNeedsRolesText, ex);
            }

            switch (outcome)
            {
                case "not_found":
                    return new ServerEditWrite(ServerEditWriteKind.NotFound, expectedModifiedAt);
                case "conflict":
                    return new ServerEditWrite(ServerEditWriteKind.Conflict, expectedModifiedAt);
                case "password_needed":
                    return new ServerEditWrite(ServerEditWriteKind.PasswordNeeded, expectedModifiedAt);
                case "reference_refused":
                    return new ServerEditWrite(ServerEditWriteKind.ReferenceRefused, expectedModifiedAt);
                case "saved":
                    break;
                default:
                    throw new InvalidOperationException("The edit function answered an outcome this build does not know.");
            }

            if (newStorageKey is not null || actualStorageKey is not null)
            {
                /* After the function, in the same transaction: an occupied address returns here WITHOUT a commit, which
                   undoes the function's write (and its reload beacon), so the order of the answers is unchanged. The same
                   read judges the key the probe's connected database gives (actualStorageKey): a row that took it during
                   the probe is a second definition collecting one database, refused after the address check. */
                var actualKeyHeld = false;
                await using var others = new NpgsqlCommand(OtherServersSql, connection) { Transaction = transaction };
                others.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                others.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId }); // $1
                await using var reader = await others.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var held = StorageKeyOf(reader);
                    if (newStorageKey is not null && string.Equals(held, newStorageKey, StringComparison.OrdinalIgnoreCase))
                    {
                        return new ServerEditWrite(ServerEditWriteKind.Occupied, expectedModifiedAt);
                    }

                    actualKeyHeld |= actualStorageKey is not null && string.Equals(held, actualStorageKey, StringComparison.OrdinalIgnoreCase);
                }

                if (actualKeyHeld)
                {
                    return new ServerEditWrite(ServerEditWriteKind.ActualOccupied, expectedModifiedAt);
                }
            }

            await transaction.CommitAsync(cancellationToken);
            return new ServerEditWrite(ServerEditWriteKind.Written, written);
        }
    }
}
