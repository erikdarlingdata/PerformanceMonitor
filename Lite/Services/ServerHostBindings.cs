/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The connection settings each server was last confirmed with by the Windows user running Lite, kept in that user's own
/// config folder. servers.json is one list for every user of the computer, while a saved password and the user's own sign-in
/// belong to the user, so Lite connects to a server only when the settings servers.json names now are the ones this user saved
/// or confirmed for that server. The settings kept are the ones that decide where the user's sign-in goes and where Lite runs
/// code under it: the host, the Query Store replica, the sign-in type and client IDs (a credential profile's own when the server
/// uses one, since profiles.json is in the shared folder too), the credential profile, the database and the utility database
/// (where Lite runs dbo.sp_IndexCleanup), and the encryption and certificate settings. Host names are compared as written (port
/// and instance included), trimmed and case-insensitively.
/// </summary>
public sealed class ServerHostBindings
{
    /// <summary>The file in the user's own config folder (never the shared one that holds servers.json).</summary>
    public const string FileName = "server-hosts.json";

    /// <summary>
    /// The file, in the user's own config folder beside <see cref="FileName"/>, that says a build keeping the record has started
    /// with this data folder before (see <see cref="IsFirstStartInUsedFolder"/>); <see cref="BindOnFirstRun"/> writes it.
    /// </summary>
    public const string StartedMarkerName = "server-hosts.started";

    internal const string DatabaseSetting = "database";
    internal const string UtilityDatabaseSetting = "utility database";
    internal const string EncryptionSetting = "encryption";
    internal const string CertificateSetting = "certificate check";

    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly string? _filePath;
    private readonly Func<IProfileLookup?>? _profiles;
    private readonly object _lock = new();
    private readonly Dictionary<string, Binding> _bindings = new(StringComparer.OrdinalIgnoreCase);
    private bool _hasRecord;
    private bool _firstRunDone;
    private string? _saveProblem;

    /// <summary>
    /// One server's confirmed settings. Replica, profile and database fields are empty when the server has none. The sign-in
    /// type and client IDs are the credential profile's when the server uses one; a null client ID was not recorded.
    /// </summary>
    public sealed record Binding(
        string Host,
        string Replica,
        string AuthenticationType,
        string CredentialProfileId,
        string DatabaseName,
        string UtilityDatabase,
        string EncryptMode,
        bool TrustServerCertificate,
        string? AzureClientId,
        string? ManagedIdentityClientId);

    /// <summary>The sign-in settings a connection to a server uses.</summary>
    private readonly record struct SignIn(string AuthenticationType, string AzureClientId, string ManagedIdentityClientId);

    /// <summary>Raised (outside the lock) with the sentence to show when the record could not be written.</summary>
    public event Action<string>? SaveFailed;

    /// <param name="filePath">The per-user file; null keeps the record in memory only (a <see cref="ServerManager"/> built
    /// without a per-user config folder, as tests do).</param>
    /// <param name="profiles">The credential profiles, read on every check (the app sets them after the server list loads).
    /// Null, or a lookup that returns null, finds no profile: a server that uses one is then asked about.</param>
    public ServerHostBindings(string? filePath, Func<IProfileLookup?>? profiles = null)
    {
        _filePath = filePath;
        _profiles = profiles;
        if (filePath == null || !File.Exists(filePath))
            return;

        /* A file that exists but cannot be read leaves every server unconfirmed rather than trusting servers.json as it is
           now: the first-run binding is for a user who has no record yet, not for one whose record is damaged. */
        _hasRecord = true;
        try
        {
            var saved = JsonSerializer.Deserialize<BindingsFile>(File.ReadAllText(filePath));
            if (saved?.Servers != null)
            {
                foreach (var (id, entry) in saved.Servers)
                {
                    /* A field the entry lacks reads as a value no server carries (and, for encryption and the certificate
                       check, as the strictest one), so the server is asked about again rather than passed. */
                    if (!string.IsNullOrEmpty(id) && entry != null)
                        _bindings[id] = new Binding(
                            entry.Host ?? "",
                            entry.Replica ?? "",
                            entry.AuthenticationType ?? "",
                            entry.CredentialProfileId ?? "",
                            entry.DatabaseName ?? "",
                            entry.UtilityDatabase ?? "",
                            entry.EncryptMode ?? "Strict",
                            entry.TrustServerCertificate ?? false,
                            entry.AzureClientId,
                            entry.ManagedIdentityClientId);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            AppLogger.Warn("ServerHostBindings", $"Could not read {filePath}: {ex.Message}. Every server asks for its host to be confirmed again.");
        }
    }

    /// <summary>Whether this user has a record (one existed when Lite started, or one was written since).</summary>
    public bool HasRecord
    {
        get { lock (_lock) return _hasRecord; }
    }

    /// <summary>The sentence for the last failed write of the record; null once a write succeeds.</summary>
    public string? SaveProblem
    {
        get { lock (_lock) return _saveProblem; }
    }

    /// <summary>
    /// The first-run step, for a user with no record yet. A user who had no Lite data before this start gets nothing bound:
    /// each server waits for its settings to be confirmed. A user who did (an upgrade) gets bound only the servers
    /// <paramref name="wasUsed"/> says they used, to the settings servers.json names now; the rest wait too. The record is
    /// written first, and when it cannot be written nothing is bound, so a failed write never leaves a binding behind that
    /// the next start would make again unseen. Binds nothing once a record exists (it only writes the marker, so a record whose marker write failed earlier does not leave the folder reading as an upgrade), and runs once per instance. Returns how
    /// many servers were bound.
    /// </summary>
    public int BindOnFirstRun(IEnumerable<ServerConnection> servers, bool hadLiteData, Func<ServerConnection, bool> wasUsed)
    {
        string? problem;
        int bound;
        /* Built before the lock: a binding reads the credential profiles, which take a lock of their own. */
        var chosen = new Dictionary<string, Binding>(StringComparer.OrdinalIgnoreCase);
        if (hadLiteData)
        {
            foreach (var server in servers)
            {
                if (wasUsed(server))
                    chosen[server.Id] = BindingFor(server);
            }
        }

        lock (_lock)
        {
            if (_firstRunDone)
                return 0;

            /* A record that already exists (an earlier start whose marker write failed, or a file that does not parse) binds
               nothing, but the marker still goes in: without it every later start reads the folder as an upgrade and runs the
               stored-server-name sweep again. */
            if (_hasRecord)
            {
                _firstRunDone = true;
                WriteStartedMarker();
                return 0;
            }
            _firstRunDone = true;

            problem = TryWrite(chosen);
            if (problem == null)
            {
                foreach (var (id, binding) in chosen)
                    _bindings[id] = binding;
                _hasRecord = true;
            }

            /* The marker goes in after the record, so a start that never got here derives "used before" again. A failed write
               for a folder with no earlier Lite data still gets it: there is nothing to bind on a retry, and a folder this
               build created must not read as an upgrade on the next start. A failed write for an upgrade does not, so the
               next start binds the servers that have rows. */
            if (problem == null || !hadLiteData)
                WriteStartedMarker();

            bound = problem == null ? chosen.Count : 0;
        }

        Report(problem);
        return bound;
    }

    /// <summary>
    /// Null when the server's settings are the ones this user confirmed; otherwise what changed. An encryption or certificate
    /// setting stricter than the confirmed one is not a change: the record takes the stricter one, so a later step back to
    /// the old setting is asked about.
    /// </summary>
    /// <param name="server">The server as servers.json names it, or as a dialog would save it.</param>
    /// <param name="recordStricter">False for a dialog's unsaved settings: a stricter setting there is not recorded.</param>
    public ServerHostChange? Check(ServerConnection server, bool recordStricter = true)
    {
        string? problem = null;
        ServerHostChange? change;
        var signIn = SignInFor(server);
        lock (_lock)
        {
            if (!_bindings.TryGetValue(server.Id, out var bound))
                return new ServerHostChange(Normalize(server.ServerName), null, Normalize(server.QueryStoreReplica), ReplicaChanged: false);

            var hostChanged = !SameHost(bound.Host, server.ServerName);
            /* A removed replica sends nothing anywhere, so only a replica that is set and differs needs confirming. */
            var replicaChanged = Normalize(server.QueryStoreReplica).Length > 0 && !SameHost(bound.Replica, server.QueryStoreReplica);
            var settings = SettingChanges(bound, server, signIn);

            if (!hostChanged && !replicaChanged && settings.Count == 0)
            {
                if (recordStricter && IsStricter(bound, server))
                {
                    _bindings[server.Id] = bound with
                    {
                        EncryptMode = EncryptRank(server.EncryptMode) > EncryptRank(bound.EncryptMode) ? Normalize(server.EncryptMode) : bound.EncryptMode,
                        TrustServerCertificate = bound.TrustServerCertificate && server.TrustServerCertificate
                    };
                    problem = SaveLocked();
                }

                change = null;
            }
            else
            {
                change = new ServerHostChange(Normalize(server.ServerName), bound.Host, Normalize(server.QueryStoreReplica), replicaChanged)
                {
                    ConfirmedReplica = bound.Replica,
                    Settings = settings
                };
            }
        }

        Report(problem);
        return change;
    }

    /// <summary>Binds the server to the settings it names now (the user saved it, added it, or confirmed it).</summary>
    public void Confirm(ServerConnection server)
    {
        string? problem;
        var binding = BindingFor(server);
        lock (_lock)
        {
            _bindings[server.Id] = binding;
            problem = SaveLocked();
            if (problem == null)
                _hasRecord = true;
        }

        Report(problem);
    }

    /// <summary>Drops the server's binding (the server was removed).</summary>
    public void Forget(string serverId)
    {
        string? problem = null;
        lock (_lock)
        {
            if (_bindings.Remove(serverId))
                problem = SaveLocked();
        }

        Report(problem);
    }

    /// <summary>The host as written, trimmed. Comparison is case-insensitive (<see cref="SameHost"/>).</summary>
    internal static string Normalize(string? host) => (host ?? "").Trim();

    internal static bool SameHost(string? a, string? b) =>
        string.Equals(Normalize(a), Normalize(b), StringComparison.OrdinalIgnoreCase);

    /// <summary>Optional 0, Mandatory 1, Strict 2. A blank or unknown value is Mandatory, as the connection string builds it.</summary>
    internal static int EncryptRank(string? mode) => Normalize(mode) switch
    {
        "Optional" => 0,
        "Strict" => 2,
        _ => 1
    };

    private static bool IsStricter(Binding bound, ServerConnection server) =>
        EncryptRank(server.EncryptMode) > EncryptRank(bound.EncryptMode) ||
        (bound.TrustServerCertificate && !server.TrustServerCertificate);

    /// <summary>
    /// The sign-in settings a connection to the server uses: its credential profile's own when it names one (a connection
    /// never reads the copy in servers.json then), else the server's. Null when the named profile is not found.
    /// </summary>
    private SignIn? SignInFor(ServerConnection server)
    {
        if (string.IsNullOrEmpty(server.CredentialProfileId))
            return new SignIn(Normalize(server.AuthenticationType), Normalize(server.AzureClientId), Normalize(server.ManagedIdentityClientId));

        var profile = _profiles?.Invoke()?.GetProfile(server.CredentialProfileId);
        return profile == null
            ? null
            : new SignIn(Normalize(profile.AuthType), Normalize(profile.AzureClientId), Normalize(profile.ManagedIdentityClientId));
    }

    private static List<ServerSettingChange> SettingChanges(Binding bound, ServerConnection server, SignIn? signIn)
    {
        var changes = new List<ServerSettingChange>();
        if (!SameHost(bound.CredentialProfileId, server.CredentialProfileId))
            changes.Add(new ServerSettingChange("credential profile", OrNone(bound.CredentialProfileId), OrNone(server.CredentialProfileId)));
        else if (signIn == null)
            changes.Add(new ServerSettingChange("credential profile", OrNone(bound.CredentialProfileId), "a profile Lite cannot find"));

        if (signIn is { } now)
        {
            if (!string.Equals(bound.AuthenticationType, now.AuthenticationType, StringComparison.Ordinal))
                changes.Add(new ServerSettingChange("sign-in method", AuthName(bound.AuthenticationType), AuthName(now.AuthenticationType)));
            if (bound.AzureClientId == null || !SameHost(bound.AzureClientId, now.AzureClientId))
                changes.Add(new ServerSettingChange("application (client) ID", Recorded(bound.AzureClientId), OrNone(now.AzureClientId)));
            if (bound.ManagedIdentityClientId == null || !SameHost(bound.ManagedIdentityClientId, now.ManagedIdentityClientId))
                changes.Add(new ServerSettingChange("managed identity client ID", Recorded(bound.ManagedIdentityClientId), OrNone(now.ManagedIdentityClientId)));
        }

        if (!SameHost(bound.DatabaseName, server.DatabaseName))
            changes.Add(new ServerSettingChange(DatabaseSetting, OrNone(bound.DatabaseName), OrNone(server.DatabaseName)));
        if (!SameHost(bound.UtilityDatabase, server.UtilityDatabase))
            changes.Add(new ServerSettingChange(UtilityDatabaseSetting, OrNone(bound.UtilityDatabase), OrNone(server.UtilityDatabase)));
        if (EncryptRank(server.EncryptMode) < EncryptRank(bound.EncryptMode))
            changes.Add(new ServerSettingChange(EncryptionSetting, OrNone(bound.EncryptMode), OrNone(server.EncryptMode)));
        if (server.TrustServerCertificate && !bound.TrustServerCertificate)
            changes.Add(new ServerSettingChange(CertificateSetting, "checked", "trusted without a check"));
        return changes;
    }

    private static string Recorded(string? value) => value == null ? "not recorded" : OrNone(value);

    private static string OrNone(string? value) => Normalize(value).Length == 0 ? "none" : Normalize(value);

    private static string AuthName(string? type) => Normalize(type) switch
    {
        AuthenticationTypes.Windows => "Windows sign-in",
        AuthenticationTypes.SqlServer => "SQL Server login",
        AuthenticationTypes.EntraMFA => "Microsoft Entra MFA",
        AuthenticationTypes.ServicePrincipal => "service principal",
        AuthenticationTypes.ManagedIdentity => "managed identity",
        AuthenticationTypes.EntraDefaultCredential => "Microsoft Entra default credential",
        AuthenticationTypes.EntraDeviceCode => "Microsoft Entra device code",
        "" => "none",
        var other => other
    };

    /// <summary>The server's settings as a binding. A profile that is not found records no sign-in; it is asked about anyway.</summary>
    private Binding BindingFor(ServerConnection server)
    {
        var signIn = SignInFor(server) ?? new SignIn("", "", "");
        return new(
            Normalize(server.ServerName),
            Normalize(server.QueryStoreReplica),
            signIn.AuthenticationType,
            Normalize(server.CredentialProfileId),
            Normalize(server.DatabaseName),
            Normalize(server.UtilityDatabase),
            Normalize(server.EncryptMode),
            server.TrustServerCertificate,
            signIn.AzureClientId,
            signIn.ManagedIdentityClientId);
    }

    /// <summary>
    /// Whether this start is the first one, by a build that keeps the record, with a data folder an earlier Lite used
    /// (<paramref name="databasePath"/> or settings.json exists): only that start binds the servers the user used. Reads only:
    /// <see cref="BindOnFirstRun"/> writes <see cref="StartedMarkerName"/> once the record is written, so a start that stops
    /// before the servers are bound (the local store would not open, the window was closed first) leaves no marker and the next
    /// start works it out from the data file again. After the marker is written every start reads false, so a folder this
    /// build created, or one whose record was written, is never taken for an earlier Lite's, and Lite asks again about each
    /// server it has no record for.
    /// </summary>
    /// <param name="databasePath">monitor.duckdb.</param>
    /// <param name="configDirectory">The user's own config folder (settings.json and the record).</param>
    public static bool IsFirstStartInUsedFolder(string databasePath, string configDirectory)
    {
        if (File.Exists(Path.Combine(configDirectory, StartedMarkerName)))
            return false;

        return File.Exists(databasePath) || File.Exists(Path.Combine(configDirectory, "settings.json"));
    }

    /// <summary>Records, beside the record file, that this build has run its first-run step for the folder.</summary>
    private void WriteStartedMarker()
    {
        if (_filePath == null)
            return;

        var marker = Path.Combine(Path.GetDirectoryName(_filePath)!, StartedMarkerName);
        try
        {
            if (File.Exists(marker))
                return;

            Directory.CreateDirectory(Path.GetDirectoryName(marker)!);
            File.WriteAllText(marker,
                $"Lite first started with this data folder on {DateTime.UtcNow.ToString("u", CultureInfo.InvariantCulture)}. " +
                $"While this file exists, a missing {FileName} means Lite asks again about each server before it connects.");
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            AppLogger.Warn("ServerHostBindings", $"Could not write {marker}: {ex.Message}");
        }
    }

    private void Report(string? problem)
    {
        if (problem != null)
            SaveFailed?.Invoke(problem);
    }

    /// <summary>Writes the record; null on success, else the sentence to show (also kept in <see cref="SaveProblem"/>).</summary>
    private string? SaveLocked() => TryWrite(_bindings);

    private string? TryWrite(IReadOnlyDictionary<string, Binding> bindings)
    {
        if (_filePath == null)
            return null;

        try
        {
            var file = new BindingsFile();
            foreach (var (id, binding) in bindings)
            {
                file.Servers[id] = new BindingEntry
                {
                    Host = binding.Host,
                    Replica = binding.Replica,
                    AuthenticationType = binding.AuthenticationType,
                    CredentialProfileId = binding.CredentialProfileId,
                    DatabaseName = binding.DatabaseName,
                    UtilityDatabase = binding.UtilityDatabase,
                    EncryptMode = binding.EncryptMode,
                    TrustServerCertificate = binding.TrustServerCertificate,
                    AzureClientId = binding.AzureClientId,
                    ManagedIdentityClientId = binding.ManagedIdentityClientId
                };
            }

            Directory.CreateDirectory(Path.GetDirectoryName(_filePath)!);
            var temp = _filePath + ".tmp";
            File.WriteAllText(temp, JsonSerializer.Serialize(file, s_jsonOptions));
            File.Move(temp, _filePath, overwrite: true);
            _saveProblem = null;
            return null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* What is in memory still holds for this run. The user is told, and after a restart Lite asks again for what it
               could not keep. */
            AppLogger.Warn("ServerHostBindings", $"Could not save {_filePath}: {ex.Message}");
            _saveProblem =
                $"Lite could not save the server settings you confirmed to {_filePath}: {ex.Message} " +
                "What you confirm holds until Lite closes; after a restart Lite asks again before it connects.";
            return _saveProblem;
        }
    }

    private sealed class BindingsFile
    {
        public int Version { get; set; } = 3;
        public Dictionary<string, BindingEntry?> Servers { get; set; } = new();
    }

    private sealed class BindingEntry
    {
        public string? Host { get; set; }
        public string? Replica { get; set; }
        public string? AuthenticationType { get; set; }
        public string? CredentialProfileId { get; set; }
        public string? DatabaseName { get; set; }
        public string? UtilityDatabase { get; set; }
        public string? EncryptMode { get; set; }
        public bool? TrustServerCertificate { get; set; }
        public string? AzureClientId { get; set; }
        public string? ManagedIdentityClientId { get; set; }
    }
}

/// <summary>One confirmed setting that differs now: its name, the confirmed value and the value servers.json names.</summary>
public sealed record ServerSettingChange(string Name, string From, string To);

/// <summary>
/// A server whose settings are not the ones this user confirmed for it.
/// </summary>
/// <param name="Host">The host servers.json names now.</param>
/// <param name="ConfirmedHost">The host this user last confirmed; null when this user has never confirmed this server.</param>
/// <param name="Replica">The Query Store replica servers.json names now (empty when none).</param>
/// <param name="ReplicaChanged">True when the Query Store replica is set and is not the confirmed one.</param>
public sealed record ServerHostChange(string Host, string? ConfirmedHost, string Replica, bool ReplicaChanged)
{
    private const int MaxShownLength = 80;
    private const int ShownHead = 38;
    private const int ShownTail = 39;

    /// <summary>The Query Store replica this user last confirmed (empty when none).</summary>
    public string ConfirmedReplica { get; init; } = "";

    /// <summary>The other confirmed settings that differ now (sign-in method, profile, databases, encryption).</summary>
    public IReadOnlyList<ServerSettingChange> Settings { get; init; } = [];

    /// <summary>True when the host itself is not the confirmed one (or this user never confirmed one).</summary>
    public bool HostChanged => ConfirmedHost == null || !ServerHostBindings.SameHost(ConfirmedHost, Host);

    /// <summary>
    /// True when the server signs in with a password or client secret saved for it alone: the change is then answered by
    /// entering that secret again. Every other mode (Windows, the Entra modes, managed identity, a credential profile) is
    /// answered by confirming the change.
    /// </summary>
    public static bool AsksForSecret(ServerConnection server) =>
        string.IsNullOrEmpty(server.CredentialProfileId) &&
        (server.AuthenticationType == AuthenticationTypes.SqlServer ||
         server.AuthenticationType == AuthenticationTypes.ServicePrincipal);

    /// <summary>
    /// A value from servers.json as it is shown: control characters, line and paragraph separators and bidi controls become
    /// '?' as in <see cref="ServerHostSetting"/>'s refusal text, every other character outside printable ASCII is written as
    /// \uXXXX (so a look-alike letter cannot pass for the one it imitates), and text over 80 characters keeps its first 38 and
    /// last 39 around "...", so both ends of a long host name show.
    /// </summary>
    public static string Shown(string? value)
    {
        var text = value ?? "";
        var builder = new StringBuilder(text.Length);
        foreach (var c in text)
        {
            /* 0x2028 and 0x2029 are the line and paragraph separators; anything above 0x7E is outside printable ASCII. */
            if (IsHidden(c))
                builder.Append('?');
            else if (c > (char)0x7E)
                builder.Append("\\u").Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
            else
                builder.Append(c);
        }

        var cut = builder.ToString();
        return cut.Length > MaxShownLength ? cut[..ShownHead] + "..." + cut[^ShownTail..] : cut;
    }

    private static bool IsHidden(char c) =>
        (char.IsControl(c) && c != (char)0x09) || IsBidiControl(c) || c == (char)0x2028 || c == (char)0x2029;

    private static bool IsBidiControl(char c) =>
        c == (char)0x061C || c == (char)0x200E || c == (char)0x200F || (c >= (char)0x202A && c <= (char)0x202E) || (c >= (char)0x2066 && c <= (char)0x2069);

    /// <summary>What changed, as plain sentences, without the closing instruction.</summary>
    public string DescribeChange()
    {
        if (ConfirmedHost == null)
            return $"You have not connected to this server's host, {Shown(Host)}, from this Windows account yet.";

        var sentences = new List<string>();
        if (HostChanged)
            sentences.Add($"The host name for this server changed from {Shown(ConfirmedHost)} to {Shown(Host)}.");
        if (ReplicaChanged)
            sentences.Add($"The Query Store replica for this server changed from {(ConfirmedReplica.Length == 0 ? "none" : Shown(ConfirmedReplica))} to {Shown(Replica)}.");
        foreach (var setting in Settings)
            sentences.Add($"The {setting.Name} for this server changed from {Shown(setting.From)} to {Shown(setting.To)}.");
        return string.Join(" ", sentences);
    }

    /// <summary>The plain sentence the server's status, the connect prompt and the Edit dialog show.</summary>
    public string Describe(ServerConnection server)
    {
        var what = DescribeChange();

        if (!AsksForSecret(server))
            return what + " Open Edit Server and save to confirm it before Lite connects.";

        return server.AuthenticationType == AuthenticationTypes.ServicePrincipal
            ? what + " Enter the client secret again to connect to it."
            : what + " Enter the password again to connect to it.";
    }
}

/// <summary>
/// The Edit Server dialog's question before it connects (Test, the replica Test, or Save) to a server whose settings this
/// user has not confirmed, with a credential the user did not type in the dialog: the server's saved password, a credential
/// profile's secret, or the user's own Windows or Entra sign-in. Kept apart from the window so the decision can be tested.
/// </summary>
public static class HostConfirmationGate
{
    /// <summary>The settings a password typed in the dialog does not confirm: typing it says nothing about where it is sent.</summary>
    private static readonly HashSet<string> s_askedWithTypedSecret = new(StringComparer.Ordinal)
    {
        ServerHostBindings.DatabaseSetting,
        ServerHostBindings.UtilityDatabaseSetting,
        ServerHostBindings.EncryptionSetting,
        ServerHostBindings.CertificateSetting
    };

    /// <summary>
    /// The question to ask, or null when none is needed: the dialog adds a new server (the user typed every setting in it),
    /// the settings in the dialog are the confirmed ones, or the credential is a password or secret the user typed in the
    /// dialog just now (typing it is the confirmation, as on Save). A typed password does not confirm a change to the
    /// database, the utility database, the encryption or the certificate check: those are still asked about.
    /// </summary>
    /// <param name="change">The change between the dialog's settings and this user's record; null when they match.</param>
    /// <param name="isEdit">True for Edit Server, false for Add Server.</param>
    /// <param name="credential">What the connection signs in with, in words ("your Windows sign-in", "the credential profile
    /// 'Ops'", "the saved password").</param>
    /// <param name="secretTypedNow">True when the connection sends a password or client secret the user typed in the
    /// dialog, not one read from the saved credentials.</param>
    /// <param name="replica">True for the Query Store replica Test: the question names the replica.</param>
    /// <param name="confirmedInDialog">True when the user already answered Yes to this server's own question in this dialog: the
    /// replica Test then asks nothing more (#6559). That question names the replica when there is one.</param>
    public static string? Question(ServerHostChange? change, bool isEdit, string credential, bool secretTypedNow, bool replica = false, bool confirmedInDialog = false)
        => QuestionCore(change, isEdit, credential, secretTypedNow, replica, confirmedInDialog);

    /// <summary>What a connection in the given mode signs in with, in words for <see cref="Question"/>.</summary>
    public static string CredentialWords(string authenticationType, string? profileName)
    {
        if (profileName != null)
            return $"the credential profile '{ServerHostChange.Shown(profileName)}'";

        return authenticationType switch
        {
            AuthenticationTypes.Windows => "your Windows sign-in",
            AuthenticationTypes.SqlServer => "the saved SQL Server login",
            AuthenticationTypes.ServicePrincipal => "the saved client secret",
            AuthenticationTypes.ManagedIdentity => "this computer's managed identity",
            AuthenticationTypes.EntraDefaultCredential => "the Microsoft Entra sign-in on this computer",
            _ => "your Microsoft Entra sign-in"
        };
    }

    private static string? QuestionCore(ServerHostChange? change, bool isEdit, string credential, bool secretTypedNow, bool replica, bool confirmedInDialog)
    {
        if (change == null || !isEdit)
            return null;

        /* #6559: no replica typed, nothing to connect to, so nothing to ask; a question built from an empty name would be a blank. */
        if (replica && change.Replica.Length == 0)
            return null;

        /* The replica Test runs after the server's own Test, which already asked about these settings. */
        if (secretTypedNow)
        {
            return !replica && change.Settings.Any(setting => s_askedWithTypedSecret.Contains(setting.Name))
                ? $"Connect to {ServerHostChange.Shown(change.Host)} with these settings?\n\n" + change.DescribeChange()
                : null;
        }

        if (replica)
        {
            if (confirmedInDialog || (!change.ReplicaChanged && change.ConfirmedHost != null))
                return null;
            var wasReplica = change.ConfirmedReplica.Length == 0 ? "none" : ServerHostChange.Shown(change.ConfirmedReplica);
            return $"Connect to the Query Store replica {ServerHostChange.Shown(change.Replica)} (was {wasReplica}) with {credential}?";
        }

        var was = change.ConfirmedHost == null ? "not confirmed from this Windows account" : "was " + ServerHostChange.Shown(change.ConfirmedHost);
        /* #6559: a server never confirmed here has no "changed" replica to describe, so its one question names the replica too. */
        var withReplica = change.ConfirmedHost == null && change.Replica.Length > 0
            ? $" and the Query Store replica {ServerHostChange.Shown(change.Replica)}"
            : "";
        var question = $"Connect to {ServerHostChange.Shown(change.Host)}{withReplica} ({was}) with {credential}?";
        return change.ConfirmedHost != null && (!change.HostChanged || change.ReplicaChanged || change.Settings.Count > 0)
            ? question + "\n\n" + change.DescribeChange()
            : question;
    }
}

/// <summary>
/// Thrown in place of a connection string for a server whose settings this user has not confirmed (see
/// <see cref="ServerHostBindings"/>). An <see cref="InvalidOperationException"/>, so every caller that already reports a
/// connection string it could not resolve reports this one the same way.
/// </summary>
public sealed class ServerHostNotConfirmedException : InvalidOperationException
{
    public ServerHostNotConfirmedException(string message) : base(message)
    {
    }
}
