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
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One row of the servers table, as the viewer's server list shows it. Was a positional record; it is now
/// a class because the ported Lite server-row chrome needs mutable, change-notifying runtime state on each
/// row — the favorite star (<see cref="IsFavorite"/>, matched from the viewer's registry) and the
/// collection-freshness status dot (<see cref="IsOnline"/> / <see cref="HasCollectorErrors"/> /
/// <see cref="AwaitingFirstCollection"/> → <see cref="CardStatus"/> → <see cref="DotStatus"/>) update in
/// place on the refresh timers without resetting the list's selection.
/// The Postgres-sourced fields stay immutable (get-only); only the sidebar overlay state is settable.
/// Equality is now reference-based, which every consumer already relies on (they key on
/// <see cref="ServerId"/> or hold the list's own instances).
/// </summary>
public sealed class DarlingServer : INotifyPropertyChanged
{
    /// <param name="engineKind">
    /// <c>servers.engine_kind</c> as the store holds it (#2530) - one of
    /// <see cref="MonitoredEngineKind.All"/>, or null for a row no connect has stamped since the V82 rung
    /// landed. Defaulted so the two reader call sites are the only places that have to know the column
    /// exists, and so the test fakes that construct a SQL Server row keep compiling unchanged.
    /// </param>
    /// <param name="engineEdition">
    /// <c>servers.sql_engine_edition</c>. Carried beside the kind because
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> takes BOTH axes and asks kind first: a
    /// PostgreSQL target's edition is 0, which the edition axis correctly reads as "no claim". Passing 0
    /// here for a server whose edition has not been read is exactly that same silence.
    /// </param>
    public DarlingServer(
        int serverId,
        string serverName,
        string displayName,
        bool isEnabled,
        int? sqlMajorVersion,
        decimal monthlyCostUsd = 0,
        string? engineKind = null,
        int engineEdition = CollectorEngineCapability.UnknownEngineEdition)
    {
        ServerId = serverId;
        ServerName = serverName;
        DisplayName = displayName;
        IsEnabled = isEnabled;
        SqlMajorVersion = sqlMajorVersion;
        MonthlyCostUsd = monthlyCostUsd;
        EngineKind = engineKind;
        EngineEdition = engineEdition;
    }

    public int ServerId { get; }
    public string ServerName { get; }
    public string DisplayName { get; }
    public bool IsEnabled { get; }
    public int? SqlMajorVersion { get; }
    public decimal MonthlyCostUsd { get; }

    /// <summary>The raw <c>servers.engine_kind</c> token, or null when the store makes no claim. Kept raw
    /// rather than decoded to an enum so an unrecognised token written by a NEWER service survives the trip
    /// to <see cref="EngineDescription"/>, which shows the operator the literal string to search for.</summary>
    public string? EngineKind { get; }

    /// <summary><c>servers.sql_engine_edition</c>; <see cref="CollectorEngineCapability.UnknownEngineEdition"/>
    /// when the probe has not run or the target has no such property (every PostgreSQL target).</summary>
    public int EngineEdition { get; }

    /// <summary>
    /// True only when the store SAYS this target is PostgreSQL. Absence is not evidence for either engine,
    /// so a null or unrecognised token is false and the server keeps the SQL Server surface - the
    /// pre-#2530 behaviour, which is the only safe default for the servers that have not reconnected since
    /// the engine-kind rung landed.
    /// </summary>
    public bool IsPostgres => MonitoredEngineKind.IsPostgres(EngineKind);

    /// <summary>
    /// How the engine reads in the per-server header, or null when the store makes no claim - in which case
    /// the header shows NO engine label rather than "SQL Server", because the tabs such a server gets are a
    /// default rather than a finding. An unrecognised token renders as the raw token: the describer's
    /// "an unrecognised engine" is worded to sit mid-sentence in the capability messages and reads wrong as
    /// a label, and the literal string is what an operator would grep their store for.
    /// </summary>
    public string? EngineDescription =>
        !MonitoredEngineKind.IsKnown(EngineKind)
            ? (string.IsNullOrWhiteSpace(EngineKind) ? null : EngineKind.Trim())
            : MonitoredEngineKind.DescribeEngineKind(EngineKind);

    /// <summary>"SQL Server 2022"-style label for the server list; empty when the version is unknown.</summary>
    public string VersionLabel => ViewerDataService.SqlVersionLabel(SqlMajorVersion);

    // ── Runtime-only sidebar state (not from Postgres; drives the ported Lite server-row chrome) ──

    private bool _isFavorite;

    /// <summary>Whether the user pinned this server (from the viewer's registry, matched by name). Drives the star.</summary>
    public bool IsFavorite
    {
        get => _isFavorite;
        set
        {
            if (_isFavorite != value)
            {
                _isFavorite = value;
                OnPropertyChanged(nameof(IsFavorite));
            }
        }
    }

    private bool? _isOnline;

    /// <summary>Freshness "reachability": true = fresh/stale (dot green/amber), false = offline (red), null = unknown.</summary>
    public bool? IsOnline
    {
        get => _isOnline;
        set
        {
            if (_isOnline != value)
            {
                _isOnline = value;
                OnPropertyChanged(nameof(IsOnline));
                RaiseDotChanged();
            }
        }
    }

    private bool _hasCollectorErrors;

    /// <summary>Warning (amber) state — in the viewer this means the collection has gone stale.</summary>
    public bool HasCollectorErrors
    {
        get => _hasCollectorErrors;
        set
        {
            if (_hasCollectorErrors != value)
            {
                _hasCollectorErrors = value;
                OnPropertyChanged(nameof(HasCollectorErrors));
                RaiseDotChanged();
            }
        }
    }

    private bool _awaitingFirstCollection;

    /// <summary>
    /// True when no collection has EVER landed for this server: the service hasn't reached it yet — a
    /// registered-but-queued server during bootstrap, not a dead one. The row had no such flag until #2473,
    /// which is precisely why its dot went grey "Unknown" while the Overview card one panel over said amber
    /// "Awaiting first collection" about the same server, off the same freshness call.
    /// </summary>
    public bool AwaitingFirstCollection
    {
        get => _awaitingFirstCollection;
        set
        {
            if (_awaitingFirstCollection != value)
            {
                _awaitingFirstCollection = value;
                OnPropertyChanged(nameof(AwaitingFirstCollection));
                RaiseDotChanged();
            }
        }
    }

    /// <summary>
    /// The sidebar row's status, as a VALUE — the same <see cref="ServerCollectionStatus"/> the Overview card
    /// renders (#2473). This row used to derive its own copy of the status ladder from the same flags, on a
    /// different type, on a different surface, and with only four of the five states: the sidebar and the card
    /// could therefore say different things about one server, and for a never-collected server they did. Both
    /// now read <see cref="ServerCollectionStatusRules.Classify"/>, which is the collapse #2429 argued for —
    /// with one discriminant there is no flag combination left for the renderings to disagree about.
    /// </summary>
    public ServerCollectionStatus CardStatus =>
        ServerCollectionStatusRules.Classify(IsOnline, HasCollectorErrors, AwaitingFirstCollection);

    /// <summary>
    /// Sidebar status-dot vocabulary — the SAME words the Overview card's <c>StatusDisplay</c> shows, because
    /// both render <see cref="CardStatus"/>. They are also the <c>DataTrigger</c> values MainWindow.xaml keys
    /// the Ellipse fill off, so a state with no trigger falls through to the muted grey default rather than
    /// failing anything — which is how "Awaiting first collection" was silently grey. The trigger set is
    /// pinned against the enum in <c>ViewerSidebarDotRendersTheCardStatusTests</c>.
    /// </summary>
    public string DotStatus => CardStatus.Word();

    /// <summary>
    /// What the dot means, for the ToolTip the sidebar Ellipse carries — the answer to #2422 one surface over,
    /// and the thing that makes an amber dot legible at all. The card's amber covers two states (a stale
    /// collection and a never-collected server) and the card disambiguates them with a WORD; a dot has no room
    /// for one, so the tooltip does that job instead. The first line is
    /// <see cref="ServerCollectionStatusRules.Headline"/>, word-for-word what the Overview card says for the
    /// same state.
    ///
    /// <para>The second line names the axis. In Darling every one of these words is a COLLECTION answer —
    /// there is no live ping to a monitored server — which is the opposite of Lite, where the identically
    /// coloured dot reports a connection check. A reader who uses both should not have to infer which. The
    /// third names the gesture THIS surface supports: <c>ServerList_MouseDoubleClick</c> opens the tab, and a
    /// single click only selects, so naming one would be naming a no-op.</para>
    /// </summary>
    public string DotTooltip => string.Join(
        "\n",
        CardStatus.Headline(),
        "Darling has no live ping: this is how old the newest collection is, not a connection check.",
        "Double-click the row to open this server's tab");

    /// <summary>Raises the change notifications for everything derived from the three status flags. One
    /// helper rather than three call sites per setter: <see cref="DotTooltip"/> was added after
    /// <see cref="DotStatus"/>, and a derived member that a setter forgets to announce is a dot that stops
    /// updating in place — silent, and only visible on a list refresh.</summary>
    private void RaiseDotChanged()
    {
        OnPropertyChanged(nameof(CardStatus));
        OnPropertyChanged(nameof(DotStatus));
        OnPropertyChanged(nameof(DotTooltip));
    }

    /// <summary>
    /// Sets the dot from the same collection-freshness classification the Overview cards use
    /// (<see cref="ServerSummaryItem.ClassifyFreshness"/>), through the same
    /// <see cref="ServerCollectionStatusRules.FlagsFor"/> mapping: Fresh → Online, Stale → the amber Warning,
    /// Offline → red, NeverCollected → the amber "Awaiting first collection" (the service hasn't reached the
    /// server yet — during a fleet bootstrap that is "queued", not "dead"). Both instants are UTC (the store
    /// is naive UTC; nowUtc is <see cref="DateTime.UtcNow"/>).
    ///
    /// <para>This method used to set two flags out of three by hand and drop the awaiting marker on the floor.
    /// Nothing about a block of assignments makes a missing one visible, which is why the flags now arrive as
    /// a single value (#2473).</para>
    /// </summary>
    public void ApplyFreshness(DateTime? lastCollectionUtc, DateTime nowUtc)
    {
        var flags = ServerCollectionStatusRules.FlagsFor(
            ServerSummaryItem.ClassifyFreshness(lastCollectionUtc, nowUtc));
        IsOnline = flags.IsOnline;
        HasCollectorErrors = flags.HasCollectorErrors;
        AwaitingFirstCollection = flags.AwaitingFirstCollection;
    }

    // ── Per-server alert "needs attention" badge state (from the polled alert history, ack-aware) ──
    // Set by MainWindow.UpdateServerAttention from the ServerAttentionDeriver output, gated through the
    // viewer-local ViewerAlertStateService (ack-until-worse). Drives the sidebar row badge, mirroring
    // Lite's per-tab alert badge.

    private int _attentionCount;
    private bool _attentionIsCritical;
    private string? _attentionTooltip;

    /// <summary>Active (unacknowledged, un-muted) alert count for this server; 0 hides the badge.</summary>
    public int AttentionCount => _attentionCount;

    /// <summary>True when the badge should show (there is at least one active alert to surface).</summary>
    public bool HasAttention => _attentionCount > 0;

    /// <summary>The badge count text, capped like Lite's tab badge ("99+" past 99).</summary>
    public string AttentionText => _attentionCount > 99
        ? "99+"
        : _attentionCount.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Badge severity vocabulary — "Critical" / "Warning" / "None" — so the sidebar row's DataTriggers colour
    /// the badge (ErrorBrush / WarningBrush) the same way Lite maps deadlock→red, else→orange-red.
    /// </summary>
    public string AttentionSeverity => _attentionCount <= 0
        ? "None"
        : (_attentionIsCritical ? "Critical" : "Warning");

    /// <summary>The badge tooltip: the metric breakdown for this server (empty when no attention).</summary>
    public string? AttentionTooltip => _attentionTooltip;

    /// <summary>
    /// Updates the sidebar alert badge in place (no list reset). Pass <paramref name="count"/> 0 to clear it
    /// (acknowledged / no alerts). Raises change notifications only when something actually changed.
    /// </summary>
    public void SetAttention(int count, bool isCritical, string? tooltip)
    {
        if (_attentionCount == count
            && _attentionIsCritical == isCritical
            && string.Equals(_attentionTooltip, tooltip, StringComparison.Ordinal))
        {
            return;
        }

        _attentionCount = count;
        _attentionIsCritical = isCritical;
        _attentionTooltip = tooltip;

        OnPropertyChanged(nameof(AttentionCount));
        OnPropertyChanged(nameof(HasAttention));
        OnPropertyChanged(nameof(AttentionText));
        OnPropertyChanged(nameof(AttentionSeverity));
        OnPropertyChanged(nameof(AttentionTooltip));
    }

    // ── Whole-server alert silence indicator (#2031) ──
    // Set by MainWindow's silenced-state refresh (riding the same alert poll that drives the badge) and
    // immediately by the Silence/Unsilence handlers. Drives the sidebar row's muted-bell glyph and the
    // context menu's Silence/Unsilence exclusivity.

    private bool _isSilenced;

    /// <summary>True when a whole-server alert silence (store-side mute rule) is active for this server.</summary>
    public bool IsSilenced => _isSilenced;

    /// <summary>Updates the silenced indicator in place; raises change notification only on a real flip.</summary>
    public void SetSilenced(bool silenced)
    {
        if (_isSilenced == silenced)
        {
            return;
        }

        _isSilenced = silenced;
        OnPropertyChanged(nameof(IsSilenced));
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

/// <summary>
/// The viewer's reads of the Darling Postgres store — the server list, plus the surfaces in the
/// partials (the Overview lanes' total-wait + memory trends and per-lane baselines in
/// <c>ViewerDataService.OverviewLanes.cs</c>, the per-tab reads in <c>.Cpu.cs</c>, <c>.Waits.cs</c>,
/// <c>.BlockingTrends.cs</c>, <c>.FileIo.cs</c>, <c>.TempDb.cs</c>, <c>.Config.cs</c>, and
/// <c>.RunningJobs.cs</c>, the Daily Summary + Collection Health reads in <c>.DailySummary.cs</c> /
/// <c>.CollectionHealth.cs</c>, and the wave-2/3 reads in <c>.QueryStats.cs</c>, <c>.Blocking.cs</c>,
/// <c>.Findings.cs</c>, <c>.AlertHistory.cs</c>, and <c>.MuteRules.cs</c>).
/// Connections come from a pooled <see cref="NpgsqlDataSource"/>, so the window can run its
/// per-tab queries concurrently. The SQL lives in public constants so tests can pin the
/// load-bearing clauses without a live Postgres.
/// All timestamps in the store are naive UTC (`timestamp without time zone`), so DateTime
/// parameters are sent with DateTimeKind.Unspecified — since Npgsql 6.0 a Kind=Utc DateTime
/// maps strictly to timestamptz and throws against naive columns.
/// </summary>
public sealed partial class ViewerDataService : IAsyncDisposable
{
    /// <summary>
    /// The observed server registry. <c>engine_kind</c> (#2530) and <c>sql_engine_edition</c> ride along
    /// because the per-server tab set and every PostgreSQL panel's own explanation are derived from them -
    /// see <see cref="ViewerPostgresTabs"/>. Both are read here AND in
    /// <c>ViewerDataService.MonitoredServers.cs</c>'s <c>ManagedServersSql</c>: that one is what the sidebar
    /// actually uses on a seeded store, so a discriminator added to only this query would have left every
    /// real deployment on the SQL Server tab set.
    /// </summary>
    public const string ServersSql =
        "SELECT server_id, server_name, display_name, is_enabled, sql_major_version, COALESCE(monthly_cost_usd, 0), engine_kind, COALESCE(sql_engine_edition, 0) FROM servers ORDER BY display_name";

    /// <summary>
    /// The authoritative read-only probe (V8 security hardening): does the connected role hold INSERT
    /// on a <c>config</c> table? True → the admin role (or an owner) — the mute / alert-dismiss /
    /// analysis-mute writes are available. False → the read-only viewer role — those surfaces degrade.
    /// This is the source of truth over <c>connectAs</c> (which only picks a credential and doesn't
    /// apply in BYO mode), because it reflects the connection's ACTUAL privileges. The bare table name
    /// resolves through search_path to <c>config.config_mute_rules</c>.
    /// </summary>
    public const string ReadOnlyProbeSql = "SELECT has_table_privilege('config_mute_rules', 'INSERT')";

    private readonly NpgsqlDataSource _dataSource;

    /// <param name="connectionString">The Postgres connection string (managed-derived or BYO from darling.json).</param>
    /// <param name="connectionTimeoutSeconds">
    /// The viewer's "Connection timeout" preference (<see cref="ViewerAppSettings.ConnectionTimeoutSeconds"/>,
    /// Lite's "increase for VPN/remote" knob). When supplied it is applied to the pooled store connections as
    /// Npgsql's connect <c>Timeout</c> — set on the managed-derived string (which carries none) and appended to a
    /// BYO string only when the operator did not already specify one (their explicit Timeout wins). Null leaves
    /// the string untouched (Npgsql's 15s default). This restores the setting the Settings window has always
    /// saved but nothing consumed.
    /// </param>
    public ViewerDataService(string connectionString, int? connectionTimeoutSeconds = null)
    {
        var effectiveConnectionString = connectionTimeoutSeconds is int seconds
            ? ApplyConnectionTimeout(connectionString, seconds)
            : connectionString;
        _dataSource = NpgsqlDataSource.Create(effectiveConnectionString);
        StoreIsOnThisMachine = StoreHostIsLoopback(connectionString);
    }

    /// <summary>
    /// Whether this viewer's store is reached over loopback — the best available proxy for "the Darling service
    /// runs on THIS machine" (#2279).
    ///
    /// <para><b>Why it is a proxy for that at all.</b> A DPAPI blob is <c>LocalMachine</c>-scoped, so a
    /// credential this viewer encrypts is decryptable only on this machine, and the service is the thing that
    /// has to decrypt it. The managed deploy builds its store connection on literal <c>127.0.0.1</c>
    /// (<c>ViewerSettings</c>, matching the service's own <c>DarlingManagedPostgres.BuildConnectionString</c>),
    /// and the service runs where its managed store runs. So a loopback store means viewer and service share a
    /// machine and a saved credential will work — which is the single-box deploy the DPAPI design targets, and
    /// where a warning would be pure noise.</para>
    ///
    /// <para><b>What it deliberately does not claim.</b> A non-loopback store does NOT prove the viewer is
    /// remote — a bring-your-own store on another host with the service local is a real configuration, and it
    /// reads as false here. That is why #2279 warns rather than refuses: this signal is good enough to decide
    /// whether to SAY something, and not good enough to decide whether to BLOCK. Getting that backwards would
    /// refuse a legitimate first-run Add on the service host.</para>
    /// </summary>
    public bool StoreIsOnThisMachine { get; }

    /// <summary>
    /// True when a store connection string names a loopback host, or names none at all (#2279).
    ///
    /// <para>Static and pure so the rule is testable without a store — it is the whole basis of the warning, and
    /// a viewer cannot be stood up in a unit test. Uses the base <see cref="DbConnectionStringBuilder"/> rather
    /// than Npgsql's, for the reason documented on <see cref="ApplyConnectionTimeout"/>: the Npgsql builder
    /// answers <c>ContainsKey</c> for every KNOWN key rather than only the present ones, so it cannot tell an
    /// omitted host from a specified one.</para>
    ///
    /// <para>An omitted host counts as loopback because that is what it MEANS — Npgsql defaults to localhost, so
    /// a string with no <c>Host</c> is a local store and must not warn. <c>::1</c> and <c>localhost</c> are
    /// included alongside <c>127.0.0.1</c> even though the managed path always writes the literal IPv4 form,
    /// because a hand-written BYO string pointing at the local box is still local and warning about it would be
    /// wrong.</para>
    /// </summary>
    internal static bool StoreHostIsLoopback(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return true;
        }

        string? host = null;
        try
        {
            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            if (builder.ContainsKey("Host"))
            {
                host = builder["Host"]?.ToString();
            }
            else if (builder.ContainsKey("Server"))
            {
                host = builder["Server"]?.ToString();
            }
        }
        catch (ArgumentException)
        {
            /* An unparseable string is not evidence the store is remote, and this decides only whether to show
               a hint — so fail toward silence rather than toward a warning nobody can act on. */
            return true;
        }

        if (string.IsNullOrWhiteSpace(host))
        {
            return true;
        }

        var trimmed = host.Trim();
        return trimmed.Equals("127.0.0.1", StringComparison.Ordinal)
            || trimmed.Equals("::1", StringComparison.Ordinal)
            || trimmed.Equals("localhost", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Applies the viewer's connect-timeout preference to a connection string, but ONLY when the string does not
    /// already specify a <c>Timeout</c> — so an operator's explicit Timeout in a BYO <c>postgres.connectionString</c>
    /// always wins, while the managed-derived string (which never sets one) and a BYO string that omits it both
    /// pick up the preference. <paramref name="timeoutSeconds"/> is Npgsql's connect timeout in seconds (the
    /// caller clamps it to 5–60). Pure + string-only, so it is unit-tested without a live Postgres. Detection
    /// uses the base <see cref="DbConnectionStringBuilder"/>, whose <c>ContainsKey</c> reflects exactly the keys
    /// present in the string (NpgsqlConnectionStringBuilder overrides ContainsKey to answer for every KNOWN
    /// keyword, which can't tell "set" from "settable").
    /// </summary>
    internal static string ApplyConnectionTimeout(string connectionString, int timeoutSeconds)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
        if (!builder.ContainsKey("Timeout"))
        {
            builder["Timeout"] = timeoutSeconds;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// True when the connected role cannot write the operator-config tables (the read-only
    /// <c>viewer</c> role, or any connection lacking config INSERT). Set by
    /// <see cref="DetectReadOnlyAsync"/>; the write surfaces gate on it. Defaults false (writable) until
    /// probed, then fails safe to true if the probe cannot run.
    /// </summary>
    public bool IsReadOnly { get; private set; }

    /// <summary>
    /// Runs the <see cref="ReadOnlyProbeSql"/> capability probe and records <see cref="IsReadOnly"/>.
    /// Called once after the service connects, before the write affordances are shown. A probe that
    /// throws (table missing on a mis-provisioned store, permission quirk, transient error) fails safe
    /// to read-only, so the UI hides writes rather than dead-clicking into a permission error; the
    /// reactive 42501 catch in the write paths is the backstop if the probe and reality ever disagree.
    /// </summary>
    public async Task<bool> DetectReadOnlyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(ReadOnlyProbeSql);
            var canInsert = await command.ExecuteScalarAsync(cancellationToken);
            IsReadOnly = canInsert is not true;
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            /* Finding B3: a failure to REACH the store is not a read-only seat — surface it as unreachable so
               the shell shows the "is the service running?" message instead of hiding every write affordance
               behind a false read-only verdict. */
            throw new ViewerStoreUnreachableException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A server RESPONSE we couldn't interpret (a permission quirk, a missing table on a
               mis-provisioned store, a transient error) fails safe to read-only, so the UI hides writes
               rather than dead-clicking into a permission error. */
            IsReadOnly = true;
        }

        return IsReadOnly;
    }

    /// <summary>
    /// Opens one real connection to prove the store is reachable, so the shell can tell an UNREACHABLE store
    /// (the service down, or a wrong host/port/database in darling.json's postgres section) apart from a legit
    /// read-only seat — the two the old connect path collapsed into a false "read-only" verdict (finding B3).
    /// A connection-level failure is rethrown as <see cref="ViewerStoreUnreachableException"/>; a server that
    /// RESPONDS (even to refuse authentication) is "reachable" and any such error propagates for the caller's
    /// generic handling. The <see cref="NpgsqlDataSource"/> is lazy, so this is the first live connect.
    /// </summary>
    public async Task EnsureStoreReachableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        }
        catch (Exception ex) when (IsConnectionFailure(ex))
        {
            throw new ViewerStoreUnreachableException(ex);
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> is a failure to TALK to Postgres (socket refused, host not found,
    /// connect timeout) rather than a server error RESPONSE. Npgsql surfaces a server response as a
    /// <see cref="PostgresException"/> (it carries a SQLSTATE); any other <see cref="NpgsqlException"/> — or a
    /// bare socket / timeout — means the server was never reached. Drives the unreachable-vs-read-only split
    /// (finding B3), so a down store shows "is the service running?" not a false read-only verdict.
    /// </summary>
    internal static bool IsConnectionFailure(Exception ex) =>
        (ex is NpgsqlException and not PostgresException)
        || ex is System.Net.Sockets.SocketException
        || ex is TimeoutException;

    /// <summary>
    /// Probes the store's effective schema version through <c>information_schema</c> (plus <c>pg_indexes</c>
    /// for the one index sentinel) — the version the viewer gates on at connect (finding B1). The authoritative
    /// <c>darling_schema_version</c> table is owner-only (a viewer/admin role can't read it, and even
    /// <c>MAX(version)</c> can itself throw 42501), so rather than read the version number this checks whether
    /// the store carries the schema OBJECTS the recent migrations added — objects any role can see. Newest
    /// first: V27's <c>deadlocks.database_name</c> column (the Azure per-database watermark key, #1535),
    /// V26's <c>config_notification.generic_url</c> column (the generic webhook channel, #1506), V25's
    /// <c>agent_status</c> table, V24's <c>job_history</c> table, V22's <c>idx_index_object_stats_latest</c> index (the FinOps Index Analysis
    /// supporting index — indexes are not listed in <c>information_schema</c>, so this one reads the
    /// world-readable <c>pg_indexes</c> catalog), V21's <c>default_trace_events</c> table (the shared Default
    /// Trace collector), V20's <c>notify_connection_changes</c> column (config_alert_settings), V19's
    /// <c>analysis_state</c> table, V18's <c>alert_delivery_mode_override</c> column (the very column whose
    /// absence throws the raw 42703 the finding reproduces on Add/Edit Server), and V17's
    /// <c>config_monitored_servers</c> table (the config control plane). <see cref="MapProbedSchemaVersion"/>
    /// reduces the flags to the highest satisfied version. Each sentinel appears only in a store at its
    /// migration or later: a fresh store gets them all at once (it runs straight through to the latest
    /// version), an upgraded store gets each exactly at its migration.
    ///
    /// <para>The seventh sentinel (V23) is different in kind: V23 makes <c>collection_log</c> a TimescaleDB
    /// hypertable, and its only schema effect is engine-dependent. So this checks <c>collection_log</c> IS a
    /// hypertable OR the store has no <c>timescaledb</c> extension — because on plain PostgreSQL V23 is a no-op
    /// (the guarded migration skips the conversion), so a plain-PG store at V23 is object-identical to V22 and
    /// must not be gated. <b>Crucially it is written PLAIN-POSTGRESQL-SAFE:</b> it reads the core <c>pg_inherits</c>
    /// catalog for collection_log's chunk children (TimescaleDB 2.x links chunks to the hypertable root via
    /// inheritance — note a freshly-converted, still-empty hypertable has no chunks yet, a negligible window since
    /// collection_log gets a row every collection cycle) rather than <c>timescaledb_information.hypertables</c> — that view does not exist without the
    /// extension, and referencing it here would make the WHOLE probe throw on plain PG (returning null → the
    /// gate fails open → EVERY plain-PG store, even genuinely-behind ones, loses connect-time gating). This
    /// composite is only meaningful once V22's index is present (its <c>NOT EXISTS timescaledb</c> arm is true
    /// for ALL plain-PG stores regardless of version), so <see cref="MapProbedSchemaVersion"/> gates it behind
    /// the V22 sentinel rather than treating it as a standalone newest-first arm.
    /// </summary>
    public const string StoreSchemaProbeSql = @"
SELECT
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'config_monitored_servers'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_monitored_servers' AND column_name = 'alert_delivery_mode_override'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'analysis_state'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_connection_changes'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'default_trace_events'),
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_index_object_stats_latest'),
    (
        EXISTS (SELECT 1 FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'collection_log' AND n.nspname = 'collect')
        OR NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')
    ),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'job_history'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'agent_status'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'generic_url'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'deadlocks' AND column_name = 'database_name'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_stats' AND column_name = 'replica_role'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'long_query_completions'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'web_enabled'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'custom_views'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'server_tags'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_connection_down_at_startup'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'ag_database_replica_states'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'notify_ag_health'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'ag_disconnect_refire_minutes'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'ag_database_replica_states' AND column_name = 'est_send_drain_time_min'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_plan_dim'),
    EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ix_query_stats_digest_floor'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'blocking_wait_seconds_threshold'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_stats' AND column_name = 'runtime_stats_interval_id'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'pagerduty_routing_key'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_notification' AND column_name = 'pagerduty_proxy'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'collector_state'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'plan_correction'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pvs_stats'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'pvs_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'database_state_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'server_tags' AND column_name = 'colour'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_stats' AND column_name = 'host_object_name'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'analysis_findings' AND column_name = 'drill_down_json'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'store_metrics'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_plan_dim' AND column_name = 'query_plan_gz'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'self_disk_free_warn_percent'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'store_metrics' AND column_name = 'last_run_duration_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'store_job_cadence_warn_percent'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'query_store_backfill_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'query_store_text_budget_mb'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'database_state_expected' AND column_name = 'last_alerted_state'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'incident_occurrences'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'plan_xml_compression'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_monitored_servers' AND column_name = 'engine'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_blocking_edges'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_plan_map'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_statement_text'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_text'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'plan_content_retention_days'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'query_store_health'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'query_store_text' AND column_name = 'query_hash'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_service' AND column_name = 'compose_statement_timeout_seconds'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'config_alert_settings' AND column_name = 'file_growth_enabled'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'collection_log' AND column_name = 'slowest_item_ms'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'tempdb_stats' AND column_name = 'max_size_mb'),
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'servers' AND column_name = 'engine_kind'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_database_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_index_usage_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_table_bloat_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_session_states'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_plan_capture_readiness'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_write_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_extension_availability'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_lock_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_column_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_replication_stats'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_buffer_usage'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_index_bloat'),
    /* V95 probes a COLUMN, not a table. The three tables it touches all already exist at V94, so table
       existence cannot separate the rungs and information_schema.columns is the only sentinel that can. */
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_column_stats'
                                                     AND   column_name = 'database_name'),
    EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_name = 'pg_wait_sampling')";

    /// <summary>The store schema version this viewer build requires — the highest migration it knows
    /// (<see cref="StorageVersion.SchemaVersion"/>). The connect-time gate blocks a store below this.</summary>
    public static int RequiredStoreSchemaVersion => StorageVersion.SchemaVersion;

    /// <summary>
    /// Reads the store's effective schema version via <see cref="StoreSchemaProbeSql"/>. Returns null when it
    /// can't be determined (any non-cancellation error — an unreachable store, an <c>information_schema</c>
    /// quirk) so the connect-time gate FAILS OPEN: a possibly-healthy store is never blocked by a probe
    /// hiccup. A truly-down store is caught first by <see cref="EnsureStoreReachableAsync"/> (finding B3), and
    /// the executor 42703/42P01 translation (finding B2) is the write-path backstop.
    /// </summary>
    public async Task<int?> GetStoreSchemaVersionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(StoreSchemaProbeSql);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                return MapProbedSchemaVersion(reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10), reader.GetBoolean(11), reader.GetBoolean(12), reader.GetBoolean(13), reader.GetBoolean(14), reader.GetBoolean(15), reader.GetBoolean(16), reader.GetBoolean(17), reader.GetBoolean(18), reader.GetBoolean(19), reader.GetBoolean(20), reader.GetBoolean(21), reader.GetBoolean(22), reader.GetBoolean(23), reader.GetBoolean(24), reader.GetBoolean(25), reader.GetBoolean(26), reader.GetBoolean(27), reader.GetBoolean(28), reader.GetBoolean(29), reader.GetBoolean(30), reader.GetBoolean(31), reader.GetBoolean(32), reader.GetBoolean(33), reader.GetBoolean(34), reader.GetBoolean(35), reader.GetBoolean(36), reader.GetBoolean(37), reader.GetBoolean(38), reader.GetBoolean(39), reader.GetBoolean(40), reader.GetBoolean(41), reader.GetBoolean(42), reader.GetBoolean(43), reader.GetBoolean(44), reader.GetBoolean(45), reader.GetBoolean(46), reader.GetBoolean(47), reader.GetBoolean(48), reader.GetBoolean(49), reader.GetBoolean(50), reader.GetBoolean(51), reader.GetBoolean(52), reader.GetBoolean(53), reader.GetBoolean(54), reader.GetBoolean(55), reader.GetBoolean(56), reader.GetBoolean(57), reader.GetBoolean(58), reader.GetBoolean(59), reader.GetBoolean(60), reader.GetBoolean(61), reader.GetBoolean(62), reader.GetBoolean(63), reader.GetBoolean(64), reader.GetBoolean(65), reader.GetBoolean(66), reader.GetBoolean(67), reader.GetBoolean(68), reader.GetBoolean(69), reader.GetBoolean(70), reader.GetBoolean(71));
            }

            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// Maps the <see cref="StoreSchemaProbeSql"/> sentinels to the store's effective version, newest present
    /// wins. V23 is folded INTO the V22 arm rather than being a standalone newest-first arm: once the V22 index
    /// is present, <paramref name="hasCollectionLogHypertableOrPlainPg"/> (collection_log is a hypertable, OR
    /// the store is plain PostgreSQL where V23 is an object-invisible no-op) → 23, else → 22. It cannot be a
    /// top arm because its plain-PG side is true for EVERY plain-PG store regardless of version, so it must be
    /// gated behind V22's engine-agnostic index. Below V22: <paramref name="hasDefaultTraceEvents"/> (V21) → 21,
    /// else <paramref name="hasAlertTuningKnobs"/> (V20) → 20, else <paramref name="hasAnalysisState"/> (V19) →
    /// 19, else <paramref name="hasAlertDeliveryOverride"/> (V18) → 18, else
    /// <paramref name="hasConfigControlPlane"/> (V17) → 17, else 16 — the "older than the V17 config control
    /// plane" floor (the exact pre-17 version isn't probed, but it is below what the viewer needs). Pure, so it
    /// is unit-tested without a live store; any schema bump past the newest arm trips the pinning test that keeps
    /// this in step with <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    internal static int MapProbedSchemaVersion(bool hasConfigControlPlane, bool hasAlertDeliveryOverride, bool hasAnalysisState, bool hasAlertTuningKnobs, bool hasDefaultTraceEvents, bool hasIndexObjectStatsLatestIndex, bool hasCollectionLogHypertableOrPlainPg, bool hasJobHistory, bool hasAgentStatus, bool hasGenericWebhook, bool hasDeadlocksDatabaseName, bool hasQueryStoreReplicaRole, bool hasLongQueryCompletions, bool hasWebDashboardConfig, bool hasCustomViews, bool hasServerTags, bool hasConnectionRefireKnobs = false, bool hasAgCollectors = false, bool hasAgAlertKnobs = false, bool hasAgLatencyColumns = false, bool hasAgDisconnectRefire = false, bool hasPayloadDimensions = false, bool hasDimFloorIndexes = false, bool hasBlockingWaitThreshold = false, bool hasQueryStoreIntervalIdentity = false, bool hasPagerDutyWebhook = false, bool hasPagerDutyProxy = false, bool hasCollectorState = false, bool hasPlanCorrection = false, bool hasPvsStats = false, bool hasPvsPressureKnobs = false, bool hasDatabaseStateAlert = false, bool hasServerTagColour = false, bool hasQueryStatsHostObject = false, bool hasFindingDrillDown = false, bool hasStoreMetrics = false, bool hasPlanDimGzip = false, bool hasSelfAlertKnobs = false, bool hasJobMetricsColumns = false, bool hasJobCadenceKnob = false, bool hasBackfillSwitch = false, bool hasCollectorMemoryKnobs = false, bool hasDatabaseStateEdgeMemory = false, bool hasIncidentOccurrences = false, bool hasPlanXmlCompressionKnob = false, bool hasMonitoredServerEngine = false, bool hasPgBlockingEdges = false, bool hasQueryStorePlanMap = false, bool hasPgStatementText = false, bool hasQueryStoreText = false, bool hasPlanContentRetentionKnob = false, bool hasQueryStoreHealth = false, bool hasQueryStoreTextHash = false, bool hasComposeTimeoutKnob = false, bool hasFileGrowthAlert = false, bool hasCollectionLogFanoutRollup = false, bool hasTempDbMaxSize = false, bool hasServerEngineKind = false, bool hasPgDatabaseStats = false, bool hasPgIndexUsageStats = false, bool hasPgTableBloatStats = false, bool hasPgSessionStates = false, bool hasPgPlanCaptureReadiness = false, bool hasPgWriteStats = false, bool hasPgExtensionAvailability = false, bool hasPgLockStats = false, bool hasPgColumnStats = false, bool hasPgReplicationStats = false, bool hasPgBufferUsage = false, bool hasPgIndexBloat = false, bool hasPgPerDatabaseAttribution = false, bool hasPgWaitSampling = false)
    {
        /* V71 (the PostgreSQL blocking-edges rung): a table-existence sentinel and now the newest-first arm.
           A collector table would ordinarily get no arm at all — see the V63-V69 note below — but the TOP
           rung always needs one, whatever it happens to be, because RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion and a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch on a store that is current. Being a collector table makes it no less
           reliable as a sentinel; it simply is not interesting for any other reason.

           Deliberately NOT spelled with its collect.<table> name here, and this is the only place in the
           file where that matters. ViewerCollectorCoverageTests scans this reader layer for collector table
           names by plain substring to decide which tables the viewer actually reads; it strips the probe's
           information_schema lines precisely so a migration sentinel cannot fake coverage, but a PROSE
           mention has no such line to strip. Naming the table in this comment made the table look read,
           which silently exempted it from the coverage ratchet — the exact failure that pin exists to
           catch, reported as a stale allow-list entry. */
        /* V72 (the Query Store plan map, #2210): table-existence sentinel, and it has to sit ABOVE the V71
           arm because these are evaluated newest-first — a V72 store also has V71's table, so testing V71
           first would report every V72 store as V71 and the viewer would show a spurious upgrade banner
           against a store that is actually current.

           The gate matters rather than being bookkeeping: the SERVICE writes this map on every plan fetch and
           resolves plan XML through it, so a viewer pointed at a V71 store while the service expects V72 would
           find no map rows and render every plan as "not yet collected" — indistinguishable from a healthy
           store that simply has not fetched yet, which is the one failure mode this design works hardest to
           avoid being silent about.

           The table is named in the probe line above and deliberately NOT in this prose, per the V71 arm's
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment, so a
           prose mention would exempt the table from it. */
        /* #2150: newest first, and the arm below STAYS for the same reason every previous one did — a store
           migrated to exactly 73 must map to 73 rather than falling through. The table is named only in the
           probe line, not in this prose, per the V71 finding: the coverage ratchet strips
           information_schema lines but cannot strip a comment, so a prose mention would exempt the table
           from it. */
        /* #2316: newest first. The arm below STAYS — a store migrated to exactly 74 must map to 74
           rather than falling through. The column is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a
           comment, so a prose mention would exempt it. */
        /* #2319: newest first. The arm below STAYS — a store migrated to exactly 75 must map to 75
           rather than falling through. The table is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip
           a comment, so a prose mention would exempt it. */
        /* #2312: newest first. The arm below STAYS — a store migrated to exactly 76 must map to 76
           rather than falling through. The column is named only in the probe line, not in this prose,
           per the V71 finding: the coverage ratchet strips information_schema lines but cannot strip
           a comment, so a prose mention would exempt it. */
        /* #2357: newest first. The arm below STAYS — a store migrated to exactly 77 must map to 77 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment, so a
           prose mention would exempt it. */
        /* #2349: newest first. The arm below STAYS — a store migrated to exactly 78 must map to 78 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment. */
        /* #2472: newest first. The arm below STAYS — a store migrated to exactly 79 must map to 79 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           This rung's gate earns its place rather than being bookkeeping. The service writes the fan-out
           rollup on every productive per-database run and Collection Health reads it; a viewer on a V79
           store would find the column missing and the read would throw rather than degrade, so the banner
           has to fire before the tab does. */
        /* #2515: newest first. The arm below STAYS — a store migrated to exactly 80 must map to 80 rather
           than falling through. The column is named only in the probe line, not in this prose, per the V71
           finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           The reason to gate is the standing invariant rather than a read that would throw — no viewer query
           names this column; the SERVICE's alert adapter is what reads it. RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, so a fully-migrated store has to map to EXACTLY this or the banner
           reports a mismatch on a store that is current. What the banner buys on the way is worth having: a
           store still at 80 has no ceiling recorded anywhere in its history, so every tempdb percentage in it
           is the old distance-to-next-autogrow measurement, and the operator should know that before reading
           one. */
        /* #2540: newest first, and the arms below STAY — a store migrated to exactly 85 must map to 85
           rather than falling through. The table is named only in the probe line, not in this prose, per
           the V71 finding: the coverage ratchet strips information_schema lines but cannot strip a comment.

           A collector-table rung would ordinarily get no arm at all — see the V63-V69 note below — and this
           one is here for the standing invariant: it is the TOP rung, RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, and a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch against a store that is current. Nothing in the viewer reads the table;
           the read that does is on the MCP surface.

           The three comment blocks that used to stack here were moved down onto the arms they describe
           (#2530 → V82, #2539 → V83, #2542 → V85). They had drifted upward as each new rung inserted its
           own block above them, which is how a comment ends up explaining an arm two rungs away from the
           one it was written for. Keep the block with its arm. */
        /* V96 (#2603): pg_wait_sampling. The TOP rung, so it must map exactly —
           RequiredStoreSchemaVersion is StorageVersion.SchemaVersion and a fully-migrated store has to land
           here rather than on V95. */
        if (hasPgWaitSampling)
        {
            return 96;
        }

        /* V95 (#2599): database_name on the three per-database PostgreSQL tables. Was the top rung until
           V96 landed and keeps its own arm, because a store stopped between the two is a real state during
           an interrupted upgrade. */
        if (hasPgPerDatabaseAttribution)
        {
            return 95;
        }

        /* V94 (#2561): b-tree index bloat, measured. Was the top rung until V95 landed and keeps its own
           arm, because a store stopped between the two is a real state during an interrupted upgrade. */
        if (hasPgIndexBloat)
        {
            return 94;
        }

        /* V93 (#2544, buffers): what is resident in shared buffers. Was the top rung until V94 landed and
           keeps its own arm, because a store stopped between the two is a real state during an interrupted
           migration. Formerly the TOP rung — RequiredStoreSchemaVersion is StorageVersion.SchemaVersion and a
           fully-migrated store must map to exactly that, or the version banner reports a mismatch on a store
           that is current. */
        if (hasPgBufferUsage)
        {
            return 93;
        }

        /* V92 (#2544, replication): connected standbys and their distance from the primary. Was the top rung
           until V93 landed and keeps its own arm, because a store stopped between the two is a real state
           during an interrupted migration. */
        if (hasPgReplicationStats)
        {
            return 92;
        }

        /* V91 (#2543): per-column planner statistics. Was the top rung until V92 landed and keeps its own
           arm, because a store stopped between the two is a real state during an interrupted migration. */
        if (hasPgColumnStats)
        {
            return 91;
        }

        /* V90 (#2544, locks): lock state by mode, type and relation. Was the top rung until V91 landed and
           keeps its own arm, because a store stopped between the two is a real state during an interrupted
           migration. */
        if (hasPgLockStats)
        {
            return 90;
        }

        /* V89 (#2545): which extensions this target has, could have, or cannot have. Was the top rung until
           V90 landed and keeps its own arm, because a store stopped between the two is a real state during
           an interrupted migration. */
        if (hasPgExtensionAvailability)
        {
            return 89;
        }

        /* V88 (#2544): the write side — checkpoints, background writer, WAL. Was the top rung until V89
           landed and keeps its own arm, because a store stopped between the two is a real state during an
           interrupted migration. */
        if (hasPgWriteStats)
        {
            return 88;
        }

        /* V87 (#2564): whether a PostgreSQL target could capture plans at all. Was the top rung until V88
           landed and is kept as its own arm rather than folded away, because a store stopped between the two
           is a real state during an interrupted migration. */
        if (hasPgPlanCaptureReadiness)
        {
            return 87;
        }

        if (hasPgSessionStates)
        {
            return 86;
        }

        /* #2542: below the top arm now, and still not redundant — a store stopped between the V84 and V85
           rungs is a real state during an interrupted migration, and without its own arm it would report 84
           while carrying V85's table. */
        if (hasPgTableBloatStats)
        {
            return 85;
        }

        /* #2541: the index-usage rung. Below V85, above V83, for the reason every arm here is ordered
           newest-first — a V86 store also has V84's table, so testing V84 first would report every current
           store as two rungs behind and raise an upgrade banner against a store that is fine. */
        if (hasPgIndexUsageStats)
        {
            return 84;
        }

        /* #2539: the arm below STAYS — a store migrated to exactly 82 must map to 82 rather than falling
           through. The table is named only in the probe line, not in this prose, per the V71 finding.

           A collector-table rung would ordinarily get no arm at all; this one was the TOP rung when it
           landed, which is why it has one. Nothing in the viewer reads the table; the read that does is on
           the MCP surface. */
        if (hasPgDatabaseStats)
        {
            return 83;
        }

        /* #2530: the arm below STAYS — a store migrated to exactly 81 must map to 81 rather than falling
           through. The column is named only in the probe line, not in this prose, per the V71 finding.

           This rung's gate is worth having for what it tells the OPERATOR, not only for the standing
           RequiredStoreSchemaVersion invariant: a store below it records nothing that says a target is
           PostgreSQL, so every PostgreSQL server in it is indistinguishable from a SQL Server that has
           never connected — and every surface that branches on engine kind is therefore reading a fact
           that store cannot supply. */
        if (hasServerEngineKind)
        {
            return 82;
        }

        if (hasTempDbMaxSize)
        {
            return 81;
        }

        if (hasCollectionLogFanoutRollup)
        {
            return 80;
        }

        if (hasFileGrowthAlert)
        {
            return 79;
        }

        if (hasComposeTimeoutKnob)
        {
            return 78;
        }

        if (hasQueryStoreTextHash)
        {
            return 77;
        }

        if (hasQueryStoreHealth)
        {
            return 76;
        }

        if (hasPlanContentRetentionKnob)
        {
            return 75;
        }

        if (hasQueryStoreText)
        {
            return 74;
        }

        /* #2219: newest first. The previous arm STAYS — a store migrated to exactly 72 must still map to 72
           rather than falling through to the pre-PostgreSQL floor. */
        if (hasPgStatementText)
        {
            return 73;
        }

        if (hasQueryStorePlanMap)
        {
            return 72;
        }

        if (hasPgBlockingEdges)
        {
            return 71;
        }

        /* V70 (the monitored-server engine + port columns): column-existence sentinel, newest-first arm, and
           the reason to gate is the standing invariant rather than a 42703 — RequiredStoreSchemaVersion is
           StorageVersion.SchemaVersion, so a fully-migrated store must map to EXACTLY that or the version
           banner reports a mismatch on a store that is current.

           V63-V69 (the seven PostgreSQL collector tables) get no arms of their own deliberately: they add
           tables in `collect` that no viewer read names, so a store between them is only ever transient
           mid-migration, and the invariant that has to hold is "fully migrated maps to the top rung" — which
           V70, the top rung, is what senses. */
        if (hasMonitoredServerEngine)
        {
            return 70;
        }

        /* V62 (the #2171 plan-XML codec knob): column-existence sentinel, newest-first arm.
           config_service.plan_xml_compression exists only at V62 or later. Sits directly above the
           V61 arm it merged over - a store carrying both sentinels is V62 and must map there, not to
           the first older arm that happens to match. */
        if (hasPlanXmlCompressionKnob)
        {
            return 62;
        }

        /* V61 (per-fingerprint occurrence counters, #2216): table-existence sentinel, newest-first arm.
           config.incident_occurrences exists only at V61 or later.

           Same reason to gate as V60 rather than V59: nothing in the VIEWER names this table (it is the
           alert engine's accumulator memory, written and read by the service), so the viewer would not
           42703 against a V60 store. The gate exists to keep the standing invariant —
           RequiredStoreSchemaVersion is StorageVersion.SchemaVersion, so a fully-migrated store has to map
           to exactly that or the version banner reports a mismatch on a store that is current. The
           consequence of a V60 store is on the SERVICE side: the occurrence load/save would fail, the
           accumulator would fall back to reporting the total as the window count, and #2216's counter would
           silently be a gauge again. */
        if (hasIncidentOccurrences)
        {
            return 61;
        }

        /* V60 (database-state edge memory, #2166): column-existence sentinel, newest-first arm.
           config.database_state_expected.last_alerted_state exists only at V60 or later.

           Unlike the V59 arm below, the reason to gate here is NOT that the viewer would 42703: the
           expected-state editor names only expected_state / is_user_override, so nothing in the viewer
           touches either V60 column. The gate is the standing invariant instead — RequiredStoreSchemaVersion
           is StorageVersion.SchemaVersion, so a fully-migrated store must map to exactly that or the version
           banner reports a mismatch on a store that is actually current. The columns are written by the
           SERVICE (the alert engine's edge memory) and read by its deviation query, so it is the service that
           would fail on a V59 store, which is precisely why the viewer must not report V59 as good. Stated
           explicitly because the V59 wording does not transfer, and someone deciding later whether this gate
           can be relaxed needs the real reason. */
        if (hasDatabaseStateEdgeMemory)
        {
            return 60;
        }

        /* V59 (the collector memory knobs, #2164 + #2170): column-existence sentinel, newest-first arm.
           config_service.query_store_text_budget_mb exists only at V59 or later. The viewer NAMES both new
           columns in ServiceConfigSelectSql/UpdateFlagsSql, so against a V58 store the Settings read would
           fail 42703 — the gate must refuse it, and a fully-migrated V59 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasCollectorMemoryKnobs)
        {
            return 59;
        }

        /* V58 (the Query Store backfill off switch, #2167): column-existence sentinel, newest-first arm.
           config_service.query_store_backfill_enabled exists only at V58 or later. The viewer NAMES it in
           ServiceConfigSelectSql/UpdateFlagsSql, so against a V57 store the Settings read would fail
           42703 — the gate must refuse it, and a fully-migrated V58 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasBackfillSwitch)
        {
            return 58;
        }

        /* V57 (the Store Job Over Cadence warning knob, #2136): column-existence sentinel, newest-first
           arm. config_alert_settings.store_job_cadence_warn_percent exists only at V57 or later. The
           viewer NAMES it in AlertSettingsSelectSql/Upsert, so against a V56 store the Settings read
           would fail 42703 — the gate must refuse it, and a fully-migrated V57 store must map to
           exactly RequiredStoreSchemaVersion. */
        if (hasJobCadenceKnob)
        {
            return 57;
        }

        /* V56 (background-job self-metrics columns, #2136): column-existence sentinel, newest-first arm.
           store_metrics.last_run_duration_ms exists only at V56 or later. The viewer never reads these
           columns — they feed the service's own capacity series (job duration vs schedule cadence) over
           MCP/REST — so like V53's rung nothing in the viewer would fail against a V55 store. The rung
           exists so a fully-migrated store maps to exactly RequiredStoreSchemaVersion instead of capping
           at 55 and tripping the connect-time gate against a healthy store. Under-reporting is the
           guarded failure. */
        if (hasJobMetricsColumns)
        {
            return 56;
        }

        /* V55 (self-alert threshold knobs, #2107): column-existence sentinel, newest-first arm.
           config_alert_settings.self_disk_free_warn_percent exists only at V55 or later. The viewer
           NAMES the V55 columns in AlertSettingsSelectSql/Upsert, so against a V54 store the
           Settings read would fail 42703 — the gate must refuse it, and a fully-migrated V55 store
           must map to exactly RequiredStoreSchemaVersion. */
        if (hasSelfAlertKnobs)
        {
            return 55;
        }

        /* V54 (gzip plan-dim content, #2069): column-existence sentinel, newest-first arm.
           query_plan_dim.query_plan_gz exists only at V54 or later. The viewer NAMES the column in
           its plan-fetch reads (gz-else-text coalesce), so against a V53 store those reads would
           fail 42703 — the gate must refuse it, and a fully-migrated V54 store must map to exactly
           RequiredStoreSchemaVersion. */
        if (hasPlanDimGzip)
        {
            return 54;
        }

        /* V53 (store self-metrics, #2068): table-existence sentinel, newest-first arm.
           collect.store_metrics exists only at V53 or later. The viewer never reads this table — it is
           the service's own hourly capacity series (per-hypertable size/compression, dimension row
           counts, whole-store size), surfaced over MCP/REST — so like V44's rung nothing in the viewer
           would fail against a V52 store. The rung exists so a fully-migrated store maps to exactly
           RequiredStoreSchemaVersion instead of capping at 52 and tripping the connect-time gate against
           a healthy store. Under-reporting is the guarded failure. */
        if (hasStoreMetrics)
        {
            return 53;
        }

        /* V52 (persisted finding drill-down, #2060): column-existence sentinel, newest-first arm.
           analysis_findings.drill_down_json exists only at V52 or later. The viewer never names the
           column (it reads findings through its own SELECT list), so nothing in the viewer fails
           against a V51 store — the rung exists for the same reason V44's does: a probe that cannot
           SEE the newest migration maps every fully-migrated store below RequiredStoreSchemaVersion
           and the connect-time gate refuses healthy stores. Under-reporting is the guarded failure. */
        if (hasFindingDrillDown)
        {
            return 52;
        }

        /* V51 (query-stats host object, #2012 stage 2): column-existence sentinel, newest-first arm.
           collect.query_stats.host_object_name exists only at V51 or later. The viewer NAMES this column
           in its Top Queries read (TopQueriesSql groups by it and the LATERAL filters on it), so against
           a V50 store the grid read would fail 42703 outright — the gate must refuse it, and a
           fully-migrated V51 store must map to exactly RequiredStoreSchemaVersion. */
        if (hasQueryStatsHostObject)
        {
            return 51;
        }

        /* V50 (server-tag colour, #2008 2a): column-existence sentinel, newest-first arm.
           config.server_tags.colour exists only at V50 or later. The viewer names the colour column in its
           tag SELECT, so a fully-migrated V50 store must map to exactly RequiredStoreSchemaVersion rather
           than capping at 49 and tripping the connect-time gate against a healthy store — the same rule as
           every newest-column rung below. */
        if (hasServerTagColour)
        {
            return 50;
        }

        /* V49 (database-state alert): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.database_state_enabled exists only at V49 or later. The viewer names this
           column in its alert-settings SELECT and upsert, so a fully-migrated V49 store maps to exactly
           RequiredStoreSchemaVersion rather than capping at 48 and tripping the connect-time gate against a
           healthy store. */
        if (hasDatabaseStateAlert)
        {
            return 49;
        }

        /* V48 (PVS-pressure alert knobs, #1984): column-existence sentinel, newest-first arm.
           config_alert_settings.pvs_enabled exists only at V48 or later. The viewer names all three
           V48 columns in its alert-settings SELECT and upsert, so against a V47 store the Settings
           window read would fail outright with 42703 rather than degrade — the same reason the V40
           blocking_wait rung gates. */
        if (hasPvsPressureKnobs)
        {
            return 48;
        }

        /* V47 (ADR persistent version store, #1951): table-existence sentinel, newest-first arm.
           collect.pvs_stats exists only at V47 or later. Unlike V44 the viewer DOES read this table —
           the FinOps PVS grid queries it by name — so a V46 store would fail 42P01 rather than degrade,
           and the gate must refuse it. */
        if (hasPvsStats)
        {
            return 47;
        }

        /* V46 (automatic plan correction, #1952): table-existence sentinel, newest-first arm.
           collect.plan_correction exists only at V46 or later. Note the gap — 45 is permanently unused
           (it was reserved for #1951 before that lane was renumbered to 47), so there is no V45 rung and
           there never will be a store carrying one. */
        if (hasPlanCorrection)
        {
            return 46;
        }

        /* V44 (collector state, #1962): table-existence sentinel, newest-first arm.
           collect.collector_state exists only at V44 or later. The viewer never READS this table — it is
           service-only state (default_trace_events' last-seen trace file) with no view and no viewer query
           — so unlike the arms below, nothing in the viewer would fail against a V43 store. The rung
           exists so a FULLY-migrated V44 store maps to 44 rather than capping at 43: the connect-time gate
           compares the probe against RequiredStoreSchemaVersion, and a probe that cannot see the newest
           migration reports every healthy store as skewed and refuses to open it. Under-reporting is the
           failure mode this ladder guards, and it does not care whether the viewer reads the table. */
        if (hasCollectorState)
        {
            return 44;
        }

        /* V43 (PagerDuty proxy, #1945): column-existence sentinel, newest-first arm.
           config_notification.pagerduty_proxy exists only at V43 or later. The viewer MUST gate on it: the
           Settings notification read and upsert name the column, so pointed at a V42 store they would fail
           42703 rather than degrade. */
        if (hasPagerDutyProxy)
        {
            return 43;
        }

        /* V42 (PagerDuty webhook): column-existence sentinel, newest-first arm.
           config_notification.pagerduty_routing_key exists only at V42 or later. */
        if (hasPagerDutyWebhook)
        {
            return 42;
        }

        /* V41 (#1841 tier 2 Query Store interval identity): engine-agnostic column-existence sentinel,
           newest-first arm. query_store_stats.runtime_stats_interval_id exists only at V41 or later. The
           viewer MUST gate on it: every Query Store aggregate read now names that column inside its dedup
           partition, so pointed at a V40 store the Query Store tab would fail outright (42703) rather than
           degrade. The COLUMN is the sentinel, not the table — V41 only widens a table that has existed
           since V1. */
        if (hasQueryStoreIntervalIdentity)
        {
            return 41;
        }

        /* V40 (#1839 total-blocked-wait gate): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.blocking_wait_seconds_threshold exists only at V40 or later. The viewer MUST
           gate on it: its Settings window reads and upserts that column by name, so pointed at a V39 store it
           would fail the alert-settings read outright (42703) rather than degrade. */
        if (hasBlockingWaitThreshold)
        {
            return 40;
        }

        /* V39 (#1795 dimension GC measured bound): index-existence sentinel, newest-first arm — the
           same pg_indexes idiom as the V22 sentinel. ix_query_stats_digest_floor exists only at V39
           or later. The viewer itself never reads the index; the arm exists so a fully-migrated V39
           store maps to exactly RequiredStoreSchemaVersion instead of capping at 38 and tripping the
           connect-time gate against a perfectly healthy store. */
        if (hasDimFloorIndexes)
        {
            return 39;
        }

        /* V38 (#1767 query payload dimensions): table-existence sentinel, newest-first arm.
           query_plan_dim exists only at V38 or later. The viewer MUST gate on it: at V38 the
           collectors stop writing query_text/query_plan_xml inline, so a pre-V38 viewer pointed at a
           V38 store would read NULL for every new row's text and plan — no error, just a product
           that quietly shows nothing. */
        if (hasPayloadDimensions)
        {
            return 38;
        }

        /* V37 (#1696 AG disconnect re-fire): engine-agnostic column-existence sentinel, newest-first arm.
           config_alert_settings.ag_disconnect_refire_minutes exists only at V37 or later. */
        if (hasAgDisconnectRefire)
        {
            return 37;
        }

        /* V36 (#991 addendum, AG latency columns): engine-agnostic COLUMN-existence sentinel — V36 only
           widens the V34 table, so the table-existence arm below cannot distinguish the two. Newest-first. */
        if (hasAgLatencyColumns)
        {
            return 36;
        }

        /* V35 (#991 Availability Group alert knobs): engine-agnostic column-existence sentinel, newest-first
           arm. config_alert_settings.notify_ag_health exists only at V35 or later. */
        if (hasAgAlertKnobs)
        {
            return 35;
        }

        /* V34 (#991 Availability Group collectors): engine-agnostic table-existence sentinel, newest-first
           arm. The AG database-grain collector table exists only at V34 or later. (Named only in the probe
           SQL, deliberately not repeated here: ViewerCollectorCoverageTests scans this layer by substring
           and strips the probe's information_schema lines, so spelling the table in prose would make it
           read as "covered" and quietly exempt it from the coverage ratchet.) */
        if (hasAgCollectors)
        {
            return 34;
        }

        /* V33 (#1659 connection-alert opt-ins): engine-agnostic column-existence sentinel, newest-first
           arm. config_alert_settings.notify_connection_down_at_startup exists only at V33 or later. */
        if (hasConnectionRefireKnobs)
        {
            return 33;
        }

        /* V32 (fleet tags): engine-agnostic table-existence sentinel, newest-first arm.
           config.server_tags exists only at V32 or later. Adding a migration WITHOUT adding an arm here
           is the trap this ladder exists to prevent: the probe would cap at the previous version, so a
           fully-migrated store would still read as older than RequiredStoreSchemaVersion and the
           connect-time gate would refuse to open the viewer — permanently, and deploying the service
           first would not help. */
        if (hasServerTags)
        {
            return 32;
        }

        /* V31 (#1563 custom views): engine-agnostic table-existence sentinel, newest-first arm.
           config.custom_views exists only at V31 or later. */
        if (hasCustomViews)
        {
            return 31;
        }

        /* V30 (#1562 web dashboard toggle): engine-agnostic column-existence sentinel, newest-first arm.
           config_service.web_enabled exists only at V30 or later. */
        if (hasWebDashboardConfig)
        {
            return 30;
        }

        /* V29 (#1496 long-query completion trace): engine-agnostic table-existence sentinel, newest-first
           arm. The long_query_completions table exists only at V29 or later. */
        if (hasLongQueryCompletions)
        {
            return 29;
        }

        /* V28 (#1546 Query Store replica attribution): engine-agnostic column-existence sentinel,
           newest-first arm. query_store_stats.replica_role exists only at V28 or later. */
        if (hasQueryStoreReplicaRole)
        {
            return 28;
        }

        /* V27 (#1535 Azure per-database deadlock capture): engine-agnostic column-existence sentinel,
           newest-first arm. deadlocks.database_name exists only at V27 or later. */
        if (hasDeadlocksDatabaseName)
        {
            return 27;
        }

        /* V26 (#1506 generic webhook): another engine-agnostic column-existence sentinel.
           config_notification.generic_url exists only at V26 or later. */
        if (hasGenericWebhook)
        {
            return 26;
        }

        /* V24/V25 (#1433 Job History tab) are engine-agnostic table-existence sentinels, so they ARE
           standalone newest-first arms (unlike the V23 hypertable composite): agent_status (V25) present →
           25, else job_history (V24) present → 24, else fall through to the V22/V23 composite below. A
           fully-migrated store has both. */
        if (hasAgentStatus)
        {
            return 25;
        }

        if (hasJobHistory)
        {
            return 24;
        }

        if (hasIndexObjectStatsLatestIndex)
        {
            /* V22's index is the newest ENGINE-AGNOSTIC sentinel and the floor for V23. V23's only schema
               effect — collection_log becoming a hypertable — is visible ONLY on a TimescaleDB store; on plain
               PostgreSQL V23 is a no-op, so a plain-PG store at V23 is object-identical to V22 and the composite
               is true via its no-extension arm (correctly reporting 23 — nothing for the viewer to gate on). On
               a Timescale store the hypertable distinguishes a real V23 from a store still at V22. */
            return hasCollectionLogHypertableOrPlainPg ? 23 : 22;
        }

        if (hasDefaultTraceEvents)
        {
            return 21;
        }

        if (hasAlertTuningKnobs)
        {
            return 20;
        }

        if (hasAnalysisState)
        {
            return 19;
        }

        if (hasAlertDeliveryOverride)
        {
            return 18;
        }

        if (hasConfigControlPlane)
        {
            return 17;
        }

        return 16;
    }

    /// <summary>All registered servers, ordered as the server list displays them.</summary>
    public async Task<List<DarlingServer>> GetServersAsync(CancellationToken cancellationToken = default)
    {
        var servers = new List<DarlingServer>();

        await using var command = _dataSource.CreateCommand(ServersSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverName = reader.GetString(1);
            servers.Add(new DarlingServer(
                reader.GetInt32(0),
                serverName,
                reader.IsDBNull(2) ? serverName : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(7)));
        }

        return servers;
    }

    /// <summary>
    /// Product-name label for a sql_major_version (2016+ is what the product supports; older or
    /// unknown majors fall back to a bare version tag, null to empty).
    /// </summary>
    public static string SqlVersionLabel(int? sqlMajorVersion) => sqlMajorVersion switch
    {
        null => "",
        11 => "SQL Server 2012",
        12 => "SQL Server 2014",
        13 => "SQL Server 2016",
        14 => "SQL Server 2017",
        15 => "SQL Server 2019",
        16 => "SQL Server 2022",
        17 => "SQL Server 2025",
        _ => $"SQL Server v{sqlMajorVersion}",
    };

    /// <summary>
    /// The active server's UTC offset in minutes from its most recent <c>server_properties</c> row — the
    /// value the Server-time display mode adds to the naive-UTC store (UTC + offset = server local). The
    /// naive-UTC → display conversion every rendered timestamp uses lives in <see cref="ViewerTimeHelper"/>
    /// (it replaced this class's former fixed <c>ToLocalTime</c>); this only fetches the per-server offset
    /// that mode feeds into it. Returns null when the column has not been collected yet (an older store
    /// just migrated, or a server whose properties collector has not re-run since the V16 upgrade); the
    /// caller then keeps the viewer's machine-local offset so Server mode degrades to ~Local until then.
    /// </summary>
    public const string ServerUtcOffsetSql = @"
SELECT utc_offset_minutes
FROM server_properties
WHERE server_id = $1
AND   utc_offset_minutes IS NOT NULL
ORDER BY collection_time DESC
LIMIT 1";

    public async Task<int?> GetServerUtcOffsetMinutesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerUtcOffsetSql);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull
            ? null
            : Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Binds the standard per-server window parameters shared by the settable-window tab reads:
    /// $1 server_id, $2 window start, $3 window end. Both bounds are sent Kind=Unspecified because the
    /// store's columns are naive UTC (<c>timestamp without time zone</c>) — a Kind=Utc DateTime would
    /// map to timestamptz and throw since Npgsql 6.0 (see the class remarks).
    /// </summary>
    internal static void AddWindowParameters(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
    }

    /// <summary>Postgres SQLSTATE 42501 (insufficient_privilege) — a write refused on a read-only connection.</summary>
    internal const string InsufficientPrivilegeSqlState = "42501";

    /// <summary>Postgres SQLSTATE 42703 (undefined_column) — a write referenced a column a lagging store
    /// has not migrated in yet (schema skew).</summary>
    internal const string UndefinedColumnSqlState = "42703";

    /// <summary>Postgres SQLSTATE 42P01 (undefined_table) — a write referenced a table a lagging store has
    /// not migrated in yet (schema skew).</summary>
    internal const string UndefinedTableSqlState = "42P01";

    /// <summary>
    /// Executes a write command, translating a permission-denied failure — a write attempted on the
    /// read-only viewer role, or after grants changed under a running app — into a
    /// <see cref="ViewerReadOnlyException"/> the UI shows as a clear "read-only connection" message
    /// instead of a raw Postgres error. The reactive backstop to the proactive hide/disable; any other
    /// failure propagates unchanged.
    /// </summary>
    internal async Task<int> ExecuteWriteAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    /// <summary>
    /// The <c>RETURNING</c>-shaped sibling of <see cref="ExecuteWriteAsync"/>: runs a write that yields a
    /// scalar (e.g. the <c>config_command.command_id</c> a command enqueue returns), translating a 42501 on a
    /// read-only seat into <see cref="ViewerReadOnlyException"/> the same way. Any other failure propagates.
    /// </summary>
    internal async Task<object?> ExecuteWriteScalarAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        try
        {
            return await command.ExecuteScalarAsync(cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState)
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();
}

/// <summary>
/// A write was attempted on a read-only Darling connection (the least-privilege <c>viewer</c> role,
/// or any connection lacking <c>config</c> INSERT). Thrown by the write paths when Postgres returns
/// 42501 so the UI shows a clear, actionable message rather than a raw permission error. The proactive
/// <see cref="ViewerDataService.IsReadOnly"/> gating normally hides the affordances first; this covers
/// the race where the probe and the live grants disagree.
/// </summary>
public sealed class ViewerReadOnlyException : Exception
{
    public ViewerReadOnlyException(Exception innerException)
        : base(
            "This viewer is connected with a read-only role, so it can't change mute rules, dismiss alerts, " +
            "or mute findings. Set postgres.connectAs to \"admin\" in darling.json and restart the viewer to " +
            "enable these actions.",
            innerException)
    {
    }
}

/// <summary>
/// The Darling store could not be reached at connect — the service is down, or the postgres section of
/// darling.json points at the wrong host/port/database (a connection-level failure, not a server error
/// response). Thrown by <see cref="ViewerDataService.EnsureStoreReachableAsync"/> and, as a backstop, by
/// <see cref="ViewerDataService.DetectReadOnlyAsync"/> so the shell shows a dedicated "is the service
/// running?" message instead of misreading an unreachable store as a read-only seat (finding B3).
/// </summary>
public sealed class ViewerStoreUnreachableException : Exception
{
    public ViewerStoreUnreachableException(Exception innerException)
        : base(
            "Can't reach the Darling store — is the Darling service running? Check the postgres section of " +
            "darling.json (the host, port, and database must point at the running service's store). " +
            /* #2117: the swallowed detail cost a field operator hours — a TLS chain rejection, a wrong
               password, a pg_hba refusal, and a dead host all read identically without it. */
            $"Underlying error: {innerException?.Message?.Split('\n')[0].TrimEnd('\r') ?? "(none)"}",
            innerException)
    {
    }
}

/// <summary>
/// A control-plane write referenced a column or table the connected store has not migrated in yet — the
/// store's schema is BEHIND the version this viewer build needs (Postgres 42703 undefined_column / 42P01
/// undefined_table). Thrown by the write executors so the UI shows a clear "update the service" message
/// instead of a raw "column ... does not exist" error (finding B2). The connect-time gate
/// (<see cref="ViewerDataService.GetStoreSchemaVersionAsync"/>, finding B1) normally blocks a skewed store
/// before any write; this is the write-path backstop for a column referenced ahead of a lagging store.
/// </summary>
public sealed class ViewerSchemaSkewException : Exception
{
    public ViewerSchemaSkewException(Exception innerException)
        : base(
            $"This viewer needs Darling store schema v{ViewerDataService.RequiredStoreSchemaVersion}, but the " +
            "store is missing an expected column or table. Update or restart the Darling service so it " +
            "migrates the store, then reopen the viewer.",
            innerException)
    {
    }
}
