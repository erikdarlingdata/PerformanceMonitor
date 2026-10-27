/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Why a read was refused: the sentence the caller sees, the seconds to wait, and the counts it names.</summary>
internal sealed record ReadAdmissionRefusal(string Sentence, int RetryAfterSeconds, int Running, int Queued);

/// <summary>#6390 S2c: the pieces of a composed-panel run that only a host can supply. The service passes its own (the
/// server-name resolver with its refusals and peer lookups, the registry lookup, the store-setting deadline read); the Viewer
/// passes ones built on what it has. The run itself, <see cref="ComposePanelRunner.RunComposedPanelCoreAsync"/>, is the same
/// code for both.</summary>
/// <param name="ResolveServerNames">Places the scoped names through the host's resolver. The second value is the refusal
/// as the resolver wrote it (several servers match), else null.</param>
/// <param name="FindUnregistered">The scoped names no registry row carries; the last argument is called when the lookup faulted.</param>
/// <param name="ResolveComposedQuerySeconds">The operator's compose statement timeout, read from the store once per run.</param>
/// <param name="AddDataStartInstants">Adds the data-start sentence and the instants it names to the answer (web only).</param>
internal sealed record ComposeRunHost(
    Func<NpgsqlDataSource, List<string>?, CancellationToken, ILogger?, Task<(List<string>? Scope, string? Refusal)>> ResolveServerNames,
    Func<NpgsqlDataSource, IReadOnlyList<string>?, CancellationToken, ILogger?, Action?, Task<IReadOnlyList<string>?>> FindUnregistered,
    Func<NpgsqlDataSource, CancellationToken, Task<int>> ResolveComposedQuerySeconds,
    Action<JsonObject, string, DateTime, DateTime, DateTime> AddDataStartInstants);

/// <summary>Where the moved run notes what it did into the host's ambient read scope (the service's <c>ReadScope</c>), which
/// the run cannot name. The service installs its scope at start (a module initializer); a host with none leaves the defaults,
/// which note nothing and log through <see cref="System.Diagnostics.Trace"/> as an unscoped caller always did.</summary>
internal static class ComposeReadNotes
{
    internal static Action<string, string?> NoteSourceSink { get; set; } = static (_, _) => { };
    internal static Action<ReadFallback> NoteSink { get; set; } = static _ => { };
    internal static Action<ReadFallback, string, Exception?> NoteFallbackSink { get; set; } = static (kind, site, ex) =>
        System.Diagnostics.Trace.TraceWarning(ex is null ? $"{site} fell back to raw ({kind})" : $"{site} fell back to raw ({kind}): {ex.GetType().Name}");
    internal static Action<string, Exception?> WarnSink { get; set; } = static (message, ex) =>
        System.Diagnostics.Trace.TraceWarning(ex is null ? message : $"{message}: {ex.GetType().Name}");
    internal static Func<ILogger?> CurrentLoggerSource { get; set; } = static () => null;

    internal static ILogger? CurrentLogger => CurrentLoggerSource();
    internal static void NoteSource(string source, string? reason = null) => NoteSourceSink(source, reason);
    internal static void Note(ReadFallback kind) => NoteSink(kind);
    internal static void NoteFallback(ReadFallback kind, string site, Exception? ex) => NoteFallbackSink(kind, site, ex);
    internal static void Warn(string message, Exception? ex) => WarnSink(message, ex);
}

/// <summary>A silent read fallback a reader noted into the current read scope (#5097).</summary>
internal enum ReadFallback
{
    /// <summary>The gate said the interval table serves, the table read faulted, and raw answered.</summary>
    FallbackRaw,

    /// <summary>The source decision itself (or the connection / transaction it needs) faulted, so raw answered
    /// without a decision. Outranks <see cref="FallbackRaw"/> and <see cref="LedgerUncovered"/> when more than one was
    /// noted in one call.</summary>
    GateFailed,

    /// <summary>The hourly-edges count guard found the hour ledger does not cover the window yet (#4605), so the
    /// route was not taken and raw answered. An expected state, not a fault: it is noted without a log line and shows as
    /// <c>fallback_raw</c> in <c>get_read_latency</c>. Ranks below <see cref="GateFailed"/> and <see cref="FallbackRaw"/>,
    /// which are the bigger facts when noted in the same call.</summary>
    LedgerUncovered,
}

/// <summary>The fixed vocabulary of <see cref="ComposeRunHost.NoteSource"/>.</summary>
internal static class ComposeReadSources
{
    internal const string Raw = "raw";
    internal const string IntervalTable = "interval_table";
    internal const string HourlyEdges = "hourly_edges";
}

    /// <summary>The discriminated outcome of <see cref="RunComposedPanelAsync"/>: the <c>{sql, rows,
    /// annotations}</c> payload on success, or an error the caller maps onto its own surface (HTTP status /
    /// MCP envelope). <see cref="IsServerError"/> distinguishes a client-correctable error — a bad spec or a
    /// bounded query failure (statement_timeout / Postgres error), i.e. HTTP 400 — from an unexpected internal
    /// failure (HTTP 500). <see cref="Fault"/> (#4283 review round 1, M1) rides along on a BadRequest for a
    /// PostgresException the panel author cannot act on — the real exception, so the WEB endpoint mapping can
    /// answer it through the same backstop as an uncaught one; <see cref="Error"/>/<see cref="IsServerError"/>
    /// stay what they always were either way, so run_custom_view_panel's MCP answer never changes.
    /// <see cref="AuthorSqlState"/> (#4293 round 2) is set only on the author-actionable PostgresException arm,
    /// so the web log line can name the SQLSTATE without re-deriving it.</summary>
    internal readonly record struct ComposeRunOutcome(JsonObject? Payload, string? Error, bool IsServerError, PostgresException? Fault = null, string? AuthorSqlState = null, bool IsNotFound = false, ReadAdmissionRefusal? Busy = null, string? PayloadText = null)
    {
        /// <summary>#5506 part 3: the panel was refused at admission. The MCP tool answers <c>busy</c>, the web route 429.</summary>
        internal static ComposeRunOutcome Refused(ReadAdmissionRefusal refusal) => new(null, refusal.Sentence, false, null, null, false, refusal);

        /// <summary>W13: the scoped server is not registered and nothing is stored under its name. The MCP tool answers <c>not_found</c>, the web route 404.</summary>
        internal static ComposeRunOutcome NotFound(string error) => new(null, error, false, null, null, true);

        internal static ComposeRunOutcome Ok(JsonObject payload) => new(payload, null, false);

        /// <summary>#5506 part 3 (round 1, C1): a panel answered from the read cache. <see cref="PayloadText"/> is the text the
        /// run that stored it answered, so the caller writes the same bytes; <see cref="Payload"/> is parsed from it for a
        /// caller that reads the object.</summary>
        internal static ComposeRunOutcome FromCache(string payloadText) =>
            new((JsonObject)JsonNode.Parse(payloadText)!, null, false, PayloadText: payloadText);

        internal static ComposeRunOutcome BadRequest(string error) => new(null, error, false);

        internal static ComposeRunOutcome BadRequest(string error, PostgresException fault) => new(null, error, false, fault);

        internal static ComposeRunOutcome AuthorQueryError(string error, string? sqlState) => new(null, error, false, null, sqlState);

        internal static ComposeRunOutcome ServerError(string error) => new(null, error, true);
    }

/// <summary>#6390 S2c: the composed-panel run core, moved out of the service's web endpoints so the Viewer runs panels with
/// the same code. Moved as it was; only what reached the service's own types now comes in through <see cref="ComposeRunHost"/>.</summary>
internal static class ComposePanelRunner
{
    /// <summary>The SQLSTATE of a store <c>temp_file_limit</c> / configuration-limit refusal (53400); <c>ReadOutcomeClassifier</c> in the service shares it.</summary>
    internal const string ConfigurationLimitExceededSqlState = "53400";

    /// <summary>#4283 review round 1 (M1): the PostgresException SQLSTATEs a Custom Views panel author can act
    /// on by editing their own panel — a statement_timeout cancel, or a class-22/class-42 error other than
    /// 42501 (insufficient_privilege, which names a STORE role problem, not the panel), or (#4605) a 53400
    /// <c>configuration_limit_exceeded</c> — the viewer/mcp role's <c>temp_file_limit</c> refusing the panel's
    /// own on-disk spill, which the author fixes the same way they fix a statement_timeout cancel: narrow the
    /// panel. Everything else (28P01 auth failure, 53300 too-many-connections, 57P01 admin shutdown, 3D000
    /// unknown database, ...) is a STORE fault the author cannot fix.</summary>
    internal static bool IsComposeRunAuthorActionable(string? sqlState) =>
        sqlState == "57014"
        || sqlState == ConfigurationLimitExceededSqlState
        || (sqlState is { Length: 5 } && sqlState.StartsWith("22", StringComparison.Ordinal))
        || (sqlState is { Length: 5 } && sqlState.StartsWith("42", StringComparison.Ordinal) && sqlState != "42501");

    /// <summary>#4605: the caller-facing text for a composed read the viewer/mcp role's <c>temp_file_limit</c>
    /// refused (SQLSTATE 53400) — named separately from the generic "Query failed: {MessageText}" text
    /// (#4283) because the store's own wording ("temporary file size exceeds temp_file_limit") names an
    /// internal setting the panel author has no way to change; this names the ACTIONS they can take
    /// instead.</summary>
    internal const string TempFileLimitExceededMessage =
        "This panel needed more temporary disk space than a dashboard read may use. Narrow the time window, choose an hourly or daily grain, or add a filter.";

    /// <summary>Seconds the WEB compose path adds to the resolved <c>compose_statement_timeout_seconds</c> when it
    /// sets the client <c>CommandTimeout</c>, so the server's 57014 (recorded as Timeout, with the actionable
    /// text) arrives before Npgsql's own client timer fires. The client deadline is then a backstop for a server
    /// that never answers. The client deadline still fires first in two cases, and the outcome is Timeout in
    /// both: pooled sessions keep an older, higher <c>statement_timeout</c> after an operator lowers
    /// <c>compose_statement_timeout_seconds</c> (the new value takes effect on each role's next session), and the
    /// owner-login fallback has no server-side <c>statement_timeout</c>, so the client deadline is the only bound.
    /// The MCP <c>run_custom_view_panel</c> caller passes no headroom and keeps its equal-deadline race; either
    /// side's timeout answers the same statement-timeout text there, because it passes the remap flag too.</summary>
    internal const int ComposeClientDeadlineHeadroomSeconds = 5;

    /// <summary>W13: the plain message for a composed run whose scope names only unregistered servers and whose rows hold no value,
    /// else null. <paramref name="unregistered"/> is <c>ComposeServerScope.FindUnregisteredAsync</c>'s answer. When the registry
    /// lookup itself failed (<paramref name="lookupFailed"/>) that answer is every name, a fallback and not a finding, so the message
    /// is null and the run keeps the old empty answer. The check covers only the run's window, and the text says so.</summary>
    internal static string? UnknownServerMessage(IReadOnlyList<string>? scope, IReadOnlyList<string>? unregistered, JsonNode? rows, bool lookupFailed = false)
    {
        if (lookupFailed) return null;
        if (scope is null || scope.Count == 0 || unregistered is null) return null;
        if (!scope.All(name => unregistered.Contains(name, StringComparer.Ordinal))) return null;
        var hasValue = rows is JsonArray array
            && array.Any(row => row is JsonObject o && o.Any(property => property.Value is not null));
        if (hasValue) return null;
        var names = string.Join(", ", scope.Select(n => "'" + LogTextSanitizer.Sanitize(n, 128) + "'"));
        return scope.Count == 1
            ? $"No server named {names} is registered, and nothing is stored under that name in this time range."
            : $"No server named {names} is registered, and nothing is stored under those names in this time range.";
    }

    /// <summary>The client <c>CommandTimeout</c> for a composed query whose server-side statement_timeout is
    /// <paramref name="serverSeconds"/>, with <paramref name="headroomSeconds"/> of headroom.</summary>
    internal static int ComposeClientDeadlineSeconds(int serverSeconds, int headroomSeconds) => serverSeconds + headroomSeconds;

    /// <summary>The caller-facing text of a composed read cancelled by the statement timeout, whether the server's
    /// 57014 or the client's own timer got there first.</summary>
    internal const string StatementTimeoutText = "Query failed: canceling statement due to statement timeout";

    /// <summary>Raised by the compose runners when Npgsql's own client timer fires inside
    /// <c>ExecuteReaderAsync</c> or the row <c>ReadAsync</c> loop of a connection that was already open. The
    /// original <see cref="NpgsqlException"/> rides as the inner exception. Two cases look the same to the driver
    /// and both become this marker: a stalled send of the query (the write timeout is the command's
    /// <c>CommandTimeout</c>), and a pooled connection whose peer went away, whose first I/O happens inside
    /// <c>ExecuteReaderAsync</c>. A failure to open the connection never becomes this marker (see
    /// <see cref="ComposeStoreOpenException"/>). One limit: with <c>Multiplexing=true</c> in a bring-your-own
    /// store connection string, a connect failure surfaces from <c>ExecuteReaderAsync</c> and becomes this
    /// marker; the managed store's connection string does not set it (Npgsql's default is off).</summary>
    internal sealed class ComposeStatementClientTimeoutException : Exception
    {
        internal ComposeStatementClientTimeoutException(NpgsqlException inner)
            : base(inner.Message, inner)
        {
        }
    }

    /// <summary>Raised when a compose runner cannot open its store connection (pool wait, DNS, TCP connect, TLS or
    /// startup or login exchange). The original exception rides as the inner exception: an
    /// <see cref="NpgsqlException"/>, or the bare <see cref="TimeoutException"/> the driver throws when a first-time
    /// setup wait ends (#5016). No statement ran, so this is never a statement timeout.</summary>
    internal sealed class ComposeStoreOpenException : Exception
    {
        internal ComposeStoreOpenException(Exception inner)
            : base(inner.Message, inner)
        {
        }

        /// <summary>True when the open failed with a driver exception only after the connection <c>Timeout</c> had elapsed
        /// (see <see cref="PostgresOpenTimeout.IsTimedOutByClock"/>), whatever shape the driver wrapped the failure in.</summary>
        internal bool FailedAfterTimeout { get; init; }

        /// <summary>The driver exception this wraps, for classification only (never for answer text).</summary>
        internal Exception Cause => InnerException!;
    }

    /// <summary>Opens a compose runner's store connection, before any statement runs. An <see cref="NpgsqlException"/>
    /// from the open (other than a <see cref="PostgresException"/> the server sent at login), or a bare
    /// <see cref="TimeoutException"/> (the driver's wait for a data source's first-time setup, #5016), is raised as
    /// <see cref="ComposeStoreOpenException"/>. A cancellation propagates unchanged: it is the caller's.</summary>
    private static async Task<NpgsqlConnection> OpenComposeConnectionAsync(
        NpgsqlDataSource postgres, System.Threading.CancellationToken cancellationToken)
    {
        var timeout = OpenTimeoutOrZero(postgres);
        var started = Stopwatch.StartNew();
        try
        {
            return await OpenComposeConnectionCoreAsync(postgres, cancellationToken);
        }
        catch (ComposeStoreOpenException open)
        {
            /* The shape rules in PostgresOpenTimeout.IsTimedOutOpen miss a timeout that tore the socket down under the driver
               (an aborted read on a loaded runner). Elapsed time covers that without guessing the shape. */
            if (PostgresOpenTimeout.IsTimedOutByClock(open.Cause, started.Elapsed, timeout, cancellationToken.IsCancellationRequested))
            {
                throw new ComposeStoreOpenException(open.Cause) { FailedAfterTimeout = true };
            }

            throw;
        }
    }

    /// <summary>The data source's open <c>Timeout</c>, read before the open so a parse failure cannot replace the open's
    /// own error; zero (the clock rule off) when the string cannot be read.</summary>
    private static TimeSpan OpenTimeoutOrZero(NpgsqlDataSource postgres)
    {
        try
        {
            return TimeSpan.FromSeconds(new NpgsqlConnectionStringBuilder(postgres.ConnectionString).Timeout);
        }
        catch (ArgumentException)
        {
            return TimeSpan.Zero;
        }
    }

    private static async Task<NpgsqlConnection> OpenComposeConnectionCoreAsync(
        NpgsqlDataSource postgres, System.Threading.CancellationToken cancellationToken)
    {
        try
        {
            return await postgres.OpenConnectionAsync(cancellationToken);
        }
        catch (NpgsqlException ex) when (ex is not PostgresException)
        {
            throw new ComposeStoreOpenException(ex);
        }
        catch (TimeoutException ex)
        {
            throw new ComposeStoreOpenException(ex);
        }
    }

    /// <summary>The generic-exception arm of the compose runner, pulled out so a test runs it. When
    /// <paramref name="remapClientTimeout"/> is true (the web route and the MCP <c>run_custom_view_panel</c>
    /// caller), a client timer that fired while the statement executed
    /// (<see cref="ComposeStatementClientTimeoutException"/>) is the same event as the server's 57014, so it
    /// records Timeout and answers with the same text; with the flag false it stays a server error carrying the
    /// original message. A failure to open the store connection (<see cref="ComposeStoreOpenException"/>) is
    /// never a statement timeout: it answers a server error whose text says whether the open timed out, and is
    /// logged at Error with the original message.</summary>
    internal static ComposeRunOutcome FromRunException(Exception ex, bool remapClientTimeout = false)
    {
        if (ex is ComposeStatementClientTimeoutException marker)
        {
            return remapClientTimeout
                ? ComposeRunOutcome.AuthorQueryError(StatementTimeoutText, QueryStoreComposeStamp.StatementTimeoutSqlState)
                : ComposeRunOutcome.ServerError($"Error running query: {marker.InnerException!.Message}");
        }

        if (ex is ComposeStoreOpenException open)
        {
            /* #5016: whether an open timed out is not the driver's one shape (a read timeout, an expired budget, a connect, a
               pool wait, the GSS step's wrapper and a bare TimeoutException all differ), so the chain is walked in one helper. */
            return PostgresOpenTimeout.IsTimedOutOpen(open.InnerException!) || open.FailedAfterTimeout
                ? ComposeRunOutcome.ServerError($"Error running query: could not get a store connection in time: {open.InnerException!.Message}")
                : ComposeRunOutcome.ServerError($"Error running query: could not open a store connection: {open.InnerException!.Message}");
        }

        return ComposeRunOutcome.ServerError($"Error running query: {ex.Message}");
    }

    /// <summary>#4293 round 2 (R2-L1, R2-L2): the compose runner's PostgresException decision, pulled out of the
    /// catch so a test runs it. <see cref="IsComposeRunAuthorActionable"/>'s SQLSTATEs count only at ERROR
    /// severity: a FATAL or PANIC is a connection-level store fault whatever its class (a startup parameter the
    /// server rejects answers FATAL 22023 or 42704, which names the configured setting and its value).</summary>
    internal static ComposeRunOutcome FromPostgresException(PostgresException ex) =>
        ex.SqlState == ConfigurationLimitExceededSqlState && ex.InvariantSeverity is not ("FATAL" or "PANIC")
            ? ComposeRunOutcome.AuthorQueryError(TempFileLimitExceededMessage, ex.SqlState)
            : IsComposeRunAuthorActionable(ex.SqlState) && ex.InvariantSeverity is not ("FATAL" or "PANIC")
                ? ComposeRunOutcome.AuthorQueryError($"Query failed: {ex.MessageText}", ex.SqlState)
                : ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}", ex);


    /// <summary>The compile-and-run body <see cref="RunComposedPanelAsync"/> wraps with latency recording --
    /// unchanged from before #4442 scope 2 added the wrapper. <paramref name="logger"/> is the one the host's
    /// read-latency recorder carries (<see cref="ReadLatencyRecorder.Logger"/>), where the data-start probe reports
    /// a failure it swallows.</summary>
    internal static async Task<ComposeRunOutcome> RunComposedPanelCoreAsync(
        ComposeRunHost host, NpgsqlDataSource postgres, JsonObject body, int clientDeadlineHeadroomSeconds, bool remapClientTimeout, bool includeDataStartFields, System.Threading.CancellationToken cancellationToken,
        ILogger? logger = null, Action<Exception>? onRunException = null, DateTime? nowUtc = null)
    {
        if (body["panel"] is not JsonObject panel)
        {
            return ComposeRunOutcome.BadRequest("Request body must be a JSON object with a 'panel'.");
        }

        /* The run path stays LENIENT about unknown keys — a definition stored before the #2733 write-path
           strictness existed must keep running — but the one mis-shape a lenient parse turns into a
           misdirecting error is named: a DOUBLY-nested spec ({"panel":{"panel":{...}}}) has no 'source' and
           used to fail as "unknown source ''", pointing at the catalog instead of the nesting. No stored
           panel can carry a 'panel' key (the write path has never accepted one that parsed), so this
           misses nothing legitimate. */
        if (panel["panel"] is JsonObject && panel["source"] is null)
        {
            return ComposeRunOutcome.BadRequest(
                "the spec is doubly nested — 'panel' should hold the panel object itself ({\"panel\":{\"source\":...}}), not another {\"panel\":{...}} wrapper.");
        }

        var (variables, variablesError) = ComposeSpec.ParseVariables(body["variables"]);
        if (variablesError is not null)
        {
            return ComposeRunOutcome.BadRequest(variablesError);
        }

        var declaredVariables = variables!.Select(v => v.Name).ToHashSet(StringComparer.Ordinal);
        var (plan, planError) = ComposeSpec.TryParsePanel(panel, declaredVariables);
        if (planError is not null)
        {
            return ComposeRunOutcome.BadRequest(planError);
        }

        var values = ParseRunValues(body["values"]);

        /* Server scope (Erik's Decision 1 fleet axis): the top-level 'server' (a name or array), else the
           $server variable value. Absent / "All" / empty => the WHOLE FLEET (no server predicate); one or
           many names => a bound server_name = ANY filter. */
        var serverScope = ParseServerScope(body, values);

        /* #5633 item 17: a spec that names a server by its display name (or in another case) is placed through the shared
           server-name resolver, the one every other server-scoped tool uses, before anything reads by name. A name matching
           several servers is refused with the resolver's own sentence. */
        var (resolvedScope, scopeRefusal) = await host.ResolveServerNames(postgres, serverScope, cancellationToken, logger);
        if (scopeRefusal is not null)
        {
            return ComposeRunOutcome.BadRequest(McpHelpers.ErrorMessageOf(scopeRefusal));
        }

        serverScope = resolvedScope;

        /* The run window (#1606): an explicit absolute pair (windowStart/windowEnd — the zoom/brush and
           historical-analysis path) takes precedence over the relative `hours` (which the frontend keeps
           sending alongside); both-or-neither, start < end, span capped at the same MaxWindowHours ceiling.
           An explicit window is an explicit request, so violations REJECT rather than silently sliding an
           endpoint. NowUtc stays the real wall clock either way — routing and the partial-window notice
           measure age from now, never from a historical window's end. */
        /* The optional nowUtc is a test seam (a live test anchors the whole run at one instant); every caller in the service
           passes nothing, so production reads the real clock exactly as before. */
        var now = nowUtc ?? DateTime.UtcNow;
        DateTime start;
        DateTime end;
        var hasWindowStart = body["windowStart"] is not null;
        var hasWindowEnd = body["windowEnd"] is not null;
        if (hasWindowStart || hasWindowEnd)
        {
            if (!hasWindowStart || !hasWindowEnd)
            {
                return ComposeRunOutcome.BadRequest("windowStart and windowEnd must be provided together.");
            }

            if (!ComposeSpec.TryParseUtcInstant(body["windowStart"], out start) || !ComposeSpec.TryParseUtcInstant(body["windowEnd"], out end))
            {
                return ComposeRunOutcome.BadRequest("windowStart/windowEnd must be ISO-8601 timestamps (UTC; a trailing Z and fractional seconds are fine).");
            }

            /* The future is empty by definition — clamp the end to now for honesty (routing only cares about
               the start's age, so this is presentation, not safety). */
            if (end > now)
            {
                end = now;
            }

            if (start >= end)
            {
                return ComposeRunOutcome.BadRequest("windowStart must be earlier than windowEnd.");
            }

            if ((end - start) > TimeSpan.FromHours(ComposeLimits.MaxWindowHours))
            {
                return ComposeRunOutcome.BadRequest($"the window spans more than the {ComposeLimits.MaxWindowHours / 24}-day ceiling; narrow it.");
            }
        }
        else
        {
            var hours = body["hours"] is JsonValue hoursValue && hoursValue.TryGetValue<int>(out var h) ? h : DefaultComposeHours;
            hours = Math.Clamp(hours, 1, ComposeLimits.MaxWindowHours);
            end = now;
            start = end.AddHours(-hours);
        }

        /* Which rollups exist in THIS store (#1665) gates the source routing below — a plain-PostgreSQL
           store (or a partially-built TimescaleDB one) must route to what it HAS, never onto a missing
           relation. Probed lazily, cached per data source. */
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);

        /* #4605: Query Store never takes the recent-window rollup route (it can't be exact,
           even on the corrected hourly), so its own bounded fast path is the wide table (V145) — decided
           HERE, in the runner, before compiling, because ComposeCompiler.Compile stays pure and never opens
           a connection. Only checked for a panel that actually reads query_store_stats; every other panel
           pays nothing extra. */
        /* #6807: ask the router first. A window older than RetentionTierRouter.RawMaxAge routes to the corrected hourly rollup
           (ComposeCompiler.BuildFactRelation returns it before any wide-table branch), so the wide table is never read: its
           eligibility gate, its size guard and its notes would all describe a table this panel does not touch. A panel the
           router sends to a rollup skips them; one that reads the wide table (a recent window, or a dimension no rollup carries) is unchanged. */
        var wideResolution = plan!.Measure.SourceTable == "query_store_stats"
            && !ComposeCompiler.ResolveFactRoute(plan, now, start, rollups, coverage).IsCagg
            ? await ResolveQueryStoreWideEligibleAsync(postgres, serverScope, start, end, hasWindowEnd ? end : (DateTime?)null, cancellationToken, plan)
            : default;
        var queryStoreWideEligible = wideResolution.Eligible;
        /* #6115: the servers the gate refused. The panel reads them raw and every other server in scope from the interval table. Null when
           every server passed (today's all-wide text) and when none did (today's all-raw text, Eligible false). */
        var rawServerIds = wideResolution.RawServerIds;

        /* #5582: a wide-table read the daily summary says cannot finish inside the statement timeout is refused here, in
           milliseconds, before any fact row is read. Both callers of this runner (the web endpoint and the MCP tool
           run_custom_view_panel) get it, because it lives in the shared body. Fails open: no built day, or a fault in
           the lookup, never refuses. The range passed is the one the wide table is read for. */
        DateTime? stampThrough = null;
        if (queryStoreWideEligible)
        {
            var countedStart = wideResolution.WideStart is { } wideReadStart && wideReadStart > start ? wideReadStart : start;
            var limit = QueryStoreWideReadGuard.LimitFor(ComposeCompiler.RankedTimeSeriesScansFactRowsTwice(plan!, start, end, wideResolution.GroupMembers));
            /* #5582 part 3: the hours the rollup answers count at ComposeLimits.StampRowWeight, but only when this panel's text really reads
               the rollup. A panel it cannot serve compiles to the wide-table text, and keeping the stamp-through would under-count it. */
            if (wideResolution.StampThrough is not null
                && ComposeCompiler.ReadsStampRollup(plan!, new ComposeRunContext(serverScope, start, end, values, rollups, now, coverage, true, wideResolution.WideStart, QueryStoreStampThrough: wideResolution.StampThrough)))
            {
                stampThrough = wideResolution.StampThrough;
            }

            /* #6115: the guard's wide estimate leaves out the servers read raw (the interval table never reads them), and ADDS the raw half's
               rows, counted from the collection log over the window's own start (the raw half is not clamped to the wide start) at 1.0 each,
               against the same limit. One statement reads both halves under one timeout, so the sum is what must fit. The wide figure is
               exactly what the guard counted before #6115 for those servers, so a split panel is never let through that today's code would
               refuse for the same wide rows; the raw rows can only refuse more. A panel whose every server passes adds nothing and is
               unchanged. A log that cannot answer adds nothing: the guard fails open, as it always has. */
            if (await QueryStoreWideReadGuard.CheckAsync(postgres, serverScope, countedStart, end, logger, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken, limit, stampThrough, rawServerIds, start) is { } tooBig)
            {
                return ComposeRunOutcome.BadRequest(tooBig);
            }
        }

        /* #5525: the scoped names that no registry row carries. The compiler scopes by server_id and keeps matching those names on the
           row's stored server_name, so a scope over a name that was never registered keeps matching what it matched before (a scoped name
           that IS registered means the server the registry holds under it now: see ComposeCompiler.ServerScope). Resolved here, before the hourly-edges snapshot below
           takes its connection, so this lookup never asks the pool for a second one while the snapshot holds the first. */
        var registryLookupFailed = false;
        var unregisteredServers = await host.FindUnregistered(postgres, serverScope, cancellationToken, logger, () => registryLookupFailed = true);

        /* #5811: a raw-route object_name panel reads module names from collect.module_map, which the worker creates at start, and reads
           procedure_stats only from the map's watermark less the refresh slack (ruling C). Where that create failed, the compile keeps the
           window-wide ranking CTE, which needs no map; with no watermark the overlay is the whole window. Asked here, before the
           hourly-edges snapshot takes its connection, and only for a panel that joins modules. A panel whose hourly-edges guard passes
           reads the watermark again inside its snapshot and uses that one; a watermark read earlier is never newer, so this one only
           widens the overlay. */
        var moduleMap = plan!.UsesModuleJoin ? await ProbeModuleMapAsync(postgres, logger, cancellationToken) : (Missing: false, Through: null);

        /* #4605: only a panel the hourly-plus-raw-edges route could serve pays for the count guard. It runs on one
           connection, in a REPEATABLE READ READ ONLY transaction the panel statement shares, so a collector batch that
           commits after the guard cannot add a row the guard never saw. Every other panel opens its own connection
           inside RunComposedQueryAsync, exactly as before. */
        var hourlyEdgesCandidate = ComposeSourceRouter.HourlyRawEdgesCandidate(plan!, now, start, end, rollups, coverage);
        /* The candidate path resolves the compose deadline FIRST, so the run never holds the snapshot's connection while it asks the
           pool for a second one (at the pool's size, every panel would hold one and wait for another). The same value is the panel's
           client deadline and bounds the guard. The other path resolves it inside the try below, as it always has. */
        int? candidateComposedSeconds = null;
        HourlyEdgesSnapshot? hourlyEdgesSnapshot = null;
        if (hourlyEdgesCandidate is not null)
        {
            candidateComposedSeconds = await host.ResolveComposedQuerySeconds(postgres, cancellationToken);
            try
            {
                hourlyEdgesSnapshot = await BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate, serverScope, candidateComposedSeconds.Value, plan!.UsesModuleJoin, cancellationToken, unregisteredServers);
            }
            catch (PostgresException ex)
            {
                /* A login-time server error (53300 too-many-connections, 28P01, 57P01, 3D000) escapes OpenComposeConnectionAsync
                   unwrapped. It is answered as the panel's own failed open is answered in the main try below. */
                return FromPostgresException(ex);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The snapshot's own open failed (ComposeStoreOpenException included): the store is not reachable, which is the same
                   fault, and the same answer, as the panel's own open failing in the try below. Same seam, same outcome. */
                onRunException?.Invoke(ex);
                return FromRunException(ex, remapClientTimeout);
            }
        }

        await using var snapshot = hourlyEdgesSnapshot;

        var runContext = new ComposeRunContext(serverScope, start, end, values, rollups, now, coverage, queryStoreWideEligible, wideResolution.WideStart, HourlyEdges: snapshot?.Verdict, ModuleMapThrough: snapshot?.ModuleMapThrough ?? moduleMap.Through, UnregisteredServers: unregisteredServers, QueryStoreGroupMembers: wideResolution.GroupMembers, QueryStoreStampThrough: stampThrough, ModuleMapMissing: moduleMap.Missing, QueryStoreRawServerIds: rawServerIds);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, runContext);
        if (compileError is not null)
        {
            return ComposeRunOutcome.BadRequest(compileError);
        }

        /* #5811: a query_stats or procedure_stats panel read raw for its whole window, whose collection log says the read cannot finish
           inside the statement timeout, is refused here in milliseconds instead of after a minute of reading. Decided on the compiled
           route, so a panel the hourly-raw-edges route or a rollup serves is never counted. A panel whose hourly-edges guard did not pass
           holds the snapshot's connection, and the estimate reads there under a savepoint. Fails open, like the Query Store guard above. */
        if (QueryStoreWideReadGuard.RawCollectorFor(plan!.Measure.SourceTable, compiled!.Route) is { } rawCollector
            && await QueryStoreWideReadGuard.CheckRawAsync(postgres, snapshot?.Connection, serverScope, rawCollector, start, end, logger, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken) is { } rawTooBig)
        {
            return ComposeRunOutcome.BadRequest(rawTooBig);
        }

        ComposeReadNotes.NoteSource(
            queryStoreWideEligible ? ComposeReadSources.IntervalTable : snapshot?.Verdict is not null ? ComposeReadSources.HourlyEdges : ComposeReadSources.Raw,
            queryStoreWideEligible && rawServerIds is { Count: > 0 } ? "split_raw_servers" : null);

        try
        {
            /* Resolved once per RUN, from the store, and deliberately not cached: see
               McpCommandDeadlines.ResolveComposedQuerySecondsAsync (the service's read, reached through host.ResolveComposedQuerySeconds) for why a value captured at host start or
               memoised per data source would be wrong for this particular knob. Threaded rather than
               re-read per query so a panel with annotation overlays pays one read, not one per source —
               which also means every query in ONE run shares one ceiling, matching the role
               statement_timeout they all run under. */
            var composedQuerySeconds = candidateComposedSeconds ?? await host.ResolveComposedQuerySeconds(postgres, cancellationToken);

            var clientSeconds = ComposeClientDeadlineSeconds(composedQuerySeconds, clientDeadlineHeadroomSeconds);
            var rows = await RunComposedQueryAsync(postgres, compiled!, clientSeconds, cancellationToken, snapshot?.Connection);
            if (snapshot is not null)
            {
                await snapshot.DisposeAsync();
            }

            /* W13: a scope naming only servers the registry does not know, over rows that hold nothing, is a server that does not exist, and
               a null aggregate row was being returned as if it were an answer. Names that ARE stored (a removed server's history, #5525)
               keep returning their rows, so this fires only when the run found nothing for them. */
            if (UnknownServerMessage(serverScope, unregisteredServers, rows, registryLookupFailed) is { } unknownServer)
            {
                return ComposeRunOutcome.NotFound(unknownServer);
            }

            /* Event-annotation overlays (design D5): one bounded, catalog-only event query per requested
               source, on the SAME window + server scope, under the same statement_timeout. Additive —
               {sql, rows} are unchanged; a panel that requests no annotations returns an empty array. */
            var annotations = await RunAnnotationsAsync(postgres, plan!, runContext, clientSeconds, cancellationToken);
            var payload = new JsonObject { ["sql"] = compiled!.Sql, ["rows"] = rows, ["annotations"] = annotations };
            /* Partial window, and says so (#1665): when the route landed on a tier whose retention cannot
               reach the window's start on a retention-active store, tell the caller instead of quietly
               returning a short slice of what they asked for. Additive — absent when the window fits.
               Joined with the row-cap notice (#1687), because the two truncate a panel from OPPOSITE
               ends — retention loses the oldest points, the row cap now drops them too by keeping the
               newest buckets — and a panel can genuinely hit both at once. Showing one and swallowing
               the other would under-report exactly the panel that is worst off. */
            var retentionNotice = ComposeStoreAvailability.BuildRetentionNotice(plan!.Measure.SourceTable, compiled.Route, start, now, rollups, coverage);

            /* The tier notice judges a tier by the store-wide floors, so it is silent for a source with no rollups,
               a store with no TimescaleDB, and one server whose rows start later than the rest. When it is silent,
               ask where this panel's own rows start. The Query Store wide table reports its own start in the
               history note below, so a panel it served is not asked twice. */
            ComposeStoreAvailability.DataStartNotice? dataStartNotice = null;
            if (retentionNotice is null && !queryStoreWideEligible)
            {
                dataStartNotice = await ComposeStoreAvailability.FindDataStartNoticeAsync(
                    postgres, plan.Measure.SourceTable, compiled.Route, serverScope, start, end, composedQuerySeconds, cancellationToken, logger);
                retentionNotice = dataStartNotice?.Text;
            }

            if (ComposeStoreAvailability.CombineNotices(
                    retentionNotice,
                    ComposeStoreAvailability.BuildRowCapNotice(plan.Mode, rows.Count)) is string notice)
            {
                payload["notice"] = notice;

                /* #4966: the sentence names its times in UTC, which is what any other reader of this answer (the MCP
                   tool) needs, but the page prints every time in the browser's zone. When the notice is the data-start
                   one, the answer also carries the instants it names, and the sentence, so the page can write it again
                   in its own clock. The tier notice names no instant the page prints, and carries none. */
                if (includeDataStartFields && dataStartNotice is { } found)
                {
                    host.AddDataStartInstants(payload, found.Text, found.DataStartUtc, start, end);
                }
            }

            /* #4689: the interval table served this panel from a start later than the window's, so say where it
               starts and why. Absent when the table did not serve or nothing was cut. */
            if (queryStoreWideEligible && wideResolution.WideStart is DateTime historyStart && historyStart > start)
            {
                payload["query_store_history_starts"] = McpHelpers.FormatEffectiveStart(historyStart);
                payload["query_store_history_note"] = QueryStoreHistoryNote(historyStart, wideResolution.Bound, wideResolution.SettingServer)
                    + (rawServerIds is { Count: > 0 }
                        ? " Servers whose interval table could not serve the window were read from the raw tables, from the window's own start (#6115)."
                        : string.Empty);
                if (wideResolution.SettingServer is not null)
                {
                    payload["query_store_history_set_by"] = wideResolution.SettingServer;
                }
            }

            /* #6115 review: a split run whose passing servers did not move the wide start has no history note above, so the payload always says on its
               own how many servers were read raw. */
            if (queryStoreWideEligible && rawServerIds is { Count: > 0 })
            {
                payload["query_store_raw_servers_note"] = rawServerIds.Count == 1
                    ? "1 server whose interval table could not serve the window was read from the raw tables, from the window's own start (#6115)."
                    : $"{rawServerIds.Count} servers whose interval table could not serve the window were read from the raw tables, from the window's own start (#6115).";
            }

            return ComposeRunOutcome.Ok(payload);
        }
        catch (PostgresException ex)
        {
            /* #4283 review round 1 (M1): 57014 / class 22 / class 42 except 42501 are what a panel author
               can act on — kept as Query failed: {MessageText} under 400, verbatim. Anything else (28P01
               auth failure, 53300 too-many-connections, 57P01 admin shutdown, 3D000 unknown database, ...)
               is a STORE fault the author cannot fix; the real exception rides back on Fault so the WEB
               endpoint (which has the logger) can answer through the same backstop shape #4276 gives an
               uncaught one. outcome.Error/IsServerError stay exactly what they always were either way, so
               run_custom_view_panel's MCP answer does not change.
               #4293 round 2: the decision (including the FATAL/PANIC override) lives in FromPostgresException,
               so a test can run it without a live Postgres round trip. */
            return FromPostgresException(ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Test seam: the outcome keeps only the message, so a test that wants the chain gets it here. */
            onRunException?.Invoke(ex);
            return FromRunException(ex, remapClientTimeout);
        }
    }

    /// <summary>The #4605 minimum window: below this, the wide table's own gate round trips
    /// (one per server, each a fixed handful of small reads) cost more than the read they would save, so a
    /// composed Query Store panel stays raw regardless of coverage — the same pattern
    /// <see cref="QueryStoreIntervalWide.GridWideMinWindow"/> already applies to the grid. A composed panel
    /// may span the WHOLE FLEET rather than one server, so this site keeps its own constant rather than
    /// sharing the grid's; it starts at the grid's own measured 12h pending a composer-specific measurement.</summary>
    internal static readonly TimeSpan ComposeQueryStoreWideMinWindow = QueryStoreIntervalWide.GridWideMinWindow;

    /// <summary>#4617: named so the MCP read census (<see cref="Darling.Tests.McpReadCommandTimeoutTests"/>)
    /// recognises the <c>NpgsqlCommand(string, connection)</c> construction below as a store read rather
    /// than an unrecognised receiver.</summary>
    internal const string QueryStoreWideSchemaVersionSql = "SELECT COALESCE(MAX(version), 0) FROM darling_schema_version";

    /// <summary>#4617: see <see cref="QueryStoreWideSchemaVersionSql"/>.</summary>
    private const string QueryStoreWideServerIdsSql =
        "SELECT server_id, server_name, is_enabled FROM collect.servers WHERE ($1::text[] IS NULL OR server_name = ANY($1)) ORDER BY server_id";

    /// <summary>
    /// #4605: whether a composed Query Store panel over <paramref name="start"/>..<paramref name="end"/>
    /// may read <c>collect.query_store_interval_wide</c> (V145) instead of deduping raw — decided here, in the
    /// runner, BEFORE <see cref="ComposeCompiler.Compile"/> runs, because the compiler stays pure and never
    /// opens a connection. Reuses the pure <see cref="QueryStoreIntervalWide.UseTable"/> decision (through
    /// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>, which also applies the literal-end-before-applied_through
    /// refusal; clause 6, the legacy-row probe, is not asked for here, because the composed panels read the newest snapshot per interval) for EVERY server in scope: a fleet panel
    /// (null/empty <paramref name="serverScope"/>) must pass for every server the store has rows for, or the
    /// hybrid would silently under-read a server whose table coverage lags. Any fault, a schema below V145, or
    /// a refused clause on any server in scope makes the whole panel raw — every input here leans toward raw,
    /// the same rule #3953 already applies to the single-server reads.
    /// </summary>
    internal static async Task<(bool Eligible, DateTime? WideStart, QueryStoreIntervalWide.WideStartBound Bound, string? SettingServer, long? GroupMembers, DateTime? StampThrough, IReadOnlyList<int>? RawServerIds)> ResolveQueryStoreWideEligibleAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, DateTime start, DateTime end,
        DateTime? literalWindowEnd, System.Threading.CancellationToken cancellationToken, PanelPlan? groupMembersFor = null)
    {
        if (end - start < ComposeQueryStoreWideMinWindow)
        {
            return default;
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

            int schemaVersion;
            await using (var probe = new NpgsqlCommand(QueryStoreWideSchemaVersionSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds })
            {
                schemaVersion = (int)(await probe.ExecuteScalarAsync(cancellationToken))!;
            }

            if (schemaVersion < 145)
            {
                return default;
            }

            var wideServers = new List<(int Id, string Name)>();
            /* #5582 L4: the stamp-through lookup takes every server in scope, enabled or not, because the read and the fleet stale arm
               cover a disabled server's rows too. The eligibility checks and the group-member count below still see the enabled ones only. */
            var scopedServerIds = new List<int>();
            await using (var servers = new NpgsqlCommand(QueryStoreWideServerIdsSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds })
            {
                servers.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
                    Value = serverScope is { Count: > 0 } ? (object)serverScope.ToArray() : DBNull.Value,
                });
                await using var reader = await servers.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    scopedServerIds.Add(reader.GetInt32(0));
                    if (reader.GetBoolean(2))
                    {
                        wideServers.Add((reader.GetInt32(0), reader.GetString(1)));
                    }
                }
            }

            if (wideServers.Count == 0)
            {
                return default;
            }

            /* #4689: every server in scope reads from ONE common start, the latest of the per-server read
               starts, so each is exact from there; Bound and SettingServer are the bound and the server_name (the spelling
               the panel's rows carry) of the server that set it. */
            var wideStart = start;
            var bound = QueryStoreIntervalWide.WideStartBound.Window;
            string? settingServer = null;

            /* The floors are one store-wide answer: the first server to reach that step reads them into this
               cache and the rest reuse them; a check that refuses before that step never reads them. */
            var storeWide = new QueryStoreIntervalWide.StoreWideInputsCache();

            /* #6807: the gate's per-server table floor, read for every server that has a coverage row in one statement and not once per
               server (42 round trips, about 0.75 s a panel, on a store with 42 servers). A server with no coverage row refuses ahead of
               the floor step, so it is left out. The gate takes each floor from the cache and gives the answer it gave before. */
            var covered = await ReadQueryStoreCoveredServersAsync(connection, cancellationToken);
            await storeWide.PrefetchTableFloorsAsync(
                connection, wideServers.Where(s => covered.Contains(s.Id)).Select(s => s.Id).ToList(), StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);

            /* #6115: the gate decides per server, and a server that fails no longer sends the whole panel to raw: it reads raw, and the
               servers that pass read the interval table (the split). Each server is in exactly one set, so no row is read twice.
               A server with no coverage row AND no Query Store row in the window is neither: it adds no cost to either half, so it
               must not make the fleet read raw (option 1 of the issue). It is classified with the passing servers, because the wide
               half is "every server not read raw", and a server that reads nothing there costs nothing; and it is left out of the
               wide start, because it has no history to bound. A server with no coverage row and rows in the window cannot use the
               interval table, so it reads raw like any refused server. Decided from the two small probes below, never from the gate,
               whose own answer for such a server is always "refuse". */
            var rawServers = new List<int>();
            var passed = 0;
            foreach (var (serverId, serverName) in wideServers)
            {
                if (!covered.Contains(serverId) && !await QueryStoreServerHasRowsAsync(connection, serverId, start, end, cancellationToken))
                {
                    continue;
                }

                var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                    connection, serverId, start, end, literalWindowEnd, ComposeQueryStoreWideMinWindow,
                    StorageCommandDeadlines.ComposedReadSeconds, ComposeReadNotes.CurrentLogger, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken, storeWide: storeWide);
                if (!plan.UseTable)
                {
                    if (plan.DecisionFailed)
                    {
                        ComposeReadNotes.Note(ReadFallback.GateFailed);
                    }

                    /* A server whose decision faulted reads raw too: raw is the exact source, and a fault leans toward it, as it always did. */
                    rawServers.Add(serverId);
                    continue;
                }

                passed++;
                if (plan.ReadStart > wideStart)
                {
                    wideStart = plan.ReadStart;
                    bound = plan.StartBound;
                    settingServer = serverName;
                }
            }

            /* No server could read the interval table: the whole panel reads raw, as it did before #6115. */
            if (passed == 0)
            {
                return default;
            }

            /* A server in scope that is not enabled is not gated, and the all-wide text has always read its retained rows from the
               interval table unchecked. The split keeps that: it is on the wide side, and costs the raw half nothing. */
            IReadOnlyList<int>? rawServerIds = rawServers.Count > 0 ? rawServers : null;

            /* #5582: for a Query Store RankedTimeSeries panel, the members of its group dimension, on this same connection, so the
               compiler can bound the single-scan base CTE (buckets x members). Null for any other panel, and when unknown. */
            long? groupMembers = groupMembersFor is null
                ? null
                : await QueryStoreGroupMembers.ResolveAsync(connection, groupMembersFor, wideServers.Count, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken, ComposeReadNotes.CurrentLogger);

            /* #5582 part 3: where the compose rollup stops answering, on this same connection. Null (today's route) on any fault. */
            var stampThrough = await ResolveQueryStoreStampThroughAsync(
                connection, (rawServerIds is null ? scopedServerIds : scopedServerIds.Except(rawServerIds).ToList()).ToArray(), wideStart, end, schemaVersion, cancellationToken);

            return (true, wideStart, bound, settingServer, groupMembers, stampThrough, rawServerIds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ComposeReadNotes.NoteFallback(ReadFallback.GateFailed, "#4605 compose Query Store wide-table eligibility check", ex);
            return default;
        }
    }

    /// <summary>#6115: the servers with a coverage row for the interval table. $1 none.</summary>
    internal const string QueryStoreCoveredServersSql = "SELECT server_id FROM collect.query_store_interval_wide_coverage";

    /// <summary>#6115: whether a server holds any raw Query Store row in the window ($1 server, $2 start, $3 end). One index probe on
    /// (server_id, collection_time), and it stops at the first row.</summary>
    internal const string QueryStoreServerHasRowsSql =
        "SELECT EXISTS (SELECT 1 FROM collect.query_store_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)";

    private static async Task<HashSet<int>> ReadQueryStoreCoveredServersAsync(NpgsqlConnection connection, System.Threading.CancellationToken cancellationToken)
    {
        var covered = new HashSet<int>();
        await using var command = new NpgsqlCommand(QueryStoreCoveredServersSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            covered.Add(reader.GetInt32(0));
        }

        return covered;
    }

    private static async Task<bool> QueryStoreServerHasRowsAsync(
        NpgsqlConnection connection, int serverId, DateTime start, DateTime end, System.Threading.CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(QueryStoreServerHasRowsSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// #5582 part 3: the first hour of the window the compose rollup (V173) cannot answer. The hours checked run from
    /// <c>date_trunc('hour', wideStart)</c> to the hour of the window end. An hour is unusable when it is missing from
    /// <c>query_store_compose_stamp_hours</c> (never built), or when a <c>_built</c> pair for a server in scope has
    /// <c>built_seq IS DISTINCT FROM late_seq</c> (a late write since the build). The statement's own stale arm covers the
    /// race between this lookup and the read, so this is only where the rollup arm stops and the wide-table tail starts.
    /// Null means today's route: the schema is below V173, the very first hour is already unusable (nothing to gain), or
    /// the lookup faulted (it leans toward the route that needs no rollup). A usable run to the end is capped at the window
    /// end, because a later instant makes the rollup arm read rows the statement's outer WHERE throws away.
    /// </summary>
    internal static async Task<DateTime?> ResolveQueryStoreStampThroughAsync(
        NpgsqlConnection connection, int[] serverIds, DateTime wideStart, DateTime end, int schemaVersion, System.Threading.CancellationToken cancellationToken)
    {
        if (schemaVersion < QueryStoreComposeStamp.RungVersion || end <= wideStart)
        {
            return null;
        }

        try
        {
            await using var lookup = new NpgsqlCommand(QueryStoreStampThroughSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds };
            lookup.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(wideStart, DateTimeKind.Unspecified) });
            lookup.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
            lookup.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, Value = serverIds });
            var found = await lookup.ExecuteScalarAsync(cancellationToken);
            /* The series stops at the hour of the window end, so a found hour is never after the end, and it is never before the hour
               of wideStart: the one test against wideStart covers "the very first hour is already unusable". */
            var through = found is DateTime unusable ? unusable : end;
            return through <= wideStart ? null : DateTime.SpecifyKind(through, DateTimeKind.Unspecified);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ComposeReadNotes.NoteFallback(ReadFallback.GateFailed, "#5582 compose rollup stamp-through lookup", ex);
            return null;
        }
    }

    /// <summary>#5582 part 3: see <see cref="ResolveQueryStoreStampThroughAsync"/>. $1 wide start, $2 window end, $3 server ids in scope.</summary>
    internal const string QueryStoreStampThroughSql = """
        SELECT MIN(g.hour)
        FROM generate_series(date_trunc('hour', $1::timestamp), date_trunc('hour', $2::timestamp), interval '1 hour') AS g(hour)
        WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_compose_stamp_hours h WHERE h.hour = g.hour)
           OR EXISTS (SELECT 1 FROM collect.query_store_compose_stamp_built b
                      WHERE b.hour = g.hour AND b.server_id = ANY($3::integer[]) AND b.built_seq IS DISTINCT FROM b.late_seq)
        """;

    /// <summary>#4689: the note a Compose Query Store panel carries when the interval table served it from a
    /// start later than the window's. Same wording as the MCP top-queries table route, and #4966 the same text for
    /// the start: the one <c>query_store_history_starts</c> prints beside it, UTC with the Z.</summary>
    internal static string QueryStoreHistoryNote(DateTime historyStart, QueryStoreIntervalWide.WideStartBound bound, string? settingServer = null) =>
        QueryStoreIntervalWide.HistoryNote(McpHelpers.FormatEffectiveStart(historyStart), bound, manyServers: true, settingServer);

    /// <summary>Default compose-run window (hours) when the request omits one.</summary>
    internal const int DefaultComposeHours = 24;

    /* The absolute-instant parser lives in ComposeSpec.TryParseUtcInstant (#2735): the run path's
       windowStart/windowEnd and a stored cell range's windowStart/windowEnd are the SAME vocabulary, so
       they share one parser and can never drift on what "a timestamp" means. */

    /// <summary>Resolves the run's server scope (Erik's Decision 1): the top-level <c>server</c> (a name or an
    /// array of names), else the <c>$server</c> variable value. Absent / "All" / empty ⇒ null (the whole fleet,
    /// no server predicate); one or many names ⇒ that list, which the compiler binds as a
    /// <c>server_name = ANY($n)</c> text[] parameter — never resolved to ids or interpolated.</summary>
    private static List<string>? ParseServerScope(JsonObject body, Dictionary<string, string?> values)
    {
        var names = new List<string>();

        switch (body["server"])
        {
            case JsonArray array:
                foreach (var item in array)
                {
                    if (item is JsonValue v && v.TryGetValue<string>(out var s))
                    {
                        AddServer(names, s);
                    }
                }

                break;
            case JsonValue value when value.TryGetValue<string>(out var single):
                AddServer(names, single);
                break;
            default:
                if (values.TryGetValue(ComposeVariable.ServerDimension, out var fromVariable))
                {
                    AddServer(names, fromVariable);
                }

                break;
        }

        return names.Count == 0 ? null : names;
    }

    /// <summary>Adds a server name to the scope, treating null/empty and the literal "All" as "no scope" (fleet).</summary>
    private static void AddServer(List<string> names, string? name)
    {
        if (!string.IsNullOrEmpty(name) && !string.Equals(name, "All", StringComparison.OrdinalIgnoreCase))
        {
            names.Add(name);
        }
    }

    /// <summary>Parses the run's resolved variable values (<c>{name: scalar}</c>) into the string map the
    /// compiler binds $var filters from. A non-scalar value maps to null (an unresolved variable).</summary>
    private static Dictionary<string, string?> ParseRunValues(JsonNode? node)
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (node is JsonObject obj)
        {
            foreach (var kvp in obj)
            {
                values[kvp.Key] = kvp.Value is JsonValue value
                    ? (value.TryGetValue<string>(out var s) ? s : value.ToString())
                    : null;
            }
        }

        return values;
    }

    /// <summary>#4605: the most the count guard's server-side <c>statement_timeout</c> may be, in seconds. The guard runs under
    /// <c>min(this, the compose deadline)</c>, so a lower operator deadline is never exceeded by the guard.</summary>
    internal const int HourlyEdgesGuardMaxSeconds = 15;

    /// <summary>#4605: how far the guard's client-side deadline sits above its server-side <c>statement_timeout</c>, so the
    /// server's cancel wins the race.</summary>
    internal const int HourlyEdgesGuardClientHeadroomSeconds = 5;

    /// <summary>#4605: the guard's server-side deadline, in whole seconds: <c>min(15, composedSeconds)</c>, and never below one.</summary>
    internal static int HourlyEdgesGuardSeconds(int composedSeconds) => Math.Max(1, Math.Min(HourlyEdgesGuardMaxSeconds, composedSeconds));

    /// <summary>#4605: the snapshot's isolation is set by <c>BeginTransactionAsync</c>; this makes it read only, before any query runs.</summary>
    private const string HourlyEdgesReadOnlySql = "SET TRANSACTION READ ONLY";

    /// <summary>#4605: a savepoint so a guard fault (a 15 s timeout cancels the statement and aborts the transaction) can be
    /// undone and the panel statement still run on the same connection and snapshot.</summary>
    private const string HourlyEdgesSavepointSql = "SAVEPOINT hourly_edges_guard";

    /// <summary>#4605: undoes a failed guard, and with it the guard's <c>SET LOCAL</c>.</summary>
    private const string HourlyEdgesRollbackToSavepointSql = "ROLLBACK TO SAVEPOINT hourly_edges_guard";

    /// <summary>#4605: the guard's own, shorter server-side deadline, in force for the guard statement only. SET takes no bound
    /// parameter, so the text is built from an integer count of seconds and nothing else.</summary>
    internal static string HourlyEdgesGuardTimeoutSql(int guardSeconds) =>
        string.Create(CultureInfo.InvariantCulture, $"SET LOCAL statement_timeout = '{guardSeconds}s'");

    /// <summary>#4605: a savepoint around the module map watermark read, so a fault in that read (the map's state table missing, say)
    /// is undone and the panel statement still runs on the same connection and snapshot.</summary>
    private const string HourlyEdgesModuleMapSavepointSql = "SAVEPOINT hourly_edges_module_map";

    /// <summary>#5811: whether <c>collect.module_map</c> exists, as a catalog lookup.</summary>
    internal const string ModuleMapExistsSql = "SELECT to_regclass('collect.module_map') IS NOT NULL";

    /// <summary>
    /// #5811: whether <c>collect.module_map</c> is missing from this store, so a raw-route module join compiles the window-wide
    /// ranking CTE that needs no map, and otherwise the map's watermark (<see cref="DarlingModuleMap.ReadWatermarkAsync"/>, null when
    /// there is none or its read fails), which bounds the module CTE's procedure_stats overlay (ruling C). A lookup that faults answers
    /// missing: the old CTE is slower, but it runs on any store.
    /// </summary>
    private static async Task<(bool Missing, DateTime? Through)> ProbeModuleMapAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(ModuleMapExistsSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds };
            if (await command.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return (true, null);
            }

            return (false, await DarlingModuleMap.ReadWatermarkAsync(connection, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "The #5811 module map lookup failed; the panel ranks procedure_stats over its window instead.");
            return (true, null);
        }
    }

    /// <summary>#4605: undoes the watermark read's savepoint, whether the read succeeded or failed.</summary>
    private const string HourlyEdgesModuleMapRollbackSql = "ROLLBACK TO SAVEPOINT hourly_edges_module_map";

    /// <summary>#4605: puts the role's <c>statement_timeout</c> (the compose deadline) back for the panel statement. <c>DEFAULT</c> is
    /// the value the session started with, which for the compose role is the operator's configured deadline, so the panel
    /// keeps exactly the timeout it has today. Still <c>LOCAL</c>, so nothing outlives the transaction.</summary>
    private const string HourlyEdgesRestoreTimeoutSql = "SET LOCAL statement_timeout = DEFAULT";

    /// <summary>#4605: the connection and the open REPEATABLE READ READ ONLY transaction one hourly-edges panel run shares between
    /// its count guard and its panel statement, plus the guard's verdict (null when the guard did not pass). Disposing it ends
    /// the transaction, which only read, and returns the connection.</summary>
    private sealed class HourlyEdgesSnapshot : IAsyncDisposable
    {
        private readonly NpgsqlTransaction _transaction;
        private bool _disposed;

        public HourlyEdgesSnapshot(NpgsqlConnection connection, NpgsqlTransaction transaction, ComposeHourlyEdgesVerdict? verdict, DateTime? moduleMapThrough = null)
        {
            Connection = connection;
            _transaction = transaction;
            Verdict = verdict;
            ModuleMapThrough = moduleMapThrough;
        }

        public NpgsqlConnection Connection { get; }

        public ComposeHourlyEdgesVerdict? Verdict { get; }

        /// <summary>The module map's watermark, read in this snapshot, or null when the panel does not join modules, the
        /// guard did not pass, or the map has no watermark.</summary>
        public DateTime? ModuleMapThrough { get; }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            await _transaction.DisposeAsync();
            await Connection.DisposeAsync();
        }
    }

    /// <summary>The <see cref="Exception.Data"/> key marking a guard rollback fault that was already logged.</summary>
    private const string HourlyEdgesLoggedKey = "pm.hourly-edges.logged";

    /// <summary>Notes a fault that abandoned the snapshot start. A guard rollback fault was already logged by the verdict path, so
    /// it is only noted; any other fault is noted and logged.</summary>
    internal static void NoteHourlyEdgesSnapshotStartFault(Exception ex)
    {
        if (ex.Data.Contains(HourlyEdgesLoggedKey))
        {
            ComposeReadNotes.Note(ReadFallback.GateFailed);
        }
        else
        {
            ComposeReadNotes.NoteFallback(ReadFallback.GateFailed, "#4605 compose hourly-edges snapshot start", ex);
        }
    }

    /// <summary>Opens the run's connection, begins the snapshot transaction and runs the count guard in it. A failed OPEN throws
    /// <see cref="ComposeStoreOpenException"/> to the caller. A fault after the open (the begin, the read-only step, a guard
    /// rollback that itself failed) returns null and the panel opens a fresh connection and reads raw; a guard fault that was
    /// undone cleanly returns a snapshot whose verdict is null and the panel reads raw on it. A cancellation propagates.
    /// When the panel joins modules and the guard passed, the module map's watermark is read in the same transaction, so the
    /// watermark and the panel statement see one snapshot.</summary>
    private static async Task<HourlyEdgesSnapshot?> BeginHourlyEdgesSnapshotAsync(
        NpgsqlDataSource postgres, ComposeHourlyEdgesCandidate candidate, IReadOnlyList<string>? serverScope, int composedSeconds,
        bool readModuleMapWatermark, System.Threading.CancellationToken cancellationToken, IReadOnlyList<string>? unregisteredServers = null)
    {
        /* The open is outside the try on purpose: a store that cannot be reached throws ComposeStoreOpenException to the caller, who
           answers it the way it answers the panel's own failed open. Opening again here would cost a second open timeout. */
        var connection = await OpenComposeConnectionAsync(postgres, cancellationToken);
        NpgsqlTransaction? transaction = null;
        try
        {
            transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await ExecuteSnapshotStatementAsync(connection, HourlyEdgesReadOnlySql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);
            var verdict = await ResolveHourlyEdgesVerdictAsync(connection, candidate, serverScope, composedSeconds, cancellationToken, unregisteredServers);
            var moduleMapThrough = verdict is not null && readModuleMapWatermark
                ? await ReadModuleMapWatermarkInSnapshotAsync(connection, cancellationToken)
                : null;
            return new HourlyEdgesSnapshot(connection, transaction, verdict, moduleMapThrough);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            NoteHourlyEdgesSnapshotStartFault(ex);
            if (transaction is not null)
            {
                await transaction.DisposeAsync();
            }

            await connection.DisposeAsync();

            return null;
        }
        catch (OperationCanceledException)
        {
            if (transaction is not null)
            {
                await transaction.DisposeAsync();
            }

            await connection.DisposeAsync();

            throw;
        }
    }

    /// <summary>Reads the module map's watermark on the snapshot connection. The read swallows its own faults and returns null, but
    /// a failed statement aborts the transaction, so the read runs under a savepoint that is always rolled back to: a fault is
    /// undone and the panel reads without the map, and a clean read loses nothing. A rollback that itself fails is rethrown to
    /// the snapshot begin, which abandons the snapshot.</summary>
    private static async Task<DateTime?> ReadModuleMapWatermarkInSnapshotAsync(NpgsqlConnection connection, System.Threading.CancellationToken cancellationToken)
    {
        await ExecuteSnapshotStatementAsync(connection, HourlyEdgesModuleMapSavepointSql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);
        var watermark = await DarlingModuleMap.ReadWatermarkAsync(connection, cancellationToken);
        await ExecuteSnapshotStatementAsync(connection, HourlyEdgesModuleMapRollbackSql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);
        return watermark;
    }

    /// <summary>Opens a transaction on a composed panel's own connection and makes it read only before any statement runs, the way the
    /// hourly-edges snapshot does. The pool this reads on holds write grants, so a compiled statement that tried to change anything is
    /// refused by the store here (SQLSTATE 25006) instead of relying on the SQL the compiler writes. The caller disposes it (a rollback).</summary>
    internal static async Task<NpgsqlTransaction> BeginComposeReadOnlyTransactionAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await ExecuteSnapshotStatementAsync(connection, HourlyEdgesReadOnlySql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);
            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }

    private static async Task ExecuteSnapshotStatementAsync(
        NpgsqlConnection connection, string sql, int commandSeconds, System.Threading.CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// #4605: runs <see cref="IntervalRollupCountGuard.QueryStatsSql"/> on the run's snapshot connection and returns the verdict
    /// the compiler needs to take the hourly-plus-raw-edges route, or null. The guard runs under its own 15 s
    /// <c>statement_timeout</c>, which is put back to the compose deadline before the panel statement. Only a count of 0 is a
    /// pass; any other count, a ledger that does not cover the window (<see cref="IntervalRollupCountGuard.UncoveredResult"/>, noted as
    /// <see cref="ReadFallback.LedgerUncovered"/> with no log line), any fault and any timeout return null, and the panel reads raw.
    /// A store before V164 has no ledger table, and it is found by its schema version, not by the guard's fault: right after the
    /// savepoint, and before the guard's statement timeout and SQL, the run reads the version (<see cref="QueryStoreWideSchemaVersionSql"/>,
    /// the Query Store route's own probe). Below <see cref="QueryStatsHourLedger.RungVersion"/> the guard never runs; the read is
    /// noted <see cref="ReadFallback.GateFailed"/> with no log line, for a state the store's migration ends. An undefined table on a
    /// store at the rung is a real fault and takes the normal path below: one Warning. The guard's <c>$3</c> is bound
    /// from <see cref="ComposeSourceRouter.NormalizeServerScope"/> alone, and the verdict carries that same scope, so a verdict
    /// proven for one scope never serves another. The guard proves the rollup over the scope the panel read takes: by server id, plus
    /// <c>$4</c> (<paramref name="unregisteredServers"/>, the names the registry does not hold) when the run has any (#5525). A failed guard is undone to its savepoint so the transaction stays usable
    /// for the raw panel statement. If that undo itself fails, the failure is rethrown and the snapshot is abandoned.
    /// </summary>
    internal static async Task<ComposeHourlyEdgesVerdict?> ResolveHourlyEdgesVerdictAsync(
        NpgsqlConnection connection, ComposeHourlyEdgesCandidate candidate, IReadOnlyList<string>? serverScope, int composedSeconds,
        System.Threading.CancellationToken cancellationToken, IReadOnlyList<string>? unregisteredServers = null)
    {
        var guardSeconds = HourlyEdgesGuardSeconds(composedSeconds);
        var scope = ComposeSourceRouter.NormalizeServerScope(serverScope);
        try
        {
            await ExecuteSnapshotStatementAsync(connection, HourlyEdgesSavepointSql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);

            /* A store before V164 has no ledger table, and its migration ends that state at the next start. Its schema version says so,
               so read it first (the Query Store wide-table route's own probe, #4617) and do not run a guard that cannot plan: the read
               stays a failed gate (gate_failures in get_read_latency) with no Warning per panel run. The probe sits inside the savepoint,
               so a fault in it is undone like the guard's, and ahead of the guard's statement timeout, so nothing needs putting back.
               Every guard fault, an undefined table on a store at the rung included, still reaches the catch and warns once. */
            int schemaVersion;
            await using (var probe = new NpgsqlCommand(QueryStoreWideSchemaVersionSql, connection) { CommandTimeout = StorageCommandDeadlines.ComposedReadSeconds })
            {
                schemaVersion = Convert.ToInt32(await probe.ExecuteScalarAsync(cancellationToken));
            }

            if (schemaVersion < QueryStatsHourLedger.RungVersion)
            {
                ComposeReadNotes.Note(ReadFallback.GateFailed);
                return null;
            }

            await ExecuteSnapshotStatementAsync(connection, HourlyEdgesGuardTimeoutSql(guardSeconds), StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);

            long mismatches;
            /* Bound to a local so the construction reads `new NpgsqlCommand(<identifier>, connection)`, the shape McpReadCommandTimeoutTests'
               store-receiver census recognises; a method call as the first argument is an unclassifiable receiver there (#5525). */
            var guardSql = IntervalRollupCountGuard.SqlFor(unregisteredServers is { Count: > 0 });
            await using (var guard = new NpgsqlCommand(guardSql, connection) { CommandTimeout = guardSeconds + HourlyEdgesGuardClientHeadroomSeconds })
            {
                guard.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(candidate.HourStartUtc, DateTimeKind.Unspecified) });
                guard.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(candidate.HourEndUtc, DateTimeKind.Unspecified) });
                guard.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
                    Value = scope is null ? DBNull.Value : scope.ToArray(),
                });
                if (unregisteredServers is { Count: > 0 })
                {
                    /* #5525: the names the registry does not hold, matched on the row's stored name, exactly as the panel read matches them. */
                    guard.Parameters.Add(new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text,
                        Value = unregisteredServers.ToArray(),
                    });
                }

                mismatches = Convert.ToInt64(await guard.ExecuteScalarAsync(cancellationToken));
            }

            await ExecuteSnapshotStatementAsync(connection, HourlyEdgesRestoreTimeoutSql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);

            /* The hour ledger does not cover the window yet (its counted_since is after the window start, or its state row is missing).
               That is an expected state, not a fault and not a mismatch: nothing is logged, the scope notes the reason so the read shows
               as a fallback in get_read_latency, and the panel reads raw on this same transaction (#4605). */
            if (mismatches == IntervalRollupCountGuard.UncoveredResult)
            {
                ComposeReadNotes.Note(ReadFallback.LedgerUncovered);
                return null;
            }

            return mismatches == 0
                ? new ComposeHourlyEdgesVerdict(candidate.SourceTable, candidate.HourStartUtc, candidate.HourEndUtc, scope)
                : null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ComposeReadNotes.NoteFallback(ReadFallback.GateFailed, "#4605 compose hourly-edges count guard", ex);

            try
            {
                await ExecuteSnapshotStatementAsync(connection, HourlyEdgesRollbackToSavepointSql, StorageCommandDeadlines.ComposedReadSeconds, cancellationToken);
            }
            catch (Exception rollbackEx) when (rollbackEx is not OperationCanceledException)
            {
                ComposeReadNotes.Warn("#4605 compose hourly-edges guard rollback failed", rollbackEx);
                rollbackEx.Data[HourlyEdgesLoggedKey] = true;

                /* The transaction cannot be trusted (the client timer may have broken the connection): rethrow to the snapshot
                   begin, which disposes it and returns no snapshot, so the panel reads raw on a fresh connection. */
                throw;
            }

            return null;
        }
    }

    /// <summary>Runs a compiled composed query on the viewer pool and serializes its rows to a JSON array of
    /// <c>{column: value}</c> objects — generic over the SELECT shape (bucket / group dims / value).</summary>
    internal static async Task<JsonArray> RunComposedQueryAsync(
        NpgsqlDataSource postgres, ComposeCompiled compiled, int clientDeadlineSeconds, System.Threading.CancellationToken cancellationToken,
        NpgsqlConnection? sharedConnection = null)
    {
        /* The connection is opened first and on its own, so a pool-wait or connect timeout (also an
           NpgsqlException over a TimeoutException) is never mistaken for the client timer of a running
           statement. Only a timeout raised below, while the statement executes, becomes the marker.
           A caller that already holds the run's connection (the hourly-edges snapshot, #4605) passes it and keeps
           ownership; every other caller leaves it null and this method opens, and disposes, its own. */
        NpgsqlConnection? ownedConnection = null;
        NpgsqlTransaction? ownedTransaction = null;
        if (sharedConnection is null)
        {
            ownedConnection = await OpenComposeConnectionAsync(postgres, cancellationToken);
            ownedTransaction = await BeginComposeReadOnlyTransactionAsync(ownedConnection, cancellationToken);
        }

        await using var ownedScope = ownedConnection;
        await using var ownedTransactionScope = ownedTransaction;
        var connection = sharedConnection ?? ownedConnection!;
        await using var command = new NpgsqlCommand(compiled.Sql, connection, ownedTransaction);
        command.CommandTimeout = clientDeadlineSeconds;
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new JsonArray();
        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new JsonObject();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : DbValueToJson(reader.GetValue(i));
                }

                rows.Add(row);
            }
        }
        catch (NpgsqlException ex) when (ex.InnerException is TimeoutException)
        {
            throw new ComposeStatementClientTimeoutException(ex);
        }

        return rows;
    }

    /// <summary>Compiles + runs the panel's event-annotation overlays (design D5) on the viewer pool, returning
    /// the added top-level <c>annotations</c> array: one <c>{source, events:[{ts, label}]}</c> object per requested
    /// source (an EMPTY events list when a source has no events in the window). Each query is catalog-only,
    /// schema-qualified <c>collect.*</c>, window + server-scoped, and capped — the same discipline and viewer
    /// <c>statement_timeout</c> backstop as the measure query. An annotation query failure surfaces through the
    /// caller's try/catch exactly like the measure query's (a runaway overlay fails the run with a clear 400,
    /// rather than silently dropping markers the caller can't tell are missing).</summary>
    private static async Task<JsonArray> RunAnnotationsAsync(
        NpgsqlDataSource postgres, PanelPlan plan, ComposeRunContext runContext, int clientDeadlineSeconds, System.Threading.CancellationToken cancellationToken)
    {
        var annotations = new JsonArray();
        var serverClocks = plan.Annotations.Any(a => a.Frame == AnnotationClockFrame.ServerLocal)
            ? await ReadServerClocksAsync(postgres, runContext, clientDeadlineSeconds, cancellationToken)
            : ComposeCompiler.NoServerClocks;
        foreach (var (source, compiled) in ComposeCompiler.CompileAnnotations(plan, runContext, serverClocks))
        {
            var events = await RunComposedQueryAsync(postgres, compiled, clientDeadlineSeconds, cancellationToken);
            annotations.Add(new JsonObject { ["source"] = source, ["events"] = events });
        }

        return annotations;
    }

    /// <summary>Each panel server's clock, read before a server-local annotation query so that query can place
    /// every marker by the offset in force on its own date (#4821). Same pool and same timeout as the
    /// annotation queries; a failure surfaces through the caller's try/catch the same way.</summary>
    private static async Task<IReadOnlyDictionary<string, PerformanceMonitor.Analysis.Baselines.ServerClock>> ReadServerClocksAsync(
        NpgsqlDataSource postgres, ComposeRunContext runContext, int clientDeadlineSeconds, System.Threading.CancellationToken cancellationToken)
    {
        var compiled = ComposeCompiler.CompileServerClockRead(runContext);
        await using var connection = await OpenComposeConnectionAsync(postgres, cancellationToken);
        await using var transaction = await BeginComposeReadOnlyTransactionAsync(connection, cancellationToken);
        await using var command = new NpgsqlCommand(compiled.Sql, connection, transaction);
        command.CommandTimeout = clientDeadlineSeconds;
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await ComposeCompiler.ReadServerClocksAsync(reader, cancellationToken);
        }
        catch (NpgsqlException ex) when (ex.InnerException is TimeoutException)
        {
            throw new ComposeStatementClientTimeoutException(ex);
        }
    }

    internal static JsonValue? DbValueToJson(object value) => value switch
    {
        DateTime dt => JsonValue.Create(dt.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create((double)f),
        decimal m => JsonValue.Create((double)m),
        long l => JsonValue.Create(l),
        int n => JsonValue.Create(n),
        short s => JsonValue.Create((int)s),
        bool b => JsonValue.Create(b),
        string str => JsonValue.Create(str),
        _ => JsonValue.Create(value.ToString()),
    };
}
