/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #4277: pins every STORE session's <c>timezone</c> to UTC, client-side — defence in depth alongside the
/// managed runtime's own v9 <c>postgresql.conf</c> block (<c>DarlingManagedPostgres.BuildTimeZoneConfAppend</c>),
/// which only reaches a MANAGED store. A bring-your-own store keeps whatever zone its owner set, or whatever
/// initdb took from its host, and every timestamp column in both stores is <c>timestamp without time zone</c>
/// holding naive UTC. A naive-to-<c>timestamptz</c> comparison — a bare <c>now()</c> in the SQL text, or a
/// bound <c>DateTime</c> with <c>Kind=Utc</c> — is resolved by PostgreSQL at the SESSION's <c>TimeZone</c>, so a
/// non-UTC store session silently shifts those reads (measured on #4277: the same one-hour predicate returned
/// 66 rows under UTC and 343 under America/New_York — five hours of data under a one-hour window).
///
/// <para>Every STORE call site — the worker's own store connection, the MCP/web host connections, the CLI
/// verbs, the managed-runtime bootstrap/upgrade, the viewer, the store connection self-test — routes its
/// resolved connection string through this ONE helper, right before the connection or data source is created.
/// A MONITORED target's connection string never does: its own session zone is read on purpose (see
/// <c>PgTargetBaselineProvider.Clock.cs</c>, which resolves a target's local clock from its collected
/// <c>pg_server_config</c> snapshot, not from a live session), and pinning it there would make that resolver's
/// own read wrong.</para>
///
/// <para>Overwrites a caller-supplied <c>Timezone</c> rather than leaving it — unlike
/// <c>DarlingWorker.EnsureStoreSearchPath</c>'s set-if-absent for <c>SearchPath</c>, and unlike the viewer's
/// connect <c>Timeout</c>, where an operator's explicit value wins. The store's columns are naive UTC
/// regardless of what a caller's connection string says, so there is no reading of "the caller meant it";
/// a store connection string that names another zone is always a mistake this pin corrects.</para>
/// </summary>
public static class DarlingStoreConnection
{
    /// <summary>
    /// The pin itself: Npgsql's <c>Timezone</c> connection-string keyword, forced to <c>UTC</c> — by APPENDING
    /// it to the caller's own string, never by rewriting the string through a builder. A builder round trip
    /// writes an explicit empty value (<c>Password=''</c>, <c>Root Certificate=""</c>, ...) as a bare
    /// <c>Key=</c>, and its own reader treats that as "not set" — silently dropping the keyword and letting
    /// Npgsql fall back to a <c>PG*</c> environment variable or a default file where the caller's empty value
    /// meant "never fall back" (round-1 review on #4285's PR). Parsing once first keeps today's exception on a
    /// malformed string at this exact statement; the parsed builder is then discarded, so every OTHER keyword
    /// in the caller's string survives byte for byte. Npgsql keeps only the LAST value for a repeated keyword,
    /// so appending still overwrites any <c>Timezone</c>/<c>TimeZone</c> the caller's string already sets,
    /// wherever it appears and in whatever case — measured against trailing <c>;</c>, trailing spaces, and an
    /// odd-case existing key, all of which still parse to a single UTC <c>Timezone</c> after the append.
    /// </summary>
    public static string PinSessionTimeZoneUtc(string connectionString)
    {
        /* Parse once, so a malformed string fails here exactly as Npgsql's own constructor would — the
           discarded builder exists only for that validation, never for re-serializing the string. */
        _ = new NpgsqlConnectionStringBuilder(connectionString);

        return connectionString + ";Timezone=UTC";
    }

    /// <summary>The session setting the MCP host puts on its store connections (#6281), and the value it carries. The store's guard
    /// on the tag writes (V209) refuses an MCP session by this marker as well as by the <c>mcp</c> login, so an MCP that connects as
    /// the store's owner, or as another login, is held to the same rule.</summary>
    public const string McpSurfaceSetting = "darling.surface";

    /// <summary>The value of <see cref="McpSurfaceSetting"/> on an MCP connection.</summary>
    public const string McpSurfaceValue = "mcp";

    /// <summary>
    /// Forces <c>-c darling.surface=mcp</c> into the connection's startup <c>Options</c> (#6281), keeping any <c>Options</c> the
    /// caller already set and putting the marker LAST, so it wins over a <c>darling.surface</c> the caller's own string names
    /// (Postgres applies <c>-c</c> options in order). Appended as a quoted <c>Options</c> keyword, like
    /// <see cref="PinSessionTimeZoneUtc"/>, so every other keyword survives byte for byte. Only the MCP host calls it: the
    /// service, the Viewer and the web must not carry the marker.
    /// </summary>
    public static string WithMcpSurfaceMarker(string connectionString)
    {
        var existing = new NpgsqlConnectionStringBuilder(connectionString).Options;
        var options = string.IsNullOrWhiteSpace(existing)
            ? $"-c {McpSurfaceSetting}={McpSurfaceValue}"
            : $"{existing.Trim()} -c {McpSurfaceSetting}={McpSurfaceValue}";
        return connectionString + ";Options=\"" + options.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    /// <summary>
    /// The ONE place every STORE connection string picks up its surface's <c>ApplicationName</c> (#4479) —
    /// <c>DarlingManagedPostgres.WebApplicationName</c>/<c>McpApplicationName</c> (#4442) generalized to
    /// every other surface that opens a store connection: the service's own collection loop, the CLI verbs,
    /// the managed-runtime bootstrap/upgrade, and the viewer. Set-if-ABSENT, unlike
    /// <see cref="PinSessionTimeZoneUtc"/>'s overwrite: a bring-your-own connection string that already
    /// names an <c>ApplicationName</c> (an operator's own monitoring convention) keeps it, because there is
    /// no store-side reading of "the caller meant it" here the way there is for the naive-UTC columns —
    /// this is purely a courtesy label for <c>pg_stat_activity</c> and a store log's <c>%a</c>, and the
    /// operator's own choice is the more informative one when they made one.
    /// </summary>
    public static string WithApplicationName(string connectionString, string applicationName)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (!string.IsNullOrEmpty(builder.ApplicationName))
        {
            return connectionString;
        }

        builder.ApplicationName = applicationName;
        return builder.ConnectionString;
    }

    /// <summary>
    /// The text a site prints or logs when a STORE connection string cannot be used, in place of the
    /// exception's own message. Npgsql reports a keyword it cannot set as <c>Couldn't set &lt;keyword&gt;</c>, and
    /// the .NET parser lowercases that keyword. A password with an unquoted <c>;</c> makes the text from that
    /// <c>;</c> to the next <c>=</c> the "keyword", and a password that starts with <c>=</c> puts the whole
    /// password there, so the message (and its <c>ParamName</c>, and the inner exception) can quote part of a
    /// secret. A bad value for a real keyword (<c>Port</c>, <c>SSL Mode</c>) names only the key, and the value
    /// sits in the inner exception. A password whose tail becomes a <c>Host</c> value with a ':' in it fails later,
    /// when Npgsql splits that value into host and port, and that exception's message quotes the text after the ':'.
    ///
    /// <para>This method never includes <c>ex.Message</c> or any inner exception's message. It has three outputs:
    /// (1) when <see cref="ArgumentException.ParamName"/> is a keyword that Npgsql accepts
    /// (<see cref="NpgsqlConnectionStringBuilder.ContainsKey"/>), the CANONICAL spelling of that keyword, taken
    /// from Npgsql's own list (its <c>Port</c>, its <c>SSL Mode</c>) and never the spelling in the exception, which
    /// can be a spelling that came from the secret; (2) one fixed sentence, written here, for each of Npgsql's own
    /// setting conflicts (Multiplexing without Pooling and the like), which the exception's type and Npgsql's fixed
    /// message identify and which carry no operator text; (3) one fixed hint for everything else. The output is
    /// never built from the secret. It does depend on the string in one narrow way: a fragment of a password that
    /// is itself the name of a Npgsql keyword (<c>;port=x</c>) prints that keyword's canonical name, never its value.
    /// A site that passes this text to a logger must not also pass the exception: a logger appends the
    /// exception's message (and, on the console and in the Event Log, its inner exceptions).</para>
    /// </summary>
    public static string DescribeParseFailure(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        if (ex is ArgumentException { ParamName: { Length: > 0 } paramName }
            && FindCanonicalKeyword(paramName) is { } keyword)
        {
            return "Npgsql rejected the value for the '" + keyword + "' keyword. Check that value. " + ParseErrorNotShown;
        }

        if (DescribeSettingsConflict(ex) is { } conflict)
        {
            return conflict;
        }

        return "Npgsql rejected the string. A value that contains ';' or '=' must be quoted, for example Password='pa;ss'. "
            + ParseErrorNotShown;
    }

    /// <summary>
    /// <see cref="DescribeParseFailure"/>'s text when <paramref name="connectionString"/> cannot be used, or null
    /// when it can. For a site that builds a data source from the string right afterwards and wants to say why it
    /// cannot, without a catch around the statement that builds it.
    ///
    /// <para>The proof is Npgsql's own construction step, the one <see cref="NpgsqlDataSource"/>'s <c>Create</c>
    /// runs at those sites, and not only the string parser: construction also runs Npgsql's validation, which
    /// splits a <c>Host</c> value on its last ':' and range-checks the port (a password whose tail is
    /// <c>Host=x:y</c> throws a <see cref="FormatException"/> that quotes <c>y</c>), and which rejects setting
    /// conflicts. The string is built the way those sites build theirs (an application name, then the session time
    /// zone pin), and the data source is disposed at once. Nothing is opened: no socket, no pool connection. A
    /// string that passes here does not throw from a site's <c>Create</c> for a parse or a validation reason.</para>
    /// </summary>
    public static string? TryDescribeParseFailure(string connectionString)
    {
        try
        {
            /* A string with no Host is not a quoting mistake, so it gets its own sentence. It names no text of the string. */
            if (string.IsNullOrWhiteSpace(new NpgsqlConnectionStringBuilder(connectionString).Host))
            {
                return MissingHostHint;
            }

            using var dataSource = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(connectionString, ProofApplicationName)));
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return DescribeParseFailure(ex);
        }
    }

    /// <summary>
    /// Npgsql's canonical spelling of the keyword <paramref name="paramName"/> names, or null. Both checks must
    /// pass: Npgsql accepts the name as a keyword (its own case and synonym rules, which include some non-ASCII
    /// characters that fold to ASCII ones), and it equals, ignoring case and spaces, the display name of one
    /// public property of <see cref="NpgsqlConnectionStringBuilder"/>. The result is that display name, so no
    /// character of <paramref name="paramName"/> reaches the output. A synonym that is not a display name
    /// gets null and so the fixed hint.
    /// </summary>
    private static string? FindCanonicalKeyword(string paramName)
    {
        if (!new NpgsqlConnectionStringBuilder().ContainsKey(paramName))
        {
            return null;
        }

        var squashed = paramName.Replace(" ", string.Empty, StringComparison.Ordinal);
        foreach (var canonical in CanonicalKeywords.Value)
        {
            if (string.Equals(canonical.Replace(" ", string.Empty, StringComparison.Ordinal), squashed, StringComparison.OrdinalIgnoreCase))
            {
                return canonical;
            }
        }

        return null;
    }

    private static readonly Lazy<string[]> CanonicalKeywords = new(() =>
        typeof(NpgsqlConnectionStringBuilder)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(property => property.GetCustomAttribute<DisplayNameAttribute>()?.DisplayName)
            .Where(name => !string.IsNullOrEmpty(name))
            .Select(name => name!)
            .ToArray());

    /// <summary>
    /// Our own sentence for a setting conflict that Npgsql's construction step rejects. Npgsql's messages for these
    /// are fixed text with no operator input, but they are Npgsql's to reword and one of them embeds numbers, so
    /// the exception's type and message are only compared against Npgsql's known text here and never printed. A
    /// message that a later Npgsql rewords gets null (so the general hint), and the tests that pin each conflict
    /// against the real Npgsql fail to say so.
    /// </summary>
    private static string? DescribeSettingsConflict(Exception ex)
    {
        var message = ex.Message;
        string? conflict = null;

        if (ex is ArgumentException)
        {
            if (message.Equals("Pooling must be on to use multiplexing", StringComparison.Ordinal))
            {
                conflict = "Multiplexing needs Pooling to be on.";
            }
            else if (message.StartsWith("SSL Mode has to be Require or higher to be used with direct SSL Negotiation", StringComparison.Ordinal))
            {
                conflict = "SSL Negotiation=Direct needs SSL Mode Require, Verify CA or Verify Full.";
            }
            else if (message.StartsWith("Connection can't have 'Max Pool Size'", StringComparison.Ordinal))
            {
                conflict = "Maximum Pool Size is smaller than Minimum Pool Size.";
            }
        }
        else if (ex is NotSupportedException
            && message.StartsWith("Target Session Attributes other then Any is only supported with multiple hosts", StringComparison.Ordinal))
        {
            conflict = "Target Session Attributes other than Any needs more than one host in Host.";
        }

        return conflict is null ? null : "Npgsql rejected the settings: " + conflict;
    }

    /// <summary>The application name on the data source <see cref="TryDescribeParseFailure"/> builds and disposes. It
    /// is never used, because nothing is opened.</summary>
    private const string ProofApplicationName = "darling-connection-string-proof";

    private const string MissingHostHint = "The connection string has no Host. Add Host=<server name or address> to postgres.connectionString.";

    private const string ParseErrorNotShown = "The parse error is left out on purpose, because it can quote part of a secret.";
}
