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
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The layered store-connection self-test behind the Viewer's connect-failure "Run self-test"
/// (#1954 bullet 3; bullets 1–2 — path provenance and the non-secret parse summary — shipped in
/// #1966). A failed <c>NpgsqlConnection.Open</c> collapses five distinct failure layers into one
/// generic error; this walks them separately — config parse, DNS, TCP, TLS + authentication,
/// schema-version gate — and reports exactly which layer failed, so "firewall problem" is
/// diagnosable by elimination (DNS passed, TCP did not) from the Viewer's own seat. The
/// service-side <c>--test-connection</c> is the shape precedent; this is its store-facing twin.
///
/// <para>TLS and authentication share one network exchange (Npgsql negotiates SSL and then
/// authenticates inside a single Open), so they are probed by one Open and attributed by
/// exception shape — see <see cref="ClassifyOpenFailure"/>, which is pure and pinned by tests.
/// Every layer failure is a RESULT, never a throw; layers after a failure report NotReached.</para>
///
/// <para>Read-only by construction: the schema layer runs the same
/// <c>SELECT COALESCE(MAX(version), 0) FROM darling_schema_version</c> read the migration applier
/// uses (after the same session-scoped <c>SET search_path</c>), and never creates the version
/// table or takes the migration advisory lock.</para>
/// </summary>
public static class StoreConnectionSelfTest
{
    /// <summary>Layer names, in probe order — shared with the report formatter and the tests.</summary>
    public const string ConfigLayer = "config";
    public const string DnsLayer = "dns";
    public const string TcpLayer = "tcp";
    public const string TlsLayer = "tls";
    public const string AuthLayer = "auth";
    public const string SchemaLayer = "schema";

    /// <summary>
    /// The report's first line. Public so the Viewer can splice a RE-RUN's report over a previous
    /// one in the failure overlay instead of stacking duplicates.
    /// </summary>
    public const string ReportHeader = "Connection self-test:";

    /// <summary>Per-network-layer budget. Npgsql's own Timeout governs the Open probe.</summary>
    private static readonly TimeSpan NetworkProbeTimeout = TimeSpan.FromSeconds(8);

    public enum LayerOutcome
    {
        Passed,
        Failed,
        /// <summary>Probe not applicable here (an IP-literal host needs no DNS; SSL Mode=Disable has no TLS).</summary>
        Skipped,
        /// <summary>An earlier layer failed, so this one never ran.</summary>
        NotReached
    }

    /// <summary>One probed layer's verdict. <see cref="Detail"/> is operator-facing prose and never
    /// carries credentials — it is built from layer facts, not from the connection string.</summary>
    public sealed record LayerResult(string Layer, LayerOutcome Outcome, string Detail, TimeSpan Elapsed)
    {
        public override string ToString() => $"{Layer}: {Outcome} — {Detail}";
    }

    /// <summary>
    /// Runs the full ladder against <paramref name="connectionString"/>. Never converts a failure
    /// into a throw (a malformed string is the config layer's failure); always returns all six
    /// layers in probe order. The one exception is GENUINE caller cancellation, which propagates
    /// as <see cref="OperationCanceledException"/> instead of being fabricated into a layer
    /// verdict like "refused/unreachable" — cancelling the probe is not a network finding.
    /// </summary>
    public static async Task<IReadOnlyList<LayerResult>> RunAsync(
        string connectionString, CancellationToken cancellationToken = default)
    {
        var results = new List<LayerResult>(6);

        /* ---- config: parse the string and extract the endpoint facts the ladder needs. */
        NpgsqlConnectionStringBuilder builder;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (Exception ex)
        {
            results.Add(new LayerResult(ConfigLayer, LayerOutcome.Failed,
                $"connection string did not parse: {ex.Message}", stopwatch.Elapsed));
            AddNotReached(results, DnsLayer, TcpLayer, TlsLayer, AuthLayer, SchemaLayer);
            return results;
        }

        var host = builder.Host;
        var port = builder.Port;
        if (string.IsNullOrWhiteSpace(host))
        {
            results.Add(new LayerResult(ConfigLayer, LayerOutcome.Failed,
                "connection string parsed but names no Host", stopwatch.Elapsed));
            AddNotReached(results, DnsLayer, TcpLayer, TlsLayer, AuthLayer, SchemaLayer);
            return results;
        }

        var sslDisabled = builder.SslMode == SslMode.Disable;
        results.Add(new LayerResult(ConfigLayer, LayerOutcome.Passed,
            $"parsed; endpoint {host}:{port.ToString(CultureInfo.InvariantCulture)}, ssl mode {builder.SslMode}", stopwatch.Elapsed));

        /* ---- dns: IP literals need no lookup; otherwise resolve and name what we got. */
        IPAddress[] addresses;
        stopwatch.Restart();
        if (IPAddress.TryParse(host, out var literal))
        {
            addresses = new[] { literal };
            results.Add(new LayerResult(DnsLayer, LayerOutcome.Skipped,
                "host is an IP literal; no DNS lookup needed", stopwatch.Elapsed));
        }
        else
        {
            try
            {
                using var dnsCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                dnsCts.CancelAfter(NetworkProbeTimeout);
                addresses = await Dns.GetHostAddressesAsync(host, dnsCts.Token);
                if (addresses.Length == 0)
                {
                    results.Add(new LayerResult(DnsLayer, LayerOutcome.Failed,
                        $"'{host}' resolved to no addresses", stopwatch.Elapsed));
                    AddNotReached(results, TcpLayer, TlsLayer, AuthLayer, SchemaLayer);
                    return results;
                }

                results.Add(new LayerResult(DnsLayer, LayerOutcome.Passed,
                    $"'{host}' → {string.Join(", ", addresses.Select(a => a.ToString()))}", stopwatch.Elapsed));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                results.Add(new LayerResult(DnsLayer, LayerOutcome.Failed,
                    $"could not resolve '{host}': {Trim(ex)}", stopwatch.Elapsed));
                AddNotReached(results, TcpLayer, TlsLayer, AuthLayer, SchemaLayer);
                return results;
            }
        }

        /* ---- tcp: reach the port on any resolved address. DNS passing while TCP fails is the
                by-elimination firewall signature the issue names. */
        stopwatch.Restart();
        string? tcpFailure = null;
        var connected = false;
        foreach (var address in addresses)
        {
            try
            {
                using var client = new TcpClient(address.AddressFamily);
                using var tcpCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                tcpCts.CancelAfter(NetworkProbeTimeout);
                await client.ConnectAsync(address, port, tcpCts.Token);
                results.Add(new LayerResult(TcpLayer, LayerOutcome.Passed,
                    $"connected to {address}:{port.ToString(CultureInfo.InvariantCulture)}", stopwatch.Elapsed));
                connected = true;
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                tcpFailure = $"timed out after {NetworkProbeTimeout.TotalSeconds:0}s reaching {address}:{port.ToString(CultureInfo.InvariantCulture)} (DNS passed — a filtered port or firewall is the usual cause)";
            }
            catch (Exception ex)
            {
                tcpFailure = $"{address}:{port.ToString(CultureInfo.InvariantCulture)} refused/unreachable: {Trim(ex)}";
            }
        }

        if (!connected)
        {
            results.Add(new LayerResult(TcpLayer, LayerOutcome.Failed,
                tcpFailure ?? "no address could be tried", stopwatch.Elapsed));
            AddNotReached(results, TlsLayer, AuthLayer, SchemaLayer);
            return results;
        }

        /* ---- tls + auth: one Open, attributed by exception shape. On success both layers pass
                (tls reports Skipped instead when the string disables SSL outright). */
        stopwatch.Restart();
        NpgsqlConnection? connection = null;
        try
        {
            connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            results.Add(TlsPassResult(builder, sslDisabled, stopwatch.Elapsed));
            results.Add(new LayerResult(AuthLayer, LayerOutcome.Passed,
                $"authenticated as '{builder.Username}' to database '{builder.Database}'", stopwatch.Elapsed));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            throw;
        }
        catch (Exception ex)
        {
            var (failedLayer, detail) = ClassifyOpenFailure(ex);
            if (failedLayer == AuthLayer)
            {
                /* Auth failing means the TLS exchange (if any) already succeeded. */
                results.Add(TlsPassResult(builder, sslDisabled, stopwatch.Elapsed));
                results.Add(new LayerResult(AuthLayer, LayerOutcome.Failed, detail, stopwatch.Elapsed));
            }
            else
            {
                results.Add(new LayerResult(TlsLayer, LayerOutcome.Failed, detail, stopwatch.Elapsed));
                results.Add(new LayerResult(AuthLayer, LayerOutcome.NotReached, "TLS failed first", stopwatch.Elapsed));
            }

            AddNotReached(results, SchemaLayer);
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            return results;
        }

        /* ---- schema: the applier's own version read, read-only, compared to this build's gate. */
        stopwatch.Restart();
        try
        {
            await using (connection)
            {
                using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection) { CommandTimeout = (int)NetworkProbeTimeout.TotalSeconds })
                {
                    await setPath.ExecuteNonQueryAsync(cancellationToken);
                }

                int storeVersion;
                using (var readVersion = new NpgsqlCommand(
                    "SELECT COALESCE(MAX(version), 0) FROM darling_schema_version", connection) { CommandTimeout = (int)NetworkProbeTimeout.TotalSeconds })
                {
                    storeVersion = Convert.ToInt32(
                        await readVersion.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
                }

                var (outcome, detail) = EvaluateSchemaGate(storeVersion, StorageVersion.SchemaVersion);
                results.Add(new LayerResult(SchemaLayer, outcome, detail, stopwatch.Elapsed));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PostgresException pg) when (pg.SqlState is PostgresErrorCodes.UndefinedTable
                                                        or PostgresErrorCodes.InsufficientPrivilege)
        {
            /* Two non-exceptional shapes need distinguishing, and both need a second, PRIVILEGE-FREE
               probe: the version table is OWNER-ONLY (the viewer role gets 42501 — the viewer's own
               startup gate probes schema by SHAPE for exactly this reason), and a missing table (42P01)
               may mean "not a Darling store at all". information_schema is NOT that probe — it hides
               tables the current role has no privilege on (a live viewer-role test misread a healthy
               store as not-a-store through it); to_regclass resolves through pg_class, which is
               catalog-visible to every role regardless of table privileges. */
            results.Add(await ProbeStoreShapeAsync(connectionString, pg.SqlState, stopwatch, cancellationToken));
        }
        catch (Exception ex)
        {
            results.Add(new LayerResult(SchemaLayer, LayerOutcome.Failed,
                $"version read failed: {Trim(ex)}", stopwatch.Elapsed));
        }

        return results;
    }

    /// <summary>
    /// The schema layer's fallback when <c>darling_schema_version</c> is unreadable: report whether
    /// this is a Darling store at all (via information_schema, readable by every role), and be honest
    /// about what could not be checked. A viewer-role connection lands here by DESIGN (the version
    /// table is owner-only), so "store detected, exact version unreadable by this role" is a Skipped,
    /// not a Failed — the Viewer's startup shape-probe gate still covers real skew.
    /// </summary>
    private static async Task<LayerResult> ProbeStoreShapeAsync(
        string connectionString, string sqlState, Stopwatch stopwatch, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            /* Both tables, both historical homes (V8 moved the store into collect; pre-V8 lived in
               public) — one privilege-free catalog round-trip. */
            using var probe = new NpgsqlCommand(
                @"SELECT (to_regclass('collect.collection_log') IS NOT NULL OR to_regclass('public.collection_log') IS NOT NULL),
                         (to_regclass('collect.darling_schema_version') IS NOT NULL OR to_regclass('public.darling_schema_version') IS NOT NULL)",
                connection) { CommandTimeout = (int)NetworkProbeTimeout.TotalSeconds };
            using var reader = await probe.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            var storeShapeFound = reader.GetBoolean(0);
            var versionTableFound = reader.GetBoolean(1);

            if (!storeShapeFound && !versionTableFound)
            {
                return new LayerResult(SchemaLayer, LayerOutcome.Failed,
                    "no darling_schema_version and no collection_log — this database is not (yet) a Darling store; point the viewer at the store the service writes, or start the service once to create it",
                    stopwatch.Elapsed);
            }

            return sqlState == PostgresErrorCodes.InsufficientPrivilege
                ? new LayerResult(SchemaLayer, LayerOutcome.Skipped,
                    "Darling store detected, but the version table is readable only by the store owner (this looks like the least-privilege viewer role) — the viewer's own startup gate checks schema skew by shape instead",
                    stopwatch.Elapsed)
                : new LayerResult(SchemaLayer, LayerOutcome.Failed,
                    "collection_log exists but darling_schema_version does not — a partially created store; start the service against it so migrations can finish",
                    stopwatch.Elapsed);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new LayerResult(SchemaLayer, LayerOutcome.Failed,
                $"version read failed ({sqlState}), and the fallback shape probe also failed: {Trim(ex)}", stopwatch.Elapsed);
        }
    }

    /// <summary>
    /// Attributes a failed <c>Open</c> to the TLS or the auth layer — pure so the table is pinned
    /// by unit tests. Auth-layer shapes: the PostgreSQL authentication/authorization SQLSTATEs
    /// (28P01 invalid password, 28000 invalid authorization, 3D000 unknown database — the server
    /// only says so AFTER a successful handshake+auth exchange reaches it). TLS-layer shapes:
    /// <see cref="AuthenticationException"/> anywhere in the chain (certificate validation,
    /// protocol mismatch), or an Npgsql failure whose message names SSL/TLS/certificate. Anything
    /// else unrecognized defaults to the TLS layer with the raw message, because it happened
    /// between TCP (already proven) and authentication (never reported).
    /// </summary>
    public static (string Layer, string Detail) ClassifyOpenFailure(Exception ex)
    {
        if (FindPostgres(ex) is { } pg)
        {
            return pg.SqlState switch
            {
                PostgresErrorCodes.InvalidPassword =>
                    (AuthLayer, $"the server rejected the password for '{pg.MessageText.Split('"').Skip(1).FirstOrDefault() ?? "the user"}' (28P01)"),
                PostgresErrorCodes.InvalidAuthorizationSpecification =>
                    (AuthLayer, $"authorization rejected: {pg.MessageText} (28000 — wrong user, or pg_hba.conf does not allow this client)"),
                PostgresErrorCodes.InvalidCatalogName =>
                    (AuthLayer, $"authenticated, but the database does not exist: {pg.MessageText} (3D000)"),
                _ => (AuthLayer, $"server rejected the session: {pg.MessageText} ({pg.SqlState})")
            };
        }

        for (Exception? walk = ex; walk is not null; walk = walk.InnerException)
        {
            if (walk is AuthenticationException)
            {
                return (TlsLayer, $"TLS handshake failed: {walk.Message} — wrong/expired certificate, hostname mismatch, or the server does not speak TLS on this port");
            }
        }

        var message = Trim(ex);
        if (message.Contains("SSL", StringComparison.OrdinalIgnoreCase)
            || message.Contains("TLS", StringComparison.OrdinalIgnoreCase)
            || message.Contains("certificate", StringComparison.OrdinalIgnoreCase))
        {
            return (TlsLayer, $"TLS negotiation failed: {message}");
        }

        return (TlsLayer, $"connection failed after TCP connect (unclassified — reported at the TLS layer by elimination): {message}");
    }

    /// <summary>
    /// The schema-gate verdict — pure. Equal versions pass; an older store says the SERVICE must
    /// upgrade it (the viewer never migrates); a newer store says this viewer is the old half; and
    /// 0 rows means the version table exists but has never been stamped.
    /// </summary>
    public static (LayerOutcome Outcome, string Detail) EvaluateSchemaGate(int storeVersion, int viewerVersion)
    {
        if (storeVersion == viewerVersion)
        {
            return (LayerOutcome.Passed, $"store schema v{storeVersion.ToString(CultureInfo.InvariantCulture)} matches this viewer");
        }

        if (storeVersion == 0)
        {
            return (LayerOutcome.Failed,
                "darling_schema_version has no rows — the store was never migrated; start the service against it once");
        }

        return storeVersion < viewerVersion
            ? (LayerOutcome.Failed,
                $"store schema v{storeVersion.ToString(CultureInfo.InvariantCulture)} is OLDER than this viewer (v{viewerVersion.ToString(CultureInfo.InvariantCulture)}) — upgrade/restart the service so it migrates the store")
            : (LayerOutcome.Failed,
                $"store schema v{storeVersion.ToString(CultureInfo.InvariantCulture)} is NEWER than this viewer (v{viewerVersion.ToString(CultureInfo.InvariantCulture)}) — upgrade the viewer to match the service");
    }

    /// <summary>
    /// The operator-facing report: one line per layer plus a verdict line naming the first failed
    /// layer (or "all layers passed"). Pure — the Viewer appends this to the failure overlay and
    /// the log; tests pin it.
    /// </summary>
    public static string FormatReport(IReadOnlyList<LayerResult> results)
    {
        var report = new StringBuilder();
        report.AppendLine(ReportHeader);
        foreach (var result in results)
        {
            report.AppendLine(CultureInfo.InvariantCulture,
                $"  [{OutcomeMark(result.Outcome)}] {result.Layer,-6} {result.Detail} ({result.Elapsed.TotalMilliseconds:0} ms)");
        }

        var firstFailure = results.FirstOrDefault(r => r.Outcome == LayerOutcome.Failed);
        if (firstFailure is not null)
        {
            report.Append(CultureInfo.InvariantCulture, $"Verdict: failed at the {firstFailure.Layer} layer.");
            return report.ToString();
        }

        /* Don't overclaim what a Skipped layer didn't prove (an unproven TLS handshake under
           ssl mode Prefer, or the owner-only version table under the viewer role). */
        var skipped = results.Where(r => r.Outcome == LayerOutcome.Skipped).Select(r => r.Layer).ToList();
        report.Append(skipped.Count == 0
            ? "Verdict: all layers passed — the store is reachable, authenticated, and schema-compatible."
            : $"Verdict: no layer failed ({string.Join(", ", skipped)} skipped — see above).");
        return report.ToString();
    }

    /// <summary>
    /// The TLS layer's result when Open SUCCEEDED (directly or by reaching authentication).
    /// Honest about the negotiated modes: under Prefer/Allow a successful Open does not prove a
    /// handshake happened — the server may simply not offer TLS — so those report Skipped with the
    /// caveat rather than a hollow Passed. Require/VerifyCA/VerifyFull cannot succeed without the
    /// handshake, so only those earn a Passed.
    /// </summary>
    private static LayerResult TlsPassResult(NpgsqlConnectionStringBuilder builder, bool sslDisabled, TimeSpan elapsed)
    {
        if (sslDisabled)
        {
            return new LayerResult(TlsLayer, LayerOutcome.Skipped, "SSL Mode=Disable — no TLS handshake to test", elapsed);
        }

        return builder.SslMode is SslMode.Prefer or SslMode.Allow
            ? new LayerResult(TlsLayer, LayerOutcome.Skipped,
                $"ssl mode {builder.SslMode} — connection opened, but this mode does not require TLS, so the handshake is unproven; use Require/VerifyFull to test it", elapsed)
            : new LayerResult(TlsLayer, LayerOutcome.Passed, $"handshake OK (ssl mode {builder.SslMode})", elapsed);
    }

    private static string OutcomeMark(LayerOutcome outcome) => outcome switch
    {
        LayerOutcome.Passed => "PASS",
        LayerOutcome.Failed => "FAIL",
        LayerOutcome.Skipped => "skip",
        _ => " -- "
    };

    private static void AddNotReached(List<LayerResult> results, params string[] layers)
    {
        foreach (var layer in layers)
        {
            results.Add(new LayerResult(layer, LayerOutcome.NotReached, "an earlier layer failed", TimeSpan.Zero));
        }
    }

    private static PostgresException? FindPostgres(Exception ex)
    {
        for (Exception? walk = ex; walk is not null; walk = walk.InnerException)
        {
            if (walk is PostgresException pg)
            {
                return pg;
            }
        }

        return null;
    }

    /// <summary>Innermost message — the outer Npgsql wrapper usually just repeats "an exception occurred".</summary>
    private static string Trim(Exception ex)
    {
        var inner = ex;
        while (inner.InnerException is not null)
        {
            inner = inner.InnerException;
        }

        return inner.Message;
    }
}
