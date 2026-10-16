/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Per-server and per-database threshold overrides for <see cref="AlertEngine"/> (#5910). The engine asks for
/// every tunable threshold (each <see cref="AlertThresholdKey"/>) through <see cref="Resolve"/>, handing over the
/// global value it read from <see cref="IAlertEngineSettings"/>; an implementation returns the value that applies
/// to that server, or to that database on that server, and returns <c>globalValue</c> when it holds no override.
///
/// <para><b>Optional.</b> The engine takes it as a nullable constructor argument, and null is exactly the engine
/// before #5910: every threshold is the global one. Lite passes none, so it stays advise-only.</para>
///
/// <para><b>Contract.</b> Called on the sweep path, once per check per server and once per row for the
/// database-scoped keys, so it must answer from memory: no I/O and no blocking. The engine uses the value it
/// returns as-is (apart from re-ordering the two Warning/Critical pairs, which no single-key check can keep in
/// order), so an implementation returns only values the global setting's own validation would accept. A throw is
/// logged and the engine falls back to the global value for that read rather than lose the whole check.</para>
/// </summary>
public interface IAlertThresholdOverrides
{
    /// <summary>
    /// The value of <paramref name="key"/> for <paramref name="serverKey"/>, or for <paramref name="database"/> on
    /// that server. <paramref name="database"/> is non-null only for a key whose
    /// <see cref="AlertThresholdKeys.ScopeOf"/> is <see cref="AlertThresholdScope.ServerAndDatabase"/>, and only on
    /// a read that evaluates one database's row; it is null on every server-level read of the same key.
    /// Return <paramref name="globalValue"/> when nothing overrides it.
    /// </summary>
    int Resolve(AlertThresholdKey key, string serverKey, string? database, int globalValue);
}
