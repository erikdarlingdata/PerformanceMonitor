/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Base for collector definitions whose query text is PostgreSQL — including Amazon Aurora
/// PostgreSQL. Exists for exactly one reason: <see cref="TargetEngine"/> is sealed to
/// <see cref="CollectorTargetEngine.PostgreSql"/> here, so a Postgres definition cannot forget to
/// declare its dialect.
/// <para>That footgun is otherwise real and silent. <see cref="ICollectorSchemaInfo.TargetEngine"/>
/// defaults to <see cref="CollectorTargetEngine.SqlServer"/> — deliberately, so the 41 existing
/// definitions needed no edit — which means a new Postgres definition that derived from
/// <see cref="CollectorDefinitionBase{TRow}"/> and omitted the override would be advertised as T-SQL
/// and dispatched at SQL Server targets, where its query would fail every cycle. Deriving from this
/// class instead makes the dialect structural rather than a line someone has to remember.</para>
/// <para>Everything else is inherited unchanged: <see cref="CollectorDefinitionBase{TRow}.AppliesTo"/>
/// stays available for gating WITHIN Postgres (server version floors, Aurora-only surfaces, whether
/// an extension is installed), because the engine check is composed on top by
/// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>.</para>
/// </summary>
/// <typeparam name="TRow">The definition's row type.</typeparam>
public abstract class PostgresCollectorDefinitionBase<TRow> : CollectorDefinitionBase<TRow>
{
    /// <summary>
    /// Always <see cref="CollectorTargetEngine.PostgreSql"/>. Sealed so a derived definition cannot
    /// re-declare itself as another engine.
    /// </summary>
    public sealed override CollectorTargetEngine TargetEngine => CollectorTargetEngine.PostgreSql;
}
