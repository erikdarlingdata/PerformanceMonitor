/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The statements the service sends to a PostgreSQL target that it can know at startup (#7681). The shape registry
/// (<see cref="PgOwnStatement"/>) lives in process memory and fills as <see cref="PgOwnStatement.Mark"/> runs, so after a restart the
/// first hourly text fetch, which rides the first statement-statistics run, would not know a daily collector's statement and
/// would not count its pre-marker entry as the service's. The service registers these once, before the first collector runs.
/// <para>Every PostgreSQL collector comes from <see cref="CollectorCatalog.All"/>, the list the runner dispatches from, so a new
/// collector is covered without anyone adding it here. A statement whose text depends on the target (an extension's schema, the
/// server version, a watermark) is registered from the default context and again when it is first sent; the store keeps a learned
/// pair meanwhile (see <c>PgStatementText.UpsertSql</c>).</para>
/// </summary>
internal static class PgServiceStatements
{
    /// <summary>The probe and helper statements outside the collectors, without their markers.</summary>
    internal static IReadOnlyList<string> ProbeTexts { get; } = new[]
    {
        PgFileSettingsCapability.ProbeSql,
        PgLogFormatCapability.ProbeSql,
        PgReadBinaryFileCapability.ProbeSql,
        DarlingCollectorRunner.LogTimezoneReadSql,
        DarlingCollectorRunner.PgExtensionSchemasSql,
        DarlingServerConnector.PostgresDetectionQueryText,
        DarlingServerConnector.PostgresWaitSamplingProbeQueryText,
        DarlingServerConnector.PostgresIdentityQueryText,
        DarlingServerConnector.PostgresIdentityPrivilegeQueryText,
        PostgresTargetProvider.SearchPathSql,
        HypotheticalIndexExperiment.ExperimentNamesSql,
    };

    /// <summary>
    /// The statement each PostgreSQL collector in the catalog builds for a default context. A definition that builds none without a
    /// target is named in <paramref name="needsTarget"/>: <c>pg_cpu_utilization</c> has no SQL route (it throws
    /// <see cref="NotSupportedException"/>, its rows come from the RDS ingestor) and <c>pg_log_events</c> needs the log route and key
    /// (<see cref="InvalidOperationException"/>); the log tail runs every minute, so it registers itself within one. One that fails
    /// any other way is left out and named in <paramref name="unbuildable"/>.
    /// </summary>
    internal static List<string> CollectorTexts(List<string>? unbuildable = null, List<string>? needsTarget = null)
    {
        var texts = new List<string>();
        foreach (var definition in CollectorCatalog.All)
        {
            if (definition.TargetEngine != CollectorTargetEngine.PostgreSql)
            {
                continue;
            }

            try
            {
                var query = definition.BuildQueryUntyped(new CollectorContext
                {
                    ServerId = 0,
                    ServerName = string.Empty,
                    CollectionTime = DateTime.UtcNow,
                    Deltas = new CollectorDeltaCalculator(),
                    Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                });
                if (query is not null)
                {
                    texts.Add(query.Text);
                }
            }
            catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException)
            {
                needsTarget?.Add(definition.Name);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                unbuildable?.Add(definition.Name);
            }
        }

        return texts;
    }

    /// <summary>Registers every statement above. Called once, before the first collector can run.</summary>
    internal static void Register()
    {
        foreach (var text in CollectorTexts())
        {
            PgOwnStatement.RegisterKnown(text);
        }

        foreach (var text in ProbeTexts)
        {
            PgOwnStatement.RegisterKnown(text);
        }

        /* The named-zone statements are marked where they are declared, so reading the fields registers them. */
        _ = PgLogNamedZone.ResolveSql.Length + PgLogNamedZone.ResolveKnownZoneSql.Length;
    }
}
