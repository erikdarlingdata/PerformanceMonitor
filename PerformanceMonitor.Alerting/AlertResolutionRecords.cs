/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The history row both products write when an alert clears (#6003, #6010). One builder, so Lite's and Darling's rows
/// cannot drift: the investigation <c>resolved</c> detector in each store reads exactly the <see cref="ResolvesMember"/>
/// this writes. Darling's <c>DarlingSelfAlertEvaluator.BuildResolutionRecord</c> forwards here.
/// </summary>
public static class AlertResolutionRecords
{
    /// <summary>The member of a resolution row's <c>context_json</c> that names the metric the row resolves.</summary>
    public const string ResolvesMember = "Resolves";

    /// <summary>
    /// Builds a resolved-flavored history row for a recovered condition: the resolution title as the row's metric,
    /// <c>resolved</c> as its value, the statement-masked message, and a context naming the FIRING metric.
    ///
    /// <para>"No send channel" is stated by <see cref="AlertDelivery.NoChannelApplies"/> rather than spelled as a delivered
    /// row. Saying it with <c>AlertSent: true</c> made <c>alert_sent</c> mean "delivered" on a fired row and "no channel
    /// applies" here, so no aggregate over the column meant anything: the delivered fraction rose with the number of
    /// resolutions.</para>
    /// </summary>
    public static AlertHistoryRecord BuildResolutionRecord(AlertResolution resolution, string? contextJson = null) => new(
        resolution.ServerKey, resolution.ServerName, resolution.Title,
        CurrentValueText: "resolved", ThresholdValueText: "",
        NumericCurrentValue: null, NumericThresholdValue: null,
        Delivery: AlertDelivery.NoChannelApplies(),
        Muted: false, DetailText: SensitiveStatements.Text(resolution.Message),
        ContextJson: ResolutionContextJson(contextJson, resolution.MetricName));

    /// <summary>
    /// A resolution row's <c>context_json</c>: the caller's context, when it gave one, plus <see cref="ResolvesMember"/> naming
    /// the FIRING metric. The row's <c>metric_name</c> is the resolution's title ("CPU Resolved"), not the metric that fired
    /// ("High CPU"), so without this member nothing in the store ties a clear back to the alert it ends. A context that is
    /// not a JSON object is replaced: no writer passes one.
    /// </summary>
    public static string ResolutionContextJson(string? contextJson, string metricName)
    {
        JsonObject node;
        try
        {
            node = string.IsNullOrWhiteSpace(contextJson)
                ? new JsonObject()
                : JsonNode.Parse(contextJson) as JsonObject ?? new JsonObject();
        }
        catch (JsonException)
        {
            /* A resolution context that is not JSON is replaced: the caller's text, not a store read. */
            node = new JsonObject();
        }

        node[ResolvesMember] = metricName;
        return node.ToJsonString();
    }
}
