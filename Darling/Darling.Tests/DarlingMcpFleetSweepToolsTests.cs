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
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the fleet sweep MCP slice (#3466 lane 4) — <c>get_sweep_reports</c>, the read-only tool over the
/// SAME <c>FleetSweepPresentation</c> builders and store reads the web feed serves — in the
/// <c>get_alert_settings</c> sibling shape: the tool surface is EXACTLY the one name (static, on a
/// <c>[McpServerToolType]</c> class, returning <c>Task&lt;string&gt;</c>); the param contract is pinned
/// with every parameter optional; the advertised schema is Gemini-clean (#1074) with no required params;
/// every refusal returns BEFORE the store is touched (proven on a dead data source); and the description
/// carries the two field contracts an agent plans against — the mute-header semantics and the
/// quiet-is-not-clean liveness block — because for this tool the description IS the interface the
/// spec's non-goals section designed.
///
/// <para>The wire shapes themselves are lane 3's <c>FleetSweepWebFeedTests</c>' property — the builders
/// are shared, so a shape pinned there is pinned for this tool — and the live end-to-end pass (real
/// store, real rows, the tool called against a real TimescaleDB) runs in the out-of-band harness the
/// lane's verification uses, since this project cannot execute on the store's platform matrix.</para>
/// </summary>
public sealed class DarlingMcpFleetSweepToolsTests
{
    /// <summary>A dead data source (unroutable port) — proves every refusal path bails WITHOUT opening a
    /// connection, the <c>DarlingMcpAlertToolsSurfaceAndSqlTests</c> idiom.</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpFleetSweepTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheOneSweepReportRead()
    {
        var toolMethods = ToolMethods();
        var only = Assert.Single(toolMethods);

        Assert.Equal("get_sweep_reports", only.GetCustomAttribute<McpServerToolAttribute>()!.Name);
        Assert.NotNull(typeof(DarlingMcpFleetSweepTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.True(only.IsStatic, "get_sweep_reports must be static");
        Assert.True(only.ReturnType == typeof(Task<string>), "get_sweep_reports must return Task<string>");
    }

    [Fact]
    public void ParamContract_MatchesTheContract_AndEveryParamIsOptional()
    {
        var method = ToolMethods().Single();
        var described = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .ToArray();

        Assert.Equal(new[] { "hours_back", "as_of", "sweep_id", "watch_state" }, described.Select(p => p.Name).ToArray());
        Assert.All(described, p => Assert.True(p.HasDefaultValue, $"{p.Name} must be optional — the tool answers with no input at all"));
    }

    /// <summary>#2548's rule applied to sweep ids: they are tick-scale integers past the 2^53 boundary
    /// where a JSON number silently loses digits in a double-based client, so the parameter is TEXT and
    /// the tool parses it exactly or refuses. A <c>long</c> here would accept a rounded neighbor and
    /// answer the honest-looking "no such sweep".</summary>
    [Fact]
    public void SweepId_IsAStringParameter_TheJsonNumberRoundTripRule()
    {
        var sweepId = ToolMethods().Single().GetParameters().Single(p => p.Name == "sweep_id");
        Assert.Equal(typeof(string), sweepId.ParameterType);
    }

    /// <summary>The shared <c>as_of</c> contract, verbatim — every windowed read on both SKUs describes
    /// the parameter with the ONE constant, so two surfaces cannot describe the same knob two ways.</summary>
    [Fact]
    public void AsOf_CarriesTheSharedDescription_Verbatim()
    {
        var asOf = ToolMethods().Single().GetParameters().Single(p => p.Name == "as_of");
        Assert.Equal(McpHelpers.AsOfDescription, asOf.GetCustomAttribute<DescriptionAttribute>()!.Description);
    }

    /// <summary>
    /// The description is this tool's real interface — the spec's non-goals split makes
    /// <c>get_sweep_reports</c> the narration substrate an agent reads, so the two field contracts must be
    /// IN the description rather than in a document the agent never sees: a sweep taken under master-off
    /// means DELIVERY WAS OFF (the mute header's meaning), with the ledger present-and-empty vs absent
    /// distinction stated; and <c>instruments_alive: false</c> means the sweep could not prove its own
    /// data sources — quiet is not clean, an empty window on such a sweep is unreadable, not healthy.
    /// </summary>
    [Fact]
    public void Description_CarriesTheMuteSemantics_AndTheQuietIsNotCleanBlock()
    {
        var description = ToolMethods().Single()
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("MUTE SEMANTICS", description, StringComparison.Ordinal);
        Assert.Contains("DELIVERY WAS OFF", description, StringComparison.Ordinal);
        Assert.Contains("would_have_paged", description, StringComparison.Ordinal);
        Assert.Contains("muted, and nothing would have paged", description, StringComparison.Ordinal);
        Assert.Contains("ABSENT on an alerts-on sweep", description, StringComparison.Ordinal);
        Assert.Contains("QUIET IS NOT CLEAN", description, StringComparison.Ordinal);
        Assert.Contains("instruments_alive", description, StringComparison.Ordinal);
        Assert.Contains("instrument_liveness", description, StringComparison.Ordinal);
        /* The knob's home is named, so an agent that wants the cadence goes to the settings read. */
        Assert.Contains("fleet_sweep", description, StringComparison.Ordinal);
        /* #3898 Phase 2: the instructions' cross-server paragraph carried this cadence fact; once that
           paragraph was cut, the description became the only place an agent could learn it. */
        Assert.Contains("at most one daily rollup", description, StringComparison.Ordinal);
    }

    /// <summary>#3487's output side, stated where an agent reads: every sweep id the payload carries
    /// rides as a JSON string — the input half of #2548's rule was always described, and the output
    /// half now closes the loop, so a quoted id reads as the contract rather than a quirk. The
    /// emission itself is pinned in <c>FleetSweepWebFeedTests</c> on the shared builders.</summary>
    [Fact]
    public void Description_StatesTheIds_AreEmittedAsStrings()
    {
        var description = ToolMethods().Single()
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("spelled as a JSON string", description, StringComparison.Ordinal);
        Assert.Contains("pass one back verbatim", description, StringComparison.Ordinal);
    }

    /* ---------------- refusals, before the store is ever touched ---------------- */

    [Theory]
    [InlineData(0, null, null, null, "Invalid hours_back value '0'")]
    [InlineData(-3, null, null, null, "Invalid hours_back value '-3'")]
    [InlineData(999, null, null, null, "exceeds maximum")]
    [InlineData(4, "not-a-time", null, null, "Invalid as_of value")]
    [InlineData(4, "2099-01-01T00:00:00Z", null, null, "in the future")]
    [InlineData(1, null, null, "garbage", "omit for open + carried")]
    [InlineData(1, null, null, "Open", "omit for open + carried")] /* exact spellings — the web feed's rule */
    [InlineData(1, null, "abc", null, "Invalid sweep_id value 'abc'")]
    [InlineData(1, null, "12.5", null, "Invalid sweep_id value '12.5'")]
    [InlineData(1, null, "99999999999999999999999999", null, "Invalid sweep_id value")] /* overflows long */
    public async Task BadInput_IsRefused_WithoutTouchingTheStore(
        int hoursBack, string? asOf, string? sweepId, string? watchState, string expectedFragment)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        var result = await DarlingMcpFleetSweepTools.GetSweepReports(dead, NullLogger.Instance, hoursBack, asOf, sweepId, watchState);

        /* The refusal is the `invalid` envelope since #3739; the fragments are pinned on its sentence. */
        Assert.True(McpHelpers.IsRefusalEnvelope(result), result);
        Assert.Contains(expectedFragment, McpHelpers.ErrorMessageOf(result), StringComparison.Ordinal);
        /* A refusal, not a swallowed store error — the dead store was never reached. */
        Assert.DoesNotContain("Error during get_sweep_reports", result, StringComparison.Ordinal);
    }

    /// <summary>With <c>sweep_id</c> set the span knobs are not consulted at all — the one-sweep ask is a
    /// different question, and refusing its id exactly (rather than binding a window it did not ask about)
    /// keeps the two modes from answering each other's mistakes.</summary>
    [Fact]
    public async Task ASweepIdAsk_IsParsedBeforeAnyWindowBinding()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);

        /* The out-of-range hours_back would refuse FIRST if the window were bound on this path. */
        var result = await DarlingMcpFleetSweepTools.GetSweepReports(dead, NullLogger.Instance, 999, null, "abc", null);

        Assert.Contains("Invalid sweep_id value 'abc'", McpHelpers.ErrorMessageOf(result), StringComparison.Ordinal);
    }

    /* ---------------- the shared builders: lane 3's shape pins cover this surface ---------------- */

    /// <summary>
    /// Both of this tool's read paths — the timeline and the <c>sweep_id</c> fetch — serve
    /// <c>FleetSweepPresentation</c>'s builders, and that join is what makes lane 3's shape pins THIS
    /// tool's pins too: the #3478 embedded-report contract (<c>would_have_paged</c> absent on an
    /// alerts-on sweep, stripped from legacy documents that carry it fabricated, present under mute)
    /// is held in <c>FleetSweepWebFeedTests</c> against the one builder both paths embed the stored
    /// document through. A hand-rolled node in this file would put the tool outside those pins, and
    /// the two surfaces could drift into different opinions about what a sweep looks like — the
    /// zero-drift rule this source pin makes structural.
    /// </summary>
    [Fact]
    public void BothReadPaths_ServeTheSharedBuilders_SoLane3sShapePinsCoverThisTool()
    {
        var source = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpFleetSweepTools.cs");

        Assert.Contains("FleetSweepPresentation.BuildTimelineNode(", source, StringComparison.Ordinal);
        Assert.Contains("FleetSweepPresentation.BuildSweepDetailNode(", source, StringComparison.Ordinal);

        /* #3482: the worklist rides the same builder AND the same names gate-and-read the web feed
           uses — the ValidateWatchState sharing pattern — so this tool's watch_items carry the same
           server field the page reads, resolved by the same rules, or the two surfaces could name
           the same worklist differently. */
        Assert.Contains("FleetSweepPresentation.BuildWatchItemsNode(watchItems, watchItemNames)", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFleetSweepEndpoints.ReadWatchItemNamesAsync(", source, StringComparison.Ordinal);
    }

    /* ---------------- the /api/read mirror ---------------- */

    /// <summary>The read dispatch carries the tool (the catalog-parity test in
    /// <c>DarlingWebEndpointsTests</c> enforces membership; this pins the BINDING — the query keys an
    /// operator's script would use), and <c>ExcludedToolNames</c> does NOT name it: read tools are
    /// mirrored, and the sweep tool is a read.</summary>
    [Fact]
    public void TheReadDispatch_CarriesTheTool_AndTheExclusionListDoesNot()
    {
        Assert.True(DarlingWebEndpoints.BuildReadDispatch().ContainsKey("get_sweep_reports"),
            "get_sweep_reports must be on the /api/read/* 1:1 surface — read tools are mirrored");
        Assert.DoesNotContain("get_sweep_reports", DarlingWebEndpoints.ExcludedToolNames);
    }

    /// <summary>The mirror's logger seat (#3473 review, the web half): the dispatch entry hands the
    /// tool the BUILDER's captured logger — <c>MapAll</c> builds the dispatch with the web host's
    /// service logger, the same instance the MCP host injects with <c>AddSingleton&lt;ILogger&gt;</c>
    /// — so the mirror's child reads trace into the service log instead of the hardcoded null this
    /// entry carried while the dashboard app's provider-less factory was the only alternative. The
    /// shared <c>ReadToolHandler</c> delegate stays three-seat; the logger rides by closure.</summary>
    [Fact]
    public void TheMirrorsLoggerSeat_TakesTheCapturedServiceLogger_NotAHardcodedNull()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");

        /* The entry passes the captured logger, and MapAll is the caller that supplies it. */
        Assert.Contains("DarlingMcpFleetSweepTools.GetSweepReports(pg, logger,", source, StringComparison.Ordinal);
        Assert.Contains("BuildReadDispatch(logger)", source, StringComparison.Ordinal);

        /* The delegate itself did NOT grow a seat — the ~100-entry table stays three-parameter,
           which is the whole reason the logger rides by closure. */
        Assert.Contains(
            "internal delegate Task<string> ReadToolHandler(HttpContext context, NpgsqlDataSource postgres, DarlingAnalysisService analysis);",
            source, StringComparison.Ordinal);
    }

    /* #3898 Phase 2 (D5): the instructions' duplicate cross-server paragraph naming this tool and its field
       contracts is gone — Description_CarriesTheMuteSemantics_AndTheQuietIsNotCleanBlock above is now the
       only pin on them, the tool's own description being the sole surface left to carry them. */

    /* ---------------- advertised MCP schema ---------------- */

    private static Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        /* Mirrors the host's AddSingleton<ILogger>(_logger) (#3473 review): the tool's logger is a
           DI-resolved service parameter like postgres, and this registration is what keeps it OUT of
           the advertised schema — remove it and the no-required-params assertion below goes red,
           because the logger parameter carries no default. */
        services.AddSingleton(typeof(ILogger), _ => NullLogger.Instance);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpFleetSweepTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_WithNoRequiredParams()
    {
        var tools = BuildToolSchemas();
        var tool = Assert.Single(tools).Value;

        var violations = DarlingMcpSchemaAssert.Violations(tool.Name, tool.InputSchema).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));

        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(tool.InputSchema));

        /* The service seats stay off the wire: an agent is offered the four described knobs and
           nothing else — neither the store nor the logger is a parameter a client can send. */
        Assert.DoesNotContain("logger", tool.InputSchema.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("postgres", tool.InputSchema.ToString(), StringComparison.Ordinal);
    }
}
