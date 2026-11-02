/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using T = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpPlanForceTools;

namespace Darling.Tests;

/// <summary>
/// The binding of each Level 3 tool (#7109, slice 4c), with no database. The proposal stores what <see cref="T.ProposeAsync"/> makes of the
/// tool's binding helper, PostgreSQL hands it back as jsonb text, and the press compares it with the same helper over a fresh prepare result.
/// The same facts give no changed key, a changed fact names its key, and what the design leaves out (evict's before image) is not compared.
/// </summary>
public sealed class ActionProposalBindingTests
{
    private static readonly JsonSerializerOptions JsonbLike = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>The propose side: the helper's binding as <c>ProposeAsync</c> stores it, read back as PostgreSQL's jsonb prints it (no escapes for
    /// quotes or angle brackets).</summary>
    private static JsonObject Stored(JsonObject binding) =>
        JsonNode.Parse(JsonNode.Parse(T.ToObject(binding).ToJsonString())!.ToJsonString(JsonbLike))!.AsObject();

    private static void AssertSameAndKeys(JsonObject proposed, JsonObject pressed, params string[] keys)
    {
        var stored = Stored(proposed);
        Assert.Empty(ActionProposalPress.Differences(stored, pressed));
        Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), stored.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(keys.OrderBy(k => k, StringComparer.Ordinal), pressed.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal));
    }

    private const string Statement = "EXEC [db's <x>].sys.sp_query_store_force_plan @query_id = 5, @plan_id = 7; -- \"q\" & é";

    [Fact]
    public void Force_TheSameFactsMatch_AndAnotherForcedPlanIsAChange()
    {
        T.ForcePlanFacts Facts(long? forced) => new(true, "db's", 5, 7, Statement, forced, false, null!);
        AssertSameAndKeys(T.ForcePlanBinding(Facts(3)), T.ForcePlanBinding(Facts(3)), "statement", "forced_plan_id", "is_forced");
        Assert.Equal(["forced_plan_id"], ActionProposalPress.Differences(Stored(T.ForcePlanBinding(Facts(3))), T.ForcePlanBinding(Facts(9))));
        Assert.Equal(["forced_plan_id"], ActionProposalPress.Differences(Stored(T.ForcePlanBinding(Facts(null))), T.ForcePlanBinding(Facts(9))));
    }

    [Fact]
    public void Evict_ComparesTheStatementOnly_NotTheBeforeImageOrWarnings()
    {
        var proposed = new T.EvictPlanFacts("0xAB", "DBCC FREEPROCCACHE (0xAB);", new { executions = 3, cpu = 10 }, ["w1"]);
        var pressed = new T.EvictPlanFacts("0xAB", "DBCC FREEPROCCACHE (0xAB);", new { executions = 900, cpu = 7000 }, []);
        AssertSameAndKeys(T.EvictPlanBinding(proposed), T.EvictPlanBinding(pressed), "statement");
    }

    [Fact]
    public void Recompile_TheStatementAndObjectType_AndAChangedTypeIsAChange()
    {
        T.RecompileModuleFacts Facts(string type) => new("db", "[dbo].[p]", "EXEC sys.sp_recompile N'[dbo].[p]';", type, new { name = "p" });
        AssertSameAndKeys(T.RecompileModuleBinding(Facts("SQL_STORED_PROCEDURE")), T.RecompileModuleBinding(Facts("SQL_STORED_PROCEDURE")),
            "statement", "object_type");
        Assert.Equal(["object_type"], ActionProposalPress.Differences(Stored(T.RecompileModuleBinding(Facts("SQL_STORED_PROCEDURE"))),
            T.RecompileModuleBinding(Facts("SQL_TRIGGER"))));
    }

    [Fact]
    public void CreateGuide_TheStatement()
    {
        var definition = new PlanGuideDefinition("g", "OBJECT", "SELECT 1;", "dbo.p", null, "OPTION (RECOMPILE)");
        var facts = new T.CreatePlanGuideFacts("db", definition, Statement, new { });
        AssertSameAndKeys(T.CreatePlanGuideBinding(facts), T.CreatePlanGuideBinding(facts with { Before = new { other = 1 } }), "statement");
        Assert.Equal(["statement"], ActionProposalPress.Differences(Stored(T.CreatePlanGuideBinding(facts)),
            T.CreatePlanGuideBinding(facts with { Statement = "EXEC other;" })));
    }

    [Fact]
    public void DropGuide_TheFullDefinitionAndDisabledFlag_AndADisabledOrChangedGuideIsAChange()
    {
        var guide = new PlanGuideDefinition("g", "SQL", "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= @p", null, "@p int", "OPTION (OPTIMIZE FOR (@p = 1))");
        var facts = new T.DropPlanGuideFacts("db", "g", guide, false, "EXEC sys.sp_control_plan_guide N'DROP', N'g';", new { });
        AssertSameAndKeys(T.DropPlanGuideBinding(facts), T.DropPlanGuideBinding(facts with { Guide = guide with { } }), "guide", "disabled", "statement");
        Assert.Equal(["disabled"], ActionProposalPress.Differences(Stored(T.DropPlanGuideBinding(facts)), T.DropPlanGuideBinding(facts with { Disabled = true })));
        Assert.Equal(["guide"], ActionProposalPress.Differences(Stored(T.DropPlanGuideBinding(facts)),
            T.DropPlanGuideBinding(facts with { Guide = guide with { Hints = "OPTION (MAXDOP 1)" } })));
    }

    [Fact]
    public void SqlKill_CarriesWhatTheBatchCompares_AndANewBatchIsAChange()
    {
        var login = new DateTime(2026, 11, 1, 9, 0, 0, 123, DateTimeKind.Utc);
        var ended = new DateTime(2026, 11, 1, 9, 5, 0, 457, DateTimeKind.Utc);
        T.SqlKillBinding Binding(DateTime? lastEnd, string status = "sleeping", int open = 1) =>
            new(87, login, login.AddHours(-5), lastEnd, lastEnd?.AddHours(-5), status, open);
        AssertSameAndKeys(T.SqlKillBindingObject(Binding(ended)), T.SqlKillBindingObject(Binding(ended)),
            "session_id", "login_time", "login_time_server_local", "last_request_end_time", "last_request_end_time_server_local", "status",
            "open_transaction_count");

        /* Session 87 commits, runs a new batch and sleeps again with a new transaction: only the last end time tells. */
        Assert.Equal(["last_request_end_time", "last_request_end_time_server_local"],
            ActionProposalPress.Differences(Stored(T.SqlKillBindingObject(Binding(ended))), T.SqlKillBindingObject(Binding(ended.AddSeconds(4)))));
        Assert.Equal(["last_request_end_time", "last_request_end_time_server_local"],
            ActionProposalPress.Differences(Stored(T.SqlKillBindingObject(Binding(null))), T.SqlKillBindingObject(Binding(ended))));
        Assert.Equal(["open_transaction_count", "status"],
            ActionProposalPress.Differences(Stored(T.SqlKillBindingObject(Binding(ended))), T.SqlKillBindingObject(Binding(ended, "running", 2))));
    }

    [Fact]
    public void PgKill_TheSameFactsMatch_AndANewTransactionIsAChange()
    {
        var start = new DateTime(2026, 11, 1, 9, 0, 0, DateTimeKind.Utc).AddTicks(1230);
        T.PgKillBinding Binding(DateTime? xact) => new(4242, "b-1", 1_793_000_000, xact, start, "idle in transaction");
        AssertSameAndKeys(T.PgKillBindingObject(Binding(start)), T.PgKillBindingObject(Binding(start)),
            "pid", "backend_id", "backend_start_epoch", "xact_start", "state_change", "state");
        Assert.Equal(["xact_start"], ActionProposalPress.Differences(Stored(T.PgKillBindingObject(Binding(start))),
            T.PgKillBindingObject(Binding(start.AddSeconds(1)))));
    }
}
