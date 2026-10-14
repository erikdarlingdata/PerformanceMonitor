/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>mcp.dataDetail</c> (#5632): the setting, the masker and the host filter, one test per row of the field table
/// (<see cref="McpDataDetailKeys"/>) at both masked levels, plus <c>full</c> passing everything through untouched.
/// The keyed census that proves the table covers what the readers really return lives in the six statement-filter
/// live classes.
/// </summary>
public sealed class McpDataDetailMaskerTests
{
    private const string Withheld = McpDataDetailMasker.WithheldText;

    private static JsonObject Masked(string json, McpDataDetail mode) =>
        JsonNode.Parse(McpDataDetailMasker.MaskJson(json, mode))!.AsObject();

    /* ---- the setting ---- */

    [Theory]
    [InlineData(null, McpDataDetail.Full, true)]
    [InlineData("", McpDataDetail.Full, true)]
    [InlineData(" full ", McpDataDetail.Full, true)]
    [InlineData("maskLiterals", McpDataDetail.MaskLiterals, true)]
    [InlineData("MASKLITERALS", McpDataDetail.MaskLiterals, true)]
    [InlineData("metadataOnly", McpDataDetail.MetadataOnly, true)]
    [InlineData("masked", McpDataDetail.MetadataOnly, false)]
    [InlineData("none", McpDataDetail.MetadataOnly, false)]
    public void TryParse_ReadsEachLevel_AndAnUnknownValueIsTheStrictest(string? value, McpDataDetail expected, bool known)
    {
        Assert.Equal(known, McpDataDetailModes.TryParse(value, out var mode));
        Assert.Equal(expected, mode);
        Assert.Equal(expected, McpDataDetailModes.Resolve(value));
    }

    [Fact]
    public void Word_RoundTripsEveryLevel()
    {
        foreach (McpDataDetail mode in Enum.GetValues<McpDataDetail>())
        {
            Assert.True(McpDataDetailModes.TryParse(McpDataDetailModes.Word(mode), out var back));
            Assert.Equal(mode, back);
        }
    }

    [Fact]
    public void Config_DefaultsToFull_AndReadsTheKeyBesideTheOtherMcpSettings()
    {
        const string head = @"{ ""postgres"": { ""managed"": true }, ""servers"": [ { ""host"": ""SQL2022"" } ]";
        Assert.Equal("full", DarlingConfig.Parse(head + " }").Mcp.DataDetail);
        Assert.Equal(
            "maskLiterals",
            DarlingConfig.Parse(head + @", ""mcp"": { ""enabled"": true, ""dataDetail"": ""maskLiterals"" } }").Mcp.DataDetail);
        Assert.Equal(
            "dataDetail",
            typeof(McpConfig).GetProperty(nameof(McpConfig.DataDetail))!
                .GetCustomAttributes(typeof(System.Text.Json.Serialization.JsonPropertyNameAttribute), false)
                .Cast<System.Text.Json.Serialization.JsonPropertyNameAttribute>().Single().Name);
    }

    [Fact]
    public void ResolveDataDetail_LogsAnUnknownValueOnce_AsAWarning_AndServesMetadataOnly()
    {
        var logger = new DarlingSelfAlertTests.CapturingLogger();
        Assert.Equal(McpDataDetail.MetadataOnly, DarlingMcpHostService.ResolveDataDetail("maskliteral", logger));
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Contains("mcp.dataDetail", entry.Message, StringComparison.Ordinal);
        Assert.Contains("'maskliteral'", entry.Message, StringComparison.Ordinal);

        var quiet = new DarlingSelfAlertTests.CapturingLogger();
        Assert.Equal(McpDataDetail.Full, DarlingMcpHostService.ResolveDataDetail("full", quiet));
        Assert.Empty(quiet.Entries);
    }

    /* ---- full ---- */

    [Theory]
    [InlineData("{\"query_text\":\"SELECT 1 WHERE a = 'x'\",\"login_name\":\"sa\"}")]
    [InlineData("Conversion failed for the value 'abc'.")]
    [InlineData("<deadlock><process loginname=\"sa\"><inputbuf>SELECT 'x'</inputbuf></process></deadlock>")]
    public void Full_ReturnsTheSameInstance(string output) =>
        Assert.Same(output, McpDataDetailMasker.MaskJson(output, McpDataDetail.Full));

    /* ---- row 1: statement text, mute-rule patterns, error text ---- */

    [Fact]
    public void StatementText_LiteralsReplaced_ThenWithheld()
    {
        const string json = "{\"query_text\":\"SELECT * FROM dbo.t1 WHERE a = N'secret' AND b = 42\",\"rows\":[{\"sql_text\":\"EXEC p @x = 7\"}]}";
        var masked = Masked(json, McpDataDetail.MaskLiterals);
        Assert.Equal("SELECT * FROM dbo.t1 WHERE a = N'?' AND b = ?", masked["query_text"]!.GetValue<string>());
        Assert.Equal("EXEC p @x = ?", masked["rows"]![0]!["sql_text"]!.GetValue<string>());

        var meta = Masked(json, McpDataDetail.MetadataOnly);
        Assert.Equal(Withheld, meta["query_text"]!.GetValue<string>());
        Assert.Equal(Withheld, meta["rows"]![0]!["sql_text"]!.GetValue<string>());
    }

    [Fact]
    public void MuteRulePattern_LiteralsReplaced_ThenWithheld()
    {
        const string json = "{\"query_text_pattern\":\"%WHERE CustomerId = 9001%\",\"pattern\":\"%'acme'%\"}";
        var masked = Masked(json, McpDataDetail.MaskLiterals);
        Assert.Equal("%WHERE CustomerId = ?%", masked["query_text_pattern"]!.GetValue<string>());
        Assert.Equal("%'?'%", masked["pattern"]!.GetValue<string>());

        var meta = Masked(json, McpDataDetail.MetadataOnly);
        Assert.Equal(Withheld, meta["query_text_pattern"]!.GetValue<string>());
        Assert.Equal(Withheld, meta["pattern"]!.GetValue<string>());
    }

    [Fact]
    public void ErrorText_KeyedAndWhole_LiteralsReplaced_ThenWithheld()
    {
        const string whole = "Conversion failed when converting the varchar value 'abc' to data type int.";
        Assert.Equal(
            "Conversion failed when converting the varchar value '?' to data type int.",
            McpDataDetailMasker.MaskJson(whole, McpDataDetail.MaskLiterals));
        Assert.Equal(Withheld, McpDataDetailMasker.MaskJson(whole, McpDataDetail.MetadataOnly));

        const string json = "{\"error_message\":\"Duplicate key value (42).\",\"status\":\"ok\"}";
        Assert.Equal("Duplicate key value (?).", Masked(json, McpDataDetail.MaskLiterals)["error_message"]!.GetValue<string>());
        var meta = Masked(json, McpDataDetail.MetadataOnly);
        Assert.Equal(Withheld, meta["error_message"]!.GetValue<string>());
        Assert.Equal("ok", meta["status"]!.GetValue<string>());

        Assert.Equal("Bad value '?'", McpDataDetailMasker.MaskErrorText("Bad value 'x'", McpDataDetail.MaskLiterals));
        Assert.Equal(Withheld, McpDataDetailMasker.MaskErrorText("Bad value 'x'", McpDataDetail.MetadataOnly));
    }

    [Fact]
    public void AlertContextLabels_MaskTheValueBesideAStatementLabel_AndKeepTheRest()
    {
        const string json = "{\"details\":[{\"heading\":\"Deadlock\",\"fields\":["
            + "{\"label\":\"Victim SQL\",\"value\":\"UPDATE t SET x = 5 WHERE k = 'a'\"},"
            + "{\"label\":\"Query\",\"value\":\"SELECT 1\"},"
            + "{\"label\":\"Database\",\"value\":\"Sales2026\"}]}]}";
        var fields = Masked(json, McpDataDetail.MaskLiterals)["details"]![0]!["fields"]!.AsArray();
        Assert.Equal("UPDATE t SET x = ? WHERE k = '?'", fields[0]!["value"]!.GetValue<string>());
        Assert.Equal("SELECT ?", fields[1]!["value"]!.GetValue<string>());
        Assert.Equal("Sales2026", fields[2]!["value"]!.GetValue<string>());
        Assert.Equal("Victim SQL", fields[0]!["label"]!.GetValue<string>());

        var meta = Masked(json, McpDataDetail.MetadataOnly)["details"]![0]!["fields"]!.AsArray();
        Assert.Equal(Withheld, meta[0]!["value"]!.GetValue<string>());
        Assert.Equal(Withheld, meta[1]!["value"]!.GetValue<string>());
        Assert.Equal("Sales2026", meta[2]!["value"]!.GetValue<string>());
    }

    [Fact]
    public void MaskLiterals_KeepsIdentifiersAndParameters_AndEmptiesComments()
    {
        const string sql = "SELECT [col 1], \"c2\", t1.x, @p1, $1 FROM #t2 WHERE y = $$abc$$ -- note 5\n AND z = 0x1F /* 9 */ AND w = 1.5e3 AND v = 'it''s'";
        Assert.Equal(
            "SELECT [col 1], \"c2\", t1.x, @p1, $1 FROM #t2 WHERE y = '?' --\n AND z = ? /**/ AND w = ? AND v = '?'",
            McpDataDetailMasker.MaskLiterals(sql));

        const string clean = "SELECT a FROM dbo.t1";
        Assert.Same(clean, McpDataDetailMasker.MaskLiterals(clean));
    }

    [Fact]
    public void TheStatementFiltersOwnPlaceholders_AreNotGarbled()
    {
        string json = new JsonObject { ["query_text"] = SensitiveStatements.PlaceholderText }.ToJsonString();
        Assert.Same(json, McpDataDetailMasker.MaskJson(json, McpDataDetail.MaskLiterals));
    }

    /* ---- row 2: plan XML ---- */

    private const string Plan =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"SELECT * FROM t WHERE a = 'secret1'\"><QueryPlan><ParameterList>"
        + "<ColumnReference Column=\"@p\" ParameterCompiledValue=\"(N'secret2')\" ParameterRuntimeValue=\"(N'secret3')\"/>"
        + "</ParameterList><RelOp EstimateRows=\"12.5\"><Predicate><ScalarOperator ScalarString=\"[a]=N'secret4'\">"
        + "<Const ConstValue=\"N'secret5'\"/></ScalarOperator></Predicate></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    [Fact]
    public void PlanXml_ValuesReplaced_ThenWithheld()
    {
        string json = new JsonObject { ["query_plan"] = Plan }.ToJsonString();
        string masked = Masked(json, McpDataDetail.MaskLiterals)["query_plan"]!.GetValue<string>();
        Assert.DoesNotContain("secret", masked, StringComparison.Ordinal);
        Assert.Contains("ParameterCompiledValue=\"?\"", masked, StringComparison.Ordinal);
        Assert.Contains("ConstValue=\"?\"", masked, StringComparison.Ordinal);
        Assert.Contains("ScalarString=\"?\"", masked, StringComparison.Ordinal);
        Assert.Contains("StatementText=\"SELECT * FROM t WHERE a = '?'\"", masked, StringComparison.Ordinal);
        Assert.Contains("Column=\"@p\"", masked, StringComparison.Ordinal);
        Assert.Contains("EstimateRows=\"12.5\"", masked, StringComparison.Ordinal);

        Assert.Equal(Withheld, Masked(json, McpDataDetail.MetadataOnly)["query_plan"]!.GetValue<string>());
        Assert.Equal(Withheld, McpDataDetailMasker.MaskJson(Plan, McpDataDetail.MetadataOnly));
    }

    [Fact]
    public void Xml_ThatDoesNotParse_IsWithheld()
    {
        string json = new JsonObject { ["graph"] = "<deadlock><process loginname='sa'>" }.ToJsonString();
        Assert.Equal(Withheld, Masked(json, McpDataDetail.MaskLiterals)["graph"]!.GetValue<string>());
    }

    /* ---- row 3: deadlock and blocked-process input buffers ---- */

    private const string Deadlock =
        "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
        + "<process id=\"p1\" loginname=\"CORP\\canlogin\" hostname=\"CANHOST\" clientapp=\"App 1\" waitresource=\"KEY: 5:72\">"
        + "<executionStack><frame procname=\"dbo.p\" line=\"3\">UPDATE t SET a = 'secret6' WHERE id = 1</frame></executionStack>"
        + "<inputbuf>EXEC dbo.p @x = 'secret7'</inputbuf></process></process-list>"
        + "<resource-list><keylock objectname=\"db.dbo.t\" mode=\"X\"/></resource-list></deadlock>";

    [Fact]
    public void DeadlockInputBuffers_LiteralsReplaced_ThenWithheld_WithLoginsAndHostsMasked()
    {
        string json = new JsonObject { ["deadlock_graph"] = Deadlock }.ToJsonString();
        string masked = Masked(json, McpDataDetail.MaskLiterals)["deadlock_graph"]!.GetValue<string>();
        Assert.DoesNotContain("secret", masked, StringComparison.Ordinal);
        Assert.Contains("<inputbuf>EXEC dbo.p @x = '?'</inputbuf>", masked, StringComparison.Ordinal);
        Assert.Contains("canlogin", masked, StringComparison.Ordinal);
        Assert.Contains("CANHOST", masked, StringComparison.Ordinal);

        string meta = Masked(json, McpDataDetail.MetadataOnly)["deadlock_graph"]!.GetValue<string>();
        Assert.DoesNotContain("secret", meta, StringComparison.Ordinal);
        Assert.DoesNotContain("canlogin", meta, StringComparison.Ordinal);
        Assert.DoesNotContain("CANHOST", meta, StringComparison.Ordinal);
        Assert.Contains("<inputbuf>" + Withheld + "</inputbuf>", meta, StringComparison.Ordinal);
        Assert.Contains("loginname=\"login#1\"", meta, StringComparison.Ordinal);
        Assert.Contains("hostname=\"host#1\"", meta, StringComparison.Ordinal);
        Assert.Contains("clientapp=\"App 1\"", meta, StringComparison.Ordinal);
        Assert.Contains("objectname=\"db.dbo.t\"", meta, StringComparison.Ordinal);
    }

    [Fact]
    public void ExtendedEventsDataElements_TakeTheirNameAttributeAsTheirKey()
    {
        const string xe = "<event name=\"error_reported\"><data name=\"sql_text\"><value>SELECT 'secret8'</value></data>"
            + "<action name=\"client_hostname\"><value>CANHOST2</value></action><data name=\"severity\"><value>16</value></data></event>";
        string meta = Masked(new JsonObject { ["event_xml"] = xe }.ToJsonString(), McpDataDetail.MetadataOnly)["event_xml"]!
            .GetValue<string>();
        Assert.DoesNotContain("secret8", meta, StringComparison.Ordinal);
        Assert.DoesNotContain("CANHOST2", meta, StringComparison.Ordinal);
        Assert.Contains("<value>16</value>", meta, StringComparison.Ordinal);
    }

    /* ---- row 4: parameter values ---- */

    [Fact]
    public void ParameterValues_Replaced_ThenWithheld()
    {
        const string json = "{\"parameter_values\":\"@p1 = N'secret9'\",\"compiled_value\":\"12\"}";
        var masked = Masked(json, McpDataDetail.MaskLiterals);
        Assert.Equal("?", masked["parameter_values"]!.GetValue<string>());
        Assert.Equal("?", masked["compiled_value"]!.GetValue<string>());

        var meta = Masked(json, McpDataDetail.MetadataOnly);
        Assert.Equal(Withheld, meta["parameter_values"]!.GetValue<string>());
        Assert.Equal(Withheld, meta["compiled_value"]!.GetValue<string>());
    }

    /* ---- row 5: logins and host names ---- */

    [Fact]
    public void LoginsAndHosts_Kept_ThenMaskedAndNumberedPerAnswer()
    {
        const string json = "{\"rows\":[{\"login_name\":\"sa\",\"host_name\":\"APP01\"},"
            + "{\"login_name\":\"svc\",\"host_name\":\"APP01\"},{\"login_name\":\"sa\",\"client_addr\":\"10.1.2.3\"}]}";
        Assert.Same(json, McpDataDetailMasker.MaskJson(json, McpDataDetail.MaskLiterals));

        var rows = Masked(json, McpDataDetail.MetadataOnly)["rows"]!.AsArray();
        Assert.Equal("login#1", rows[0]!["login_name"]!.GetValue<string>());
        Assert.Equal("host#1", rows[0]!["host_name"]!.GetValue<string>());
        Assert.Equal("login#2", rows[1]!["login_name"]!.GetValue<string>());
        Assert.Equal("host#1", rows[1]!["host_name"]!.GetValue<string>());
        Assert.Equal("login#1", rows[2]!["login_name"]!.GetValue<string>());
        Assert.Equal("host#2", rows[2]!["client_addr"]!.GetValue<string>());
    }

    /* ---- row 6: object and database names, counts, durations, waits, times, program names ---- */

    [Theory]
    [InlineData(McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MetadataOnly)]
    public void MetadataFields_AreKeptAtEveryLevel(McpDataDetail mode)
    {
        const string json = "{\"server_name\":\"SQL2022\",\"database_name\":\"Sales2\",\"object_name\":\"dbo.Orders1\","
            + "\"execution_count\":42,\"avg_duration_ms\":1.5,\"wait_type\":\"LCK_M_X\","
            + "\"collection_time\":\"2026-10-14T12:00:00Z\",\"program_name\":\"App 2\",\"status\":\"ok\"}";
        Assert.Same(json, McpDataDetailMasker.MaskJson(json, mode));
    }

    /* ---- earlier reports, and single fields for the composer ---- */

    [Fact]
    public void FreeText_MasksEveryUnlistedString_ForEarlierReports()
    {
        const string report = "{\"verdict\":\"confirmed\",\"summary\":\"Rows with CustomerId = 9001 and name 'acme' blocked.\"}";
        Assert.Same(report, McpDataDetailMasker.MaskJson(report, McpDataDetail.MaskLiterals));

        var masked = JsonNode.Parse(McpDataDetailMasker.MaskJson(report, McpDataDetail.MaskLiterals, freeText: true))!;
        Assert.Equal("Rows with CustomerId = ? and name '?' blocked.", masked["summary"]!.GetValue<string>());
        Assert.Equal("confirmed", masked["verdict"]!.GetValue<string>());
    }

    [Fact]
    public void MaskField_MasksByTheFieldsName()
    {
        Assert.Equal("SELECT ?", McpDataDetailMasker.MaskField("Query", "SELECT 1", McpDataDetail.MaskLiterals));
        Assert.Equal("login#1", McpDataDetailMasker.MaskField("login_name", "sa", McpDataDetail.MetadataOnly));
        Assert.Equal("Sales", McpDataDetailMasker.MaskField("Database", "Sales", McpDataDetail.MetadataOnly));
        Assert.Equal("SELECT 1", McpDataDetailMasker.MaskField("Query", "SELECT 1", McpDataDetail.Full));
    }

    [Fact]
    public void FieldTable_ListsTheAlertContextStatementLabels()
    {
        Assert.Equal(McpDataDetailClass.Statement, McpDataDetailKeys.Classify("Victim SQL"));
        Assert.Equal(McpDataDetailClass.Statement, McpDataDetailKeys.Classify("Query"));
        Assert.Equal(McpDataDetailClass.Statement, McpDataDetailKeys.Classify("Blocking Query"));
        Assert.Equal(McpDataDetailClass.Statement, McpDataDetailKeys.Classify("StatementText"));
        Assert.Equal(McpDataDetailClass.Login, McpDataDetailKeys.Classify("loginname"));
        Assert.Equal(McpDataDetailClass.Host, McpDataDetailKeys.Classify("hostname"));
        Assert.Equal(McpDataDetailClass.Kept, McpDataDetailKeys.Classify("program_name"));
        Assert.Equal(McpDataDetailClass.Kept, McpDataDetailKeys.Classify("database_name"));
    }

    /* ---- review round 1 (H1, L2): keys and labels the sweep added ---- */

    [Theory]
    [InlineData("script")]
    [InlineData("repro_script")]
    [InlineData("text_data")]
    [InlineData("top_query_text")]
    [InlineData("query_preview")]
    [InlineData("root_query")]
    [InlineData("worst_victim_query")]
    [InlineData("Process A")]
    [InlineData("Process B")]
    [InlineData("Process C")]
    [InlineData("Process D")]
    public void SweptStatementKeys_LiteralsReplaced_ThenWithheld(string key)
    {
        string json = "{\"" + key + "\":\"SELECT * FROM dbo.t1 WHERE id = 42 AND name = N'acme'\"}";
        Assert.Equal(
            "SELECT * FROM dbo.t1 WHERE id = ? AND name = N'?'",
            Masked(json, McpDataDetail.MaskLiterals)[key]!.GetValue<string>());
        Assert.Equal(Withheld, Masked(json, McpDataDetail.MetadataOnly)[key]!.GetValue<string>());
    }

    [Fact]
    public void ReproScript_SniffedDeclareValues_Replaced_ThenWithheld()
    {
        const string script = "DECLARE @p1 int = 9001;\\nDECLARE @p2 nvarchar(10) = N'acme';\\n"
            + "SELECT * FROM dbo.Orders WHERE CustomerId = @p1 AND Name = @p2;";
        const string json = "{\"kind\":\"query_store\",\"plan_found\":true,\"script\":\"" + script + "\"}";

        var literals = Masked(json, McpDataDetail.MaskLiterals);
        string masked = literals["script"]!.GetValue<string>();
        Assert.DoesNotContain("9001", masked, StringComparison.Ordinal);
        Assert.DoesNotContain("acme", masked, StringComparison.Ordinal);
        Assert.Contains("DECLARE @p1 int = ?;", masked, StringComparison.Ordinal);
        Assert.Contains("WHERE CustomerId = @p1 AND Name = @p2", masked, StringComparison.Ordinal);
        Assert.Equal("query_store", literals["kind"]!.GetValue<string>());

        Assert.Equal(Withheld, Masked(json, McpDataDetail.MetadataOnly)["script"]!.GetValue<string>());
    }

    [Fact]
    public void DefaultTraceTextData_LoginAndClientGoWithTheText()
    {
        const string json = "{\"event_name\":\"Audit Login Failed\",\"text_data\":\"Login failed for user 'bob'. [CLIENT: 10.1.2.3]\"}";
        // Under maskLiterals hosts are kept (the field table's host row), and the bracketed client address reads as an
        // identifier; the quoted login is a literal.
        string masked = Masked(json, McpDataDetail.MaskLiterals)["text_data"]!.GetValue<string>();
        Assert.Equal("Login failed for user '?'. [CLIENT: 10.1.2.3]", masked);
        Assert.Equal(Withheld, Masked(json, McpDataDetail.MetadataOnly)["text_data"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("server_principal_name")]
    [InlineData("root_username")]
    [InlineData("role_name")]
    [InlineData("excluded_logins")]
    public void SweptLoginKeys_Kept_ThenMasked(string key)
    {
        string json = "{\"" + key + "\":\"CORP\\\\alice\"}";
        Assert.Same(json, McpDataDetailMasker.MaskJson(json, McpDataDetail.MaskLiterals));
        Assert.Equal("login#1", Masked(json, McpDataDetail.MetadataOnly)[key]!.GetValue<string>());
    }

    [Fact]
    public void ExcludedLogins_ArrayAndContextLabel_MaskedNameByName()
    {
        const string array = "{\"excluded_logins\":[\"sa\",\"svc\"]}";
        var masked = Masked(array, McpDataDetail.MetadataOnly)["excluded_logins"]!.AsArray();
        Assert.Equal(new[] { "login#1", "login#2" }, masked.Select(n => n!.GetValue<string>()));

        const string label = "{\"fields\":[{\"label\":\"Excluded Logins\",\"value\":\"sa, svc\"},{\"label\":\"Excluded Count\",\"value\":\"3\"}]}";
        Assert.Same(label, McpDataDetailMasker.MaskJson(label, McpDataDetail.MaskLiterals));
        var fields = Masked(label, McpDataDetail.MetadataOnly)["fields"]!.AsArray();
        Assert.Equal("login#1, login#2", fields[0]!["value"]!.GetValue<string>());
        Assert.Equal("3", fields[1]!["value"]!.GetValue<string>());

        Assert.Equal(McpDataDetailClass.Login, McpDataDetailKeys.Classify("Excluded Logins"));
        Assert.True(McpDataDetailKeys.IsCommaList("Excluded Logins"));
        Assert.False(McpDataDetailKeys.IsCommaList("login_name"));
    }

    /* ---- review round 1 (L1): one numbering per answer ---- */

    [Fact]
    public void ForAnswer_NumbersLoginsAndHostsAcrossEveryFieldOfOneAnswer()
    {
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly);
        Assert.Equal("login#1", masker.Field("blocked_login_name", "alice"));
        Assert.Equal("login#2", masker.Field("blocking_login_name", "bob"));
        Assert.Equal("login#1", masker.Field("login_name", "alice"));
        Assert.Equal("login#2, login#3", masker.Field("Excluded Logins", "bob, carol"));
        Assert.Equal("host#1", masker.Field("host_name", "APP01"));
        Assert.Equal("{\"login_name\":\"login#3\"}", masker.Json("{\"login_name\":\"carol\"}"));

        // A new answer starts over.
        Assert.Equal("login#1", McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly).Field("login_name", "bob"));

        var full = McpDataDetailMasker.ForAnswer(McpDataDetail.Full);
        Assert.Equal("alice", full.Field("login_name", "alice"));
        Assert.Equal("SELECT 1", full.FreeText("SELECT 1"));
    }

    [Fact]
    public void Filter_NumbersLoginsAcrossTheBlocksAndStructuredContentOfOneAnswer()
    {
        const string text = "{\"rows\":[{\"login_name\":\"alice\"},{\"login_name\":\"bob\"}]}";
        const string structured = "{\"login_name\":\"bob\"}";
        var result = new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = text } },
            StructuredContent = JsonDocument.Parse(structured).RootElement.Clone(),
        };

        var masked = McpDataDetailFilter.Mask(result, McpDataDetail.MetadataOnly);
        var rows = JsonNode.Parse(((TextContentBlock)masked.Content[0]).Text)!["rows"]!.AsArray();
        Assert.Equal("login#2", rows[1]!["login_name"]!.GetValue<string>());
        Assert.Equal("login#2", masked.StructuredContent!.Value.GetProperty("login_name").GetString());
    }

    /* ---- the host filter ---- */

    [Fact]
    public void Filter_MasksTextAndStructuredContent_AndFullPassesTheSameInstance()
    {
        const string json = "{\"query_text\":\"SELECT 'x'\"}";
        var result = new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = json } },
            StructuredContent = JsonDocument.Parse(json).RootElement.Clone(),
        };

        Assert.Same(result, McpDataDetailFilter.Mask(result, McpDataDetail.Full));

        var masked = McpDataDetailFilter.Mask(result, McpDataDetail.MaskLiterals);
        Assert.Equal(
            "SELECT '?'",
            JsonNode.Parse(((TextContentBlock)masked.Content[0]).Text)!["query_text"]!.GetValue<string>());
        Assert.Equal("SELECT '?'", masked.StructuredContent!.Value.GetProperty("query_text").GetString());

        var clean = new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"n\":1}" } } };
        Assert.Same(clean, McpDataDetailFilter.Mask(clean, McpDataDetail.MetadataOnly));
    }

    [Fact]
    public void Filter_RefusesABlockItCannotRead()
    {
        var result = new CallToolResult
        {
            Content = new List<ContentBlock> { new ImageContentBlock { Data = new byte[] { 0x41, 0x41, 0x41, 0x41 }, MimeType = "image/png" } },
        };
        var refused = McpDataDetailFilter.Mask(result, McpDataDetail.MaskLiterals);
        Assert.True(refused.IsError);
        Assert.Equal(McpDataDetailMasker.Refusal, ((TextContentBlock)refused.Content[0]).Text);
    }

    [Fact]
    public async System.Threading.Tasks.Task RealHost_MasksAToolAnswerAtTheLevelItServes()
    {
        using var server = await StatementFilterCensus.BuildHostAsync();
        const string raw = "{\"query_text\":\"SELECT a FROM dbo.t WHERE b = 731900417\",\"login_name\":\"login-canary-dd\"}";

        var (full, _) = await StatementFilterCensus.FilterThroughHostAsync(server, raw);
        Assert.Contains("731900417", full, StringComparison.Ordinal);

        var (masked, maskedError) = await StatementFilterCensus.FilterThroughHostAsync(server, raw, dataDetail: McpDataDetail.MaskLiterals);
        Assert.False(maskedError);
        Assert.DoesNotContain("731900417", masked, StringComparison.Ordinal);
        Assert.Contains("login-canary-dd", masked, StringComparison.Ordinal);

        var (meta, _) = await StatementFilterCensus.FilterThroughHostAsync(server, raw, dataDetail: McpDataDetail.MetadataOnly);
        Assert.DoesNotContain("731900417", meta, StringComparison.Ordinal);
        Assert.DoesNotContain("login-canary-dd", meta, StringComparison.Ordinal);
        Assert.Contains("login#1", meta, StringComparison.Ordinal);
    }

    [Fact]
    public void HostRegistersTheFilterJustOutsideTheStatementFilter()
    {
        string source = System.IO.File.ReadAllText(System.IO.Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"));
        int dataDetail = source.IndexOf(".AddCallToolFilter(McpDataDetailFilter.Instance)", StringComparison.Ordinal);
        int statement = source.IndexOf(".AddCallToolFilter(SensitiveStatementOutputFilter.Instance)", StringComparison.Ordinal);
        int gcf = source.IndexOf(".AddCallToolFilter(GcfCallToolFilter.Instance)", StringComparison.Ordinal);
        Assert.True(gcf > 0 && dataDetail > gcf && statement > dataDetail, "filter order: GCF, then mcp.dataDetail, then the statement filter");
        Assert.Contains("AddSingleton(new McpDataDetailSetting(ResolveDataDetail(config.Mcp.DataDetail", source, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}
