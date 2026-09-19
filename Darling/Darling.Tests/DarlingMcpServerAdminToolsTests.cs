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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Ungated (no-live-store) contract for the server-onboarding MCP tools (add_servers / remove_server): the tool
/// surface is EXACTLY the two names (both static, on a [McpServerToolType] class, returning Task&lt;string&gt;),
/// the advertised tools/list schema is Gemini-clean (#1074) with the expected required-param set, and — the
/// load-bearing safety property — a structurally-bad request (malformed JSON, empty array, an entry with a bad
/// field or an unsupported Entra/MFA auth) is REJECTED as invalid WITHOUT ever probing a server or opening a store
/// connection (proved against a dead data source + a probe seam that throws if reached). The pure dedupe partition
/// (case-folded, first-occurrence-wins) is unit-tested directly. The live store INSERT/DELETE + config_version
/// bump round-trip (probe stubbed to success) is gated below.
/// </summary>
public sealed class DarlingMcpServerAdminToolsSurfaceTests
{
    /// <summary>A dead data source (unroutable port) — proves the validate-before-write path bails on a bad request
    /// WITHOUT ever opening a connection (the call returns before touching the store).</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    /// <summary>A probe seam that FAILS the test if the pure-validation path ever reaches it — so a bad request
    /// that returns before probing is proved to have skipped the network hit entirely.</summary>
    private static readonly DarlingMcpServerAdminTools.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a structurally-invalid request");

    private static readonly string[] ExpectedToolSurface =
    {
        "add_servers",
        "remove_server",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpServerAdminTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheTwoServerAdminTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ExpectedToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpServerAdminTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Theory]
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void ParamContract_BothTools_RequireTheirTarget(string toolName, string requiredCsv)
    {
        var required = McpParams(toolName).Where(p => !p.Optional).Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(requiredCsv.Split(',').OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    private static System.Collections.Generic.Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpServerAdminTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForBothTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(2, tools.Count);
        var violations = tools.Values.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Theory]
    [InlineData("add_servers", "servers_json")]
    [InlineData("remove_server", "server_name")]
    public void AdvertisedSchema_RequiredParams_MatchTheContract(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Length == 0 ? Array.Empty<string>() : expectedCsv.Split(',');
        var required = DarlingMcpSchemaAssert.RequiredOf(BuildToolSchemas()[toolName].InputSchema)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    /* ---------------- validate BEFORE write (no connection opened, no probe) ---------------- */

    [Theory]
    [InlineData("not json")]     // not valid JSON
    [InlineData("{\"host\":\"x\"}")] // a JSON object, not an array
    [InlineData("[]")]           // empty array
    public async Task AddServers_UnusablePayload_ReturnsInvalid_WithoutTouchingStoreOrProbe(string json)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(dead, json, ThrowingProbe, CancellationToken.None);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Theory]
    [InlineData("[{\"database\":\"x\"}]")]                                        // missing host
    [InlineData("[{\"host\":\"x\",\"auth\":\"Entra\"}]")]                         // bare/interactive Entra rejected
    [InlineData("[{\"host\":\"x\",\"auth\":\"EntraMFA\"}]")]                      // interactive MFA rejected (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"DeviceCode\"}]")]                    // interactive device-code rejected (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"ServicePrincipal\"}]")]             // SP without client id/secret (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"ServicePrincipal\",\"username\":\"app\"}]")] // SP without secret (#3484)
    [InlineData("[{\"host\":\"x\",\"auth\":\"SQL\"}]")]                          // SQL without username/password
    [InlineData("[{\"host\":\"x\",\"auth\":\"SQL\",\"username\":\"u\"}]")]       // SQL without password
    [InlineData("[{\"host\":\"x\",\"encrypt_mode\":\"nope\"}]")]                 // bad encrypt_mode enum
    [InlineData("[{\"host\":\"x\",\"trust_server_certificate\":\"yes\"}]")]      // bool field, wrong type
    public async Task AddServers_AllEntriesInvalid_ReturnsPerEntryInvalid_WithoutTouchingStoreOrProbe(string json)
    {
        /* Every entry is structurally invalid, so there is no candidate to dedupe / probe / insert — the store is
           never opened (the dead store would throw) and the throwing probe is never reached. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(dead, json, ThrowingProbe, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("skipped").GetInt32());
        Assert.Equal(0, doc.RootElement.GetProperty("collided").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failed").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("requested").GetInt32());
        Assert.Equal("invalid", doc.RootElement.GetProperty("results")[0].GetProperty("status").GetString());
    }

    /* ---------------- #3541 A14: writes report what happened — every status lands in exactly one counter ---------------- */

    /// <summary>Every per-row status the tool can produce, read off the constants class rather than restated
    /// here, so a sixth status added there is in this census the moment it exists.</summary>
    private static string[] AllAddStatuses() => typeof(DarlingMcpServerAdminTools.AddStatus)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(f => f.IsLiteral && f.FieldType == typeof(string))
        .Select(f => (string)f.GetRawConstantValue()!)
        .OrderBy(s => s, StringComparer.Ordinal)
        .ToArray();

    /// <summary>
    /// The whole of the summary contract: the status set and the counter map's key set are the SAME set, and
    /// every counter the map names is one the envelope carries. This is what makes "the four counters sum to
    /// requested" a property of the code rather than of the batches anyone happened to test: a status without
    /// a counter is what <c>collides</c> was for the months between #2280 and this — present in every result row,
    /// absent from every summary number.
    /// </summary>
    [Fact]
    public void EveryAddStatus_HasExactlyOneSummaryCounter_AndNoCounterIsUnnamed()
    {
        var statuses = AllAddStatuses();
        Assert.Equal(5, statuses.Length);
        Assert.Contains("collides", statuses);

        var mapped = DarlingMcpServerAdminTools.CounterOfStatus.Keys.OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(statuses, mapped);

        var counters = new[] { "added", "skipped", "collided", "failed" };
        Assert.All(DarlingMcpServerAdminTools.CounterOfStatus.Values, c => Assert.Contains(c, counters));

        /* And the one status that was missing has its OWN counter, not a seat inside failed: a collided entry
           must not be retried as sent, which is exactly what a failed entry invites. */
        Assert.Equal("collided", DarlingMcpServerAdminTools.CounterOfStatus["collides"]);
        Assert.Equal("skipped", DarlingMcpServerAdminTools.CounterOfStatus["duplicate"]);
    }

    /// <summary>
    /// The envelope over a batch that exercises EVERY status at once: requested is the input count, each
    /// counter is the number of rows with the statuses mapped to it, and the four sum to requested. Two of
    /// each so a counter that merely tested "any" would read wrong.
    /// </summary>
    [Fact]
    public void Aggregate_CountsEveryResultOnce_AndTheCountersSumToRequested()
    {
        var results = new List<DarlingMcpServerAdminTools.ServerResult>
        {
            new(0, "a", DarlingMcpServerAdminTools.AddStatus.Added, "Connected"),
            new(1, "b", DarlingMcpServerAdminTools.AddStatus.Collides, "lands elsewhere"),
            new(2, "c", DarlingMcpServerAdminTools.AddStatus.Duplicate, "seen"),
            new(3, "d", DarlingMcpServerAdminTools.AddStatus.ConnectionFailed, "no route"),
            new(4, "e", DarlingMcpServerAdminTools.AddStatus.Invalid, "bad field"),
            new(5, "f", DarlingMcpServerAdminTools.AddStatus.Added, "Connected"),
            new(6, "g", DarlingMcpServerAdminTools.AddStatus.Collides, "lands elsewhere"),
            new(7, "h", DarlingMcpServerAdminTools.AddStatus.Duplicate, "seen"),
            new(8, "i", DarlingMcpServerAdminTools.AddStatus.ConnectionFailed, "no route"),
            new(9, "j", DarlingMcpServerAdminTools.AddStatus.Invalid, "bad field"),
        };

        using var doc = JsonDocument.Parse(DarlingMcpServerAdminTools.Aggregate(results));
        var root = doc.RootElement;

        Assert.Equal(10, root.GetProperty("requested").GetInt32());
        Assert.Equal(2, root.GetProperty("added").GetInt32());
        Assert.Equal(2, root.GetProperty("skipped").GetInt32());
        Assert.Equal(2, root.GetProperty("collided").GetInt32());
        Assert.Equal(4, root.GetProperty("failed").GetInt32());

        var sum = root.GetProperty("added").GetInt32() + root.GetProperty("skipped").GetInt32()
                + root.GetProperty("collided").GetInt32() + root.GetProperty("failed").GetInt32();
        Assert.Equal(root.GetProperty("requested").GetInt32(), sum);

        /* Results echo input order and carry the per-row status the counters were derived from. */
        var statuses = root.GetProperty("results").EnumerateArray().Select(r => r.GetProperty("status").GetString()).ToArray();
        Assert.Equal(results.Select(r => r.Status).ToArray(), statuses);
    }

    /// <summary>A status the map does not know is a LOUD failure, never a row that quietly counts toward
    /// nothing — the failure shape the old three-filter summary had.</summary>
    [Fact]
    public void Aggregate_RefusesAStatusNoCounterAccountsFor()
    {
        var results = new List<DarlingMcpServerAdminTools.ServerResult>
        {
            new(0, "a", DarlingMcpServerAdminTools.AddStatus.Added, "Connected"),
            new(1, "b", "quarantined", "a status nobody mapped"),
        };

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingMcpServerAdminTools.Aggregate(results));
        Assert.Contains("quarantined", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every result-construction site in the tool names its status through <c>AddStatus</c>, never as a bare
    /// literal — the guard that keeps the census above complete. A literal at a new site would compile, be
    /// absent from the constants class, and so be absent from this file's reflection; this pin is what turns
    /// that into a red run instead of a green one over an incomplete set.
    /// </summary>
    [Fact]
    public void EveryServerResultConstruction_NamesItsStatusThroughAddStatus()
    {
        var source = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpServerAdminTools.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        /* Both construction spellings: the explicit `new ServerResult(` and the target-typed `=> new(` inside
           the local Invalid() factory. The record DECLARATION is a `ServerResult(` too and is excluded by its
           parameter list. */
        var sites = Regex.Matches(code, @"(?<new>new)\s+ServerResult\s*\(|ServerResult\s+Invalid\s*\([^)]*\)\s*=>\s*(?<new>new)\s*\(")
            .Select(m => CSharpSourceWalker.ConstructionSpanFrom(code, m.Groups["new"].Index))
            .ToList();

        Assert.True(sites.Count >= 5, $"expected the five result-construction sites, found {sites.Count} — the scan is broken");
        Assert.All(sites, span => Assert.Contains("AddStatus.", span, StringComparison.Ordinal));
    }

    /// <summary>The tool description promises the counters and the sum; a caller reads the description, not the
    /// code.</summary>
    [Fact]
    public void AddServersDescription_NamesEveryCounter_TheSumRule_AndTheCollidesStatus()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "add_servers");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        foreach (var token in new[] { "requested:N", "added:N", "skipped:N", "collided:N", "failed:N", "SUM", "\"collides\"" })
        {
            Assert.Contains(token, description, StringComparison.Ordinal);
        }

        /* And the instruction table agrees with the description — the two surfaces an agent reads. */
        Assert.Contains("`{requested, added, skipped, collided, failed, results:[{server, status, detail}]}`", DarlingMcpInstructions.Text, StringComparison.Ordinal);
        Assert.Contains("is `collides`", DarlingMcpInstructions.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3484 accepted the two non-interactive Entra modes; the instruction table went on saying
    /// "Entra/MFA/Service-Principal/Managed-Identity auth is `invalid` (Windows/SQL only)" for a release. The
    /// accepted set is read off the parser — the authority — and the table is held to it.
    /// </summary>
    [Fact]
    public void InstructionsTable_MatchesTheAuthModesTheParserAccepts()
    {
        /* The parser is the authority: these two are accepted (they parse to an entry) ... */
        foreach (var accepted in new[] { "ServicePrincipal", "ManagedIdentity" })
        {
            var json = accepted == "ManagedIdentity"
                ? "[{\"host\":\"x\",\"auth\":\"ManagedIdentity\"}]"
                : "[{\"host\":\"x\",\"auth\":\"ServicePrincipal\",\"username\":\"app\",\"password\":\"s\"}]";
            var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(json);
            Assert.Null(wholeError);
            Assert.Empty(invalid);
            Assert.Single(entries);
        }

        /* ... and these are refused. */
        foreach (var refused in new[] { "EntraMFA", "EntraDeviceCodeAuth", "EntraDefaultCredential" })
        {
            var (entries, invalid, _) = DarlingMcpServerAdminTools.ParseRequest($"[{{\"host\":\"x\",\"auth\":\"{refused}\"}}]");
            Assert.Empty(entries);
            Assert.Single(invalid);
        }

        /* The table says the same: the accepted pair is named as accepted, the interactive trio as invalid, and
           the release-old denial is gone in both of its spellings. */
        var row = DarlingMcpInstructions.Text.Split('\n').Single(l => l.Contains("| `add_servers` |", StringComparison.Ordinal));
        Assert.Contains("`ServicePrincipal` / `ManagedIdentity`", row, StringComparison.Ordinal);
        Assert.Contains("INTERACTIVE Entra modes (MFA / device-code / default-credential) are `invalid`", row, StringComparison.Ordinal);
        Assert.DoesNotContain("Windows/SQL only", row, StringComparison.Ordinal);
        Assert.DoesNotContain("Service-Principal/Managed-Identity auth is `invalid`", row, StringComparison.Ordinal);

        /* And the tool description — the other surface an agent reads — agrees. */
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "add_servers");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("\"ServicePrincipal\"", description, StringComparison.Ordinal);
        Assert.Contains("\"ManagedIdentity\"", description, StringComparison.Ordinal);
    }

    /* ---------------- #3541 A14: remove_server refuses what it cannot honor ---------------- */

    private static DarlingServerResolver.RegisteredServer Row(int id, string name, string? display = null) => new(id, name, display);

    [Fact]
    public void ResolveForRemoval_ExactStorageOrDisplayName_IsOneCandidate_MatchedExact()
    {
        var servers = new[] { Row(1, "sql-01", "Payments"), Row(2, "sql-02", "Ledger") };

        var byStorage = DarlingMcpServerAdminTools.ResolveForRemoval(servers, "SQL-02");
        Assert.Equal("exact", byStorage.MatchedBy);
        Assert.Equal(2, Assert.Single(byStorage.Candidates).ServerId);

        var byDisplay = DarlingMcpServerAdminTools.ResolveForRemoval(servers, " payments ");
        Assert.Equal("exact", byDisplay.MatchedBy);
        Assert.Equal(1, Assert.Single(byDisplay.Candidates).ServerId);
    }

    /// <summary>THE defect: a fragment two siblings contain. The read resolver returns the first by storage-name
    /// order; a delete must return both and choose neither.</summary>
    [Fact]
    public void ResolveForRemoval_FragmentSeveralServersContain_IsEveryCandidate_NotTheFirst()
    {
        var servers = new[] { Row(1, "sql-01"), Row(2, "sql-02"), Row(3, "pg-01") };

        var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, "sql-");

        Assert.Equal("partial", target.MatchedBy);
        Assert.Equal(new[] { 1, 2 }, target.Candidates.Select(c => c.ServerId).OrderBy(i => i).ToArray());
    }

    /// <summary>A partial that only ONE server contains is honored — the documented convenience survives where it
    /// is unambiguous.</summary>
    [Fact]
    public void ResolveForRemoval_UniquePartial_IsOneCandidate_MatchedPartial()
    {
        var servers = new[] { Row(1, "sql-01", "Payments"), Row(2, "sql-02", "Ledger") };

        var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, "ledg");

        Assert.Equal("partial", target.MatchedBy);
        Assert.Equal(2, Assert.Single(target.Candidates).ServerId);
    }

    /// <summary>An exact match wins over the partials that would otherwise also match it: "sql-01" against
    /// "sql-01" and "sql-010" is one server, not two.</summary>
    [Fact]
    public void ResolveForRemoval_ExactBeatsPartial_EvenWhenThePartialWouldBeAmbiguous()
    {
        var servers = new[] { Row(1, "sql-01"), Row(2, "sql-010") };

        var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, "sql-01");

        Assert.Equal("exact", target.MatchedBy);
        Assert.Equal(1, Assert.Single(target.Candidates).ServerId);
    }

    /// <summary>display_name is not unique; two registrations sharing one exactly are ambiguous too, reported as
    /// such rather than resolved to whichever the registry returned first.</summary>
    [Fact]
    public void ResolveForRemoval_SharedDisplayName_IsAmbiguousExact()
    {
        var servers = new[] { Row(1, "sql-01", "Prod"), Row(2, "sql-01:AppDb", "Prod") };

        var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, "Prod");

        Assert.Equal("exact", target.MatchedBy);
        Assert.Equal(2, target.Candidates.Count);
    }

    [Theory]
    [InlineData("nothing-like-it")]
    [InlineData("")]
    [InlineData("   ")]
    public void ResolveForRemoval_NoMatch_IsZeroCandidates(string name)
    {
        var servers = new[] { Row(1, "sql-01"), Row(2, "sql-02") };

        var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, name);

        Assert.Empty(target.Candidates);
        Assert.Equal("none", target.MatchedBy);
    }

    /// <summary>The description tells the caller what an ambiguous name does (nothing) and what comes back; the
    /// instruction table says the same.</summary>
    [Fact]
    public void RemoveServerDescription_PromisesTheAmbiguousRefusal()
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == "remove_server");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("status:\"ambiguous\"", description, StringComparison.Ordinal);
        Assert.Contains("NOTHING is deleted", description, StringComparison.Ordinal);
        Assert.Contains("ONLY when exactly one", description, StringComparison.Ordinal);
        Assert.DoesNotContain("resolved the same way the read tools resolve", description, StringComparison.Ordinal);

        /* #3653 A15/A16: the description says WHICH table the name is resolved against (the definitions the DELETE
           targets, so a never-connected server is removable), and names the two disclosures the payload gained. */
        Assert.Contains("config_monitored_servers", description, StringComparison.Ordinal);
        Assert.Contains("never connected", description, StringComparison.Ordinal);
        Assert.Contains("ever_connected", description, StringComparison.Ordinal);
        Assert.Contains("matched_in", description, StringComparison.Ordinal);

        var row = DarlingMcpInstructions.Text.Split('\n').Single(l => l.Contains("| `remove_server` |", StringComparison.Ordinal));
        Assert.Contains("status:\"ambiguous\"", row, StringComparison.Ordinal);
        Assert.Contains("deletes NOTHING", row, StringComparison.Ordinal);
        Assert.Contains("never connected", row, StringComparison.Ordinal);
        Assert.Contains("ever_connected", row, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653 A15/A16: the definitions read behind <c>remove_server</c> is over the table the DELETE targets, carries
    /// the full #2218 identity (engine + port, so a PostgreSQL definition on a host that also has a SQL Server one
    /// rebuilds to its own storage name), asks the registry only whether a row EXISTS, and keeps the store dialect
    /// every other admin-tool statement keeps (no bare now(), no N'' literals, no @named parameters).
    /// </summary>
    [Fact]
    public void DefinitionsForRemovalSql_ReadsTheDeleteTargetTable_WithTheFullIdentity_AndAnExistsAgainstTheRegistry()
    {
        var sql = DarlingMcpServerAdminTools.DefinitionsForRemovalSql;

        Assert.Contains("FROM config_monitored_servers", sql, StringComparison.Ordinal);
        foreach (var column in new[] { "server_id", "name", "host", "database", "read_only_intent", "engine", "port" })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }
        Assert.Contains("EXISTS (SELECT 1 FROM servers", sql, StringComparison.Ordinal);
        Assert.Contains("AS ever_connected", sql, StringComparison.Ordinal);
        /* Every definition, enabled or not: a disabled definition is still one an operator may remove. */
        Assert.DoesNotContain("is_enabled", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("now(", sql.ToLowerInvariant(), StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task RemoveServer_BlankName_ReturnsInvalid_WithoutTouchingStore(string name)
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.RemoveServer(dead, name);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    /* ---------------- pure parse + dedupe ---------------- */

    [Fact]
    public void ParseRequest_ValidWindowsEntry_BuildsIntegratedProbeConfig_NoSecret()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"sql01\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("sql01", entry.DisplayName);
        Assert.Equal("integrated", entry.ProbeConfig.Auth);
        Assert.Null(entry.PlaintextPassword);
        /* Fail-closed TLS defaults are the MonitoredServer defaults. */
        Assert.Equal("Mandatory", entry.ProbeConfig.EncryptMode);
        Assert.False(entry.ProbeConfig.TrustServerCertificate);
    }

    [Fact]
    public void ParseRequest_SqlEntry_CarriesPlaintextForProbe_AndAllExposedOptions()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"sql02\",\"display_name\":\"Prod\",\"database\":\"AppDb\",\"auth\":\"SQL\",\"username\":\"monitor\"," +
            "\"password\":\"p@ss\",\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":true,\"read_only_intent\":true}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("Prod", entry.DisplayName);
        Assert.Equal("sql", entry.ProbeConfig.Auth);
        Assert.Equal("monitor", entry.ProbeConfig.Username);
        Assert.Equal("p@ss", entry.PlaintextPassword);
        Assert.Equal("AppDb", entry.ProbeConfig.Database);
        Assert.Equal("Strict", entry.ProbeConfig.EncryptMode);
        Assert.True(entry.ProbeConfig.TrustServerCertificate);
        Assert.True(entry.ProbeConfig.ReadOnlyIntent);
        /* read_only_intent flows into the storage identity key (host:RO), matching the shared identity rule. */
        Assert.Equal(ServerIdHelper.BuildStorageName("sql02", "AppDb", true), entry.StorageKey);
    }

    [Fact]
    public void ParseRequest_ServicePrincipalEntry_CarriesClientIdAndSecret_ForProbe()
    {
        /* #3484: a non-interactive Entra service principal — the application/client id in username, the client
           secret in password, DPAPI-encrypted after a successful probe exactly like a SQL password. */
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"azuredb.database.windows.net\",\"database\":\"AppDb\",\"auth\":\"ServicePrincipal\"," +
            "\"username\":\"11111111-2222-3333-4444-555555555555\",\"password\":\"the-client-secret\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("serviceprincipal", entry.ProbeConfig.Auth);
        Assert.True(entry.ProbeConfig.UsesServicePrincipal);
        Assert.Equal("11111111-2222-3333-4444-555555555555", entry.ProbeConfig.Username);
        Assert.Equal("the-client-secret", entry.PlaintextPassword);
    }

    [Fact]
    public void ParseRequest_ManagedIdentityEntry_CarriesNoSecret_OptionalUserAssignedClientId()
    {
        /* #3484: managed identity is secret-less. A user-assigned identity names its client id in username; a
           system-assigned identity omits it. Either way no secret is carried or stored. */
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"azuredb.database.windows.net\",\"auth\":\"ManagedIdentity\"," +
            "\"username\":\"66666666-7777-8888-9999-000000000000\"}]");

        Assert.Null(wholeError);
        Assert.Empty(invalid);
        var entry = Assert.Single(entries);
        Assert.Equal("managedidentity", entry.ProbeConfig.Auth);
        Assert.True(entry.ProbeConfig.UsesManagedIdentity);
        Assert.Equal("66666666-7777-8888-9999-000000000000", entry.ProbeConfig.Username);
        Assert.Null(entry.PlaintextPassword);
    }

    [Fact]
    public void ParseRequest_MixedBatch_SplitsValidFromInvalid_PreservingOrder()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"good1\"},{\"auth\":\"SQL\"},{\"host\":\"good2\"}]");

        Assert.Null(wholeError);
        Assert.Equal(2, entries.Count);
        var bad = Assert.Single(invalid);
        Assert.Equal("invalid", bad.Status);
        Assert.Equal(1, bad.Order); // the second element (index 1)
    }

    [Fact]
    public void PartitionDuplicates_CaseVariantEntries_CollapseToOneAdded_OneDuplicate()
    {
        /* "SQL2016" and "sql2016" build the SAME storage identity under OrdinalIgnoreCase (BuildStorageName is
           case-preserving; the gate is case-folded), so the second is a duplicate of the first in-batch. */
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"SQL2016\"},{\"host\":\"sql2016\"}]");
        Assert.Equal(2, entries.Count);
        Assert.Equal(entries[0].StorageKey, entries[1].StorageKey, StringComparer.OrdinalIgnoreCase);

        var (ready, duplicates) = DarlingMcpServerAdminTools.PartitionDuplicates(entries, Array.Empty<string>());

        Assert.Single(ready);
        var dup = Assert.Single(duplicates);
        Assert.Equal("duplicate", dup.Status);
        Assert.Equal(1, dup.Order); // the second entry is the duplicate
    }

    [Fact]
    public void PartitionDuplicates_EntryMatchingAnExistingServer_IsSkipped()
    {
        var (entries, _, _) = DarlingMcpServerAdminTools.ParseRequest("[{\"host\":\"sql2019\"}]");

        /* Seed the gate with the SAME server (a case variant of the existing store key) → no ready, one duplicate. */
        var (ready, duplicates) = DarlingMcpServerAdminTools.PartitionDuplicates(entries, new[] { "SQL2019" });

        Assert.Empty(ready);
        Assert.Single(duplicates);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the server-onboarding MCP tools against a real PostgreSQL — the
/// probe is STUBBED to success (no live SQL Server), so this exercises the STORE side: add_servers INSERTs the
/// config_monitored_servers rows (SQL-auth password DPAPI-encrypted at rest, Windows-auth server secret-free),
/// the case-folded duplicate is skipped, the write self-bumps config_version (the reload beacon) via the existing
/// trigger, a #2280 collision is refused and COUNTED (#3541 A14: <c>collided</c>, with the four counters summing
/// to <c>requested</c>), and remove_server refuses an ambiguous fragment with both candidates named, honors a unique
/// partial, and DELETEs on an exact name. Own-scoped per the shared-store doctrine (GUID-suffixed hosts + a finally
/// cleanup). No live SQL Server connection is made in CI — the real end-to-end probe is what the human dogfoods
/// (remove sql2016, re-add via MCP).
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpServerAdminToolsLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The stubbed probe — a healthy on-prem Enterprise box; no live SQL Server is touched.</summary>
    private static readonly DarlingMcpServerAdminTools.ServerProbe SuccessProbe =
        (_, _) => Task.FromResult(new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null));

    /// <summary>The same healthy box, but the probe REPORTS which database the connection reached — the #2280
    /// input. Every entry in a batch probed through this lands in <paramref name="connectedDatabase"/>, whatever
    /// database it declared.</summary>
    private static DarlingMcpServerAdminTools.ServerProbe ProbeReaching(string connectedDatabase) =>
        (_, _) => Task.FromResult(new ConnectionProbeResult(
            Success: true, MajorVersion: 15, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null,
            ConnectedDatabase: connectedDatabase));

    [Fact]
    public async Task AddServers_InsertsEncryptsDedupesBumpsVersion_ThenRemoveServer_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the server-admin MCP tools live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Explicit search_path so the tools' bare config_monitored_servers / servers resolve regardless of the
           store's database default (mirrors the other live tool tests). */
        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(cs)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(dataSourceConnectionString);

        var suffix = Guid.NewGuid().ToString("N")[..12];
        var sqlHost = "mcp-add-sql-" + suffix;
        var winHost = "mcp-add-win-" + suffix;
        var dbHost = "mcp-add-db-" + suffix;
        var sqlId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(sqlHost, null, false));
        var winId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(winHost, null, false));
        /* #3541 A14: the collision batch's two identities — the one that lands (dbHost:AppDb) and the one that
           must NOT (dbHost:Decoy), listed for cleanup so a regression that inserted it does not leak a row. */
        var dbAppId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(dbHost, "AppDb", false));
        var dbDecoyId = ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(dbHost, "Decoy", false));
        var password = "P@ss-" + Guid.NewGuid().ToString("N");

        await CleanupAsync(connection, ct, sqlId, winId, dbAppId, dbDecoyId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var bodySucceeded = false;
        try
        {
            var versionBefore = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));

            /* SQL-auth server + Windows-auth server + a case-variant DUPLICATE of the Windows host. */
            var json =
                $"[{{\"host\":\"{sqlHost}\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"{password}\"," +
                $"\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":true}}," +
                $"{{\"host\":\"{winHost}\"}}," +
                $"{{\"host\":\"{winHost.ToUpperInvariant()}\"}}]";
            var added = await DarlingMcpServerAdminTools.AddServersAsync(postgres, json, SuccessProbe, ct);
            using (var doc = JsonDocument.Parse(added))
            {
                Assert.Equal(3, doc.RootElement.GetProperty("requested").GetInt32());
                Assert.Equal(2, doc.RootElement.GetProperty("added").GetInt32());
                Assert.Equal(1, doc.RootElement.GetProperty("skipped").GetInt32());
                Assert.Equal(0, doc.RootElement.GetProperty("collided").GetInt32());
                Assert.Equal(0, doc.RootElement.GetProperty("failed").GetInt32());
            }

            /* The config_monitored_servers write self-bumped config_version (the service's reload beacon) via the
               existing trg_bump_monitored_servers trigger — proving the mcp beacon column-grant covers this write. */
            var versionAfter = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));
            Assert.True(versionAfter > versionBefore, "config_version should self-bump on a config_monitored_servers write");

            /* SQL server: the password is DPAPI-ENCRYPTED at rest (not plaintext) and round-trips; the exposed TLS
               options + auth landed as sent. (Darling.Tests is net10.0-windows, so DPAPI is available here.) */
            var storedSecret = await ScalarAsync(connection, ct, $"SELECT encrypted_password FROM config_monitored_servers WHERE server_id = {sqlId}") as string;
            Assert.False(string.IsNullOrEmpty(storedSecret));
            Assert.NotEqual(password, storedSecret);
            Assert.Equal(password, DarlingSecrets.Unprotect(storedSecret!));
            Assert.Equal("sql", await ScalarAsync(connection, ct, $"SELECT auth FROM config_monitored_servers WHERE server_id = {sqlId}") as string);
            Assert.Equal("Strict", await ScalarAsync(connection, ct, $"SELECT encrypt_mode FROM config_monitored_servers WHERE server_id = {sqlId}") as string);
            Assert.True((bool)(await ScalarAsync(connection, ct, $"SELECT trust_server_certificate FROM config_monitored_servers WHERE server_id = {sqlId}"))!);

            /* Windows server: integrated auth, NO stored secret. */
            Assert.Equal("integrated", await ScalarAsync(connection, ct, $"SELECT auth FROM config_monitored_servers WHERE server_id = {winId}") as string);
            Assert.True(await ScalarAsync(connection, ct, $"SELECT encrypted_password FROM config_monitored_servers WHERE server_id = {winId}") is null or DBNull);

            /* Re-adding both existing servers → all duplicates, nothing added. */
            var again = await DarlingMcpServerAdminTools.AddServersAsync(
                postgres, $"[{{\"host\":\"{sqlHost}\"}},{{\"host\":\"{winHost}\"}}]", SuccessProbe, ct);
            using (var doc = JsonDocument.Parse(again))
            {
                Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
                Assert.Equal(2, doc.RootElement.GetProperty("skipped").GetInt32());
                Assert.Equal(2, doc.RootElement.GetProperty("requested").GetInt32());
            }

            /* #3541 A14 — the batch that used to summarise as a clean run. Three entries on one host, probed
               through a stub that reports every connection landing in AppDb: the first names AppDb and is added;
               the second names Decoy but lands in AppDb, which the first now claims → collides, NOT inserted;
               the third is a case-variant duplicate of the first → skipped. The old envelope read
               {added: 1, skipped: 1, failed: 0} for this — one entry unaccounted for, and it was the one not
               being monitored. */
            var collisionBatch = await DarlingMcpServerAdminTools.AddServersAsync(
                postgres,
                $"[{{\"host\":\"{dbHost}\",\"database\":\"AppDb\"}}," +
                $"{{\"host\":\"{dbHost}\",\"database\":\"Decoy\"}}," +
                $"{{\"host\":\"{dbHost}\",\"database\":\"appdb\"}}]",
                ProbeReaching("AppDb"), ct);
            using (var doc = JsonDocument.Parse(collisionBatch))
            {
                var root = doc.RootElement;
                Assert.Equal(3, root.GetProperty("requested").GetInt32());
                Assert.Equal(1, root.GetProperty("added").GetInt32());
                Assert.Equal(1, root.GetProperty("skipped").GetInt32());
                Assert.Equal(1, root.GetProperty("collided").GetInt32());
                Assert.Equal(0, root.GetProperty("failed").GetInt32());

                var statuses = root.GetProperty("results").EnumerateArray().Select(r => r.GetProperty("status").GetString()).ToArray();
                Assert.Equal(new[] { "added", "collides", "duplicate" }, statuses);
            }

            /* And the store agrees with the counters: the AppDb identity exists, the Decoy identity does not. */
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {dbAppId}")));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {dbDecoyId}")));

            /* #3653 A15/A16: remove_server resolves against the DEFINITIONS (config_monitored_servers), so only the
               SQL host is registered in the connected-servers registry here — the Windows host and the AppDb
               definition have NEVER connected, which is exactly the population the pre-#3653 tool (resolving
               against `servers`) answered not_found for. Every assertion below on those two is the fix. */
            await DarlingMcpTestData.RegisterServerAsync(connection, sqlId, sqlHost, ct);

            /* #3541 A14: the GUID suffix is a fragment ALL THREE defined names contain — the two that never
               connected included, since the definitions are what is matched now. The read resolver would hand
               back whichever sorts first; the delete must refuse, name all three with ever_connected per row,
               and remove none. */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, suffix)))
            {
                Assert.Equal("ambiguous", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("partial", doc.RootElement.GetProperty("matched_by").GetString());
                Assert.Equal("config_monitored_servers", doc.RootElement.GetProperty("matched_in").GetString());
                var candidates = doc.RootElement.GetProperty("candidates").EnumerateArray()
                    .ToDictionary(c => c.GetProperty("server").GetString()!, c => c.GetProperty("ever_connected").GetBoolean(), StringComparer.Ordinal);
                Assert.Equal(
                    new[] { sqlHost, winHost, dbHost + ":AppDb" }.OrderBy(n => n, StringComparer.Ordinal).ToArray(),
                    candidates.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray());
                Assert.True(candidates[sqlHost]);
                Assert.False(candidates[winHost]);
                Assert.False(candidates[dbHost + ":AppDb"]);
            }
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {sqlId}")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {winId}")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {dbAppId}")));

            /* A partial that only ONE defined name contains is honored, and says so — on a server that never
               connected (THE #3653 case: the old tool could not see it at all). */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, "add-win-" + suffix)))
            {
                Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(winHost, doc.RootElement.GetProperty("server").GetString());
                Assert.Equal("partial", doc.RootElement.GetProperty("matched_by").GetString());
                Assert.Equal("config_monitored_servers", doc.RootElement.GetProperty("matched_in").GetString());
                Assert.False(doc.RootElement.GetProperty("ever_connected").GetBoolean());
                Assert.Contains("never connected", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
            }
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {winId}")));

            /* The exact STORAGE name (host:database, as list_servers would report it once connected) removes the
               never-connected AppDb definition — exact beats partial, and the definition's identity is rebuilt
               through the same BuildStorageName the registry would have used. */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, dbHost + ":AppDb")))
            {
                Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal(dbHost + ":AppDb", doc.RootElement.GetProperty("server").GetString());
                Assert.Equal("exact", doc.RootElement.GetProperty("matched_by").GetString());
                Assert.False(doc.RootElement.GetProperty("ever_connected").GetBoolean());
            }
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {dbAppId}")));

            /* The exact name removes the SQL host — the config row is deleted; it HAD connected, and the payload
               says so (its registry row and history stay). */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, sqlHost)))
            {
                Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("exact", doc.RootElement.GetProperty("matched_by").GetString());
                Assert.True(doc.RootElement.GetProperty("ever_connected").GetBoolean());
                Assert.Contains("history are kept", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
            }
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM config_monitored_servers WHERE server_id = {sqlId}")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM servers WHERE server_id = {sqlId}")));

            /* Remove again: the servers-registry row still resolves the name, but no definition does → not_found
               that NAMES the registry as where the match came from (the darling.json-defined shape), and nothing
               is touched. */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, sqlHost)))
            {
                Assert.Equal("not_found", doc.RootElement.GetProperty("status").GetString());
                Assert.Equal("servers", doc.RootElement.GetProperty("matched_in").GetString());
                Assert.Equal("exact", doc.RootElement.GetProperty("matched_by").GetString());
                Assert.Equal(sqlHost, Assert.Single(doc.RootElement.GetProperty("candidates").EnumerateArray()).GetProperty("server").GetString());
                Assert.Contains("darling.json", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, $"SELECT count(*) FROM servers WHERE server_id = {sqlId}")));

            /* A name that does not resolve at all → not_found, with no matched_in (nothing matched anywhere). */
            using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(postgres, "no-such-server-" + suffix)))
            {
                Assert.Equal("not_found", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.TryGetProperty("matched_in", out _));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt, sqlId, winId, dbAppId, dbDecoyId));
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct, params int[] serverIds)
    {
        var list = string.Join(", ", serverIds);
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM config_monitored_servers WHERE server_id IN ({list})");
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM servers WHERE server_id IN ({list})");
    }
}
