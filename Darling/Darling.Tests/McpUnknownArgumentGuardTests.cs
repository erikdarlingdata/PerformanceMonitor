// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An argument the tool does not declare is REFUSED, never dropped (#3870) — the request-side twin of the
/// payload-contract census, pinned as a property of the whole surface rather than of one tool.
///
/// <para><b>What shipped.</b> <c>get_collection_log {"server_name":"…","hours":1,"status_filter":"failure"}</c>
/// returned two hundred rows, every one SUCCESS, at the twenty-four-hour default. <c>status_filter</c> does
/// not exist and the real window knob is <c>hours_back</c>, so BOTH arguments the caller set were dropped by
/// the SDK's bind-by-name and the tool answered a question nobody asked — with no tell in the payload beyond
/// an <c>hours_back</c> echo the caller had no reason to re-read. The tool already refused a wrong VALUE on
/// <c>limit</c> ("told no instead of quietly given 1000") while swallowing a wrong NAME.</para>
///
/// <para><b>Census design, and why THIS shape.</b> The guard is ONE call-tool filter, so the binding
/// property cannot vary per tool — every tool reaches its method through
/// <see cref="McpUnknownArgumentGuard"/> or through none. Enumerating 147 tools and invoking each with a
/// junk key would need a live Postgres store per call and would be 147 assertions of one decision. So the
/// census is split the way the architecture is: the DECISION is pinned directly against
/// <see cref="McpUnknownArgumentGuard.Refuse"/> over every REGISTERED tool's real advertised schema (so a
/// tool whose schema the guard cannot read, or whose parameters it would wrongly reject, reds here), and the
/// ROUTING is pinned by reading the host source for the single registration line (so a future host that
/// forgets the filter reds even though every tool still binds). Together those are the same claim an
/// invoke-everything census would make, without a database.</para>
///
/// <para><b>What is deliberately NOT refused.</b> An omitted optional parameter (the guard reads arrived
/// keys only), a declared parameter a code path ignores, and the protocol's own metadata — <c>_meta</c> and
/// the progress token ride SIBLINGS of <c>arguments</c> in <c>CallToolRequestParams</c>, never inside it,
/// which is asserted here rather than assumed because the whole guard would be a client-compatibility
/// hazard if it were false. Case-insensitive matching is asserted too: it mirrors the binder, so the guard
/// can only ever refuse a call the binder would already have mangled.</para>
/// </summary>
public sealed class McpUnknownArgumentGuardTests
{
    /// <summary>
    /// Every tool the Darling host registers, with the schema it advertises — built through the same
    /// <c>WithGeminiCompatibleTools</c> path the host uses, with each DI-injected service parameter
    /// registered as a null singleton (nothing is invoked here; only schemas and the guard's decision are
    /// read). The registration list is derived from the host source so this census covers what SHIPS.
    /// </summary>
    private static List<McpServerTool> RegisteredTools()
    {
        var registered = RegisteredToolTypeNames();
        var toolTypes = typeof(DarlingMcpHostService).Assembly
            .GetTypes()
            .Where(t => registered.Contains(t.Name))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(toolTypes);

        var services = new ServiceCollection();

        var serviceParamTypes = toolTypes
            .SelectMany(t => t.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(IsServiceParameter)
            .Distinct();

        foreach (var serviceType in serviceParamTypes)
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        var builder = services.AddMcpServer();

        var register = typeof(McpSchemaCompat).GetMethod(
            nameof(McpSchemaCompat.WithGeminiCompatibleTools),
            BindingFlags.Public | BindingFlags.Static)!;

        foreach (var toolType in toolTypes)
        {
            register.MakeGenericMethod(toolType).Invoke(null, new object?[] { builder });
        }

        var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToList();
    }

    /// <summary>
    /// A tool parameter is DI-injected (and excluded from the advertised schema) when its type is not a
    /// simple model-facing value — the same predicate the schema-compat census uses, for the same reason.
    /// </summary>
    /// <remarks>#3898: the shared <see cref="McpServedSchema"/> predicate, which also keeps nullable value types
    /// and arrays (get_tool_guide's <c>string[]</c>) model-facing; the old inline one called both services and
    /// dropped them from the schemas this census checks.</remarks>
    internal static bool IsServiceParameter(Type t) => McpServedSchema.IsServiceParameter(t) && t != typeof(McpToolGuideCatalog);

    private static CallToolRequestParams Call(string toolName, Dictionary<string, JsonElement> arguments) =>
        new() { Name = toolName, Arguments = arguments };

    private static Dictionary<string, JsonElement> Args(params (string Key, string Value)[] pairs)
    {
        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);

        foreach (var (key, value) in pairs)
        {
            arguments[key] = JsonSerializer.SerializeToElement(value);
        }

        return arguments;
    }

    private static string TextOf(CallToolResult result)
    {
        Assert.NotNull(result.Content);
        var block = Assert.Single(result.Content!);
        return Assert.IsType<TextContentBlock>(block).Text;
    }

    /// <summary>
    /// THE census: for every registered tool, a deliberately unknown key is refused, the refusal wears the
    /// house envelope, and the message names the offending key. One decision, asserted across the whole
    /// registered surface — so a tool added tomorrow is covered without editing this test, and a tool whose
    /// schema the guard cannot read shows up here rather than as a silent hole.
    /// </summary>
    [Fact]
    public void EveryRegisteredTool_RefusesAnUnknownArgument_AndNamesIt()
    {
        var tools = RegisteredTools();

        Assert.NotEmpty(tools);

        var unguarded = new List<string>();

        foreach (var tool in tools)
        {
            var name = tool.ProtocolTool.Name;
            var result = McpUnknownArgumentGuard.Refuse(
                Call(name, Args(("definitely_not_a_parameter", "x"))),
                tool);

            if (result is null)
            {
                unguarded.Add($"{name}: accepted an unknown argument");
                continue;
            }

            var wire = TextOf(result);

            if (!McpHelpers.IsRefusalEnvelope(wire))
            {
                unguarded.Add($"{name}: refused outside the shared refusal envelope -> {wire}");
                continue;
            }

            if (!wire.Contains("definitely_not_a_parameter", StringComparison.Ordinal))
            {
                unguarded.Add($"{name}: refusal does not name the offending key -> {wire}");
            }
        }

        Assert.True(
            unguarded.Count == 0,
            "These registered tools do not refuse an argument they never declared, so a misspelled "
            + "parameter from an agent caller is silently dropped and the tool answers a different "
            + "question:\n" + string.Join("\n", unguarded));
    }

    /// <summary>
    /// The ROUTING half of the census: the host must actually install the filter. The decision above is
    /// worth nothing if the single registration line is dropped in a future edit of the host, and no
    /// per-tool test would notice — the tools would all still bind, exactly as they did before #3870.
    /// </summary>
    [Fact]
    public void TheHost_RegistersTheUnknownArgumentGuard_AsACallToolFilter()
    {
        var path = HostSourcePath();
        var source = File.ReadAllText(path);

        Assert.True(
            Regex.IsMatch(source, @"AddCallToolFilter\(\s*McpUnknownArgumentGuard\.Instance\s*\)"),
            $"{Path.GetFileName(path)} does not register McpUnknownArgumentGuard as a call-tool filter. "
            + "Without that ONE line every tool goes back to binding arguments by name and silently "
            + "dropping the rest, which is #3870: a misspelled parameter produces a confidently wrong "
            + "answer. If the registration style changed, teach this test the new one rather than delete "
            + "it.");
    }

    /// <summary>
    /// The refusal's SHAPE, pinned on the call that motivated the issue: the key is named, the near-miss is
    /// offered, the tool's accepted parameters are listed, and <c>hints.parameter</c> carries the offending
    /// key so a client can branch on what to fix without parsing prose.
    /// </summary>
    [Fact]
    public void TheRefusal_NamesTheKey_SuggestsTheNearMiss_AndListsAcceptedParameters()
    {
        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        var result = McpUnknownArgumentGuard.Refuse(
            Call("get_collection_log", Args(("status_filter", "failure"), ("hours", "1"))),
            tool);

        Assert.NotNull(result);
        Assert.True(result!.IsError);

        var wire = TextOf(result);

        Assert.True(McpHelpers.IsRefusalEnvelope(wire), wire);

        using var document = JsonDocument.Parse(wire);
        var root = document.RootElement;

        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Equal(
            "hours",
            root.GetProperty("hints").GetProperty("parameter").GetString());

        var message = root.GetProperty("message").GetString()!;

        /* Both bogus keys are named, not just the first. */
        Assert.Contains("'hours'", message, StringComparison.Ordinal);
        Assert.Contains("'status_filter'", message, StringComparison.Ordinal);
        Assert.Contains("get_collection_log", message, StringComparison.Ordinal);

        /* The near-miss: hours -> hours_back is the whole remedy for the call in the issue. */
        Assert.Contains("hours_back", message, StringComparison.Ordinal);

        /* And the accepted list, so a model can correct itself in one turn. */
        Assert.Contains("Accepted parameters:", message, StringComparison.Ordinal);
        Assert.Contains("server_name", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard must never refuse a call that WORKS. Every parameter a tool advertises is accepted, and an
    /// omitted optional parameter is not an unknown key — the two ways a strict guard would break the
    /// surface it is supposed to protect.
    /// </summary>
    [Fact]
    public void EveryRegisteredTool_AcceptsItsOwnDeclaredParameters_AndAnEmptyCall()
    {
        var tools = RegisteredTools();
        var wrongful = new List<string>();

        foreach (var tool in tools)
        {
            var name = tool.ProtocolTool.Name;

            /* An omitted-everything call: optionality must be untouched. */
            if (McpUnknownArgumentGuard.Refuse(Call(name, new Dictionary<string, JsonElement>()), tool) is not null)
            {
                wrongful.Add($"{name}: refused a call with no arguments at all");
            }

            var schema = tool.ProtocolTool.InputSchema;
            if (schema.ValueKind != JsonValueKind.Object
                || !schema.TryGetProperty("properties", out var properties)
                || properties.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var declared = properties.EnumerateObject().ToArray();
            if (declared.Length == 0)
            {
                continue;
            }

            /* A value each parameter can take: 1 for an integer, since the guard refuses a word there just as the
               binder cannot read one, and a word for the rest. */
            var everything = declared.ToDictionary(
                p => p.Name,
                p => p.Value.TryGetProperty("type", out var type) && type.ValueKind == JsonValueKind.String && type.GetString() == "integer"
                    ? JsonSerializer.SerializeToElement(1)
                    : JsonSerializer.SerializeToElement("x"),
                StringComparer.Ordinal);

            if (McpUnknownArgumentGuard.Refuse(Call(name, everything), tool) is { } refused)
            {
                wrongful.Add($"{name}: refused its OWN declared parameters -> {TextOf(refused)}");
            }
        }

        Assert.True(
            wrongful.Count == 0,
            "The unknown-argument guard refused calls it must serve — the guard may only reject keys the "
            + "binder would have dropped:\n" + string.Join("\n", wrongful));
    }

    /// <summary>
    /// Argument names match exactly, letter case included, because that is how the SDK's binder matches them: it
    /// does not bind <c>HOURS_BACK</c> to <c>hours_back</c>, so the tool ran at its default 24 hours. The guard
    /// refuses such a key like any other unknown one and suggests the parameter it differs from only by case.
    /// </summary>
    [Fact]
    public void AKeyDifferingOnlyByCase_IsRefused_AndTheRefusalNamesTheParameter()
    {
        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        var result = McpUnknownArgumentGuard.Refuse(
            Call("get_collection_log", Args(("HOURS_BACK", "1"))),
            tool);

        var problem = McpInProcessHost.CaseRefusalProblem("get_collection_log", "HOURS_BACK", "hours_back", result);
        Assert.True(problem is null, problem);
    }

    /// <summary>
    /// The same key through a real in-process server, so the SDK's own binder is on the path: <c>get_wait_stats</c>
    /// with <c>{"HOURS_BACK": 2}</c> is refused before the tool runs. Without the refusal the binder drops the key
    /// and the tool reads its default window.
    /// </summary>
    [Fact]
    public async Task AKeyDifferingOnlyByCase_IsRefusedBeforeTheBinder_ThroughARealServer()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = await McpWholeNumberArgumentTests.StartHostAsync();

        var result = await host.Client.CallToolAsync(
            "get_wait_stats", new Dictionary<string, object?> { ["HOURS_BACK"] = 2 }, cancellationToken: ct);

        var problem = McpInProcessHost.CaseRefusalProblem("get_wait_stats", "HOURS_BACK", "hours_back", result);
        Assert.True(problem is null, problem);
    }

    /// <summary>
    /// The premise the case refusal rests on, pinned on the SDK's binder alone, with the guard left out of the host:
    /// a key that differs from a parameter only by letter case is dropped, never bound. <c>HOURS_BACK</c> carrying a
    /// value no integer parameter can take logs no binding failure, so the binder never read it. The same value
    /// under <c>hours_back</c> does log one, which shows this test can see a binding failure when there is one. If a
    /// later SDK matched names ignoring case, the first half would fail here, and the guard would be refusing calls
    /// that work.
    /// </summary>
    [Fact]
    public async Task WithoutTheGuard_TheBinderDropsAKeyDifferingOnlyByCase()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = await McpWholeNumberArgumentTests.StartHostAsync(installGuard: false);

        await host.Client.CallToolAsync(
            "get_wait_stats", new Dictionary<string, object?> { ["HOURS_BACK"] = "not-a-number" }, cancellationToken: ct);
        Assert.False(
            host.ToolExceptions.HasBindingFailure,
            "The binder read HOURS_BACK as hours_back. Logged: " + host.ToolExceptions.Describe());

        await host.Client.CallToolAsync(
            "get_wait_stats", new Dictionary<string, object?> { ["hours_back"] = "not-a-number" }, cancellationToken: ct);
        Assert.True(
            host.ToolExceptions.HasBindingFailure,
            "hours_back: \"not-a-number\" logged no binding failure, so this test cannot see one. Logged: "
            + host.ToolExceptions.Describe());
    }

    /// <summary>
    /// The protocol's own metadata cannot be refused, because it never reaches the guard: <c>_meta</c> and
    /// the progress token are SIBLINGS of <c>Arguments</c> on <see cref="CallToolRequestParams"/>. Asserted
    /// against the SDK's type rather than assumed — if a future SDK moved them INTO the arguments object,
    /// this guard would start refusing well-formed client traffic, and this is the test that would say so.
    /// </summary>
    [Fact]
    public void ProtocolMetadata_TravelsOutsideTheArgumentsObject()
    {
        var argumentsProperty = typeof(CallToolRequestParams).GetProperty("Arguments");
        Assert.NotNull(argumentsProperty);

        /* Both live on the params (via RequestParams), NOT inside the arguments dictionary. */
        Assert.NotNull(typeof(CallToolRequestParams).GetProperty("Meta"));
        Assert.NotNull(typeof(CallToolRequestParams).GetProperty("ProgressToken"));

        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        /* And a params object carrying protocol furniture but no stray argument is clean. */
        var parameters = new CallToolRequestParams
        {
            Name = "get_collection_log",
            Arguments = Args(("server_name", "SQL2022")),
        };

        Assert.Null(McpUnknownArgumentGuard.Refuse(parameters, tool));
    }

    /// <summary>
    /// The guard reads each value into its parameter's declared type to learn what the binder would do, and refuses
    /// only on a <see cref="JsonException"/>. A converter that throws anything else (an
    /// <see cref="ArgumentException"/> here) is a case the guard cannot judge, so the call passes to the binder, which
    /// answers it itself. An exception out of <see cref="McpUnknownArgumentGuard.Refuse"/> would escape the call-tool
    /// filter and fail a call the binder owns.
    /// </summary>
    [Fact]
    public void AValueWhoseConverterThrowsAnythingButAJsonError_PassesToTheBinder()
    {
        var method = typeof(ConverterThrowsProbeTool).GetMethod(nameof(ConverterThrowsProbeTool.Take))!;
        var tool = McpServerTool.Create(method, target: null, options: new McpServerToolCreateOptions());
        var name = tool.ProtocolTool.Name;
        var types = new McpToolParameterTypes();
        types.Register(name, method, include: null);

        /* The control: this tool's parameters are judged by their declared types, so the null below is a verdict and
           not a guard that never got as far as reading the value. */
        var control = McpUnknownArgumentGuard.Refuse(Call(name, Args(("count", "abc"))), tool, types);
        Assert.NotNull(control);
        using var refusal = JsonDocument.Parse(TextOf(control!));
        Assert.Contains("'count'", refusal.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

        Assert.Null(McpUnknownArgumentGuard.Refuse(Call(name, Args(("value", "anything"))), tool, types));
    }

    /// <summary>
    /// <see cref="Type.GetTypeCode(Type)"/> answers an enum's underlying integer type, so an enum parameter used to pass
    /// for a whole-number one: a value it could not read was refused as "a whole number ... with no decimal point", with
    /// no word of the names it takes. The refusal for an enum, or a list of one, names its members.
    /// </summary>
    [Fact]
    public void AnEnumParameter_IsRefusedAsOneOfItsNames_NotAsAWholeNumber()
    {
        /* A fraction, a name no member has, and a whole number past the underlying int: each is a value the binder cannot
           read for an enum, and none of them is a whole number problem. */
        foreach (var raw in new[] { "0.5", "\"Magenta\"", "3000000000" })
        {
            var message = ProbeRefusal("color", raw);

            Assert.NotNull(message);
            Assert.Contains("'color'", message, StringComparison.Ordinal);
            Assert.Contains("takes one of Crimson, Teal, Amber, and the call sent", message, StringComparison.Ordinal);
            Assert.DoesNotContain("whole number", message, StringComparison.Ordinal);
            Assert.DoesNotContain("too large", message, StringComparison.Ordinal);
        }

        var list = ProbeRefusal("colors", "[0.5]");

        Assert.NotNull(list);
        Assert.Contains("a list (a JSON array) of names from Crimson, Teal, Amber", list, StringComparison.Ordinal);
        Assert.DoesNotContain("ProbeColor", list, StringComparison.Ordinal);
        Assert.DoesNotContain("whole number", list, StringComparison.Ordinal);

        /* The control: a member name, and a number the enum holds, are read by the binder, so they are not refused. */
        Assert.Null(ProbeRefusal("color", "\"Amber\""));
        Assert.Null(ProbeRefusal("color", "1"));
    }

    /// <summary>
    /// A whole number the guard words as too large or too small is judged by its digits. The widest integer type holds
    /// 20 of them, so a run longer than that is out of every range whatever it is, and is not handed to a big-integer
    /// parse whose cost grows faster than its length. The caller sets the length of the value.
    /// </summary>
    [Theory]
    [InlineData("", false, "too large")]
    [InlineData("-", false, "too small")]
    [InlineData("", true, "too large")]
    [InlineData("-", true, "too small")]
    public void AHundredThousandDigitValueForAnInt_IsRefusedAsTooLargeOrTooSmall(string sign, bool asText, string direction)
    {
        var digits = sign + new string('9', 100_000);
        var message = ProbeRefusal("count", asText ? "\"" + digits + "\"" : digits);

        Assert.NotNull(message);
        Assert.Contains("'count'", message, StringComparison.Ordinal);
        Assert.Contains("takes a whole number, and the call sent", message, StringComparison.Ordinal);
        Assert.Contains($"which is {direction}.", message, StringComparison.Ordinal);
        AssertStatesNoRange(message);
    }

    /// <summary>
    /// A leading plus sign, and leading zeros, change neither which way a whole number is out of range nor whether it is:
    /// the zeros do not count toward the 20 digits the widest integer type holds.
    /// </summary>
    [Theory]
    [InlineData("+", 0, "3000000000", "too large")]
    [InlineData("-", 0, "3000000000", "too small")]
    [InlineData("", 30, "3000000000", "too large")]
    [InlineData("+", 30, "3000000000", "too large")]
    [InlineData("-", 30, "3000000000", "too small")]
    [InlineData("", 30, "99999999999999999999999", "too large")]
    [InlineData("-", 30, "99999999999999999999999", "too small")]
    public void ASignAndLeadingZeros_DoNotChangeWhichWayAWholeNumberIsOutOfRange(
        string sign, int zeros, string digits, string direction)
    {
        var message = ProbeRefusal("count", "\"" + sign + new string('0', zeros) + digits + "\"");

        Assert.NotNull(message);
        Assert.Contains("takes a whole number, and the call sent", message, StringComparison.Ordinal);
        Assert.Contains($"which is {direction}.", message, StringComparison.Ordinal);
        AssertStatesNoRange(message);
    }

    /// <summary>
    /// A refusal for a whole number past an <see cref="int"/> says the value is too large or too small, and states no
    /// range. The range of the CLR type is not the range the tool takes (<c>hours_back</c> is an int, and
    /// <c>McpHelpers.ValidateHoursBack</c> refuses anything outside 1-168), so quoting it gave the caller two ranges that
    /// disagree, and invited a retry the tool's own validator refuses.
    /// </summary>
    private static void AssertStatesNoRange(string message)
    {
        Assert.DoesNotContain("2147483647", message, StringComparison.Ordinal);
        Assert.DoesNotContain("-2147483648", message, StringComparison.Ordinal);
        Assert.DoesNotContain(" from ", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same signs and zeros on a value the binder reads (5) are not refused at all, so the bounded digit run does
    /// not turn a padded small number into one that is too large.
    /// </summary>
    [Theory]
    [InlineData("+", 0)]
    [InlineData("", 30)]
    [InlineData("+", 30)]
    public void ASignAndLeadingZeros_OnAWholeNumberThatFits_AreNotRefused(string sign, int zeros) =>
        Assert.Null(ProbeRefusal("count", "\"" + sign + new string('0', zeros) + "5\""));

    /// <summary>
    /// A zero written with a sign and a long run of zeros is not out of range, so it is never worded as too large or
    /// too small: the 20 digits the widest integer type holds are counted after the leading zeros, not with them. (An
    /// unsigned type does not read a signed text, so the guard has a verdict to give here.)
    /// </summary>
    [Theory]
    [InlineData("-")]
    [InlineData("+")]
    public void ASignedRunOfZeros_IsNotWordedAsOutOfRange(string sign)
    {
        var message = ProbeRefusal("huge", "\"" + sign + new string('0', 30) + "\"");

        Assert.True(
            message is null || !message.Contains("which is too", StringComparison.Ordinal),
            "A zero is within every range, but the refusal says: " + message);
    }

    /// <summary>
    /// The refusal message for one JSON value sent for one parameter of <see cref="ColorCountProbeTool"/>, registered
    /// the way the host records its tools, or null when the guard lets the call through.
    /// </summary>
    private static string? ProbeRefusal(string parameter, string rawJson)
    {
        var method = typeof(ColorCountProbeTool).GetMethod(nameof(ColorCountProbeTool.Pick))!;
        var tool = McpServerTool.Create(method, target: null, options: new McpServerToolCreateOptions());
        var name = tool.ProtocolTool.Name;
        var types = new McpToolParameterTypes();
        types.Register(name, method, include: null);

        using var document = JsonDocument.Parse(rawJson);
        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal)
        {
            [parameter] = document.RootElement.Clone(),
        };

        if (McpUnknownArgumentGuard.Refuse(Call(name, arguments), tool, types) is not { } refused)
        {
            return null;
        }

        using var refusal = JsonDocument.Parse(TextOf(refused));
        return refusal.RootElement.GetProperty("message").GetString();
    }

    /// <summary>
    /// The registrations named in the host source — the same derivation
    /// <see cref="McpToolTypeRegistrationTests"/> uses, so this census covers the tools that actually ship
    /// rather than every class in the assembly.
    /// </summary>
    internal static HashSet<string> RegisteredToolTypeNames()
    {
        var source = File.ReadAllText(HostSourcePath());

        var names = Regex
            .Matches(source, @"WithGeminiCompatibleTools<(\w+)>")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(names.Count > 0, "Found no .WithGeminiCompatibleTools<T>() registrations in the host.");

        return names;
    }

    private static string HostSourcePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null)
        {
            var candidate = Path.Combine(
                dir.FullName,
                "Darling",
                "PerformanceMonitor.Darling.Service",
                "Mcp",
                "DarlingMcpHostService.cs");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException(
            "Could not locate DarlingMcpHostService.cs by walking up from the test output directory.");
    }
}

/// <summary>A parameter type whose converter throws an <see cref="ArgumentException"/> when it reads a value, as a
/// converter written without the serializer's own exceptions in mind can.</summary>
[JsonConverter(typeof(ConverterThatThrowsOnRead))]
internal sealed class ValueWithThrowingConverter
{
}

internal sealed class ConverterThatThrowsOnRead : JsonConverter<ValueWithThrowingConverter>
{
    public override ValueWithThrowingConverter Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new ArgumentException("This converter reads nothing.");

    public override void Write(Utf8JsonWriter writer, ValueWithThrowingConverter value, JsonSerializerOptions options) =>
        throw new NotSupportedException();
}

/// <summary>The one method the guard test above registers by hand: a parameter whose converter throws, and a whole
/// number the guard can judge, so a refusal for the second proves the guard reached the first.</summary>
internal static class ConverterThrowsProbeTool
{
    public static string Take(ValueWithThrowingConverter? value = null, int count = 0) => "ok";
}

/// <summary>An enum for the tests above: its members are what a refusal for an enum parameter names.</summary>
internal enum ProbeColor
{
    Crimson,
    Teal,
    Amber,
}

/// <summary>The one method the enum and digit-run tests register by hand: an enum, a list of that enum, an
/// <see cref="int"/> and a <see cref="ulong"/>. Like <see cref="ConverterThrowsProbeTool"/> it carries no tool
/// attribute, so no census over the shipped tool types sees it.</summary>
internal static class ColorCountProbeTool
{
    public static string Pick(
        ProbeColor color = ProbeColor.Teal, ProbeColor[]? colors = null, int count = 0, ulong huge = 0) => "ok";
}
