/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one scratch database this fixture mints through ScratchPostgres holds its own servers and rows and is dropped
   with the fixture. */
/// <summary>
/// One seeded scratch store behind the MCP server the service hosts, for a test that runs one rule across every read the server lists.
///
/// <para><b>The tools come from the server.</b> The fixture builds the host's own registration
/// (<see cref="DarlingMcpHostService.RegisterToolServices"/>, <see cref="DarlingMcpHostService.ConfigureMcpServices"/> and
/// <see cref="DarlingMcpHostService.ConfigurePipeline"/>) over an in-memory transport, asks it for <c>tools/list</c>, and sends every
/// call as <c>tools/call</c>, so a call passes the same filters and argument binding a client's does. The write switch is left off, as
/// it is by default, so the list is the reads. A tool added to the server is listed, and so it is called.</para>
///
/// <para><b>The arguments come from each tool's input schema.</b> <c>server_name</c>, <c>hours_back</c> and <c>as_of</c> are set from
/// the seed's servers and window. Any other parameter is filled by its name: a parameter named like a seeded column takes that column's
/// seeded value, and one the schema gives an <c>enum</c> takes its first value. A required parameter the seed has no value for is left
/// out, the tool refuses the call, and the test that made it reports the refusal.</para>
///
/// <para><b>The seed comes from the store's own catalog.</b> Every table of the <c>collect</c> schema gets the same few rows for each
/// server, with every time column of a row set to one instant: the hour before the window, exactly the window's start, the middle of
/// the window, and exactly the window's end. Each engine has five servers that differ only in which of those instants they hold, so two
/// servers' answers to one call differ only by what a row at that instant adds. Decimal columns hold tenths, whose sums and averages
/// carry binary float artefacts unless the read rounds them, and the rows of the <see cref="Full"/> servers carry long text, so a read
/// that returns them whole is over the response budget. The window is anchored to the store's clock, read once, so the seeded hours are
/// inside every read's retention; nothing here waits, and every assert is on a row's place against the window it was called with.</para>
/// </summary>
public sealed class McpEveryReadStore : IAsyncLifetime
{
    public const string SqlServer = "sqlserver";
    public const string Postgres = "postgres";
    public const string AuroraPostgres = "aurora-postgres";

    /// <summary>The engines a server is registered as, in the order a test tries them for a tool.</summary>
    public static readonly string[] Engines = [SqlServer, Postgres, AuroraPostgres];

    /// <summary>Holds the hour before the window only: the answer a read gives when the window holds nothing.</summary>
    public const string Prior = "prior";

    /// <summary><see cref="Prior"/>, and a row exactly at the window's start.</summary>
    public const string Start = "start";

    /// <summary><see cref="Prior"/>, and a row exactly in the middle of the window, where its two halves meet.</summary>
    public const string Mid = "mid";

    /// <summary><see cref="Prior"/>, and a row exactly at the window's end.</summary>
    public const string End = "end";

    /// <summary>All four instants, with long text.</summary>
    public const string Full = "full";

    private static readonly string[] Placements = [Prior, Start, Mid, End, Full];

    /// <summary>The hours a window between <see cref="WindowStart"/> and <see cref="WindowEnd"/> spans.</summary>
    public const int WindowHours = 4;

    /// <summary>The hours of the window that holds every seeded instant, ending at <see cref="WideEnd"/>.</summary>
    public const int WideHours = 8;

    /// <summary>The distinct things (databases, queries, wait types and so on) each server holds a row for at each instant.</summary>
    private const int Variants = 8;

    private const int FirstServerId = -917001;
    private const string ServerPrefix = "mcp-every-read-";
    private const string Token = "mcp-every-read-token";

    private static readonly Regex s_serverName = new(ServerPrefix + "[a-z]+-[a-z]+", RegexOptions.Compiled);
    private static readonly Regex s_serverId = new(@"-9170\d\d", RegexOptions.Compiled);
    private static readonly Regex s_dateOrTime = new(@"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?(?:Z|[+-]\d{2}:\d{2}| UTC)?", RegexOptions.Compiled);

    private ScratchPostgres? _scratch;
    private NpgsqlDataSource? _postgres;
    private WebApplication? _app;
    private TestServer? _server;
    private readonly HashSet<string> _textColumns = new(StringComparer.Ordinal);
    private readonly HashSet<string> _idColumns = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, string> _unseeded = new(StringComparer.Ordinal);
    private int _nextRequestId;

    /// <summary>False when <c>DARLING_TEST_PG</c> is not set, so there is no store and a test skips.</summary>
    public bool Available => _server is not null;

    public const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the every-read MCP tests.";

    /// <summary>The end of the four-hour window: an hour boundary, the last whole hour before the store's clock.</summary>
    public DateTime WindowEnd { get; private set; }

    public DateTime WindowStart => WindowEnd.AddHours(-WindowHours);

    /// <summary>The instant the window's two halves meet.</summary>
    public DateTime WindowMiddle => WindowStart.AddHours(WindowHours / 2);

    /// <summary>The end of the window that holds every seeded instant.</summary>
    public DateTime WideEnd => WindowEnd.AddHours(1);

    /// <summary>Every tool the server lists, by name.</summary>
    public IReadOnlyList<McpEveryReadTool> Tools { get; private set; } = [];

    /// <summary>The <c>collect</c> tables the seed could not fill, with what the store said.</summary>
    public IReadOnlyDictionary<string, string> UnseededTables => _unseeded;

    public int SeededTableCount { get; private set; }

    public static string ServerName(string engine, string placement) =>
        ServerPrefix + (engine == SqlServer ? "ss" : engine == Postgres ? "pg" : "au") + "-" + placement;

    public static string AsOfText(DateTime end) => end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var ct = CancellationToken.None;
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct, plainPostgres: true);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        _postgres = NpgsqlDataSource.Create(_scratch.ConnectionString);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        DarlingMcpHostService.RegisterToolServices(
            builder.Services, _postgres, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null, planFetcher: null,
            NullLogger.Instance, new BaselineCache());
        DarlingMcpHostService.ConfigureMcpServices(
            builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(), NullLogger.Instance);

        _app = builder.Build();
        var host = new DarlingMcpHostService(NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
        host.ConfigurePipeline(_app, networkMode: false, networkListenIp: null, allowedCidr: System.Net.IPNetwork.Parse("127.0.0.1/32"), bearerToken: Token);
        await _app.StartAsync(ct);
        _server = _app.GetTestServer();

        Tools = await ListToolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }

        if (_postgres is not null) await _postgres.DisposeAsync();
        if (_scratch is not null) await _scratch.DisposeAsync();
    }

    /* ═══════════════════════════ the seed ═══════════════════════════ */

    private async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var clock = new NpgsqlCommand("SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '1 hour'", connection))
        {
            WindowEnd = DateTime.SpecifyKind((DateTime)(await clock.ExecuteScalarAsync(ct))!, DateTimeKind.Utc);
        }

        var prior = WindowStart.AddHours(-2);
        var instants = new (string Label, DateTime At, int Index)[]
        {
            (Prior, prior, 0), (Start, WindowStart, 1), (Mid, WindowMiddle, 2), (End, WindowEnd, 3),
        };

        await ExecAsync(connection, ct, "CREATE TEMP TABLE seed_rows (server_id int, server_name text, engine text, at timestamp, ti int, v int, big boolean, seq bigint)");

        var serverId = FirstServerId;
        long seq = 0;
        foreach (var engine in Engines)
        {
            foreach (var placement in Placements)
            {
                var id = serverId--;
                var name = ServerName(engine, placement);
                await ExecAsync(connection, ct,
                    engine == SqlServer
                        ? "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 16, $3, $3)"
                        : "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, '" + engine + "', 16, $3, $3)",
                    id, name, Naive(prior.AddHours(-1)));

                foreach (var (label, at, index) in instants)
                {
                    if (label != Prior && placement != Full && placement != label)
                    {
                        continue;
                    }

                    for (var v = 0; v < Variants; v++)
                    {
                        await ExecAsync(connection, ct, "INSERT INTO seed_rows VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                            id, name, engine, Naive(at), index, v, placement == Full, -917_000_000L - (++seq));
                    }
                }
            }
        }

        var tables = new SortedDictionary<string, List<SeedColumn>>(StringComparer.Ordinal);
        await using (var read = new NpgsqlCommand(@"
SELECT c.table_name, c.column_name, c.data_type, c.character_maximum_length,
       (c.is_nullable = 'NO'), (c.column_default IS NOT NULL OR c.is_identity = 'YES'), (c.is_generated <> 'NEVER')
FROM information_schema.columns c
JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE c.table_schema = 'collect' AND t.table_type = 'BASE TABLE' AND c.table_name <> 'servers'
  AND NOT EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class k ON k.oid = i.inhrelid JOIN pg_namespace n ON n.oid = k.relnamespace
                  WHERE n.nspname = c.table_schema AND k.relname = c.table_name)
ORDER BY c.table_name, c.ordinal_position", connection))
        {
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var table = reader.GetString(0);
                if (!tables.TryGetValue(table, out var columns)) tables[table] = columns = [];
                columns.Add(new SeedColumn(reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6)));
            }
        }

        /* The collection log is the store's record of collecting, not collected data: every read asks it whether the server was being
           collected. Every server gets the same log, a successful run of each of its engine's collectors at each instant, so no two
           servers differ in it. */
        tables.Remove("collection_log");
        await ExecAsync(connection, ct, "CREATE TEMP TABLE seed_collectors (name text, sql_server boolean)");
        foreach (var collector in CollectorCatalog.All)
        {
            await ExecAsync(connection, ct, "INSERT INTO seed_collectors VALUES ($1, $2)", collector.Name, collector.TargetEngine == CollectorTargetEngine.SqlServer);
        }

        await ExecAsync(connection, ct, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT -918000000 - row_number() OVER (), s.server_id, s.server_name, c.name, t.at, 100, 'SUCCESS', 10
FROM (SELECT DISTINCT server_id, server_name, engine FROM seed_rows) s
JOIN seed_collectors c ON c.sql_server = (s.engine = '" + SqlServer + @"')
CROSS JOIN (SELECT DISTINCT at FROM seed_rows) t");

        foreach (var (table, columns) in tables)
        {
            /* Every column the seed has a value for first; when the store refuses that row (a check constraint on a column the seed
               filled by type), only the columns a row cannot do without. */
            var problem = await TryFillAsync(connection, table, columns, everyColumn: true, ct)
                ?? await TryFillAsync(connection, table, columns, everyColumn: false, ct);
            if (problem is not { Length: 0 })
            {
                _unseeded[table] = problem ?? "refused";
                continue;
            }

            SeededTableCount++;
            foreach (var column in columns)
            {
                if (IsText(column.Type)) _textColumns.Add(column.Name);
                if (IsWhole(column.Type) && IsIdName(column.Name)) _idColumns.Add(column.Name);
            }
        }
    }

    private sealed record SeedColumn(string Name, string Type, int? MaxLength, bool NotNull, bool HasDefault, bool Generated);

    /// <summary>Empty when the rows went in, null when this fill was refused and the narrower one is worth a try, else what the store said.</summary>
    private static async Task<string?> TryFillAsync(NpgsqlConnection connection, string table, List<SeedColumn> columns, bool everyColumn, CancellationToken ct)
    {
        var names = new List<string>();
        var values = new List<string>();
        foreach (var column in columns)
        {
            if (column.Generated) continue;
            var keyed = column.Name is "server_id" or "server_name" || IsTime(column.Type);
            if (!keyed && column.HasDefault) continue;
            if (!keyed && !everyColumn && !column.NotNull) continue;

            var value = ValueFor(column);
            if (value is null)
            {
                if (!column.NotNull) continue;
                return $"column {column.Name} is NOT NULL with no default and type {column.Type}, which the seed has no value for";
            }

            names.Add("\"" + column.Name + "\"");
            values.Add(value);
        }

        var hasServer = columns.Any(c => c.Name == "server_id");
        var sql = $"INSERT INTO collect.\"{table}\" ({string.Join(", ", names)}) SELECT {string.Join(", ", values)} FROM seed_rows r"
            + (hasServer ? "" : $" WHERE r.server_name = '{ServerName(SqlServer, Full)}'")
            + " ORDER BY r.seq DESC ON CONFLICT DO NOTHING";
        try
        {
            await ExecAsync(connection, ct, sql);
            return "";
        }
        catch (PostgresException ex)
        {
            return everyColumn ? null : ex.SqlState + ": " + ex.MessageText;
        }
    }

    private static bool IsTime(string type) => type.StartsWith("timestamp", StringComparison.Ordinal) || type == "date";

    private static bool IsText(string type) => type is "text" or "character varying" or "character";

    private static bool IsWhole(string type) => type is "integer" or "bigint" or "smallint";

    private static bool IsIdName(string name) => name == "id" || name.EndsWith("_id", StringComparison.Ordinal);

    private static bool IsLongTextName(string name) =>
        name.EndsWith("_text", StringComparison.Ordinal) || name.EndsWith("_line", StringComparison.Ordinal)
        || name.EndsWith("message", StringComparison.Ordinal) || name.EndsWith("_statement", StringComparison.Ordinal);

    /// <summary>The SQL expression the seed writes into a column for the row <c>r</c>, or null when it has none for the column's type.</summary>
    private static string? ValueFor(SeedColumn column)
    {
        var name = column.Name;
        if (name == "server_id") return "r.server_id";
        if (name == "server_name") return "r.server_name";

        switch (column.Type)
        {
            case "timestamp without time zone": return "r.at";
            case "timestamp with time zone": return "(r.at AT TIME ZONE 'UTC')";
            case "date": return "r.at::date";
            case "integer" or "bigint" or "smallint":
                if (name is "collection_id" or "log_id") return "r.seq";
                /* A server-local time is read through the server's distance from UTC; at zero, every time column of a row is one instant. */
                if (name.StartsWith("utc_offset", StringComparison.Ordinal)) return "0";
                /* An id names the thing the row is about, the same at every instant; a measure moves with the instant. */
                return IsIdName(name) ? "(1000 + r.v)" : "(7 + 3 * r.v + 4 * r.ti)";
            case "numeric" or "double precision" or "real":
                /* Tenths: 0.1 + 0.2 is 0.30000000000000004 in binary floating point, and a third of a sum has no end. */
                return $"(0.1 + 0.2 * r.v + 0.7 * r.ti)::{column.Type}";
            case "boolean": return "FALSE";
            case "jsonb" or "json": return $"'{{}}'::{column.Type}";
            case "bytea": return "decode(lpad(to_hex(161 + r.v), 16, '0'), 'hex')";
            case "uuid": return "md5(r.v::text)::uuid";
            case "interval": return "interval '1 second'";
            case "text" or "character varying" or "character":
                var text = TextExpression(name);
                return column.MaxLength is int max ? $"left({text}, {max})" : text;
            default: return null;
        }
    }

    private static string TextExpression(string name)
    {
        if (name.EndsWith("_hash", StringComparison.Ordinal) || name.EndsWith("_handle", StringComparison.Ordinal))
        {
            return "('0x' || upper(lpad(to_hex(161 + r.v), 16, '0')))";
        }

        if (name.EndsWith("_xml", StringComparison.Ordinal) || name == "query_plan") return "'<seed />'";
        if (name.EndsWith("_json", StringComparison.Ordinal)) return "'{}'";

        var value = $"('{SeedText(name)}' || CASE WHEN r.v = 0 THEN '' ELSE '_' || r.v END)";
        return IsLongTextName(name) ? $"({value} || CASE WHEN r.big THEN repeat(' seeded words', 200) ELSE '' END)" : value;
    }

    private static string SeedText(string column) => "seed_" + column;

    /// <summary>The value the seed's first thing holds in the column a parameter is named after, or null when no seeded column has that name.</summary>
    public object? SeededValue(string parameter, bool whole)
    {
        if (whole)
        {
            return _idColumns.Contains(parameter) ? 1000L : null;
        }

        if (!_textColumns.Contains(parameter)) return null;
        if (parameter.EndsWith("_hash", StringComparison.Ordinal) || parameter.EndsWith("_handle", StringComparison.Ordinal)) return "0x00000000000000A1";
        return SeedText(parameter);
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.AddWithValue(value ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* ═══════════════════════════ the server ═══════════════════════════ */

    private async Task<JsonObject> SendAsync(string method, JsonObject parameters)
    {
        var request = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["id"] = Interlocked.Increment(ref _nextRequestId),
            ["method"] = method,
            ["params"] = parameters,
        }.ToJsonString();

        var context = await _server!.SendAsync(c =>
        {
            c.Request.Method = "POST";
            c.Request.Path = "/";
            c.Request.Headers.Host = "localhost";
            c.Request.Headers.Accept = "application/json, text/event-stream";
            c.Request.ContentType = "application/json";
            var bytes = Encoding.UTF8.GetBytes(request);
            c.Request.Body = new System.IO.MemoryStream(bytes);
            c.Request.ContentLength = bytes.Length;
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        using var reader = new System.IO.StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync();
        var data = body.Split('\n').Select(l => l.TrimEnd('\r')).FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
        return JsonNode.Parse(data is null ? body : data["data:".Length..].Trim())!.AsObject();
    }

    private async Task<IReadOnlyList<McpEveryReadTool>> ListToolsAsync()
    {
        var tools = new List<McpEveryReadTool>();
        string? cursor = null;
        do
        {
            var parameters = new JsonObject();
            if (cursor is not null) parameters["cursor"] = cursor;
            var result = (await SendAsync("tools/list", parameters))["result"]!.AsObject();
            foreach (var tool in result["tools"]!.AsArray())
            {
                tools.Add(new McpEveryReadTool((string)tool!["name"]!, JsonDocument.Parse(tool["inputSchema"]!.ToJsonString()).RootElement.Clone()));
            }

            cursor = (string?)result["nextCursor"];
        }
        while (cursor is not null);

        return tools.OrderBy(t => t.Name, StringComparer.Ordinal).ToList();
    }

    /// <summary>Sends one <c>tools/call</c> and returns what the server answered.</summary>
    public async Task<McpEveryReadAnswer> CallAsync(string tool, IReadOnlyDictionary<string, object?> arguments)
    {
        var sent = new JsonObject();
        foreach (var (name, value) in arguments) sent[name] = JsonSerializer.SerializeToNode(value);

        var envelope = await SendAsync("tools/call", new JsonObject { ["name"] = tool, ["arguments"] = sent });
        if (envelope["result"] is not JsonObject result)
        {
            return new McpEveryReadAnswer(envelope["error"]?.ToJsonString() ?? envelope.ToJsonString(), true);
        }

        var text = string.Concat((result["content"] as JsonArray ?? []).Select(block => (string?)block?["text"] ?? ""));
        return new McpEveryReadAnswer(text, (bool?)result["isError"] == true);
    }

    /* ═══════════════════════════ the arguments ═══════════════════════════ */

    /// <summary>The row caps a read's schema can carry, by the names the reads share.</summary>
    public static readonly string[] RowCapParameters = ["limit", "top"];

    /// <summary>
    /// The arguments for one call of <paramref name="tool"/>, built from its input schema: the server when the tool takes one, the window
    /// when it takes one, and every required parameter the seed has a value for. <paramref name="also"/> is added as given.
    /// </summary>
    public Dictionary<string, object?> Arguments(
        McpEveryReadTool tool, string? serverName, int hoursBack, DateTime asOf, IReadOnlyDictionary<string, object?>? also = null)
    {
        var arguments = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var parameter in tool.Parameters)
        {
            switch (parameter.Name)
            {
                case "server_name":
                    if (serverName is not null) arguments["server_name"] = serverName;
                    break;
                case "hours_back":
                    arguments["hours_back"] = hoursBack;
                    break;
                case "as_of":
                    arguments["as_of"] = AsOfText(asOf);
                    break;
                case "timeout_seconds":
                    /* A read that waits for something to happen is told not to. */
                    arguments["timeout_seconds"] = 0;
                    break;
                default:
                    if (parameter.Required && ValueFor(parameter) is { } value) arguments[parameter.Name] = value;
                    break;
            }
        }

        if (also is not null)
        {
            foreach (var (name, value) in also) arguments[name] = value;
        }

        return arguments;
    }

    /// <summary>The seed's value for a parameter: the schema's first <c>enum</c> value, else the value of the seeded column of its name.</summary>
    public object? ValueFor(McpEveryReadParameter parameter) =>
        parameter.FirstEnumValue ?? (parameter.Type is "string" or "integer" ? SeededValue(parameter.Name, parameter.Type == "integer") : null);

    /// <summary>
    /// One call of <paramref name="tool"/> over the window that holds every seeded instant, for the <see cref="Full"/> server of the first
    /// engine it answers data for. A tool that takes no server is called once. When no engine's call answers data, the last answer is
    /// returned, and its <see cref="McpEveryReadAnswer.NotDataReason"/> says why.
    /// </summary>
    public async Task<(Dictionary<string, object?> Arguments, McpEveryReadAnswer Answer)> WideReadAsync(
        McpEveryReadTool tool, IReadOnlyDictionary<string, object?>? also = null)
    {
        (Dictionary<string, object?>, McpEveryReadAnswer)? last = null;
        foreach (var engine in tool.Takes("server_name") ? Engines : [SqlServer])
        {
            var arguments = Arguments(tool, tool.Takes("server_name") ? ServerName(engine, Full) : null, WideHours, WideEnd, also);
            var answer = await CallAsync(tool.Name, arguments);
            last = (arguments, answer);
            if (answer.NotDataReason is null) break;
        }

        return last!.Value;
    }

    /// <summary>The call's arguments as a caller would write them, for a finding.</summary>
    public static string Describe(IReadOnlyDictionary<string, object?> arguments) =>
        string.Join(", ", arguments.Select(a => a.Key + ": " + JsonSerializer.Serialize(a.Value)));

    /* ═══════════════════════════ comparing two answers ═══════════════════════════ */

    /// <summary>
    /// An answer with what tells one seeded server from another taken out (its name and id) and every array put in one order, so the
    /// answers two servers give to the same call compare equal exactly when they say the same thing.
    /// </summary>
    public static string Comparable(string reply)
    {
        var text = s_serverId.Replace(s_serverName.Replace(reply, "SERVER"), "0");
        try
        {
            var builder = new StringBuilder();
            Write(JsonNode.Parse(text), builder);
            return builder.ToString();
        }
        catch (JsonException)
        {
            return text;
        }
    }

    /// <summary>A comparable answer with every date and time in it written as one word, so two answers that differ only in when compare equal.</summary>
    public static string WithoutTimes(string comparable) => s_dateOrTime.Replace(comparable, "TIME");

    private static void Write(JsonNode? node, StringBuilder builder)
    {
        switch (node)
        {
            case JsonObject o:
                builder.Append('{');
                foreach (var (key, value) in o)
                {
                    builder.Append('"').Append(key).Append("\":");
                    Write(value, builder);
                    builder.Append(',');
                }

                builder.Append('}');
                break;
            case JsonArray a:
                var items = a.Select(item =>
                {
                    var one = new StringBuilder();
                    Write(item, one);
                    return one.ToString();
                }).OrderBy(s => s, StringComparer.Ordinal);
                builder.Append('[').AppendJoin(',', items).Append(']');
                break;
            default:
                builder.Append(node?.ToJsonString() ?? "null");
                break;
        }
    }
}

/// <summary>One tool as the server lists it: its name and its input schema.</summary>
public sealed class McpEveryReadTool
{
    public McpEveryReadTool(string name, JsonElement schema)
    {
        Name = name;
        var required = schema.TryGetProperty("required", out var r) && r.ValueKind == JsonValueKind.Array
            ? r.EnumerateArray().Select(e => e.GetString()).ToHashSet(StringComparer.Ordinal)
            : [];
        var parameters = new List<McpEveryReadParameter>();
        if (schema.TryGetProperty("properties", out var properties) && properties.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in properties.EnumerateObject())
            {
                parameters.Add(new McpEveryReadParameter(
                    property.Name, TypeOf(property.Value), required.Contains(property.Name),
                    property.Value.TryGetProperty("enum", out var values) && values.ValueKind == JsonValueKind.Array && values.GetArrayLength() > 0
                        && values[0].ValueKind == JsonValueKind.String ? values[0].GetString() : null));
            }
        }

        Parameters = parameters;
    }

    public string Name { get; }

    public IReadOnlyList<McpEveryReadParameter> Parameters { get; }

    public bool Takes(string parameter) => Parameters.Any(p => p.Name == parameter);

    /// <summary>The first type a schema property names that is not <c>null</c>: <c>"string"</c> for <c>["string","null"]</c>.</summary>
    private static string TypeOf(JsonElement property)
    {
        if (!property.TryGetProperty("type", out var type)) return "";
        if (type.ValueKind == JsonValueKind.String) return type.GetString() ?? "";
        return type.ValueKind == JsonValueKind.Array
            ? type.EnumerateArray().Select(t => t.GetString() ?? "").FirstOrDefault(t => t != "null") ?? ""
            : "";
    }
}

public sealed record McpEveryReadParameter(string Name, string Type, bool Required, string? FirstEnumValue);

/// <summary>What the server answered to one call: the text of its content, and whether it marked the result an error.</summary>
public sealed record McpEveryReadAnswer(string Text, bool IsError)
{
    /// <summary>The words an answer's <c>status</c> carries when it holds no data: the four kinds of nothing, a refusal and a failure.
    /// A data answer can carry a <c>status</c> of its own vocabulary, which is not one of these.</summary>
    private static readonly HashSet<string> s_noData = new(StringComparer.Ordinal) { "empty", "unavailable", "not_collected", "precondition", "invalid", "error" };

    /// <summary>The answer's <c>status</c> word when it is one that says the answer holds no data, else null: the answer is data.</summary>
    public string? Status
    {
        get
        {
            try
            {
                using var document = JsonDocument.Parse(Text);
                return document.RootElement.ValueKind == JsonValueKind.Object
                    && document.RootElement.TryGetProperty("status", out var status) && status.ValueKind == JsonValueKind.String
                    && s_noData.Contains(status.GetString() ?? "")
                    ? status.GetString()
                    : null;
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }

    /// <summary>Why this answer is not data, in the answer's own words, or null when it is data.</summary>
    public string? NotDataReason
    {
        get
        {
            var status = Status;
            if (status is null && !IsError) return null;
            var message = "";
            try
            {
                using var document = JsonDocument.Parse(Text);
                if (document.RootElement.ValueKind == JsonValueKind.Object && document.RootElement.TryGetProperty("message", out var m)
                    && m.ValueKind == JsonValueKind.String)
                {
                    message = m.GetString() ?? "";
                }
            }
            catch (JsonException)
            {
                message = Text;
            }

            message = message.Split('\n')[0];
            return (status ?? "error") + ": " + (message.Length > 160 ? message[..160] : message);
        }
    }
}
