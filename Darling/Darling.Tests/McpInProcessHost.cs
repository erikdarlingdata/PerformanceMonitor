/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A real MCP server and client in one process, joined by two pipes. The server registers the given tool types
/// through <see cref="McpSchemaCompat.WithGeminiCompatibleTools"/> and installs
/// <see cref="McpUnknownArgumentGuard"/> as its call-tool filter, as the host does, so a call runs through the
/// SDK's real JSON-RPC, filter pipeline and argument binder. A test can leave the guard out to see what the
/// SDK's binder does on its own.
///
/// <para>Each service parameter is bound to an uninitialized instance of its type, never to a working one, so
/// service binding succeeds and the first thing that can fail is the binding of the caller's arguments. A call
/// that gets past argument binding reaches the tool body and fails there, on the empty service. Nothing here
/// touches a database. The exceptions the server logs for failed tool calls are kept in
/// <see cref="ToolExceptions"/>, so a test can tell a binding failure from a body failure.</para>
/// </summary>
internal sealed class McpInProcessHost : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly CancellationTokenSource _stop;
    private readonly Task _serverRun;
    private readonly List<object> _inertInstances;

    private McpInProcessHost(
        ServiceProvider provider, CancellationTokenSource stop, Task serverRun, McpClient client, ToolExceptionLog log,
        List<object> inertInstances)
    {
        _provider = provider;
        _stop = stop;
        _serverRun = serverRun;
        Client = client;
        ToolExceptions = log;
        _inertInstances = inertInstances;
    }

    public McpClient Client { get; }

    public ToolExceptionLog ToolExceptions { get; }

    /// <summary>The tools the server registered, with the schemas it advertises.</summary>
    public IReadOnlyList<McpServerTool> RegisteredTools => _provider.GetServices<McpServerTool>().ToList();

    /// <param name="toolTypes">The tool classes to register, as the host registers them.</param>
    /// <param name="isServiceParameter">Which parameter types the host resolves from DI rather than from the call.</param>
    /// <param name="inertInstanceFor">An inert instance for a service type that cannot be left uninitialized (an
    /// abstract class or an interface), or null for a type it does not cover.</param>
    /// <param name="installGuard">False to leave <see cref="McpUnknownArgumentGuard"/> out, so a call meets the SDK's
    /// binder with nothing in front of it.</param>
    /// <param name="cancellationToken">Cancels the client's start.</param>
    public static async Task<McpInProcessHost> StartAsync(
        IReadOnlyCollection<Type> toolTypes,
        Func<Type, bool> isServiceParameter,
        Func<Type, object?> inertInstanceFor,
        bool installGuard,
        CancellationToken cancellationToken)
    {
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var log = new ToolExceptionLog();

        var services = new ServiceCollection();
        services.AddLogging(logging => logging.AddProvider(log));

        /* The container never disposes an instance it is handed, so the host disposes the inert ones itself (a data
           source holds a pool). An uninitialized object is left alone: its constructor never ran. */
        var inertInstances = new List<object>();
        foreach (var serviceType in ServiceParameterTypes(toolTypes, isServiceParameter))
        {
            object instance;
            if (serviceType.IsAbstract || serviceType.IsInterface)
            {
                instance = inertInstanceFor(serviceType)
                    ?? throw new InvalidOperationException($"No inert instance for the abstract service type {serviceType.FullName}.");
                inertInstances.Add(instance);
            }
            else
            {
                instance = RuntimeHelpers.GetUninitializedObject(serviceType);
            }

            services.AddSingleton(serviceType, instance);
        }

        var builder = services.AddMcpServer()
            .WithStreamServerTransport(clientToServer.Reader.AsStream(), serverToClient.Writer.AsStream());

        if (installGuard)
        {
            builder = builder.WithRequestFilters(filters => filters.AddCallToolFilter(McpUnknownArgumentGuard.Instance));
        }

        var register = typeof(McpSchemaCompat).GetMethod(
            nameof(McpSchemaCompat.WithGeminiCompatibleTools),
            BindingFlags.Public | BindingFlags.Static)!;

        foreach (var toolType in toolTypes)
        {
            register.MakeGenericMethod(toolType).Invoke(null, new object?[] { builder });
        }

        var provider = services.BuildServiceProvider();
        var stop = new CancellationTokenSource();
        var serverRun = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);

        var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()),
            cancellationToken: cancellationToken);

        return new McpInProcessHost(provider, stop, serverRun, client, log, inertInstances);
    }

    /// <summary>The text of a call's single content block.</summary>
    public static string TextOf(CallToolResult result)
    {
        var block = Assert.Single(result.Content);
        return Assert.IsType<TextContentBlock>(block).Text;
    }

    /// <summary>
    /// Null when <paramref name="result"/> is the guard's whole-number refusal for <paramref name="parameter"/>:
    /// the shared refusal envelope, <c>hints.parameter</c> set to the parameter, and a message that names the
    /// parameter and the tool, says <paramref name="expectedPhrase"/>, and lists the accepted parameters.
    /// Otherwise, what is wrong with it, prefixed with the tool name.
    /// </summary>
    public static string? WholeNumberRefusalProblem(string toolName, string parameter, string expectedPhrase, CallToolResult result)
    {
        var text = TextOf(result);

        if (result.IsError != true || !McpHelpers.IsRefusalEnvelope(text))
        {
            return $"{toolName}: {text}";
        }

        using var document = JsonDocument.Parse(text);
        var root = document.RootElement;
        var hinted = root.TryGetProperty("hints", out var hints) && hints.TryGetProperty("parameter", out var hint)
            ? hint.GetString()
            : null;
        var message = root.GetProperty("message").GetString() ?? "";

        if (hinted != parameter)
        {
            return $"{toolName}: the refusal names '{hinted}', not '{parameter}' -> {message}";
        }

        foreach (var expected in new[] { $"'{parameter}'", $"'{toolName}'", expectedPhrase, "Accepted parameters:" })
        {
            if (!message.Contains(expected, StringComparison.Ordinal))
            {
                return $"{toolName}: the refusal does not say \"{expected}\" -> {message}";
            }
        }

        return null;
    }

    /// <summary>The message of the refusal envelope in <paramref name="result"/>, parsed from its JSON so escapes
    /// such as <c>😀</c> read back as the characters they stand for; null when it is not a refusal.</summary>
    public static string? RefusalMessage(CallToolResult result)
    {
        var text = TextOf(result);
        if (result.IsError != true || !McpHelpers.IsRefusalEnvelope(text))
        {
            return null;
        }

        using var document = JsonDocument.Parse(text);
        return document.RootElement.GetProperty("message").GetString();
    }

    /// <summary>
    /// Null when <paramref name="result"/> is the guard's unknown-argument refusal for <paramref name="sentKey"/>, a key
    /// that differs from <paramref name="parameter"/> only by letter case: the shared refusal envelope,
    /// <c>hints.parameter</c> set to the key as it was sent, and a message that names the key, suggests the parameter,
    /// says names must match in letter case, and lists the accepted parameters. Otherwise, what is wrong with it,
    /// prefixed with the tool name. A null <paramref name="result"/> is the guard's own answer when it passes a call.
    /// </summary>
    public static string? CaseRefusalProblem(string toolName, string sentKey, string parameter, CallToolResult? result)
    {
        if (result is null)
        {
            return $"{toolName}: the guard passed '{sentKey}', so the binder would drop it and the tool would run without it";
        }

        var text = TextOf(result);

        if (result.IsError != true || !McpHelpers.IsRefusalEnvelope(text))
        {
            return $"{toolName}: '{sentKey}' was not refused, so the call went on to the tool -> {text}";
        }

        using var document = JsonDocument.Parse(text);
        var root = document.RootElement;
        var hinted = root.TryGetProperty("hints", out var hints) && hints.TryGetProperty("parameter", out var hint)
            ? hint.GetString()
            : null;
        var message = root.GetProperty("message").GetString() ?? "";

        if (hinted != sentKey)
        {
            return $"{toolName}: the refusal names '{hinted}', not '{sentKey}' -> {message}";
        }

        foreach (var expected in new[]
                 {
                     $"Unknown argument '{sentKey}'", $"'{sentKey}' -> '{parameter}'", "including letter case", "Accepted parameters:"
                 })
        {
            if (!message.Contains(expected, StringComparison.Ordinal))
            {
                return $"{toolName}: the refusal does not say \"{expected}\" -> {message}";
            }
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await _stop.CancelAsync();

        try
        {
            await _serverRun;
        }
        catch (OperationCanceledException)
        {
        }

        _stop.Dispose();
        await _provider.DisposeAsync();

        foreach (var instance in _inertInstances)
        {
            switch (instance)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync();
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }
    }

    private static IEnumerable<Type> ServiceParameterTypes(IEnumerable<Type> toolTypes, Func<Type, bool> isServiceParameter) =>
        toolTypes
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(t => t != typeof(CancellationToken) && isServiceParameter(t))
            .Distinct();

    /// <summary>The exceptions the server logged, in order, with the logger category that logged each one.</summary>
    internal sealed class ToolExceptionLog : ILoggerProvider
    {
        private readonly ConcurrentQueue<(string Category, Exception Exception)> _entries = new();

        public IReadOnlyList<(string Category, Exception Exception)> Entries => _entries.ToArray();

        /// <summary>
        /// Whether any logged exception is an argument-binding failure: a <see cref="JsonException"/> anywhere in its
        /// chain, which is what the SDK's binder throws when it cannot read an argument's value.
        /// </summary>
        public bool HasBindingFailure => Entries.Any(e => Chain(e.Exception).Any(x => x is JsonException));

        /// <summary>Each logged exception as its category and the types in its chain, for a failure message.</summary>
        public string Describe() =>
            Entries.Count == 0
                ? "nothing was logged"
                : string.Join("; ", Entries.Select(e => e.Category + ": " + string.Join(" <- ", Chain(e.Exception).Select(x => x.GetType().Name))));

        private static IEnumerable<Exception> Chain(Exception exception)
        {
            for (Exception? current = exception; current is not null; current = current.InnerException)
            {
                yield return current;
            }
        }

        public ILogger CreateLogger(string categoryName) => new Logger(categoryName, _entries);

        public void Dispose()
        {
        }

        private sealed class Logger(string category, ConcurrentQueue<(string, Exception)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (exception is not null)
                {
                    entries.Enqueue((category, exception));
                }
            }
        }
    }
}
