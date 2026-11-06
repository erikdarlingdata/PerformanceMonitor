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
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A loopback stand-in for the Slack Web API (#7656). It records each request's path, Authorization header and body, and answers
/// with the scripted JSON bodies in order, the last one repeating. The real Slack API is never called: the client under test is
/// pointed at <see cref="ApiBase"/>. A <see cref="TcpListener"/> for the same reason <c>CapturingWebhookEndpoint</c> gives: HttpListener
/// wants a URL ACL on Windows.
/// </summary>
internal sealed class FakeSlackApi : IDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accepting;
    private readonly (int Status, string Body)[] _responses;
    private readonly ConcurrentQueue<(string Path, string Authorization, string Body)> _requests = new();
    private int _served;

    public FakeSlackApi(params (int Status, string Body)[] responses)
    {
        _responses = responses.Length == 0 ? [(200, """{"ok":true,"channel":"C0FAKE","ts":"1700000000.000100"}""")] : responses;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        ApiBase = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/api/";
        _accepting = Task.Run(AcceptLoopAsync);
    }

    public string ApiBase { get; }

    public IReadOnlyList<(string Path, string Authorization, string Body)> Requests => _requests.ToArray();

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                using var stream = client.GetStream();
                var (path, authorization, body) = await ReadRequestAsync(stream);
                _requests.Enqueue((path, authorization, body));
                var index = Math.Min(Interlocked.Increment(ref _served), _responses.Length) - 1;
                var (status, responseBody) = _responses[index];
                var bytes = Encoding.UTF8.GetBytes(responseBody);
                var head = Encoding.ASCII.GetBytes(
                    $"HTTP/1.1 {status} {(status == 200 ? "OK" : "Error")}\r\nContent-Type: application/json\r\nContent-Length: {bytes.Length}\r\nConnection: close\r\n\r\n");
                await stream.WriteAsync(head, _stop.Token);
                await stream.WriteAsync(bytes, _stop.Token);
                await stream.FlushAsync(_stop.Token);
            }
        }
        catch (OperationCanceledException) { /* Dispose */ }
        catch (SocketException) { /* listener stopped */ }
        catch (ObjectDisposedException) { /* listener stopped */ }
    }

    private static async Task<(string Path, string Authorization, string Body)> ReadRequestAsync(NetworkStream stream)
    {
        var buffer = new byte[16 * 1024];
        var received = new List<byte>(16 * 1024);
        int headerEnd;
        while ((headerEnd = IndexOfHeaderEnd(received)) < 0)
        {
            var read = await stream.ReadAsync(buffer);
            if (read == 0)
            {
                break;
            }

            received.AddRange(new ArraySegment<byte>(buffer, 0, read));
        }

        if (headerEnd < 0)
        {
            return ("", "", Encoding.UTF8.GetString(received.ToArray()));
        }

        var headers = Encoding.ASCII.GetString(received.ToArray(), 0, headerEnd);
        var lines = headers.Split("\r\n");
        var path = lines[0].Split(' ').Skip(1).FirstOrDefault() ?? "";
        var authorization = lines.Skip(1)
            .FirstOrDefault(l => l.StartsWith("Authorization:", StringComparison.OrdinalIgnoreCase))?["Authorization:".Length..].Trim() ?? "";
        var contentLength = lines.Skip(1)
            .Where(l => l.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
            .Select(l => int.Parse(l["Content-Length:".Length..].Trim(), System.Globalization.CultureInfo.InvariantCulture))
            .FirstOrDefault();
        var bodyStart = headerEnd + 4;
        while (received.Count - bodyStart < contentLength)
        {
            var read = await stream.ReadAsync(buffer);
            if (read == 0)
            {
                break;
            }

            received.AddRange(new ArraySegment<byte>(buffer, 0, read));
        }

        var all = received.ToArray();
        var body = Encoding.UTF8.GetString(all, bodyStart, Math.Min(contentLength, all.Length - bodyStart));
        return (path, authorization, body);
    }

    private static int IndexOfHeaderEnd(List<byte> bytes)
    {
        for (var i = 0; i + 3 < bytes.Count; i++)
        {
            if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n')
            {
                return i;
            }
        }

        return -1;
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        try { _accepting.Wait(TimeSpan.FromSeconds(2)); } catch (AggregateException) { }
        _stop.Dispose();
    }
}

/// <summary>
/// #7656: the Slack bot's posts. The client posts <c>chat.postMessage</c> with the bot token as a bearer header, every post sets
/// <c>unfurl_links</c> and <c>unfurl_media</c> to false, a reply carries the thread's <c>ts</c>, Slack's <c>ok:false</c> answer is the
/// error, and the token never appears in an error, whatever the server echoes back.
/// </summary>
public sealed class SlackBotClientTests
{
    private const string Token = "xoxb-0000-test-token-value";

    [Fact]
    public async Task Post_sends_bearer_token_unfurl_off_and_thread_ts()
    {
        using var api = new FakeSlackApi();
        var client = new SlackBotClient(Token, api.ApiBase);

        var result = await client.PostAsync("C0CHAN", "hello & <world>", blocks: null, threadTs: "1700000000.000001", CancellationToken.None);

        Assert.True(result.Ok, result.Error);
        Assert.Equal("1700000000.000100", result.Ts);
        var request = Assert.Single(api.Requests);
        Assert.Equal("/api/chat.postMessage", request.Path);
        Assert.Equal("Bearer " + Token, request.Authorization);
        using var body = JsonDocument.Parse(request.Body);
        var root = body.RootElement;
        Assert.Equal("C0CHAN", root.GetProperty("channel").GetString());
        Assert.Equal("hello & <world>", root.GetProperty("text").GetString());
        Assert.Equal("1700000000.000001", root.GetProperty("thread_ts").GetString());
        Assert.False(root.GetProperty("unfurl_links").GetBoolean());
        Assert.False(root.GetProperty("unfurl_media").GetBoolean());
    }

    [Fact]
    public async Task Parent_post_carries_no_thread_ts_and_blocks_ride_in_the_body()
    {
        using var api = new FakeSlackApi();
        var client = new SlackBotClient(Token, api.ApiBase);
        var blocks = new List<object> { new { type = "section", text = new { type = "mrkdwn", text = "*Server:* one" } } };

        var result = await client.PostAsync("C0CHAN", "fallback", blocks, threadTs: null, CancellationToken.None);

        Assert.True(result.Ok, result.Error);
        using var body = JsonDocument.Parse(Assert.Single(api.Requests).Body);
        Assert.False(body.RootElement.TryGetProperty("thread_ts", out _));
        Assert.Equal(1, body.RootElement.GetProperty("blocks").GetArrayLength());
        Assert.False(body.RootElement.GetProperty("unfurl_links").GetBoolean());
        Assert.False(body.RootElement.GetProperty("unfurl_media").GetBoolean());
    }

    [Fact]
    public async Task Slack_ok_false_is_the_error_and_a_non_200_is_an_http_error()
    {
        using var api = new FakeSlackApi((200, """{"ok":false,"error":"channel_not_found"}"""), (503, "<html>busy</html>"));
        var client = new SlackBotClient(Token, api.ApiBase);

        var first = await client.PostAsync("C0CHAN", "x", null, null, CancellationToken.None);
        var second = await client.PostAsync("C0CHAN", "x", null, null, CancellationToken.None);

        Assert.False(first.Ok);
        Assert.Equal("channel_not_found", first.Error);
        Assert.Null(first.Ts);
        Assert.False(second.Ok);
        Assert.Contains("503", second.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_error_never_carries_the_token_even_when_the_server_echoes_it()
    {
        /* A proxy or a misconfigured endpoint can echo the request, header and all. The error text a caller logs must not. */
        using var api = new FakeSlackApi(
            (200, $$$"""{"ok":false,"error":"invalid_auth sent {{{Token}}}"}"""),
            (401, $"Unauthorized: Bearer {Token}"));
        var client = new SlackBotClient(Token, api.ApiBase);

        var first = await client.PostAsync("C0CHAN", "x", null, null, CancellationToken.None);
        var second = await client.PostAsync("C0CHAN", "x", null, null, CancellationToken.None);

        Assert.False(first.Ok);
        Assert.False(second.Ok);
        Assert.DoesNotContain(Token, first.Error ?? "", StringComparison.Ordinal);
        Assert.DoesNotContain(Token, second.Error ?? "", StringComparison.Ordinal);
        Assert.DoesNotContain(Token, client.ToString() ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_connection_failure_is_an_error_without_the_token()
    {
        /* Port 9 on loopback: nothing listens, so the connect fails. The exception text is what a caller would log. */
        var client = new SlackBotClient(Token, "http://127.0.0.1:9/api/");

        var result = await client.PostAsync("C0CHAN", "x", null, null, CancellationToken.None);

        Assert.False(result.Ok);
        Assert.False(string.IsNullOrWhiteSpace(result.Error));
        Assert.DoesNotContain(Token, result.Error!, StringComparison.Ordinal);
    }

    [Fact]
    public void Redact_replaces_every_copy_of_the_token()
    {
        var text = $"a {Token} b {Token}";
        var redacted = SlackBotClient.Redact(text, Token);
        Assert.DoesNotContain(Token, redacted, StringComparison.Ordinal);
        Assert.Equal(2, redacted.Split(SlackBotClient.RedactedToken).Length - 1);
    }
}

/// <summary>
/// #7656: what each thread post says. One escape helper covers every collected value, each post's free text follows
/// <c>mcp.dataDetail</c> (<c>maskLiterals</c> masks it, <c>metadataOnly</c> leaves it out and keeps verdicts, states and links), and the
/// once keys are one per request and post.
/// </summary>
public sealed class SlackThreadPostsTests
{
    private static readonly Guid RequestId = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private const string Base = "https://dash.example.test:5443";

    private static List<string> Strings(SlackThreadPostContent content)
    {
        var all = new List<string> { content.Text };
        void Walk(JsonElement e)
        {
            switch (e.ValueKind)
            {
                case JsonValueKind.String:
                    all.Add(e.GetString()!);
                    break;
                case JsonValueKind.Array:
                    foreach (var x in e.EnumerateArray()) Walk(x);
                    break;
                case JsonValueKind.Object:
                    foreach (var p in e.EnumerateObject()) Walk(p.Value);
                    break;
            }
        }

        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(content.Blocks));
        Walk(doc.RootElement);
        return all;
    }

    [Theory]
    [InlineData("a & b", "a &amp; b")]
    [InlineData("<!channel> <@U1>", "&lt;!channel&gt; &lt;@U1&gt;")]
    [InlineData("<https://x.test|link>", "&lt;https://x.test|link&gt;")]
    [InlineData("", "")]
    [InlineData(null, "")]
    public void Escape_turns_amp_lt_gt_into_entities(string? raw, string expected)
    {
        Assert.Equal(expected, SlackThreadPosts.Escape(raw));
    }

    [Fact]
    public void Request_post_escapes_collected_names_and_links_the_request()
    {
        var facts = new SlackRequestFacts(RequestId, "srv <one> & co", "Blocking", "critical", "alert", Base);

        var post = SlackThreadPosts.RequestPost(facts, McpDataDetail.Full);

        var strings = Strings(post);
        Assert.Contains(strings, s => s.Contains("srv &lt;one&gt; &amp; co", StringComparison.Ordinal));
        Assert.DoesNotContain(strings, s => s.Contains("<one>", StringComparison.Ordinal));
        Assert.Contains(strings, s => s.Contains(Base + "/#/investigations?request=" + RequestId.ToString("D"), StringComparison.Ordinal));
        Assert.Contains("Blocking", post.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Request_post_without_a_base_url_has_no_link()
    {
        var post = SlackThreadPosts.RequestPost(new SlackRequestFacts(RequestId, "srv", "Blocking", "warning", "alert", ""), McpDataDetail.Full);
        Assert.DoesNotContain(Strings(post), s => s.Contains("investigations?request=", StringComparison.Ordinal));
    }

    [Fact]
    public void Report_post_follows_the_data_detail_modes()
    {
        var content = new ReportNoticeContent(RequestId, "needs_person", "srv-one", "Blocking",
            "Session 87 ran WHERE name = 'Contoso Ltd' and blocked", [new ReportNoticeChange("set_config", "threshold 'x'", "done")], Base);

        var full = SlackThreadPosts.ReportPost(content, McpDataDetail.Full);
        var masked = SlackThreadPosts.ReportPost(content, McpDataDetail.MaskLiterals);
        var metadata = SlackThreadPosts.ReportPost(content, McpDataDetail.MetadataOnly);

        Assert.Contains(Strings(full), s => s.Contains("'Contoso Ltd'", StringComparison.Ordinal));
        Assert.DoesNotContain(Strings(masked), s => s.Contains("Contoso", StringComparison.Ordinal));
        /* The prose masker keeps the shape and replaces each literal, numbers included: "Session ? ran WHERE name = '?' and blocked". */
        Assert.Contains(Strings(masked), s => s.Contains("Session ? ran WHERE name = '?' and blocked", StringComparison.Ordinal));
        Assert.DoesNotContain(Strings(metadata), s => s.Contains("Session", StringComparison.Ordinal));
        Assert.DoesNotContain(Strings(metadata), s => s.Contains("Contoso", StringComparison.Ordinal));
        /* The verdict, the server and the link stay at every mode. */
        foreach (var post in new[] { full, masked, metadata })
        {
            Assert.Contains(Strings(post), s => s.Contains("needs_person", StringComparison.Ordinal));
            Assert.Contains(Strings(post), s => s.Contains("srv-one", StringComparison.Ordinal));
            Assert.Contains(Strings(post), s => s.Contains("investigations?request=", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void Change_post_masks_the_object_and_keeps_tool_and_result_at_metadata_only()
    {
        var full = SlackThreadPosts.ChangePost("set_alert_threshold", "cpu 'prod-x' in tenant_a", "done", McpDataDetail.Full);
        var metadata = SlackThreadPosts.ChangePost("set_alert_threshold", "cpu 'prod-x' in tenant_a", "done", McpDataDetail.MetadataOnly);

        Assert.Contains(Strings(full), s => s.Contains("'prod-x'", StringComparison.Ordinal));
        Assert.DoesNotContain(Strings(metadata), s => s.Contains("prod-x", StringComparison.Ordinal));
        Assert.Contains(Strings(metadata), s => s.Contains("set_alert_threshold", StringComparison.Ordinal));
        Assert.Contains(Strings(metadata), s => s.Contains("done", StringComparison.Ordinal));
    }

    [Fact]
    public void Proposal_post_keeps_kind_server_state_and_link_and_withholds_the_statement_at_metadata_only()
    {
        var full = SlackThreadPosts.ProposalPost("kill_session", "srv-one", "pending", "KILL 87 -- 'why'", Base + "/#/admin/proposals?id=1", McpDataDetail.Full);
        var metadata = SlackThreadPosts.ProposalPost("kill_session", "srv-one", "pending", "KILL 87 -- 'why'", Base + "/#/admin/proposals?id=1", McpDataDetail.MetadataOnly);

        Assert.Contains(Strings(full), s => s.Contains("KILL 87", StringComparison.Ordinal));
        Assert.DoesNotContain(Strings(metadata), s => s.Contains("KILL 87", StringComparison.Ordinal));
        foreach (var post in new[] { full, metadata })
        {
            Assert.Contains(Strings(post), s => s.Contains("kill_session", StringComparison.Ordinal));
            Assert.Contains(Strings(post), s => s.Contains("pending", StringComparison.Ordinal));
            Assert.Contains(Strings(post), s => s.Contains("admin/proposals?id=1", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void Claim_and_resolution_posts_carry_their_facts_escaped()
    {
        var claim = SlackThreadPosts.ClaimPost("agent <alpha>", 2);
        var resolved = SlackThreadPosts.ResolutionPost("Blocking & CPU");

        Assert.Contains(Strings(claim), s => s.Contains("agent &lt;alpha&gt;", StringComparison.Ordinal));
        Assert.Contains(Strings(claim), s => s.Contains("2", StringComparison.Ordinal));
        Assert.Contains(Strings(resolved), s => s.Contains("Blocking &amp; CPU", StringComparison.Ordinal));
    }

    [Fact]
    public void Once_keys_are_one_per_request_and_post()
    {
        var other = Guid.NewGuid();
        Assert.Equal("slack_thread:" + RequestId.ToString("D") + ":request", SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.RequestKey));
        Assert.NotEqual(SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.RequestKey), SlackThreadPosts.OnceKey(other, SlackThreadPosts.RequestKey));
        Assert.NotEqual(SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.EventKey("claimed", 1)), SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.EventKey("claimed", 2)));
        Assert.NotEqual(SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.EventKey("action_done", 10)), SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.EventKey("proposal_created", 11)));
    }
}

/// <summary>
/// #7656: the thread poster. A practice request gets no thread and no mark, a route with no bot channel posts nothing, the request
/// post becomes the thread parent and its thread is saved, every later post goes into that thread, a linked request uses its parent's
/// thread, the once mark is written before the send so a replay sends nothing, and a failed send is reported, not retried here.
/// </summary>
public sealed class SlackThreadPosterTests
{
    private static readonly Guid RequestId = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly Guid LinkedId = Guid.Parse("22222222-2222-2222-2222-222222222222");

    private sealed class MemoryThreads : ISlackRequestThreadStore
    {
        public readonly Dictionary<Guid, SlackRequestThread> Threads = new();
        public readonly HashSet<string> Marks = new(StringComparer.Ordinal);
        public readonly List<string> MarkOrder = new();

        /// <summary>Every mark and every send, in the order they happened, when a recorder shares this list.</summary>
        public readonly List<string> Events = new();

        public Task<SlackRequestThread?> FindAsync(Guid threadRequestId, CancellationToken cancellationToken) =>
            Task.FromResult(Threads.TryGetValue(threadRequestId, out var t) ? t : null);

        public Task SaveAsync(SlackRequestThread thread, CancellationToken cancellationToken)
        {
            Threads[thread.RequestId] = thread;
            return Task.CompletedTask;
        }

        public Task<bool> MarkOnceAsync(Guid requestId, int routeId, int serverId, string postKey, string onceKey, CancellationToken cancellationToken)
        {
            MarkOrder.Add(onceKey);
            Events.Add("mark");
            return Task.FromResult(Marks.Add(onceKey));
        }

        public Task ReleaseMarkAsync(string onceKey, CancellationToken cancellationToken)
        {
            Marks.Remove(onceKey);
            Events.Add("release");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingPosts : ISlackBotPosts
    {
        public readonly List<(string Channel, string Text, string? ThreadTs)> Posts = new();
        public Queue<SlackPostResult> Results = new();

        /// <summary>The store whose <see cref="MemoryThreads.Events"/> each send is added to, so a test reads the order of marks and sends.</summary>
        public MemoryThreads? Store { get; init; }

        public Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken)
        {
            Posts.Add((channel, text, threadTs));
            Store?.Events.Add("post");
            return Task.FromResult(Results.Count > 0 ? Results.Dequeue() : new SlackPostResult(true, "1700000000." + Posts.Count.ToString("D6"), channel, null));
        }
    }

    private static SlackThreadTarget Target(string? channel = "C0BOT", bool practice = false, Guid? threadOf = null) =>
        new(RequestId, threadOf ?? RequestId, RouteId: 7, ServerId: 3, BotChannel: channel, Practice: practice);

    private static SlackThreadPostContent Content(string text) => new(text, [new { type = "section", text = new { type = "mrkdwn", text } }]);

    [Fact]
    public async Task A_practice_request_gets_no_thread_and_no_mark()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        var outcome = await poster.PostAsync(Target(practice: true), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.Practice, outcome);
        Assert.Empty(posts.Posts);
        Assert.Empty(threads.Marks);
        Assert.Empty(threads.Threads);
    }

    [Fact]
    public async Task A_route_without_a_bot_channel_posts_nothing()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        Assert.Equal(SlackThreadPostOutcome.NoChannel, await poster.PostAsync(Target(channel: null), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None));
        Assert.Equal(SlackThreadPostOutcome.NoChannel, await poster.PostAsync(Target(channel: "  "), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None));
        Assert.Empty(posts.Posts);
        Assert.Empty(threads.Marks);
    }

    [Fact]
    public async Task The_request_post_opens_the_thread_and_later_posts_reply_in_it()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        var opened = await poster.PostAsync(Target(), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);
        var claimed = await poster.PostAsync(Target(), SlackThreadPosts.EventKey("claimed", 1), Content("claimed"), CancellationToken.None);
        var reported = await poster.PostAsync(Target(), SlackThreadPosts.EventKey("reported", 3), Content("reported"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.Posted, opened);
        Assert.Equal(SlackThreadPostOutcome.Posted, claimed);
        Assert.Equal(SlackThreadPostOutcome.Posted, reported);
        Assert.Equal(3, posts.Posts.Count);
        Assert.Null(posts.Posts[0].ThreadTs);
        Assert.Equal("C0BOT", posts.Posts[0].Channel);
        var thread = Assert.Single(threads.Threads).Value;
        Assert.Equal(RequestId, thread.RequestId);
        Assert.Equal("C0BOT", thread.ChannelId);
        Assert.Equal(posts.Posts[1].ThreadTs, thread.ThreadTs);
        Assert.Equal(posts.Posts[2].ThreadTs, thread.ThreadTs);
    }

    [Fact]
    public async Task The_once_mark_is_written_before_the_send_and_a_replay_sends_nothing()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts { Store = threads };
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        var first = await poster.PostAsync(Target(), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);
        var again = await poster.PostAsync(Target(), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.Posted, first);
        Assert.Equal(SlackThreadPostOutcome.AlreadyPosted, again);
        Assert.Single(posts.Posts);
        Assert.Equal([SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.RequestKey)], threads.MarkOrder.Distinct().ToArray());
        /* The mark came first, then the one send; the replay took the mark and sent nothing. */
        Assert.Equal(["mark", "post", "mark"], threads.Events);
    }

    [Fact]
    public async Task A_linked_request_posts_into_its_parents_thread()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);
        await poster.PostAsync(Target(), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);
        var parentTs = threads.Threads[RequestId].ThreadTs;

        var linked = new SlackThreadTarget(LinkedId, RequestId, 7, 3, "C0BOT", Practice: false);
        var outcome = await poster.PostAsync(linked, SlackThreadPosts.RequestKey, Content("linked opened"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.Posted, outcome);
        Assert.Equal(parentTs, posts.Posts[^1].ThreadTs);
        Assert.Single(threads.Threads);
        Assert.Contains(SlackThreadPosts.OnceKey(LinkedId, SlackThreadPosts.RequestKey), threads.Marks);
    }

    [Fact]
    public async Task A_later_post_with_no_thread_posts_nothing()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        var outcome = await poster.PostAsync(Target(), SlackThreadPosts.EventKey("reported", 3), Content("reported"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.NoThread, outcome);
        Assert.Empty(posts.Posts);
        Assert.Empty(threads.Marks);
    }

    [Fact]
    public async Task A_failed_parent_post_saves_no_thread_and_a_failed_reply_is_reported()
    {
        var threads = new MemoryThreads();
        var posts = new RecordingPosts();
        posts.Results.Enqueue(new SlackPostResult(false, null, null, "channel_not_found"));
        var poster = new SlackThreadPoster(threads, posts, NullLogger<SlackThreadPoster>.Instance);

        var failed = await poster.PostAsync(Target(), SlackThreadPosts.RequestKey, Content("opened"), CancellationToken.None);

        Assert.Equal(SlackThreadPostOutcome.Failed, failed);
        Assert.Empty(threads.Threads);
        /* The mark stays: a post is announced at most once, delivered or not, as the report notice is. */
        Assert.Contains(SlackThreadPosts.OnceKey(RequestId, SlackThreadPosts.RequestKey), threads.Marks);
    }
}
