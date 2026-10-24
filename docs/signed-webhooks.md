# Signed webhooks (Darling)

Audience: operators.

Darling can sign each post that it sends to a generic webhook. A receiver checks the signature to know that the post came from Darling and that nobody changed it on the way. The signature follows the [Standard Webhooks](https://www.standardwebhooks.com/) format, so most languages already have a library that checks it.

Signing applies to the generic webhook only. Teams, Slack and PagerDuty posts are not signed, because those services have their own authentication.

## When a post is signed

- A generic webhook URL is signed when it has a signing key. A URL with no key is sent unsigned, as before.
- The generic URL in Settings has its own key. Each notification route that sets its own generic URL has its own key. A route URL is never signed with the key from Settings.
- A route that does not set a generic URL uses the URL and the key from Settings.

## Get a key

Darling makes the key. It shows the key one time, when it makes it. Copy the key into your receiver at that time. Darling cannot show the key again.

Make a key in the Darling Viewer. The Viewer needs a connection that can write settings. A read-only connection cannot make or remove a key.

To make a key for the generic webhook in Settings:

1. Open **Settings** and go to the generic webhook section.
2. Type the URL and save the settings. A key is tied to the saved URL, so the URL must be saved first.
3. Select **Generate key**.
4. Copy the key from the box that appears. The box says that the key will not be shown again.
5. Give the key to your receiver.

To make a key for a route that has its own generic URL:

1. Open **Notification Routes**. If the route is new, save it first. Then edit the route.
2. In the route window, select **Generate key**. The key is made at once. You do not need to select **OK**.
3. Copy the key from the box that appears and give it to your receiver.

After the key is saved, the block reads "Saved". The Viewer never shows the key again, and it does not copy the key for you.

To rotate a key, select **Rotate key** and confirm. The Viewer shows the new key one time. The old key keeps signing next to the new one for 7 days, and the block shows the date that the old key stops. Each post carries both signatures during that time, so your receiver can switch to the new key without a gap.

To remove a key, select **Remove key** and confirm. This removes the current key and the old key. Posts to that URL are then sent without a signature.

A test sent with **Send Test** from the Viewer is not signed, because the service signs its posts and the Viewer cannot open the key. A receiver that checks signatures will refuse a test from the Viewer.

The web dashboard shows the state of each key on the **Notification Routes** page. It shows whether a key is set and whether a rotation is in its window. It shows when the window ends and when the last rotation began. The web dashboard cannot make, rotate or remove a key, and it never shows a key. It does not show when the first key was made, because the store does not keep that time.

Darling keeps the key sealed in its store, in the same way as the generic webhook headers. The key never appears in the logs, in the health text, in MCP output or in a diagnostics bundle.

A key is made for one generic URL and one proxy. If you change the URL or the proxy, the old key stops working. Remove the key, save the new URL or proxy, and then make a new key.

## What a signed post carries

Each signed post has three headers:

| Header | What it holds |
| --- | --- |
| `webhook-id` | A unique id for this post, such as `msg_3f2a...`. |
| `webhook-timestamp` | The time Darling sent the post, in whole seconds since 1970-01-01 UTC. |
| `webhook-signature` | One or more signatures, separated by a space. Each one is `v1,` followed by a base64 value. |

The key text starts with `whsec_`. The rest of the key is base64. To get the key bytes, remove `whsec_` and decode the base64.

Each signature is an HMAC-SHA256 hash. The hash input is the id, a period, the timestamp, a period, and the raw body bytes:

```text
<webhook-id>.<webhook-timestamp>.<raw body>
```

Use the body bytes exactly as they arrived. If you parse the JSON and then write it again, the bytes change and the check fails.

## Investigation request events

An agent route (a notification route set to investigate) gets one signed post for each investigation request event, sent to the route's own generic webhook URL with that route's key. The events are:

- `opened`: a request opened.
- `joined`: a later alert joined the request with a metric it did not have yet, or a higher severity. A join that brings neither is not posted.
- `resolved`: every metric linked to the request has cleared since its last alert.
- `expired`: the request reached its expiry.
- `claims_exhausted`: the request ran out of claims before its expiry.
- `evidence_ready`: the request's evidence is collected.

`webhook-id` is the event's own id, the same value as `event_id` in the body. The body is JSON:

```json
{
  "type": "investigation.resolved",
  "event_id": "8a1f0c2e-5b7d-4e2a-9c61-0f3d2b7a4e10",
  "event": "resolved",
  "occurred_at": "2026-10-17T14:05:00.0000000Z",
  "request_id": "3c9e1b52-7f04-4d8b-a2e6-51b0c8d9f7a3",
  "route_id": 4,
  "server_id": 7,
  "server_name": "sql-prod-01",
  "family": "resources",
  "reason": null,
  "detail": null
}
```

`reason` names the metric for `opened` and `joined`, and is null for every other event. `detail` carries only these members, and is null for every other event:

- `joined`: `new_metric` (true or false) and `severity` (`warning` or `critical`).
- `expired` and `claims_exhausted`: `unclaimed`, true when no agent was working on the request.
- `evidence_ready`: `sections` (ready), `not_collected` and `pending`, each a list of section names.
- Any event of a practice request (a replay of a judged case that the Viewer opens) also carries `practice_of`, the request id of the case it replays, so you can tell a replay from a live request. A live request never has it.

Each event is posted at most once. Darling writes the event's delivery mark before it sends, so a post that fails is logged and not sent again. A route with no generic URL gets no post. A post that answers with a redirect is not followed. Your receiver should still keep each `webhook-id` it accepted, as described above.

When a request expires and no agent was working on it, Darling tells a person instead, once. It tries these in order and uses the first that applies:

1. The agent route's fallback route, when it is enabled and has email, Teams or Slack set.
2. The agent route's own email, Teams and Slack.
3. The service's own alert, "Investigation Not Picked Up".

## Key rotation

When you make a new key for a URL that already has one, Darling keeps the old key for 7 days. During those 7 days, each post carries two signatures: one from the new key and one from the old key. A receiver that still has the old key continues to accept posts. Put the new key into your receiver during those 7 days. After 7 days, Darling stops using the old key.

If Darling cannot read the old key during those 7 days, it drops it. It then signs with the new key only and says so in the service log.

## What the receiver must check

Darling only signs. The receiver must do these checks:

1. Find a signature in `webhook-signature` that matches the hash you compute with your key. If none matches, reject the post.
2. If `webhook-timestamp` is more than 5 minutes from your own clock, reject the post. This stops an old post from being sent again later.
3. Keep each `webhook-id` that you accepted until the post's own `webhook-timestamp` is more than 5 minutes from your clock. Reject a post whose id you already accepted. Count the time from the timestamp in the post, not from when it arrived. A sender whose clock runs ahead of yours sends timestamps that stay valid longer. Keep the id just as long. Darling uses a new id for each post.

Compare signatures with a constant-time comparison, such as `CryptographicOperations.FixedTimeEquals` in .NET or `hmac.compare_digest` in Python.

## Starter sample (C#)

This sample does the three checks. It keeps the seen ids in memory, so a receiver with more than one instance needs a shared store for them.

```csharp
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

public sealed class DarlingWebhookReceiver
{
    private static readonly TimeSpan Tolerance = TimeSpan.FromMinutes(5);
    private readonly byte[] _key;
    private readonly object _gate = new();

    // Each accepted id, with the timestamp the post carried. An id stays until that timestamp
    // is outside the window, because until then the post could still be sent again and pass.
    private readonly Dictionary<string, DateTime> _seen = new(StringComparer.Ordinal);

    public DarlingWebhookReceiver(string secret)
    {
        var text = secret.StartsWith("whsec_", StringComparison.Ordinal) ? secret[6..] : secret;
        _key = Convert.FromBase64String(text);
    }

    /// <summary>True when the post is from Darling, is fresh, and was not seen before.</summary>
    public bool Verify(string id, string timestamp, string signatureHeader, byte[] body, DateTime nowUtc)
    {
        if (string.IsNullOrEmpty(id) || signatureHeader is null) return false;

        if (!long.TryParse(timestamp, NumberStyles.None, CultureInfo.InvariantCulture, out var seconds)
            || seconds > 253402300799)
        {
            return false;
        }

        var sent = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
        if ((nowUtc - sent).Duration() > Tolerance)
        {
            return false;
        }

        var input = Encoding.UTF8.GetBytes(id + "." + timestamp + ".").Concat(body).ToArray();
        var expected = HMACSHA256.HashData(_key, input);
        var match = false;
        foreach (var part in signatureHeader.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var signature = new byte[64];
            if (part.StartsWith("v1,", StringComparison.Ordinal)
                && Convert.TryFromBase64String(part[3..], signature, out var written)
                && CryptographicOperations.FixedTimeEquals(signature.AsSpan(0, written), expected))
            {
                match = true;
            }
        }

        if (!match)
        {
            return false;
        }

        lock (_gate)
        {
            foreach (var old in _seen.Where(kv => nowUtc - kv.Value > Tolerance).Select(kv => kv.Key).ToList())
            {
                _seen.Remove(old);
            }

            return _seen.TryAdd(id, sent);
        }
    }
}
```

Call `Verify` with the three header values, the raw request body and the current UTC time. Return HTTP 2xx only when it returns true.
