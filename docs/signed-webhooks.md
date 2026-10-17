# Signed webhooks (Darling)

Darling can sign each post that it sends to a generic webhook. A receiver checks the signature to know that the post came from Darling and that nobody changed it on the way. The signature follows the [Standard Webhooks](https://www.standardwebhooks.com/) format, so most languages already have a library that checks it.

Signing applies to the generic webhook only. Teams, Slack and PagerDuty posts are not signed, because those services have their own authentication.

## When a post is signed

- A generic webhook URL is signed when it has a signing key. A URL with no key is sent unsigned, as before.
- The generic URL in Settings has its own key. Each notification route that sets its own generic URL has its own key. A route URL is never signed with the key from Settings.
- A route that does not set a generic URL uses the URL and the key from Settings.

## Get a key

Darling makes the key. It shows the key one time, when it makes it. Copy the key into your receiver at that time. Darling cannot show the key again.

[TODO: the viewer's key screen, which generates, rotates and removes a key, is not in this build yet.]

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

## Key rotation

When you make a new key for a URL that already has one, Darling keeps the old key for 7 days. During those 7 days, each post carries two signatures: one from the new key and one from the old key. A receiver that still has the old key continues to accept posts. Put the new key into your receiver during those 7 days. After 7 days, Darling stops using the old key.

## What the receiver must check

Darling only signs. The receiver must do these checks:

1. Find a signature in `webhook-signature` that matches the hash you compute with your key. If none matches, reject the post.
2. If `webhook-timestamp` is more than 5 minutes from your own clock, reject the post. This stops an old post from being sent again later.
3. Keep the `webhook-id` values that you accepted in the last 5 minutes. Reject a post whose id you already accepted. Darling uses a new id for each post.

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
    private readonly Dictionary<string, DateTime> _seen = new(StringComparer.Ordinal);

    public DarlingWebhookReceiver(string secret)
    {
        var text = secret.StartsWith("whsec_", StringComparison.Ordinal) ? secret[6..] : secret;
        _key = Convert.FromBase64String(text);
    }

    /// <summary>True when the post is from Darling, is fresh, and was not seen before.</summary>
    public bool Verify(string id, string timestamp, string signatureHeader, byte[] body, DateTime nowUtc)
    {
        var sent = DateTimeOffset.FromUnixTimeSeconds(long.Parse(timestamp, CultureInfo.InvariantCulture)).UtcDateTime;
        if ((nowUtc - sent).Duration() > Tolerance)
        {
            return false;
        }

        var input = Encoding.UTF8.GetBytes(id + "." + timestamp + ".").Concat(body).ToArray();
        var expected = HMACSHA256.HashData(_key, input);
        var match = signatureHeader.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Where(s => s.StartsWith("v1,", StringComparison.Ordinal))
            .Any(s => CryptographicOperations.FixedTimeEquals(Convert.FromBase64String(s[3..]), expected));
        if (!match)
        {
            return false;
        }

        foreach (var old in _seen.Where(kv => nowUtc - kv.Value > Tolerance).Select(kv => kv.Key).ToList())
        {
            _seen.Remove(old);
        }

        return _seen.TryAdd(id, nowUtc);
    }
}
```

Call `Verify` with the three header values, the raw request body and the current UTC time. Return HTTP 2xx only when it returns true.
