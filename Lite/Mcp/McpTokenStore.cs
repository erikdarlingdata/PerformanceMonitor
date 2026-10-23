using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>Where <see cref="McpTokenStore"/> keeps the MCP token: Windows Credential Manager in the app, a fake in a test.</summary>
internal interface IMcpTokenVault
{
    /// <summary>Reads the stored token.</summary>
    CredentialReadOutcome Read(out string? token, out string? failure);

    /// <summary>Writes the token; returns why it failed, or null on success.</summary>
    string? Write(string token);
}

/// <summary>The real vault: the <c>PerformanceMonitorLite.Mcp_McpToken</c> entry of Windows Credential Manager, through <see cref="CredentialService"/>.
/// Credential Manager entries belong to the signed-in Windows user. The entry has a prefix of its own, never the
/// <c>PerformanceMonitorLite_</c> one that a server's saved password uses, so no server Id can name it.</summary>
internal sealed class CredentialManagerMcpTokenVault : IMcpTokenVault
{
    /// <summary>The key part of the target name; the store adds <see cref="CredentialPrefix"/>.</summary>
    internal const string EntryName = "McpToken";

    /// <summary>The prefix of the token's own Credential Manager entries. A server's saved password is <c>PerformanceMonitorLite_</c>
    /// plus its Id; the 23rd character here is a dot, never an underscore, so no Id can produce this target.</summary>
    internal const string CredentialPrefix = "PerformanceMonitorLite.Mcp_";

    /// <summary>The whole target name in Credential Manager (for messages and cleanup).</summary>
    internal const string TargetName = CredentialPrefix + EntryName;

    private const string UserName = "McpToken";

    private readonly CredentialService _credentials;

    internal CredentialManagerMcpTokenVault(CredentialService credentials) => _credentials = credentials;

    public CredentialReadOutcome Read(out string? token, out string? failure)
    {
        var outcome = _credentials.TryReadCredential(EntryName, out var credential, out failure);
        token = credential?.Password;
        return outcome;
    }

    public string? Write(string token) =>
        _credentials.TrySaveCredential(EntryName, UserName, token) is { Succeeded: false } result ? result.Failure : null;
}

/// <summary>
/// Lite's MCP token. Once the MCP server is on, every request carries <c>Authorization: Bearer &lt;token&gt;</c>, like Darling's.
/// The token is made the first time the server starts with none (this includes the first start after an upgrade with MCP already
/// on) and is kept in Windows Credential Manager for the signed-in user, never in a Lite file (the MCP client keeps its own
/// copy in its config). An entry that cannot be read is never
/// replaced: the start is refused with the reason, because a new token would silently break every client that holds the old one.
/// </summary>
internal sealed class McpTokenStore
{
    /// <summary>Random bytes in a token. The token is their lowercase hex text, 64 characters (Darling's minimum is 16).</summary>
    internal const int TokenBytes = 32;

    /// <summary>A token counts only at this length or longer (Darling's floor). A shorter one still gates every request, but
    /// the write and investigation tools stay off (<see cref="McpHostService.DecideServing"/>).</summary>
    internal const int MinimumTokenLength = 16;

    /// <summary>The fewest different characters a usable token has (Darling's <c>DarlingHostBinding.MinimumDistinctTokenCharacters</c>).
    /// Sixteen copies of one letter meet the length alone, so a token with fewer than this many different characters is not used
    /// for the write or investigation tools.</summary>
    internal const int MinimumDistinctTokenCharacters = 6;

    /// <summary>
    /// True when <paramref name="token"/> is usable for the write and investigation tools: at least <see cref="MinimumTokenLength"/>
    /// characters and at least <see cref="MinimumDistinctTokenCharacters"/> different characters. The same two conditions as Darling's
    /// <c>DarlingHostBinding.DescribeTokenProblem(token) is null</c>, which <c>DarlingMcpWritePolicy.ResolveForStart</c> applies
    /// (Lite does not reference the Darling service project, so the rule is mirrored here).
    /// </summary>
    internal static bool IsUsableToken(string? token) =>
        !string.IsNullOrWhiteSpace(token)
        && token.Length >= MinimumTokenLength
        && token.Distinct().Count() >= MinimumDistinctTokenCharacters;

    /// <summary>The requirement in words, for the startup lines.</summary>
    internal static readonly string TokenRequirement =
        $"a token of at least {MinimumTokenLength} characters with at least {MinimumDistinctTokenCharacters} different characters";

    private readonly IMcpTokenVault _vault;

    internal McpTokenStore(IMcpTokenVault vault) => _vault = vault ?? throw new ArgumentNullException(nameof(vault));

    /// <summary>The app's store: Credential Manager, under <see cref="CredentialManagerMcpTokenVault.CredentialPrefix"/>, apart from
    /// the entries of Lite's saved server passwords.</summary>
    internal static McpTokenStore ForApp(Microsoft.Extensions.Logging.ILogger? logger = null) =>
        new(new CredentialManagerMcpTokenVault(new CredentialService(
            new CredentialStore(CredentialManagerMcpTokenVault.CredentialPrefix, "Performance Monitor Lite MCP token", logger))));

    /// <summary>The store over a Credential Manager prefix of a test's own.</summary>
    internal static McpTokenStore ForPrefix(string credentialPrefix) =>
        new(new CredentialManagerMcpTokenVault(new CredentialService(new CredentialStore(credentialPrefix, "Performance Monitor Lite MCP token test"))));

    /// <summary>Makes a new token and stores it over the old one, which stops working once the host is started again or reloads its
    /// token. Null with <paramref name="failure"/> set when it cannot be saved (the old entry is then unchanged).</summary>
    internal string? Replace(out string? failure)
    {
        var token = Generate();
        var writeFailure = _vault.Write(token);
        if (writeFailure is not null)
        {
            failure = "the new token could not be saved: " + writeFailure;
            return null;
        }

        failure = null;
        return token;
    }

    /// <summary>A new token: 32 random bytes as 64 lowercase hex characters.</summary>
    internal static string Generate() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(TokenBytes));

    /// <summary>The stored token, or null when there is none (<paramref name="failure"/> is null) or it cannot be read
    /// (<paramref name="failure"/> says why). Never creates one.</summary>
    internal string? Read(out string? failure)
    {
        failure = null;
        var outcome = _vault.Read(out var token, out var readFailure);
        switch (outcome)
        {
            case CredentialReadOutcome.Found:
                if (string.IsNullOrWhiteSpace(token))
                {
                    failure = "the stored entry is empty; it is left as it is";
                    return null;
                }

                return token;
            case CredentialReadOutcome.NotFound:
                return null;
            default:
                failure = "the entry could not be read: " + (readFailure ?? "no reason was given");
                return null;
        }
    }

    /// <summary>The stored token, made and saved first when there is none. Null with <paramref name="failure"/> set when it
    /// cannot be read or saved.</summary>
    internal string? GetOrCreate(out string? failure, out bool created)
    {
        created = false;
        var existing = Read(out failure);
        if (existing is not null || failure is not null)
        {
            return existing;
        }

        var token = Generate();
        var writeFailure = _vault.Write(token);
        if (writeFailure is not null)
        {
            failure = "the token could not be saved: " + writeFailure;
            return null;
        }

        created = true;
        return token;
    }

    /// <summary>The settings window's and the host's shared rule: with <see cref="McpSettings.AllowTokenlessLoopback"/> on there is
    /// no token and nothing is made; otherwise the token is read or made. <paramref name="failure"/> non-null means the
    /// server must not start.</summary>
    internal string? Resolve(McpSettings settings, out string? failure, out bool created)
    {
        failure = null;
        created = false;
        return settings.AllowTokenlessLoopback ? null : GetOrCreate(out failure, out created);
    }

    /// <summary>The client setup command with the token header, the same shape as the Darling Viewer's button.</summary>
    internal static string ClientCommand(int port, string token) =>
        $"claude mcp add --transport http --scope user sql-monitor http://localhost:{port}/ --header \"Authorization: Bearer {token}\"";

    /// <summary>Constant-time check of an <c>Authorization</c> header against the token: true only for <c>Bearer &lt;token&gt;</c>
    /// (scheme in any case) with the right token. An empty expected token never authorizes. Compares SHA-256 digests, so it
    /// leaks neither the token nor its length.</summary>
    internal static bool IsAuthorized(string? authorizationHeader, string expectedToken)
    {
        if (string.IsNullOrEmpty(expectedToken) || string.IsNullOrWhiteSpace(authorizationHeader))
        {
            return false;
        }

        const string prefix = "Bearer ";
        var value = authorizationHeader.Trim();
        if (!value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var presented = value[prefix.Length..].Trim();
        if (presented.Length == 0)
        {
            return false;
        }

        var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expectedToken));
        var presentedHash = SHA256.HashData(Encoding.UTF8.GetBytes(presented));
        return CryptographicOperations.FixedTimeEquals(expectedHash, presentedHash);
    }
}
