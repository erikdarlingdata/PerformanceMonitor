using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The floor an MCP bearer token clears before it serves the write and investigation tools, the one rule for both apps:
/// at least <see cref="MinimumLength"/> characters and at least <see cref="MinimumDistinctCharacters"/> different ones.
/// Lite's <c>McpTokenStore.IsUsableToken</c> and Darling's <c>DarlingHostBinding.DescribeTokenProblem</c> both decide
/// through <see cref="Clears"/>, and their constants are these, so the two cannot drift apart. A token under the floor
/// still gates every request where one is configured; only the write and investigation tools stay off behind it.
/// </summary>
public static class McpTokenFloor
{
    /// <summary>The fewest characters a usable token has. A generated token (Lite's 64, Darling's 32) is far above it.</summary>
    public const int MinimumLength = 16;

    /// <summary>The fewest different characters a usable token has. Length alone passes a token such as sixteen copies of
    /// one letter, which a handful of guesses finds; this keeps the floor from being met by padding.</summary>
    public const int MinimumDistinctCharacters = 6;

    /// <summary>True when <paramref name="token"/> clears the floor: not empty or whitespace, at least
    /// <see cref="MinimumLength"/> characters, and at least <see cref="MinimumDistinctCharacters"/> different characters.</summary>
    public static bool Clears(string? token) =>
        !string.IsNullOrWhiteSpace(token)
        && token.Length >= MinimumLength
        && token.Distinct().Count() >= MinimumDistinctCharacters;
}
