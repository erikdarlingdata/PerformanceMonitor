using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8115: the body of a tray toast names its server once. The alert engine writes every resolution message with the server
/// in front ("SERVER-A: Client timeouts ... are back below threshold"), because that text is also the email and chat body;
/// a firing alert's short message carries no name. The toast adds the name only when the body does not already open with it.
/// </summary>
public static class ViewerToastText
{
    public static string WithServer(string serverName, string storedServerName, string body)
    {
        body ??= "";
        if (OpensWith(body, serverName) || OpensWith(body, storedServerName))
        {
            return body;
        }

        return $"{serverName}: {body}";
    }

    private static bool OpensWith(string body, string name) =>
        !string.IsNullOrEmpty(name) && body.StartsWith(name + ": ", StringComparison.OrdinalIgnoreCase);
}
