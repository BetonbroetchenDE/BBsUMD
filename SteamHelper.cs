using Microsoft.Win32;
using System.IO;
using System.Text.RegularExpressions;

public static class SteamHelper
{
    public static string? GetSteamPath()
    {
        return Registry.GetValue(
            @"HKEY_LOCAL_MACHINE\SOFTWARE\WOW6432Node\Valve\Steam",
            "InstallPath",
            null
        ) as string
        ?? Registry.GetValue(
            @"HKEY_LOCAL_MACHINE\SOFTWARE\Valve\Steam",
            "InstallPath",
            null
        ) as string;
    }

    private static List<string> GetLibraryPaths()
    {
        var result = new List<string>();
        var steamPath = GetSteamPath();

        if (string.IsNullOrWhiteSpace(steamPath) || !Directory.Exists(steamPath))
            return result;

        result.Add(steamPath);

        var vdfPath = Path.Combine(steamPath, "steamapps", "libraryfolders.vdf");
        if (!File.Exists(vdfPath))
            return result;

        var text = File.ReadAllText(vdfPath);

        foreach (Match match in Regex.Matches(text, @"""path""\s+""([^""]+)"""))
        {
            var path = match.Groups[1].Value.Replace(@"\\", @"\");

            if (Directory.Exists(path) && !result.Contains(path, StringComparer.OrdinalIgnoreCase))
                result.Add(path);
        }

        return result;
    }

    public static bool IsGameInstalled(string gameName)
    {
        return GetGameFolder(gameName) != null;
    }

    public static string? GetGameFolder(string gameName)
    {
        foreach (var library in GetLibraryPaths())
        {
            var steamApps = Path.Combine(library, "steamapps");
            if (!Directory.Exists(steamApps))
                continue;

            foreach (var manifest in Directory.GetFiles(steamApps, "appmanifest_*.acf"))
            {
                var text = File.ReadAllText(manifest);

                var nameMatch = Regex.Match(text, @"""name""\s+""([^""]+)""");
                if (!nameMatch.Success)
                    continue;

                if (!string.Equals(nameMatch.Groups[1].Value, gameName, StringComparison.OrdinalIgnoreCase))
                    continue;

                var dirMatch = Regex.Match(text, @"""installdir""\s+""([^""]+)""");
                if (!dirMatch.Success)
                    return null;

                return Path.Combine(library, "steamapps", "common", dirMatch.Groups[1].Value);
            }
        }

        return null;
    }
}