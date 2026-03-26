using NAudio.Wave;
using Newtonsoft.Json;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Media;
using System.Net.Http;
using System.Reflection;
using System.Security.Cryptography;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;

namespace BBs_Universal_Mod_Downloader
{
    public partial class MainWindow : Window
    {
        // =========================================================
        // Static clients
        // =========================================================

        private static readonly HttpClient apiClient = CreateApiHttpClient();
        private static readonly HttpClient downloadClient = CreateDownloadHttpClient();

        // =========================================================
        // Fields
        // =========================================================

        private readonly SoundPlayer _clickPlayer = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private readonly SemaphoreSlim _modManagementLock = new(1, 1);

        private Task? _retryLoopTask;

        private WaveOutEvent? _nyanOutput;
        private WaveChannel32? _nyanVolume;
        private WaveFileReader? _nyanReader;

        private string _selectedGameName = string.Empty;
        private bool _selectedGameInstalled = false;
        private string? _selectedGameModFolder;

        private Func<Task>? _popupPrimaryActionAsync;
        private Func<Task>? _popupSecondaryActionAsync;
        private bool _popupAllowsBackdropClose = true;

        private readonly ObservableCollection<ServerModRow> _serverMods = [];
        private readonly ObservableCollection<LocalModRow> _localMods = [];

        // =========================================================
        // Enums
        // =========================================================

        private enum ViewState
        {
            Loading,
            Error,
            Main,
            ModManager
        }

        private ViewState CurrentView = ViewState.Loading;

        // =========================================================
        // DTOs / View Models
        // =========================================================

        public class PingResponse
        {
            public bool Ok { get; set; }
            public string? Message { get; set; }
        }

        public class UpdateResponse
        {
            public bool Ok { get; set; }
            public string? Version { get; set; }
            public string? Url { get; set; }
            public string? Sha256 { get; set; }
            public string? Notes { get; set; }
            public string? Error { get; set; }
            public string? Details { get; set; }
        }

        private sealed class UpdateCheckResult
        {
            public bool Success { get; init; }
            public bool IsUpdateAvailable { get; init; }
            public UpdateResponse? Update { get; init; }
            public string UserMessage { get; init; } = string.Empty;
            public string TechnicalDetails { get; init; } = string.Empty;
        }

        public class ModFileHashEntry
        {
            public string FileName { get; set; } = string.Empty;
            public string Sha256 { get; set; } = string.Empty;
            public string? Url { get; set; }
        }

        public class ModHashResponse
        {
            public bool Ok { get; set; }
            public List<ModFileHashEntry> Files { get; set; } = [];
        }

        public class Game
        {
            public string Name { get; set; } = string.Empty;
        }

        public class GameResponse
        {
            public bool Ok { get; set; }
            public List<Game> Games { get; set; } = [];
        }

        private sealed class DownloadProgressInfo
        {
            public long BytesDownloaded { get; init; }
            public long? TotalBytes { get; init; }
            public TimeSpan Elapsed { get; init; }
            public double BytesPerSecond { get; init; }
            public string Status { get; init; } = "Download läuft...";
        }

        private sealed class FileDownloadResult
        {
            public long BytesDownloaded { get; init; }
            public long? TotalBytes { get; init; }
            public TimeSpan Elapsed { get; init; }
        }

        public sealed class ServerModRow
        {
            public string FileName { get; init; } = string.Empty;
            public string StatusText { get; init; } = string.Empty;
            public ModFileHashEntry ServerFile { get; init; } = new();
        }

        public sealed class LocalModRow
        {
            public string FileName { get; init; } = string.Empty;
            public string FullPath { get; init; } = string.Empty;
            public string StatusText { get; init; } = string.Empty;
            public bool CanUpdate { get; init; }
            public ModFileHashEntry? ServerFile { get; init; }
        }

        public class GitHubReleaseResponse
        {
            public string? Tag_Name { get; set; }
            public string? Body { get; set; }
            public List<GitHubAsset> Assets { get; set; } = [];
        }

        public class GitHubAsset
        {
            public string? Name { get; set; }
            public string? Browser_Download_Url { get; set; }
        }

        // =========================================================
        // Constructor / lifecycle
        // =========================================================

        public MainWindow()
        {
            InitializeComponent();

            ServerModsItemsControl.ItemsSource = _serverMods;
            LocalModsItemsControl.ItemsSource = _localMods;
            BuildInfoText.Text = $"BBs UMD • v{GetCurrentAppVersion()}";

            ShowLoading();
            _ = InitializeAsync();
        }

        protected override void OnClosed(EventArgs e)
        {
            _cts.Cancel();

            try
            {
                _clickPlayer.Stop();

                if (_clickPlayer.Stream is IDisposable disposableClickStream)
                    disposableClickStream.Dispose();

                _nyanOutput?.Stop();
                _nyanOutput?.Dispose();
                _nyanReader?.Dispose();
                _nyanVolume?.Dispose();
            }
            catch
            {
            }

            _connectionLock.Dispose();
            _modManagementLock.Dispose();
            _cts.Dispose();

            base.OnClosed(e);
        }

        // =========================================================
        // Http / general helpers
        // =========================================================

        private static HttpClient CreateApiHttpClient()
        {
            var client = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(20)
            };

            client.DefaultRequestHeaders.UserAgent.ParseAdd("BBsUniversalModDownloader/1.0");
            return client;
        }

        private static HttpClient CreateDownloadHttpClient()
        {
            var client = new HttpClient
            {
                Timeout = Timeout.InfiniteTimeSpan
            };

            client.DefaultRequestHeaders.UserAgent.ParseAdd("BBsUniversalModDownloader/1.0");
            return client;
        }

        private static async Task<string?> GetApiStringAsync(string url, CancellationToken cancellationToken = default)
        {
            using var response = await apiClient.GetAsync(url, cancellationToken);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }

        private static string ComputeSha256(string filePath)
        {
            using var stream = File.OpenRead(filePath);
            using var sha256 = SHA256.Create();
            byte[] hash = sha256.ComputeHash(stream);
            return Convert.ToHexString(hash).ToLowerInvariant();
        }

        private static Task<string> ComputeSha256Async(string filePath, CancellationToken cancellationToken = default)
        {
            return Task.Run(() => ComputeSha256(filePath), cancellationToken);
        }

        private static Version GetCurrentAppVersion()
        {
            Assembly assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
            string? versionText = assembly.GetName().Version?.ToString();

            if (Version.TryParse(versionText, out var version))
                return version;

            return new Version(0, 0, 0, 0);
        }

        private static string FormatBytes(long bytes)
        {
            string[] units = ["B", "KB", "MB", "GB", "TB"];
            double value = bytes;
            int unitIndex = 0;

            while (value >= 1024 && unitIndex < units.Length - 1)
            {
                value /= 1024;
                unitIndex++;
            }

            return $"{value:0.##} {units[unitIndex]}";
        }

        private static string FormatDuration(TimeSpan time)
        {
            if (time.TotalHours >= 1)
                return time.ToString(@"hh\:mm\:ss");

            return time.ToString(@"mm\:ss");
        }

        private static List<string> GetLocalModFiles(string modsFolder)
        {
            if (!Directory.Exists(modsFolder))
                return [];

            return Directory
                .GetFiles(modsFolder, "pakchunk*.pak", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .Where(fileName =>
                    !string.IsNullOrWhiteSpace(fileName) &&
                    !fileName.EndsWith("-Windows.pak", StringComparison.OrdinalIgnoreCase))
                .OrderBy(fileName => fileName, StringComparer.OrdinalIgnoreCase)
                .Cast<string>()
                .ToList();
        }

        private static string BuildSingleDownloadMessage(string fileName, DownloadProgressInfo info)
        {
            string progressText = info.TotalBytes.HasValue && info.TotalBytes.Value > 0
                ? $"{FormatBytes(info.BytesDownloaded)} / {FormatBytes(info.TotalBytes.Value)} ({(double)info.BytesDownloaded / info.TotalBytes.Value:P1})"
                : $"{FormatBytes(info.BytesDownloaded)} / Gesamtgröße unbekannt";

            string speedText = info.BytesPerSecond > 0
                ? $"{FormatBytes((long)info.BytesPerSecond)}/s"
                : "-";

            return
                $"{info.Status}{Environment.NewLine}{Environment.NewLine}" +
                $"Datei: {fileName}{Environment.NewLine}" +
                $"Fortschritt: {progressText}{Environment.NewLine}" +
                $"Geschwindigkeit: {speedText}{Environment.NewLine}" +
                $"Dauer: {FormatDuration(info.Elapsed)}";
        }

        private static string BuildModDownloadMessage(int index, int total, string fileName, DownloadProgressInfo info)
        {
            string progressText = info.TotalBytes.HasValue && info.TotalBytes.Value > 0
                ? $"{FormatBytes(info.BytesDownloaded)} / {FormatBytes(info.TotalBytes.Value)} ({(double)info.BytesDownloaded / info.TotalBytes.Value:P1})"
                : $"{FormatBytes(info.BytesDownloaded)} / Gesamtgröße unbekannt";

            string speedText = info.BytesPerSecond > 0
                ? $"{FormatBytes((long)info.BytesPerSecond)}/s"
                : "-";

            return
                $"{info.Status}{Environment.NewLine}{Environment.NewLine}" +
                $"Datei {index} von {total}: {fileName}{Environment.NewLine}" +
                $"Fortschritt: {progressText}{Environment.NewLine}" +
                $"Geschwindigkeit: {speedText}{Environment.NewLine}" +
                $"Dauer: {FormatDuration(info.Elapsed)}";
        }

        private static async Task<FileDownloadResult> DownloadFileWithProgressAsync(
            string url,
            string targetPath,
            IProgress<DownloadProgressInfo>? progress,
            CancellationToken cancellationToken)
        {
            using var response = await downloadClient.GetAsync(
                url,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken
            );

            response.EnsureSuccessStatusCode();

            long? totalBytes = response.Content.Headers.ContentLength;

            await using var fileStream = new FileStream(
                targetPath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                81920,
                useAsync: true
            );

            using var contentStream = await response.Content.ReadAsStreamAsync(cancellationToken);

            var buffer = new byte[81920];
            long totalRead = 0;
            int read;

            Stopwatch sw = Stopwatch.StartNew();
            TimeSpan lastReported = TimeSpan.Zero;

            progress?.Report(new DownloadProgressInfo
            {
                BytesDownloaded = 0,
                TotalBytes = totalBytes,
                Elapsed = TimeSpan.Zero,
                BytesPerSecond = 0,
                Status = "Download gestartet..."
            });

            while ((read = await contentStream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken)) > 0)
            {
                await fileStream.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                totalRead += read;

                if (sw.Elapsed - lastReported >= TimeSpan.FromMilliseconds(200))
                {
                    double bytesPerSecond = sw.Elapsed.TotalSeconds > 0
                        ? totalRead / sw.Elapsed.TotalSeconds
                        : 0;

                    progress?.Report(new DownloadProgressInfo
                    {
                        BytesDownloaded = totalRead,
                        TotalBytes = totalBytes,
                        Elapsed = sw.Elapsed,
                        BytesPerSecond = bytesPerSecond,
                        Status = "Download läuft..."
                    });

                    lastReported = sw.Elapsed;
                }
            }

            double finalBytesPerSecond = sw.Elapsed.TotalSeconds > 0
                ? totalRead / sw.Elapsed.TotalSeconds
                : 0;

            progress?.Report(new DownloadProgressInfo
            {
                BytesDownloaded = totalRead,
                TotalBytes = totalBytes,
                Elapsed = sw.Elapsed,
                BytesPerSecond = finalBytesPerSecond,
                Status = "Download abgeschlossen."
            });

            return new FileDownloadResult
            {
                BytesDownloaded = totalRead,
                TotalBytes = totalBytes,
                Elapsed = sw.Elapsed
            };
        }

        // =========================================================
        // Startup / connection
        // =========================================================

        private async Task InitializeAsync()
        {
            try
            {
                bool continueStartup = await CheckForUpdatesOnStartupAsync();
                if (!continueStartup)
                    return;

                bool connected = await RefreshConnectionStateAsync();
                if (!connected)
                    return;

                if (_retryLoopTask == null || _retryLoopTask.IsCompleted)
                    _retryLoopTask = RetryLoopAsync();
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError(
                    "Es konnte keine Verbindung hergestellt werden.\nFehlercode: " + ex.Message,
                    ex
                );
            }
        }

        private async Task RetryLoopAsync()
        {
            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    bool ok = await RefreshConnectionStateAsync();
                    if (!ok)
                        break;

                    TimeSpan delay = CurrentView == ViewState.Main
                        ? TimeSpan.FromSeconds(10)
                        : TimeSpan.FromSeconds(3);

                    await Task.Delay(delay, _cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _retryLoopTask = null;
            }
        }

        private async Task<bool> RefreshConnectionStateAsync()
        {
            await _connectionLock.WaitAsync(_cts.Token);

            try
            {
                string? json = await GetApiStringAsync(
                    "https://api.betonbroetchen.de/mods/connect.php?action=ping",
                    _cts.Token
                );

                if (string.IsNullOrWhiteSpace(json))
                {
                    return StopWithError(
                        "Server responded, but no data was returned.",
                        string.Empty
                    );
                }

                PingResponse? ping;
                try
                {
                    ping = JsonConvert.DeserializeObject<PingResponse>(json);
                }
                catch (Exception ex)
                {
                    return StopWithError(
                        "Server responded, but the reply was not valid.",
                        ex
                    );
                }

                if (ping?.Ok != true || !string.Equals(ping.Message, "pong", StringComparison.OrdinalIgnoreCase))
                {
                    return StopWithError(
                        "Server responded, but the reply was not valid.",
                        json
                    );
                }

                ServerStatusText.Text = "Online";

                bool wasNotMain = CurrentView != ViewState.Main && CurrentView != ViewState.ModManager;

                if (wasNotMain)
                {
                    ShowMain();

                    bool startupOk = await LoadGamesAsync();
                    if (!startupOk)
                        return false;
                }

                return true;
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                return StopWithError(
                    "Es konnte keine Verbindung hergestellt werden.\nFehlercode: " + ex.Message,
                    ex
                );
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task<bool> LoadGamesAsync()
        {
            try
            {
                string? json = await GetApiStringAsync(
                    "https://api.betonbroetchen.de/mods/connect.php?action=games",
                    _cts.Token
                );

                if (string.IsNullOrWhiteSpace(json))
                {
                    return StopWithError(
                        "Server responded, but no data was returned.",
                        string.Empty
                    );
                }

                GameResponse? data = JsonConvert.DeserializeObject<GameResponse>(json);

                if (data == null || !data.Ok)
                {
                    return StopWithError(
                        "Server responded, but the reply was not valid.",
                        json
                    );
                }

                GameListPanel.Children.Clear();

                if (data.Games.Count == 0)
                {
                    GameListPanel.Children.Add(new TextBlock
                    {
                        Text = "Keine Spiele gefunden",
                        Foreground = (Brush)FindResource("TextSubBrush"),
                        FontSize = 13,
                        Margin = new Thickness(4, 4, 4, 0)
                    });

                    GameListContainer.Visibility = Visibility.Visible;
                    return true;
                }

                foreach (Game game in data.Games.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase))
                {
                    var btn = new Button
                    {
                        Content = game.Name,
                        Margin = new Thickness(0, 0, 0, 8),
                        Style = (Style)FindResource("ModernButton"),
                        Tag = game
                    };

                    btn.Click += GameButton_Click;
                    GameListPanel.Children.Add(btn);
                }

                GameListContainer.Visibility = Visibility.Visible;
                return true;
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                return StopWithError(
                    "Es konnte keine Verbindung hergestellt werden.\nFehlercode: " + ex.Message,
                    ex
                );
            }
        }

        // =========================================================
        // Update system
        // =========================================================

        private static async Task<UpdateCheckResult> GetUpdateInfoAsync()
        {
            const string requestUrl = "https://api.github.com/repos/BetonbroetchenDE/BBsUMD/releases/latest";

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
                request.Headers.UserAgent.ParseAdd("BBsUniversalModDownloader/1.0");
                request.Headers.Accept.ParseAdd("application/vnd.github+json");
                request.Headers.Add("X-GitHub-Api-Version", "2022-11-28");

                using var response = await apiClient.SendAsync(request);
                string body = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    return new UpdateCheckResult
                    {
                        Success = false,
                        UserMessage = "GitHub hat beim Laden der neuesten Version einen Fehler geliefert.",
                        TechnicalDetails = body
                    };
                }

                GitHubReleaseResponse? release = JsonConvert.DeserializeObject<GitHubReleaseResponse>(body);
                if (release == null || string.IsNullOrWhiteSpace(release.Tag_Name))
                {
                    return new UpdateCheckResult
                    {
                        Success = false,
                        UserMessage = "Die GitHub-Release-Antwort war ungültig.",
                        TechnicalDetails = body
                    };
                }

                string versionText = release.Tag_Name.TrimStart('v', 'V');

                if (!Version.TryParse(versionText, out var remoteVersion))
                {
                    return new UpdateCheckResult
                    {
                        Success = false,
                        UserMessage = "Die GitHub-Version ist ungültig.",
                        TechnicalDetails = $"Tag: {release.Tag_Name}"
                    };
                }

                GitHubAsset? exeAsset = release.Assets.FirstOrDefault(a =>
                    !string.IsNullOrWhiteSpace(a.Name) &&
                    a.Name.EndsWith(".exe", StringComparison.OrdinalIgnoreCase));

                if (exeAsset == null || string.IsNullOrWhiteSpace(exeAsset.Browser_Download_Url))
                {
                    return new UpdateCheckResult
                    {
                        Success = false,
                        UserMessage = "Im neuesten GitHub-Release wurde keine EXE gefunden.",
                        TechnicalDetails = body
                    };
                }

                GitHubAsset? shaAsset = release.Assets.FirstOrDefault(a =>
                    !string.IsNullOrWhiteSpace(a.Name) &&
                    a.Name.EndsWith(".exe.sha256", StringComparison.OrdinalIgnoreCase));

                string? sha256 = null;
                if (shaAsset != null && !string.IsNullOrWhiteSpace(shaAsset.Browser_Download_Url))
                {
                    try
                    {
                        sha256 = (await downloadClient.GetStringAsync(shaAsset.Browser_Download_Url)).Trim();
                    }
                    catch
                    {
                        sha256 = null;
                    }
                }

                Version localVersion = GetCurrentAppVersion();

                return new UpdateCheckResult
                {
                    Success = true,
                    IsUpdateAvailable = remoteVersion > localVersion,
                    Update = new UpdateResponse
                    {
                        Ok = true,
                        Version = remoteVersion.ToString(),
                        Url = exeAsset.Browser_Download_Url,
                        Sha256 = sha256,
                        Notes = release.Body
                    },
                    TechnicalDetails =
                        $"Lokale Version: {localVersion}{Environment.NewLine}" +
                        $"Server-Version: {remoteVersion}{Environment.NewLine}" +
                        $"Download-URL: {exeAsset.Browser_Download_Url}"
                };
            }
            catch (Exception ex)
            {
                return new UpdateCheckResult
                {
                    Success = false,
                    UserMessage = "Fehler beim Abrufen der GitHub-Release-Daten.",
                    TechnicalDetails = ex.ToString()
                };
            }
        }

        private async Task<bool> CheckForUpdatesOnStartupAsync()
        {
            try
            {
                UpdateCheckResult result = await GetUpdateInfoAsync();

                if (!result.Success)
                {
                    return StopWithError(
                        "Die Update-Prüfung ist fehlgeschlagen.",
                        $"{result.UserMessage}{Environment.NewLine}{Environment.NewLine}{result.TechnicalDetails}"
                    );
                }

                if (result.Update == null)
                {
                    return StopWithError(
                        "Die Update-Prüfung war formal erfolgreich, aber es wurden keine Update-Daten geliefert."
                    );
                }

                if (!result.IsUpdateAvailable)
                    return true;

                UpdateResponse update = result.Update;
                var tcs = new TaskCompletionSource<bool>();

                string message =
                    $"Es ist eine neue Version verfügbar.{Environment.NewLine}{Environment.NewLine}" +
                    $"Aktuell: {GetCurrentAppVersion()}{Environment.NewLine}" +
                    $"Neu: {update.Version}";

                if (!string.IsNullOrWhiteSpace(update.Notes))
                {
                    message +=
                        $"{Environment.NewLine}{Environment.NewLine}" +
                        $"Hinweise:{Environment.NewLine}{update.Notes}";
                }

                if (!string.IsNullOrWhiteSpace(update.Url))
                {
                    message +=
                        $"{Environment.NewLine}{Environment.NewLine}" +
                        $"Download:{Environment.NewLine}{update.Url}";
                }

                message +=
                    $"{Environment.NewLine}{Environment.NewLine}" +
                    "Wenn du ohne Update fortfährst, können Funktionen fehlerhaft sein, nicht funktionieren oder unerwartetes Verhalten auftreten." +
                    $"{Environment.NewLine}{Environment.NewLine}" +
                    "Trotzdem ohne Update fortfahren?";

                ShowDecisionPopup(
                    "Update verfügbar",
                    message,
                    "Update installieren",
                    async () =>
                    {
                        try
                        {
                            ShowBusyPopup(
                                "Update wird installiert",
                                "Download wird vorbereitet..."
                            );

                            await TryUpdateAndRestartAsync(update, _cts.Token);
                            tcs.TrySetResult(false);
                        }
                        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                        {
                            tcs.TrySetResult(false);
                        }
                        catch (Exception ex)
                        {
                            tcs.TrySetResult(
                                StopWithError(
                                    "Update konnte nicht installiert werden.",
                                    ex
                                )
                            );
                        }
                    },
                    "Trotzdem fortfahren",
                    () =>
                    {
                        ClosePopup_Click(this, new RoutedEventArgs());
                        tcs.TrySetResult(true);
                        return Task.CompletedTask;
                    }
                );

                return await tcs.Task;
            }
            catch (Exception ex)
            {
                return StopWithError(
                    "Unerwarteter Fehler beim Update-Check.",
                    ex
                );
            }
        }

        private async Task TryUpdateAndRestartAsync(UpdateResponse update, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(update.Url))
                throw new InvalidOperationException("Die Download-URL des Updates fehlt.");

            if (!Uri.TryCreate(update.Url, UriKind.Absolute, out Uri? downloadUri))
                throw new InvalidOperationException($"Die Download-URL ist ungültig: {update.Url}");

            string currentExe =
                Environment.ProcessPath
                ?? Process.GetCurrentProcess().MainModule?.FileName
                ?? throw new InvalidOperationException("Aktueller EXE-Pfad konnte nicht ermittelt werden.");

            string currentExeDir =
                Path.GetDirectoryName(currentExe)
                ?? throw new InvalidOperationException("EXE-Ordner konnte nicht ermittelt werden.");

            string tempExe = Path.Combine(
                Path.GetTempPath(),
                $"BBs Universal Mod Downloader {Guid.NewGuid():N}.exe"
            );

            try
            {
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

                var progress = new Progress<DownloadProgressInfo>(info =>
                {
                    UpdateBusyPopup(
                        "Update wird installiert",
                        BuildSingleDownloadMessage("BBs Universal Mod Downloader.exe", info)
                    );
                });

                FileDownloadResult downloadResult;

                try
                {
                    downloadResult = await DownloadFileWithProgressAsync(
                        downloadUri.ToString(),
                        tempExe,
                        progress,
                        linkedCts.Token
                    );
                }
                catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    throw new TimeoutException("Der Update-Download hat das Zeitlimit von 15 Minuten überschritten.");
                }

                if (!string.IsNullOrWhiteSpace(update.Sha256))
                {
                    UpdateBusyPopup(
                        "Update wird installiert",
                        $"Download abgeschlossen.{Environment.NewLine}{Environment.NewLine}" +
                        $"Datei: BBs Universal Mod Downloader.exe{Environment.NewLine}" +
                        $"Heruntergeladen: {FormatBytes(downloadResult.BytesDownloaded)}{Environment.NewLine}" +
                        $"Dauer: {FormatDuration(downloadResult.Elapsed)}{Environment.NewLine}{Environment.NewLine}" +
                        $"Prüfe SHA-256..."
                    );

                    string downloadedHash = await ComputeSha256Async(tempExe, cancellationToken);

                    if (!string.Equals(downloadedHash, update.Sha256, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException("Die SHA-256-Prüfsumme des Updates ist ungültig.");
                }

                UpdateBusyPopup(
                    "Update wird installiert",
                    $"Download und Prüfung abgeschlossen.{Environment.NewLine}{Environment.NewLine}" +
                    $"Datei: BBs Universal Mod Downloader.exe{Environment.NewLine}" +
                    $"Heruntergeladen: {FormatBytes(downloadResult.BytesDownloaded)}{Environment.NewLine}" +
                    $"Dauer: {FormatDuration(downloadResult.Elapsed)}{Environment.NewLine}{Environment.NewLine}" +
                    $"Anwendung wird neu gestartet..."
                );

                string backupExe = currentExe + ".bak";

                string arguments =
                    "/C " +
                    $"timeout /t 2 /nobreak > nul & " +
                    $"del /f /q \"{backupExe}\" > nul 2>&1 & " +
                    $"move /y \"{currentExe}\" \"{backupExe}\" > nul 2>&1 & " +
                    $"move /y \"{tempExe}\" \"{currentExe}\" > nul 2>&1 & " +
                    $"if exist \"{currentExe}\" start \"\" \"{currentExe}\" & " +
                    $"timeout /t 2 /nobreak > nul & " +
                    $"del /f /q \"{backupExe}\" > nul 2>&1";

                Process.Start(new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = arguments,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WorkingDirectory = currentExeDir
                });

                Application.Current.Shutdown();
            }
            catch
            {
                if (File.Exists(tempExe))
                {
                    try
                    {
                        File.Delete(tempExe);
                    }
                    catch
                    {
                    }
                }

                throw;
            }
        }

        // =========================================================
        // Steam / selected game
        // =========================================================

        private static string? GetModsFolder(string gameName)
        {
            string? gameFolder = SteamHelper.GetGameFolder(gameName);
            if (string.IsNullOrWhiteSpace(gameFolder))
                return null;

            return gameName switch
            {
                "Ready or Not" => Path.Combine(gameFolder, "ReadyOrNot", "Content", "Paks"),
                _ => null
            };
        }

        private void CheckGameState(string name)
        {
            _selectedGameInstalled = false;
            _selectedGameModFolder = null;

            if (!SteamHelper.IsGameInstalled(name))
            {
                SelectedGameInstallState.Text = "Das Spiel ist nicht installiert.";
                return;
            }

            _selectedGameInstalled = true;
            SelectedGameInstallState.Text = "Das Spiel ist installiert.";

            string? modsFolder = GetModsFolder(name);
            if (string.IsNullOrWhiteSpace(modsFolder))
                return;

            _selectedGameModFolder = modsFolder;
        }

        // =========================================================
        // Mod manager view logic
        // =========================================================

        private async Task RunExclusiveModActionAsync(Func<Task> action)
        {
            if (!await _modManagementLock.WaitAsync(0))
            {
                ShowOverlayPopup("Mod-Verwaltung", "Es läuft bereits eine Mod-Aktion.");
                return;
            }

            try
            {
                await action();
            }
            finally
            {
                _modManagementLock.Release();
            }
        }

        private void SetModManagerInteractiveState(bool enabled)
        {
            ModManagerInteractivePanel.IsEnabled = enabled;
            RefreshModManagerButton.IsEnabled = enabled;
            BackFromModManagerButton.IsEnabled = enabled;
        }

        private void ShowModActionMessage(string title, string fileName, string info, bool indeterminate = true)
        {
            ModActionTitleText.Text = title;
            ModActionFileText.Text = fileName;
            ModActionInfoText.Text = string.IsNullOrWhiteSpace(info) ? "Bitte warten..." : info;
            ModActionProgressBar.IsIndeterminate = indeterminate;

            if (!indeterminate)
                ModActionProgressBar.Value = 0;

            ModActionOverlay.Visibility = Visibility.Visible;
        }

        private void UpdateModActionProgress(string title, string fileName, DownloadProgressInfo info)
        {
            ModActionTitleText.Text = title;
            ModActionFileText.Text = fileName;

            bool hasTotal = info.TotalBytes.HasValue && info.TotalBytes.Value > 0;

            if (hasTotal)
            {
                ModActionProgressBar.IsIndeterminate = false;
                ModActionProgressBar.Value = (double)info.BytesDownloaded / info.TotalBytes!.Value * 100d;
            }
            else
            {
                ModActionProgressBar.IsIndeterminate = true;
            }

            string progressText = hasTotal
                ? $"{FormatBytes(info.BytesDownloaded)} / {FormatBytes(info.TotalBytes!.Value)} ({(double)info.BytesDownloaded / info.TotalBytes.Value:P1})"
                : $"{FormatBytes(info.BytesDownloaded)} / Gesamtgröße unbekannt";

            string speedText = info.BytesPerSecond > 0
                ? $"{FormatBytes((long)info.BytesPerSecond)}/s"
                : "-";

            ModActionInfoText.Text =
                $"{progressText}{Environment.NewLine}" +
                $"Geschwindigkeit: {speedText}{Environment.NewLine}" +
                $"Dauer: {FormatDuration(info.Elapsed)}";
        }

        private void HideModActionOverlay()
        {
            ModActionOverlay.Visibility = Visibility.Collapsed;
            ModActionProgressBar.IsIndeterminate = true;
            ModActionProgressBar.Value = 0;
            ModActionTitleText.Text = "Aktion läuft";
            ModActionFileText.Text = "Datei";
            ModActionInfoText.Text = "Bitte warten...";
        }

        private async Task LoadModManagerAsync()
        {
            if (string.IsNullOrWhiteSpace(_selectedGameModFolder))
                throw new InvalidOperationException("Kein Mod-Ordner für dieses Spiel gefunden.");

            SetModManagerInteractiveState(false);
            HideModActionOverlay();

            ModManagerTitleText.Text = $"Mods verwalten - {_selectedGameName}";
            ModManagerSubtitleText.Text = _selectedGameModFolder;
            ModManagerStatusText.Text = "Lade Mod-Status...";

            ServerModsSummaryText.Text = "Lade Server-Mods...";
            LocalModsSummaryText.Text = "Lade lokale Mods...";
            ServerModsEmptyText.Visibility = Visibility.Collapsed;
            LocalModsEmptyText.Visibility = Visibility.Collapsed;

            try
            {
                Directory.CreateDirectory(_selectedGameModFolder);

                _serverMods.Clear();
                _localMods.Clear();

                List<string> localFiles = GetLocalModFiles(_selectedGameModFolder);

                bool serverAvailable = false;
                string? serverError = null;
                ModHashResponse? serverData = null;

                try
                {
                    string? json = await GetApiStringAsync(
                        $"https://api.betonbroetchen.de/mods/connect.php?action=mod_hashes&game={Uri.EscapeDataString(_selectedGameName)}",
                        _cts.Token
                    );

                    if (string.IsNullOrWhiteSpace(json))
                    {
                        serverError = "Server responded, but no data was returned.";
                    }
                    else
                    {
                        serverData = JsonConvert.DeserializeObject<ModHashResponse>(json);

                        if (serverData == null || !serverData.Ok)
                            serverError = "Server responded, but the reply was not valid.";
                        else
                            serverAvailable = true;
                    }
                }
                catch (Exception ex)
                {
                    serverError = ex.Message;
                }

                if (serverAvailable && serverData != null)
                {
                    var serverLookup = serverData.Files.ToDictionary(
                        x => x.FileName,
                        x => x,
                        StringComparer.OrdinalIgnoreCase
                    );

                    foreach (ModFileHashEntry serverFile in serverData.Files.OrderBy(x => x.FileName, StringComparer.OrdinalIgnoreCase))
                    {
                        if (!localFiles.Contains(serverFile.FileName, StringComparer.OrdinalIgnoreCase))
                        {
                            _serverMods.Add(new ServerModRow
                            {
                                FileName = serverFile.FileName,
                                StatusText = "Nicht lokal installiert.",
                                ServerFile = serverFile
                            });
                        }
                    }

                    foreach (string localFile in localFiles)
                    {
                        string fullPath = Path.Combine(_selectedGameModFolder, localFile);

                        if (serverLookup.TryGetValue(localFile, out ModFileHashEntry? serverFile))
                        {
                            string localHash = await ComputeSha256Async(fullPath, _cts.Token);
                            bool isCurrent = string.Equals(localHash, serverFile.Sha256, StringComparison.OrdinalIgnoreCase);

                            _localMods.Add(new LocalModRow
                            {
                                FileName = localFile,
                                FullPath = fullPath,
                                CanUpdate = !isCurrent && !string.IsNullOrWhiteSpace(serverFile.Url),
                                ServerFile = serverFile,
                                StatusText = isCurrent
                                    ? "Server-Version entspricht der lokalen Version."
                                    : "Auf dem Server ist eine andere Version verfügbar."
                            });
                        }
                        else
                        {
                            _localMods.Add(new LocalModRow
                            {
                                FileName = localFile,
                                FullPath = fullPath,
                                CanUpdate = false,
                                ServerFile = null,
                                StatusText = "Nicht auf dem Server gefunden."
                            });
                        }
                    }

                    ServerModsSummaryText.Text = $"{_serverMods.Count} Mod(s) können heruntergeladen werden.";
                    LocalModsSummaryText.Text = $"{_localMods.Count} lokal installierte Mod(s).";

                    ServerModsEmptyText.Text = "Keine Server-Mods gefunden, die lokal fehlen.";
                }
                else
                {
                    foreach (string localFile in localFiles)
                    {
                        _localMods.Add(new LocalModRow
                        {
                            FileName = localFile,
                            FullPath = Path.Combine(_selectedGameModFolder, localFile),
                            CanUpdate = false,
                            ServerFile = null,
                            StatusText = string.IsNullOrWhiteSpace(serverError)
                                ? "Serverstatus unbekannt."
                                : $"Serverstatus unbekannt: {serverError}"
                        });
                    }

                    ServerModsSummaryText.Text = "Serverstatus unbekannt.";
                    LocalModsSummaryText.Text = $"{_localMods.Count} lokal installierte Mod(s).";
                    ServerModsEmptyText.Text = string.IsNullOrWhiteSpace(serverError)
                        ? "Serverstatus konnte nicht geladen werden."
                        : $"Serverstatus konnte nicht geladen werden: {serverError}";
                }

                ServerModsEmptyText.Visibility = _serverMods.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                LocalModsEmptyText.Visibility = _localMods.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                ModManagerStatusText.Text = "Bereit.";
            }
            finally
            {
                SetModManagerInteractiveState(true);
            }
        }

        private async Task ApplyServerModAsync(ModFileHashEntry file, string actionTitle)
        {
            if (string.IsNullOrWhiteSpace(_selectedGameModFolder))
                throw new InvalidOperationException("Kein Mod-Ordner für dieses Spiel gefunden.");

            if (string.IsNullOrWhiteSpace(file.Url))
                throw new InvalidOperationException($"Für die Datei '{file.FileName}' wurde keine Download-URL geliefert.");

            Directory.CreateDirectory(_selectedGameModFolder);

            string targetPath = Path.Combine(_selectedGameModFolder, file.FileName);
            string tempPath = targetPath + ".download";

            try
            {
                ShowModActionMessage(actionTitle, file.FileName, "Download wird vorbereitet...", true);

                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(15));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, timeoutCts.Token);

                var progress = new Progress<DownloadProgressInfo>(info =>
                {
                    UpdateModActionProgress(actionTitle, file.FileName, info);
                });

                FileDownloadResult result;

                try
                {
                    result = await DownloadFileWithProgressAsync(
                        file.Url,
                        tempPath,
                        progress,
                        linkedCts.Token
                    );
                }
                catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !_cts.IsCancellationRequested)
                {
                    throw new TimeoutException($"Der Download der Datei '{file.FileName}' hat das Zeitlimit von 15 Minuten überschritten.");
                }

                ShowModActionMessage(
                    actionTitle,
                    file.FileName,
                    $"Download abgeschlossen.{Environment.NewLine}" +
                    $"Heruntergeladen: {FormatBytes(result.BytesDownloaded)}{Environment.NewLine}" +
                    $"Dauer: {FormatDuration(result.Elapsed)}{Environment.NewLine}{Environment.NewLine}" +
                    $"Prüfe SHA-256...",
                    true
                );

                string downloadedHash = await ComputeSha256Async(tempPath, _cts.Token);

                if (!string.Equals(downloadedHash, file.Sha256, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"Die Hash-Prüfung ist fehlgeschlagen: {file.FileName}");

                File.Move(tempPath, targetPath, overwrite: true);
            }
            catch
            {
                if (File.Exists(tempPath))
                {
                    try
                    {
                        File.Delete(tempPath);
                    }
                    catch
                    {
                    }
                }

                throw;
            }
        }

        private async Task RemoveLocalModAsync(LocalModRow row)
        {
            if (string.IsNullOrWhiteSpace(_selectedGameModFolder))
                throw new InvalidOperationException("Kein Mod-Ordner für dieses Spiel gefunden.");

            Directory.CreateDirectory(_selectedGameModFolder);

            ShowModActionMessage(
                "Mod wird deinstalliert",
                row.FileName,
                "Datei wird entfernt...",
                true
            );

            await Task.Run(() =>
            {
                if (File.Exists(row.FullPath))
                    File.Delete(row.FullPath);
            }, _cts.Token);
        }

        // =========================================================
        // Shared popup helpers
        // =========================================================

        private void ShowBusyPopup(string title, string message)
        {
            PopupTitleText.Text = title;
            PopupMessageText.Text = string.IsNullOrWhiteSpace(message)
                ? "Bitte warten..."
                : message;

            PopupPrimaryButton.Visibility = Visibility.Collapsed;
            PopupSecondaryButton.Visibility = Visibility.Collapsed;
            PopupCloseButton.Visibility = Visibility.Collapsed;

            PopupPrimaryButton.IsEnabled = false;
            PopupSecondaryButton.IsEnabled = false;

            _popupPrimaryActionAsync = null;
            _popupSecondaryActionAsync = null;
            _popupAllowsBackdropClose = false;

            PopupOverlay.Visibility = Visibility.Visible;
            PopupOverlay.Opacity = 0;

            var fade = new DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(180));
            PopupOverlay.BeginAnimation(OpacityProperty, fade);
        }

        private void UpdateBusyPopup(string title, string message)
        {
            PopupTitleText.Text = title;
            PopupMessageText.Text = message;
        }

        private void ShowDecisionPopup(
            string title,
            string message,
            string primaryButtonText,
            Func<Task> primaryActionAsync,
            string secondaryButtonText,
            Func<Task> secondaryActionAsync)
        {
            PopupTitleText.Text = title;
            PopupMessageText.Text = string.IsNullOrWhiteSpace(message)
                ? "Keine Einträge gefunden."
                : message;

            PopupPrimaryButton.Content = primaryButtonText;
            PopupSecondaryButton.Content = secondaryButtonText;

            PopupPrimaryButton.Visibility = Visibility.Visible;
            PopupSecondaryButton.Visibility = Visibility.Visible;
            PopupCloseButton.Visibility = Visibility.Collapsed;

            PopupPrimaryButton.IsEnabled = true;
            PopupSecondaryButton.IsEnabled = true;

            _popupPrimaryActionAsync = primaryActionAsync;
            _popupSecondaryActionAsync = secondaryActionAsync;
            _popupAllowsBackdropClose = false;

            PopupOverlay.Visibility = Visibility.Visible;
            PopupOverlay.Opacity = 0;

            var fade = new DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(180));
            PopupOverlay.BeginAnimation(OpacityProperty, fade);
        }

        private void ShowOverlayPopup(string title, string message)
        {
            PopupTitleText.Text = title;
            PopupMessageText.Text = string.IsNullOrWhiteSpace(message)
                ? "Keine Einträge gefunden."
                : message;

            PopupPrimaryButton.Visibility = Visibility.Collapsed;
            PopupSecondaryButton.Visibility = Visibility.Collapsed;
            PopupCloseButton.Visibility = Visibility.Visible;

            PopupPrimaryButton.IsEnabled = true;
            PopupSecondaryButton.IsEnabled = true;

            _popupPrimaryActionAsync = null;
            _popupSecondaryActionAsync = null;
            _popupAllowsBackdropClose = true;

            PopupOverlay.Visibility = Visibility.Visible;
            PopupOverlay.Opacity = 0;

            var fade = new DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(180));
            PopupOverlay.BeginAnimation(OpacityProperty, fade);
        }

        private void ClosePopupImmediate()
        {
            PopupOverlay.BeginAnimation(OpacityProperty, null);
            PopupOverlay.Visibility = Visibility.Collapsed;

            _popupPrimaryActionAsync = null;
            _popupSecondaryActionAsync = null;
            _popupAllowsBackdropClose = true;
        }

        // =========================================================
        // Error / view helpers
        // =========================================================

        private void ShowLoading() => SwitchView(LoadingView, ViewState.Loading);
        private void ShowMain() => SwitchView(MainView, ViewState.Main);
        private void ShowModManager() => SwitchView(ModManagerView, ViewState.ModManager);

        private void ShowError(string message, string fullError)
        {
            ErrorMessageText.Text = message;
            FullErrorText.Text = fullError;

            FullErrorPanel.Visibility = Visibility.Collapsed;
            ShowFullErrorButton.Content = "Vollständigen Fehler anzeigen";

            SwitchView(ErrorView, ViewState.Error);
        }

        private bool StopWithError(string userMessage, string? technicalDetails = null)
        {
            ClosePopupImmediate();
            HideModActionOverlay();
            SetModManagerInteractiveState(true);

            ShowError(
                userMessage,
                string.IsNullOrWhiteSpace(technicalDetails) ? userMessage : technicalDetails
            );

            return false;
        }

        private bool StopWithError(string userMessage, Exception ex)
        {
            return StopWithError(userMessage, ex.ToString());
        }

        private void SwitchView(UIElement target, ViewState state)
        {
            foreach (UIElement element in new[] { LoadingView, ErrorView, MainView, ModManagerView })
            {
                element.Visibility = Visibility.Collapsed;
                element.Opacity = 0;
            }

            target.Visibility = Visibility.Visible;

            var fade = new DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(300));
            target.BeginAnimation(OpacityProperty, fade);

            CurrentView = state;
        }

        // =========================================================
        // Main events
        // =========================================================

        private void GameButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is Button btn && btn.Tag is Game game)
                {
                    _selectedGameName = game.Name;
                    SelectedGameTitle.Text = game.Name;

                    CheckGameState(game.Name);

                    GameListView.Visibility = Visibility.Collapsed;
                    GameMenuView.Visibility = Visibility.Visible;
                }
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Öffnen des Spiels.", ex);
            }
        }

        private void BackToGameList_Click(object sender, RoutedEventArgs e)
        {
            GameMenuView.Visibility = Visibility.Collapsed;
            GameListView.Visibility = Visibility.Visible;
        }

        private async void ManageMods_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (!_selectedGameInstalled)
                {
                    StopWithError("Das Spiel ist nicht installiert.");
                    return;
                }

                if (string.IsNullOrWhiteSpace(_selectedGameModFolder))
                {
                    StopWithError("Kein Mod-Ordner für dieses Spiel gefunden.");
                    return;
                }

                await RunExclusiveModActionAsync(async () =>
                {
                    ShowModManager();
                    await LoadModManagerAsync();
                });
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Unerwarteter Fehler bei der Mod-Verwaltung.", ex);
            }
        }

        private async void RefreshModManager_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await RunExclusiveModActionAsync(async () =>
                {
                    await LoadModManagerAsync();
                });
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Neuladen der Mod-Verwaltung.", ex);
            }
        }

        private void BackFromModManager_Click(object sender, RoutedEventArgs e)
        {
            ShowMain();
        }

        private async void ServerDownload_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is not Button { Tag: ServerModRow row })
                    return;

                await RunExclusiveModActionAsync(async () =>
                {
                    SetModManagerInteractiveState(false);

                    try
                    {
                        await ApplyServerModAsync(row.ServerFile, "Mod wird heruntergeladen");
                        await LoadModManagerAsync();
                        ModManagerStatusText.Text = $"{row.FileName} wurde heruntergeladen.";
                    }
                    finally
                    {
                        HideModActionOverlay();
                        SetModManagerInteractiveState(true);
                    }
                });
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Herunterladen der Mod.", ex);
            }
        }

        private async void LocalUninstall_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is not Button { Tag: LocalModRow row })
                    return;

                await RunExclusiveModActionAsync(async () =>
                {
                    SetModManagerInteractiveState(false);

                    try
                    {
                        await RemoveLocalModAsync(row);
                        await LoadModManagerAsync();
                        ModManagerStatusText.Text = $"{row.FileName} wurde deinstalliert.";
                    }
                    finally
                    {
                        HideModActionOverlay();
                        SetModManagerInteractiveState(true);
                    }
                });
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Deinstallieren der Mod.", ex);
            }
        }

        private async void LocalUpdate_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is not Button { Tag: LocalModRow row })
                    return;

                if (!row.CanUpdate || row.ServerFile == null)
                    return;

                await RunExclusiveModActionAsync(async () =>
                {
                    SetModManagerInteractiveState(false);

                    try
                    {
                        await ApplyServerModAsync(row.ServerFile, "Mod wird aktualisiert");
                        await LoadModManagerAsync();
                        ModManagerStatusText.Text = $"{row.FileName} wurde aktualisiert.";
                    }
                    finally
                    {
                        HideModActionOverlay();
                        SetModManagerInteractiveState(true);
                    }
                });
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Aktualisieren der Mod.", ex);
            }
        }

        // =========================================================
        // Shared popup events
        // =========================================================

        private async void PopupPrimaryButton_Click(object sender, RoutedEventArgs e)
        {
            if (_popupPrimaryActionAsync == null)
                return;

            PopupPrimaryButton.IsEnabled = false;
            PopupSecondaryButton.IsEnabled = false;

            try
            {
                await _popupPrimaryActionAsync();
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError("Unerwarteter Fehler.", ex);
            }
            finally
            {
                PopupPrimaryButton.IsEnabled = true;
                PopupSecondaryButton.IsEnabled = true;
            }
        }

        private async void PopupSecondaryButton_Click(object sender, RoutedEventArgs e)
        {
            if (_popupSecondaryActionAsync == null)
                return;

            PopupPrimaryButton.IsEnabled = false;
            PopupSecondaryButton.IsEnabled = false;

            try
            {
                await _popupSecondaryActionAsync();
            }
            catch (Exception ex)
            {
                StopWithError("Unerwarteter Fehler.", ex);
            }
            finally
            {
                PopupPrimaryButton.IsEnabled = true;
                PopupSecondaryButton.IsEnabled = true;
            }
        }

        private void ClosePopup_Click(object sender, RoutedEventArgs e)
        {
            _popupPrimaryActionAsync = null;
            _popupSecondaryActionAsync = null;
            _popupAllowsBackdropClose = true;

            var fade = new DoubleAnimation(1, 0, TimeSpan.FromMilliseconds(160));
            fade.Completed += (_, _) => PopupOverlay.Visibility = Visibility.Collapsed;
            PopupOverlay.BeginAnimation(OpacityProperty, fade);
        }

        private void PopupOverlay_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.OriginalSource == PopupOverlay && _popupAllowsBackdropClose)
            {
                ClosePopup_Click(sender, new RoutedEventArgs());
            }
        }

        // =========================================================
        // Error view events
        // =========================================================

        private void CopyFullError_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                Clipboard.SetText(FullErrorText.Text);
            }
            catch (Exception ex)
            {
                StopWithError("Fehler beim Kopieren des Fehlers.", ex);
            }
        }

        private void ShowFullError_Click(object sender, RoutedEventArgs e)
        {
            if (FullErrorPanel.Visibility == Visibility.Visible)
            {
                FullErrorPanel.Visibility = Visibility.Collapsed;
                ShowFullErrorButton.Content = "Vollständigen Fehler anzeigen";
            }
            else
            {
                FullErrorPanel.Visibility = Visibility.Visible;
                ShowFullErrorButton.Content = "Fehler ausblenden";
            }
        }

        private async void Retry_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                ShowLoading();

                bool continueStartup = await CheckForUpdatesOnStartupAsync();
                if (!continueStartup)
                    return;

                bool connected = await RefreshConnectionStateAsync();
                if (!connected)
                    return;

                if (_retryLoopTask == null || _retryLoopTask.IsCompleted)
                    _retryLoopTask = RetryLoopAsync();
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                StopWithError(
                    "Es konnte keine Verbindung hergestellt werden.\nFehlercode: " + ex.Message,
                    ex
                );
            }
        }

        // =========================================================
        // Window events
        // =========================================================

        private void Minimize_Click(object sender, RoutedEventArgs e)
        {
            WindowState = WindowState.Minimized;
        }

        private void Exit_Click(object sender, RoutedEventArgs e)
        {
            Application.Current.Shutdown();
        }

        private void TopBar_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left && e.OriginalSource == sender)
            {
                DragMove();
            }
        }

        // =========================================================
        // Sound events
        // =========================================================

        private void Button_ClickSound(object sender, RoutedEventArgs e)
        {
            try
            {
                _clickPlayer.Stop();

                if (_clickPlayer.Stream is IDisposable disposableStream)
                    disposableStream.Dispose();

                var asm = Assembly.GetExecutingAssembly();
                Stream? stream = asm.GetManifestResourceStream("BBs_Universal_Mod_Downloader.Sounds.MinecraftMenuSound.wav");

                if (stream == null)
                {
                    StopWithError("Embedded sound resource not found.");
                    return;
                }

                _clickPlayer.Stream = stream;
                _clickPlayer.Load();
                _clickPlayer.Play();
            }
            catch (Exception ex)
            {
                StopWithError("Sound Error", ex);
            }
        }

        private void Border_Click(object sender, MouseButtonEventArgs e)
        {
            try
            {
                var asm = Assembly.GetExecutingAssembly();
                Stream? stream = asm.GetManifestResourceStream("BBs_Universal_Mod_Downloader.Sounds.SecretSound.wav");

                if (stream == null)
                    return;

                _nyanOutput?.Stop();
                _nyanOutput?.Dispose();
                _nyanReader?.Dispose();
                _nyanVolume?.Dispose();

                _nyanReader = new WaveFileReader(stream);
                _nyanVolume = new WaveChannel32(_nyanReader)
                {
                    Volume = 0.5f
                };

                _nyanOutput = new WaveOutEvent();
                _nyanOutput.Init(_nyanVolume);
                _nyanOutput.Play();
            }
            catch (Exception ex)
            {
                StopWithError("Sound Error", ex);
            }
        }
    }
}