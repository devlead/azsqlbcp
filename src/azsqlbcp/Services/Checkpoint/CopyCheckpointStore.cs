using System.Text.Json;
using System.Text.Json.Serialization;

namespace AzSqlBcp.Services.Checkpoint;

public sealed class CopyCheckpointStore : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    private readonly string _path;
    private readonly FileStream _lockStream;
    private readonly object _writeLock = new();

    private CopyCheckpointStore(string path, FileStream lockStream)
    {
        _path = path;
        _lockStream = lockStream;
    }

    public static CopyCheckpointStore Open(string path)
    {
        var fullPath = Path.GetFullPath(path);
        var directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        var lockPath = fullPath + ".lock";
        var lockStream = new FileStream(
            lockPath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None);

        return new CopyCheckpointStore(fullPath, lockStream);
    }

    public bool Exists => File.Exists(_path) && new FileInfo(_path).Length > 0;

    public CopyCheckpoint Load()
    {
        if (!Exists)
            throw new InvalidOperationException($"Checkpoint file not found: {_path}");

        var json = File.ReadAllText(_path);
        var checkpoint = JsonSerializer.Deserialize<CopyCheckpoint>(json, JsonOptions)
            ?? throw new InvalidOperationException("Checkpoint file is invalid.");

        if (checkpoint.Version != CopyCheckpoint.CurrentVersion)
            throw new InvalidOperationException($"Unsupported checkpoint version {checkpoint.Version}.");

        return checkpoint;
    }

    public void Save(CopyCheckpoint checkpoint)
    {
        lock (_writeLock)
        {
            checkpoint.Version = CopyCheckpoint.CurrentVersion;
            var json = JsonSerializer.Serialize(checkpoint, JsonOptions);
            var directory = Path.GetDirectoryName(_path)!;
            var tempPath = Path.Combine(directory, $".{Path.GetFileName(_path)}.{Guid.NewGuid():N}.tmp");

            try
            {
                File.WriteAllText(tempPath, json);
                File.Move(tempPath, _path, overwrite: true);
            }
            finally
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
        }
    }

    public void Dispose()
    {
        _lockStream.Dispose();
    }
}
