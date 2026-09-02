using System.Text.Json.Serialization;

namespace AzSqlBcp.Services.Checkpoint;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum PartitionStatus
{
    Pending,
    InProgress,
    Completed,
    Failed
}
