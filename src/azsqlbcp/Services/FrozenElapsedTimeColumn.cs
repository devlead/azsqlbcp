using System.Collections.Concurrent;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace AzSqlBcp.Services;

/// <summary>
/// Like <see cref="ElapsedTimeColumn"/>, but freezes displayed elapsed time once a task is finished.
/// </summary>
public sealed class FrozenElapsedTimeColumn : ProgressColumn
{
    private readonly ConcurrentDictionary<ProgressTask, TimeSpan> _frozenElapsed = new();

    protected override bool NoWrap => true;

    public Style Style { get; set; } = Color.Blue;

    public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime)
    {
        if (task.IsFinished)
        {
            var elapsed = _frozenElapsed.GetOrAdd(
                task,
                _ => task.ElapsedTime ?? TimeSpan.Zero);

            return FormatElapsed(elapsed);
        }

        var live = task.ElapsedTime;
        return live is null
            ? new Markup("--:--:--")
            : FormatElapsed(live.Value);
    }

    public override int? GetColumnWidth(RenderOptions options) => 8;

    private IRenderable FormatElapsed(TimeSpan elapsed) =>
        elapsed.TotalHours > 99
            ? new Markup("**:**:**")
            : new Text($"{elapsed:hh\\:mm\\:ss}", Style);
}
