/*****************************
 * Records
 *****************************/
public record BuildData(
    string Version,
    bool IsMainBranch,
    bool ShouldNotPublish,
    bool IsLocalBuild,
    bool IsRunningOnGitHubActions,
    string? WorkflowRef,
    DirectoryPath ProjectRoot,
    FilePath ProjectPath,
    DotNetMSBuildSettings MSBuildSettings,
    DirectoryPath ArtifactsPath,
    DirectoryPath OutputPath)
{
    private const string IntegrationTest = "integrationtest";

    public DirectoryPath NuGetOutputPath { get; } = OutputPath.Combine("nuget");

    public DirectoryPath IntegrationTestPath { get; } = OutputPath.Combine(IntegrationTest);

    public string? GitHubNuGetSource { get; } = System.Environment.GetEnvironmentVariable("GH_PACKAGES_NUGET_SOURCE");
    public string? GitHubNuGetApiKey { get; } = System.Environment.GetEnvironmentVariable("GITHUB_TOKEN");

    public bool ShouldPushGitHubPackages() => !ShouldNotPublish
        && !string.IsNullOrWhiteSpace(GitHubNuGetSource)
        && !string.IsNullOrWhiteSpace(GitHubNuGetApiKey);

    public string? NuGetSource { get; } = System.Environment.GetEnvironmentVariable("NUGET_SOURCE");
    public string? NuGetApiUser { get; } = System.Environment.GetEnvironmentVariable("NUGET_USER");
    public string? NuGetApiKey { get; set; } = System.Environment.GetEnvironmentVariable("NUGET_APIKEY");

    public bool ShouldLoginNuGet() =>
        !ShouldNotPublish
        && IsRunningOnGitHubActions
        && (IsMainBranch
            || (WorkflowRef?.StartsWith("refs/tags/v", StringComparison.Ordinal) ?? false));

    public bool ShouldPushNuGetPackages() => IsMainBranch
        && !ShouldNotPublish
        && !string.IsNullOrWhiteSpace(NuGetSource)
        && !string.IsNullOrWhiteSpace(NuGetApiKey);

    public ICollection<DirectoryPath> DirectoryPathsToClean { get; } =
    [
        ArtifactsPath,
        OutputPath,
        OutputPath.Combine(IntegrationTest)
    ];
}

internal record ExtensionHelper(Func<string, CakeTaskBuilder> TaskCreate, Func<CakeReport> Run);
