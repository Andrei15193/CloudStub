Console.WriteLine("Checking if tests have been included in the release build...");

if (typeof(CloudStub.AzureDataTables.TableServiceClientStub).Assembly.GetTypes().Any(type => type.FullName?.Contains(".Tests.") ?? false))
{
    Console.Error.WriteLine("Error: Test types have been included in the release build!");
    Environment.Exit(1);
}
else
{
    Console.WriteLine("Success: No test types have been found in the release build.");
    Environment.Exit(0);
}