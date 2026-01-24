## ToDo

* Replace this with the goal of the library, provide a stub for Azure Table Storage for all kinds of testing. The aim is to make it as close as possible to the real thing making them almost indistinguishable. If there are any errors returned by the stub they should also be returned by the related SDK and vice-versa containing the same details. This allows the stub to be used reliably in all kinds of test scenarios without having to actually encounter them with an actual service. The tests are mostly exhaustive to everything that is possible.
* Add a quick getting started tutorial to make it easy for people to set this up. Include basic unit test examples and dotnet integration test examples.
* The list from below is for developers, might get removed or moved under a "contributing" section.
* Details the idea behidn the "CloudStub" prefix.

Required Software
-----------------

* dotnet 10.0 SDK
* Visual Studio Code

For local development check `TestRunContext.cs`, provide your own values to use the debugger and integrated test runner otherwise it can be a pain.

Remarks
-------

The new SDK requires dates to be specified in UTC when adding or updating entities.