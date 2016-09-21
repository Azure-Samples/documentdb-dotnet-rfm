# DocumentDB RFM Console Application


This console application creates a JavaScript stored procedure that featurizes and uploads a list of events to DocumentDB, updates the appropriate data structures to maintain feature data, and shows how to obtain the RFM features for a particular ID.

For a more details about the sample scenario and a complete end-to-end walkthrough of creating this application, please refer to our Azure blog post Real-Time Feature Engineering for Machine Learning with DocumentDB. 

## Running this sample
1. Before you can run this sample, you must have the following prerequisites:
    - An active Azure DocumentDB account - If you don't have an account, refer to the [Create a DocumentDB account](https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/) article.
    - Visual Studio 2013 (or higher).
2. Clone this repository using Git for Windows (http://www.git-scm.com/), or download the zip file.
3. From Visual Studio, open the `DocumentDBRFMConsoleApp.sln` from the root directory.
4. In Visual Studio Build menu, select **Build Solution** (or Press F6).
5. In the `Program.cs` file, located in the DocumentDBRFMConsoleApp directory, find **endpoint** and **authKey** and replace the placeholder values with the values obtained for your account. For more information on obtaining endpoint & keys for your DocumentDB account refer to [How to manage a DocumentDB account](https://azure.microsoft.com/en-us/documentation/articles/documentdb-manage-account/#keys). If you don't have an account, see [Create a DocumentDB database account](https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/) to set one up.
	```
    const string Endpoint = "~your DocumentDB endpoint here~";
    const string AuthKey = "~your auth key here~";
    ```
6. You can now run and debug the application locally by pressing **F5** in Visual Studio.

## About the code
The code included in this sample is intended to get you quickly started with a console application that demonstrates how to upload and featurize a list of events to DocumentDB, use a stored procedure to update RFM feature metadata, and retrieve RFM features for a particular ID.

In this approach, we used the combination of <entity name, entity value, feature name> as the primary key for each document. An example primary key with this strategy is <"eid", 1, "cat".> This means that we created a separate document for each feature we wanted to keep track of when the student enrollment ID is 1.

#### /DocumentDBRFMConsoleApp/Program.cs
Contains the source code for the RFM console application.

#### /DocumentDBRFMConsoleApp/Docs.cs
Contains list of events to be featurized and stored in DocumentDB. EAch event details an action a student completed. All events consist of a timestamp, a course ID (cid), student ID (uid), and enrollment ID (eid) which is unique for each course-student pair.

#### /DocumentDBRFMConsoleApp/updateFeature.js
Runs as a stored procedure. Takes as input a row of the form **{ entity: { name: “ ”, value: …}, feature: { name: “ ”, value: …} }** and updates the relevant feature metadata to produce a document of the form **{ entity: { name: "", value: "" }, feature: { name: "", value: ...}, isMetadata: true, aggregates: { "count": ..., "min": ... } }**. 

## More information
- [Azure DocumentDB Documentation](https://azure.microsoft.com/documentation/services/documentdb/)
- [Azure DocumentDB .NET SDK](https://www.nuget.org/packages/Microsoft.Azure.DocumentDB/)
- [Azure DocumentDB .NET SDK Reference Documentation](https://msdn.microsoft.com/library/azure/dn948556.aspx)

## References
* HyperLogLog data structure adapted from https://gist.github.com/terrancesnyder/3398489 and http://stackoverflow.com/questions/5990713/loglog-and-hyperloglog-algorithms-for-counting-of-large-cardinalities.
* Bloom Filter data structure adapted from https://github.com/jasondavies/bloomfilter.js.
* Count-Min Sketch data structure adapted from https://github.com/mikolalysenko/count-min-sketch.




