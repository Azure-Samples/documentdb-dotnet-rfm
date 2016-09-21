using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DocumentDBRFMConsoleApp
{
    public partial class Program
    {
        const string DbName = "db";
        const string CollectionName = "c";

        const string Endpoint = "~your DocumentDB endpoint here~";
        const string AuthKey = "~your auth key here~";

        private static int s_conflictCount = 0;

        enum ErrorCodes { RetryWith = 449, ClientClosedRequest = 499 };

        // This console application creates a sproc that updates RFM feature metadata, featurizes and
        // uploads a list of events to DocumentDB, and shows how to obtain the HyperLogLog results for
        // a particular id (<en, ev, fn> combination where en = entity name, ev = entity value, 
        // fn = feature name). This approach uses <en, ev, fn> as the primary key. Another approach
        // that was considered involved using <en, ev> as the primary key. The approach taken here 
        // (primary key = <en, ev, fn>) provides for simpler code and fewer conflicts. The trade-off
        // here is throughput.
        public static void Main(string[] args)
        {
            Console.WriteLine("Using {0}, {1}, {2}", Endpoint, DbName, CollectionName);

            // Setting the DefaultConnectionLimit to a higher limit allows for more parallel database connections,
            // this increasing write speed. Set this value to approximately the number of threads used by your application.
            System.Net.ServicePointManager.DefaultConnectionLimit = 2; // Default is 2;
            // SetMinThreads and SetMaxThreads are used for varying the degree of parallelism of your requests so that the
            // client spends little time waiting between requests.
            ThreadPool.SetMinThreads(32, 32); // 32
            ThreadPool.SetMaxThreads(32, 32); // 32

            CreateSproc().Wait();

            UpdateMLMetadata();

            OutputResults();
        }

        private static async Task CreateSproc()
        {
            string scriptFileName = @"updateFeature.js";
            string scriptName = "updateFeature";
            string scriptId = Path.GetFileNameWithoutExtension(scriptFileName);

            var client = new DocumentClient(new Uri(Endpoint), AuthKey);
            Uri collectionLink = UriFactory.CreateDocumentCollectionUri(DbName, CollectionName);

            var sproc = new StoredProcedure
            {
                Id = scriptId,
                Body = File.ReadAllText(scriptFileName)
            };
            Uri sprocUri = UriFactory.CreateStoredProcedureUri(DbName, CollectionName, scriptName);

            bool needToCreate = false;

            try
            {
                await client.ReadStoredProcedureAsync(sprocUri);
            }
            catch (DocumentClientException de)
            {
                if (de.StatusCode != HttpStatusCode.NotFound)
                {
                    throw;
                }
                else
                {
                    needToCreate = true;
                }
            }

            if (needToCreate)
            {
                await client.CreateStoredProcedureAsync(collectionLink, sproc);
            }
        }

        private static void OutputResults()
        {
            var client = new DocumentClient(new Uri(Endpoint), AuthKey);
            Uri collectionLink = UriFactory.CreateDocumentCollectionUri(DbName, CollectionName);

            string queryText = "select c.aggregates.count_unique_hll[\"value\"] from c where c.id = \"_en=eid.ev=1.fn=obj\"";
            var query = client.CreateDocumentQuery(collectionLink, queryText);

            Console.WriteLine("Result: {0}", query.ToList()[0]);
        }

        private static void UpdateMLMetadata()
        {
            var client = new DocumentClient(new Uri(Endpoint), AuthKey);

            var requestCharge = new List<double>();
            var resultTimes = new List<double>();
            int i = 0;
            Stopwatch swAll = Stopwatch.StartNew();

            int batchSize = 10;

            while (true)
            {
                RfmDoc[] docs = GetNextBatch(s_docs, ref i, batchSize);
                if (docs.Length == 0) break;

                Stopwatch sw = Stopwatch.StartNew();

                var tasks = new List<Task<StoredProcedureResponse<string>>>();
                foreach (var doc in docs)
                {
                    string[] rows = Featurize(doc);
                    foreach (string row in rows)
                    {
                        tasks.Add(UpdateRFMMetadata(client, row));
                    }
                }
                Task.WaitAll(tasks.ToArray());

                tasks.ForEach(x =>
                {
                    requestCharge.Add(x.Result.RequestCharge);
                });

                double currentPerDocTimeMs = sw.ElapsedMilliseconds / batchSize;
                resultTimes.Add(currentPerDocTimeMs);

                if (i % 25 == 0)
                {
                    Console.WriteLine("{0} updated. {1} conflicts. {2} total time (s). {3} ms per doc.", i, s_conflictCount, swAll.ElapsedMilliseconds / 1000, currentPerDocTimeMs);
                }
            }

            StringBuilder sb = new StringBuilder();
            resultTimes.ForEach((x) => { sb.Append(x); sb.Append(','); });
            Console.WriteLine("Stats: {0}", sb.ToString());

            double totalCharge = 0;
            requestCharge.ForEach(x => totalCharge += x);
            Console.WriteLine("Time: {0} s, Charge (RUs): total: {1}, per sec: {2}", swAll.ElapsedMilliseconds / 1000, totalCharge, totalCharge / (swAll.ElapsedMilliseconds / 1000));
        }

        private static async Task<StoredProcedureResponse<string>> UpdateRFMMetadata(DocumentClient client, string metaDoc)
        {
            object metaDocObj = JsonConvert.DeserializeObject(metaDoc);

            int retryCount = 100;
            while (retryCount > 0)
            {
                try
                {
                    Uri sprocUri = UriFactory.CreateStoredProcedureUri(DbName, CollectionName, "updateFeature");
                    var task = client.ExecuteStoredProcedureAsync<string>(
                        sprocUri,
                        metaDocObj);
                    return await task;
                }
                catch (DocumentClientException ex)
                {
                    DocumentClientException dce = ex;
                    if (dce == null || dce.Error == null) throw;
                    bool bConflict = dce.Error.Message.Contains("\\\"number\\\":499");
                    bool bRetryWith = dce.Error.Message.Contains("\\\"number\\\":449");
                    if (!bConflict && !bRetryWith) throw;

                    Interlocked.Increment(ref s_conflictCount);
                }
                catch (AggregateException ex)
                {
                    Exception temp = ex;
                    while (temp != null && !(temp is DocumentClientException))
                    {
                        temp = temp.InnerException;
                    }
                    DocumentClientException dce = temp as DocumentClientException;
                    if (dce == null || dce.Error == null || !dce.Error.Message.StartsWith("{")) throw;

                    RfmError rfmError = JsonConvert.DeserializeObject<RfmError>(dce.Error.Message);
                    if (rfmError.Number != (int)ErrorCodes.RetryWith || rfmError.Number != (int)ErrorCodes.ClientClosedRequest) throw;
                }
                --retryCount;
            }

            throw new Exception("All retries are over. Giving up...");
        }

        private static RfmDoc[] GetNextBatch(RfmDoc[] docs, ref int index, int batchSize = 10)
        {
            List<RfmDoc> result = new List<RfmDoc>();
            while (index < docs.Length && batchSize > 0)
            {
                result.Add(docs[index]);
                ++index;
                --batchSize;
            }
            return result.ToArray();
        }

        private static string[] Featurize(RfmDoc doc)
        {
            List<string> result = new List<string>();

            var entities = new Tuple<string, object>[] { new Tuple<string, object>("eid", doc.Eid), new Tuple<string, object>("cid", doc.Cid), 
                new Tuple<string, object>("uid", doc.Uid) };
            var features = new Tuple<string, object>[] { new Tuple<string, object>("time", doc.Time), new Tuple<string, object>("src_evt", doc.SourceEvent), 
                new Tuple<string, object>("cat", doc.Cat), new Tuple<string, object>("obj", doc.Obj) };

            foreach (var entity in entities)
            {
                foreach (var feature in features)
                {
                    StringBuilder eb = new StringBuilder();
                    StringBuilder fb = new StringBuilder();
                    StringWriter eWriter = new StringWriter(eb);
                    StringWriter fWriter = new StringWriter(fb);

                    JsonSerializer s = new JsonSerializer();
                    s.Serialize(eWriter, entity.Item2);
                    string eValue = eb.ToString();

                    s.Serialize(fWriter, feature.Item2);
                    string fValue = fb.ToString();

                    var value = string.Format(CultureInfo.InvariantCulture, "{{\"entity\":{{\"name\":\"{0}\",\"value\":{1}}},\"feature\":{{\"name\":\"{2}\",\"value\":{3}}}}}",
                        entity.Item1, eValue, feature.Item1, fValue);
                    result.Add(value);
                }
            }

            return result.ToArray();
        }

        class RfmError
        {
            [JsonProperty("number")]
            public int Number { get; set; }

            [JsonProperty("message")]
            public string Message { get; set; }
        }
    }
}
