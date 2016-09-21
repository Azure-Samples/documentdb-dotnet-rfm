using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocumentDBRFMConsoleApp
{
    class RfmDoc
    {
        [JsonProperty("eid")]
        public int Eid { get; set; }

        [JsonProperty("cid")]
        public string Cid { get; set; }

        [JsonProperty("uid")]
        public string Uid { get; set; }

        [JsonProperty("time")]
        public int Time { get; set; }

        [JsonProperty("src_evt")]
        public string SourceEvent { get; set; }

        [JsonProperty("cat")]
        public string Cat { get; set; }

        [JsonProperty("obj")]
        public string Obj { get; set; }
    }
}
