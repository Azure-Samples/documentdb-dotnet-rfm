// Run this as stored procedure.
// Input:
//    { entity: { name: "", value: ... }, feature: { name: "", value: ...} }
// Output (updates the doc):
//    { entity: { name: "", value: "" }, feature: { name: "", value: ...}, isMetadata: true, aggregates: { "count": ..., "min": ... } }
function updateFeature(doc) {
    var typedArrays = typeof ArrayBuffer !== "undefined";

    const AGGREGATE = {
        min: "min",
        max: "max",
        count_unique: "count_unique",
        count_unique_hll: "count_unique_hll",
        exists: "exists",
        count_min: "count_min",
    };

    const ERROR_CODES = {
        BAD_REQUEST: 400,
        NOT_FOUND: 404,
        CONFLICT: 409,
        RETRY_WITH: 449,
        NOT_ACCEPTED: 499
    };

    const FEATURES = { "cat": [AGGREGATE.count_unique, AGGREGATE.count_unique_hll, AGGREGATE.exists, AGGREGATE.count_min], "src_evt": AGGREGATE.count_unique, "time": [AGGREGATE.min, AGGREGATE.max], "obj": AGGREGATE.count_unique_hll };

    if (!doc || typeof (doc) !== "object") throw new Error(ERROR_CODES.BAD_REQUEST, "The 'doc' parameter is not specified or is not an object.");
    if (!doc.entity || !doc.entity.name || !doc.entity.value) throw new Error(ERROR_CODES.BAD_REQUEST, "The document's 'entity' property is not specified or is not an {name: ..., value:...} object.");
    if (!doc.feature || !doc.feature.name || !doc.feature.value) throw new Error(ERROR_CODES.BAD_REQUEST, "The document's 'feature' property is not specified or is not an {name: ..., value:...} object..");

    let aggregates = FEATURES[doc.feature.name];
    if (aggregates !== undefined) {
        if (!Array.isArray(aggregates)) aggregates = [aggregates];

        let metaDocId = composeMetaDocId(doc.entity, doc.feature.name);
        let metaDocLink = __.getAltLink() + '/docs/' + metaDocId;
        let isAccepted = __.readDocument(metaDocLink, {}, function (err, metaDoc, options) {
            if (err) {
                if (err.number == ERROR_CODES.NOT_FOUND) {
                    let result = updateMetatada(metaDoc, doc);
                    metaDoc = result.doc;
                    // Insert meta doc.
                    isAccepted = __.createDocument(__.getSelfLink(), metaDoc, {}, function (err, body, options) {
                        if (err) {
                            if (err.number == ERROR_CODES.CONFLICT) _throw(ERROR_CODES.RETRY_WITH, "One of meta documents was created by another transaction. Retry from client: " + err.message);
                            else _throw(err.number, err.message);
                        }
                    });
                    if (!isAccepted) _throwNotAccepted();
                } else _thow(ERROR_CODES.BAD_REQUEST, err.message);
            } else {
                let result = updateMetatada(metaDoc, doc);
                metaDoc = result.doc;
                if (result.isUpdated) {
                    // Replace meta doc.
                    isAccepted = __.replaceDocument(metaDocLink, metaDoc, {}, function (err, body, options) {
                        if (err) {
                            if (err.number == ERROR_CODES.RETRY_WITH) _throw(ERROR_CODES.RETRY_WITH, "One of meta documents was modified by another transaction. Retry from client: " + err.message);
                            else _throw(err.number, err.message);
                        }
                    });
                    if (!isAccepted) _throwNotAccepted();
                }
            }
        }); // readDocument.
        if (!isAccepted) _throwNotAccepted();
    }

    function updateMetatada(metaDoc, doc) {
        let isUpdated = false;
        if (!metaDoc) {
            metaDoc = new MetaDoc(doc);
            isUpdated = true;
        }

        aggregates.forEach(function (agg) {
            let aggData = metaDoc.aggregates[agg];
            switch (agg) {
                case AGGREGATE.count_unique:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new CountUniqueData();
                    if (aggData.uniqueValues[doc.feature.value] === undefined) {
                        aggData.uniqueValues[doc.feature.value] = 0;    // Insert unique value as key.
                        ++aggData.value;
                        if (!isUpdated) isUpdated = true;
                    }
                    break;
                case AGGREGATE.count_unique_hll:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new CountUniqueHLLData();
                    aggData.hll = new HyperLogLog(aggData.hll.std_error, murmurhash3_32_gc, aggData.hll.M);

                    let oldValue = aggData.value = aggData.hll.count();
                    aggData.hll.count(doc.feature.value); // add entity to hll
                    aggData.value = aggData.hll.count();

                    if (aggData.value !== oldValue && !isUpdated) isUpdated = true;
                    break;
                case AGGREGATE.min:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new AggregateData();
                    if (aggData.value === undefined) aggData.value = doc.feature.value;
                    else if (doc.feature.value < aggData.value) {
                        aggData.value = doc.feature.value;
                        if (!isUpdated) isUpdated = true;
                    }
                    break;
                case AGGREGATE.max:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new AggregateData();
                    if (aggData.value === undefined) aggData.value = doc.feature.value;
                    else if (doc.feature.value > aggData.value) {
                        aggData.value = doc.feature.value;
                        if (!isUpdated) isUpdated = true;
                    }
                    break;
                case AGGREGATE.exists:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new BF();
                    aggData.bf = new BloomFilter(1000, 4, aggData.bf.buckets);

                    let oldExistsValue = aggData.value = aggData.bf.add(doc.feature.value);
                    aggData.bf.add(doc.feature.value);
                    aggData.value = aggData.bf.test(doc.feature.value);

                    if (aggData.value !== oldExistsValue && !isUpdated) isUpdated = true;
                    break;
                case AGGREGATE.count_min:
                    if (aggData === undefined) aggData = metaDoc.aggregates[agg] = new FrequencyData();
                    aggData.cms = new CountMinSketch(aggData.cms.accuracy, aggData.cms.probIncorrect, aggData.cms.hashFunc, aggData.cms.table);

                    let oldFrequencyValue = aggData.value = aggData.cms.query(doc.feature.value);
                    aggData.cms.update(doc.feature.value, 1);
                    aggData.value = aggData.cms.query(doc.feature.value);

                    if (aggData.value !== oldFrequencyValue && !isUpdated) isUpdated = true;
                    break;
            }
        });

        return { doc: metaDoc, isUpdated: isUpdated };
    }

    function AggregateData() {   // Base class for aggregates.
        this.value = undefined;
    };

    function CountUniqueData() { // Class for AGGREGATE.count_unique.
        this.value = 0;
        this.uniqueValues = new Object(); // Hash set.
    };

    function CountUniqueHLLData() {
        this.hll = new HyperLogLog(0.175, murmurhash3_32_gc); // Change the std_error here. Set to .005 for maximum accuracy.
        this.value = 0;

        // Add element to HLL
        this.add = function (v) {
            this.hll.count(v);
        }

        // Return the cardinality estimate
        this.count = function () {
            return Math.round(this.hll.count());
        }
    }

    function BF() {
        this.bf = new BloomFilter();
        this.value = 0;

        // Add an element to the set
        this.add = function (v) {
            this.bf.add(v)
        }

        // Test if an item is in the set
        // If the test returns false, the element is not a member of the set.
        // If the test returns true, the element may or may not be a member of the set.
        this.test = function (v) {
            return this.bf.test(v)
        }
    }

    function FrequencyData() {
        this.cms = new CountMinSketch();

        // Add event to Count-Min Sketch
        this.add = function (v) {
            this.cms.update(v, 1);
        }

        // Return the frequency estimate for event of type v
        this.query = function (v) {
            return this.cms.query(v);
        }
    }

    function MetaDoc(doc) {
        return {
            id: composeMetaDocId(doc.entity, doc.feature.name),
            entity: { name: doc.entity.name, value: doc.entity.value },
            feature: { name: doc.feature.name },
            aggregates: {},
            isMetadata: true,
        }
    };

    function composeMetaDocId(entity, featureName) {
        if (!entity) _throw(ERROR_CODES.BAD_REQUEST, "The 'entity' parameter must be specified");
        if (!featureName) _throw(ERROR_CODES.BAD_REQUEST, "The 'featureName' parameter must be specified");
        return "_en=" + entity.name + ".ev=" + entity.value + ".fn=" + featureName;
    }

    function _throw(number, message) {
        throw new Error(JSON.stringify({ number: number, message: message }));
    }

    function _throwNotAccepted() {
        _throw(ERROR_CODES.NOT_ACCEPTED, "The request was not accepted due to time/CPU budget. Retry from the client.");
    }

    function murmurhash3_32_gc(key, seed) {
        var remainder, bytes, h1, h1b, c1, c1b, c2, c2b, k1, i;

        remainder = key.length & 3; // key.length % 4
        bytes = key.length - remainder;
        h1 = seed ? seed : 1;
        c1 = 0xcc9e2d51;
        c2 = 0x1b873593;
        i = 0;

        while (i < bytes) {
            k1 =
              ((key.charCodeAt(i) & 0xff)) |
              ((key.charCodeAt(++i) & 0xff) << 8) |
              ((key.charCodeAt(++i) & 0xff) << 16) |
              ((key.charCodeAt(++i) & 0xff) << 24);
            ++i;

            k1 = ((((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16))) & 0xffffffff;
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 = ((((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16))) & 0xffffffff;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);
            h1b = ((((h1 & 0xffff) * 5) + ((((h1 >>> 16) * 5) & 0xffff) << 16))) & 0xffffffff;
            h1 = (((h1b & 0xffff) + 0x6b64) + ((((h1b >>> 16) + 0xe654) & 0xffff) << 16));
        }

        k1 = 0;

        switch (remainder) {
            case 3: k1 ^= (key.charCodeAt(i + 2) & 0xff) << 16;
            case 2: k1 ^= (key.charCodeAt(i + 1) & 0xff) << 8;
            case 1: k1 ^= (key.charCodeAt(i) & 0xff);

                k1 = (((k1 & 0xffff) * c1) + ((((k1 >>> 16) * c1) & 0xffff) << 16)) & 0xffffffff;
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 = (((k1 & 0xffff) * c2) + ((((k1 >>> 16) * c2) & 0xffff) << 16)) & 0xffffffff;
                h1 ^= k1;
        }

        h1 ^= key.length;

        h1 ^= h1 >>> 16;
        h1 = (((h1 & 0xffff) * 0x85ebca6b) + ((((h1 >>> 16) * 0x85ebca6b) & 0xffff) << 16)) & 0xffffffff;
        h1 ^= h1 >>> 13;
        h1 = ((((h1 & 0xffff) * 0xc2b2ae35) + ((((h1 >>> 16) * 0xc2b2ae35) & 0xffff) << 16))) & 0xffffffff;
        h1 ^= h1 >>> 16;

        return h1 >>> 0;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////
    // HyperLogLog
    // 
    // Modified from terrancesnyder, 2013.
    // https://github.com/terrancesnyder/jam-libraries/blob/79240da36699cc3b7c91dee7cd743549d40c8297/stream-lib/hyperloglog/hyperloglog.js 
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Arguments:
    // @std_error: standard error level acceptable for your dataset. Set to .005 for maximum accuracy. Increase this to ~0.015 depending on 
    //             your set size. 
    // @hashFunc: hash function. Default currently uses FNW hash. 
    // @bytes: (optional) the bytes to populate the HLL with, used when doing intersect, union, etc.
    function HyperLogLog(std_error, hashFunc, bytes) {
        const pow_2_32 = 0xFFFFFFFF + 1;

        function fnv1a_hash(text) {
            var hash = 2166136261;
            for (var i = 0; i < text.length; ++i) {
                hash ^= text.charCodeAt(i);
                hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
            }
            return hash >>> 0;
        }

        function log2(x) {
            return Math.log(x) / Math.LN2;
        }

        function rank(hash, max) {
            var r = 1;
            while ((hash & 1) == 0 && r <= max) {
                ++r;
                hash >>>= 1;
            }
            return r;
        }

        // check if hash function supplied. If not, default to fnv hash
        hashFunc = hashFunc ? hashFunc : fnv1a_hash;

        const m1 = 1.04 / std_error;
        const k = Math.ceil(log2(m1 * m1));
        const k_comp = 32 - k;
        const m = Math.pow(2, k);

        const alpha_m = m == 16 ? 0.673
            : m == 32 ? 0.697
            : m == 64 ? 0.709
            : 0.7213 / (1 + 1.079 / m);
        // setup of bitset
        var M = [];
        if (!bytes) {
            for (var i = 0; i < m; ++i) {
                M[i] = 0;
            }
        } else {
            M = bytes;
        }

        function count(stringValue) {
            if (stringValue !== undefined && stringValue) {
                var hash = hashFunc(stringValue);
                var j = hash >>> k_comp;
                M[j] = Math.max(M[j], rank(hash, k_comp));
            }
            else {
                var c = 0.0;
                for (var i = 0; i < m; ++i) {
                    c += 1 / Math.pow(2, M[i]);
                }
                var E = alpha_m * m * m / c;

                // -- make corrections
                if (E <= 5 / 2 * m) {
                    var V = 0;
                    for (var i = 0; i < m; ++i) if (M[i] == 0)++V;
                    if (V > 0) E = m * Math.log(m / V);
                }
                else if (E > 1 / 30 * pow_2_32)
                    E = -pow_2_32 * Math.log(1 - E / pow_2_32);

                return E;
            }
        }

        return {
            // The count function to call with either the word to add to the count, or
            // with null to get the total count.
            M: M,
            std_error: std_error,
            count: count,
            union: function (ll) {
                if (ll == null) return this;
                var mergedBytes = M.slice(0); // clone array
                for (var i = 0; i < mergedBytes.length; i++) {
                    mergedBytes[i] = Math.max(mergedBytes[i], ll.M[i]);
                }
                return HyperLogLog(std_error, hashFunc, mergedBytes);
            },
            intersect: function (ll) {
                if (ll == null) return this;
                var mergedBytes = M.slice(0); // clone array
                for (var i = 0; i < mergedBytes.length; i++) {
                    mergedBytes[i] = Math.min(mergedBytes[i], ll.M[i]);
                }
                return HyperLogLog(std_error, hashFunc, mergedBytes);
            },
            without: function (ll) {
                if (ll == null) return this;
                var mergedBytes = M.slice(0); // clone array
                for (var i = 0; i < mergedBytes.length; i++) {
                    mergedBytes[i] = mergedBytes[i] > ll.M[i] ? mergedBytes[i] : 0
                }
                return HyperLogLog(std_error, hashFunc, mergedBytes);
            }
        };
    }

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Bloom Filter
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Fowler/Noll/Vo non-cryptographic hashing
    // See https://web.archive.org/web/20131019013225/http://home.comcast.net/~bretm/hash/6.html
    function fnv_1a(v) {
        var a = 2166136261; // 32 bit offset_basis
        for (var i = 0, n = v.length; i < n; ++i) {
            var c = v.charCodeAt(i),
                d = c & 0xff00;
            if (d) a = fnv_multiply(a ^ d >> 8);
            a = fnv_multiply(a ^ c & 0xff);
        }
        return fnv_mix(a);
    }

    // a * 16777619 mod 2**32
    function fnv_multiply(a) {
        return a + (a << 1) + (a << 4) + (a << 7) + (a << 8) + (a << 24);
    }

    // One additional iteration of FNV, given a hash.
    function fnv_1a_b(a) {
        return fnv_mix(fnv_multiply(a));
    }

    function fnv_mix(a) {
        a += a << 13;
        a ^= a >>> 7;
        a += a << 3;
        a ^= a >>> 17;
        a += a << 5;
        return a & 0xffffffff;
    }

    // Probabilistic data structure used to test whether an element is a member of a set.
    // m = number of bits in the bloom filter/bit vector
    // k = number of hash functions
    function BloomFilter(m, k, oldBuckets) {
        m = m || 1000;
        k = k || 4;
        var a;
        if (typeof m !== "number") {
            a = m;
            m = a.length * 32;
        }

        const n = Math.ceil(m / 32);
        var i = -1;
        var m = m = n * 32;

        if (typedArrays) {
            var kbytes = 1 << Math.ceil(Math.log(Math.ceil(Math.log(m) / Math.LN2 / 8)) / Math.LN2),
                array = kbytes === 1 ? Uint8Array : kbytes === 2 ? Uint16Array : Uint32Array,
                kbuffer = new ArrayBuffer(kbytes * k)
            if (!oldBuckets) {
                var buckets = new Int32Array(n);
                if (a) while (++i < n) buckets[i] = a[i];
            } else {
                var buckets = oldBuckets;
            }
            if (a) while (++i < n) buckets[i] = a[i];
            var _locations = new array(kbuffer);
        } else {
            if (!oldBuckets) {
                var buckets = [];
                if (a) while (++i < n) buckets[i] = a[i];
                else while (++i < n) buckets[i] = 0;
                var _locations = [];
            } else {
                buckets = oldBuckets;
            }
        }

        // Add item to bloom filter.
        // Feed item to k hash functions and set the bits.
        function add(v) {
            var l = locations(v + "");
            for (var i = 0; i < k; ++i) {
                buckets[Math.floor(l[i] / 32)] |= 1 << (l[i] % 32);
            };
        }

        function locations(v) {
            var r = _locations;
            var a = fnv_1a(v);
            var b = fnv_1a_b(a);
            var x = a % m;

            for (var i = 0; i < k; ++i) {
                r[i] = x < 0 ? (x + m) : x;
                x = (x + b) % m;
            }
            return r;
        }

        // Test if an item is in the bloom filter.
        // Feed the item to k hash functions and see if any of the bits are not set.
        // If the test returns false, the element is not a member of the set.
        // If the test returns true, the element may or may not be a member of the set.
        // False positive rate = (1 - e^(-kn/m)) ^ k 
        // k = number of hash functions
        // m = number of bits in the bloom filter
        // n = number of members in the set
        function test(v) {
            var l = locations(v + "");
            for (var i = 0; i < k; ++i) {
                var b = l[i];
                if ((buckets[Math.floor(b / 32)] & (1 << (b % 32))) === 0) {
                    return false;
                }
            }
            return true;
        }

        return {
            add: add,
            buckets: buckets,
            locations: locations,
            test: test
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Count-Min Sketch
    //////////////////////////////////////////////////////////////////////////////////////////////
    var A;
    if (typeof Uint32Array === undefined) {
        A = [0];
    } else {
        A = new Uint32Array(1);
    }

    function hashInt(x) {
        A[0] = x | 0
        A[0] -= (A[0] << 6)
        A[0] ^= (A[0] >>> 17)
        A[0] -= (A[0] << 9)
        A[0] ^= (A[0] << 4)
        A[0] -= (A[0] << 3)
        A[0] ^= (A[0] << 10)
        A[0] ^= (A[0] >>> 15)
        return A[0]
    }

    var defaultHash;
    if (typeof Float64Array !== "undefined") {
        //Typed array version
        var DOUBLE_BUFFER = new Float64Array(1)
        var INT_VIEW = new Uint32Array(DOUBLE_BUFFER.buffer)
        defaultHash = function hashTypedArray(key, bins) {
            var d = bins.length
            if (typeof key === "number") {
                if (key === key | 0) {
                    var b = hashInt(key)
                    bins[0] = b
                    for (var i = 1; i < d; ++i) {
                        b = hashInt(b)
                        bins[i] = b
                    }
                } else {
                    DOUBLE_BUFFER[0] = key
                    var b = hashInt(INT_VIEW[0] + hashInt(INT_VIEW[1]))
                    bins[0] = b
                    for (var i = 1; i < d; ++i) {
                        b = hashInt(b)
                        scratch[i] = b
                    }
                }
            } else if (typeof key === "string") {
                for (var i = 0; i < d; ++i) {
                    bins[i] = murmurhash3_32_gc(key, i)
                }
            } else if (typeof key === "object") {
                var str
                if (key.toString) {
                    str = key.toString()
                } else {
                    str = JSON.stringify(key)
                }
                for (var i = 0; i < d; ++i) {
                    bins[i] = murmurhash3_32_gcr(str, i)
                }
            } else {
                var str = key + ""
                for (var i = 0; i < d; ++i) {
                    bins[i] = murmurhash3_32_gc(str, i)
                }
            }
        }
    } else {
        //Untyped version
        defaultHash = function hashNoTypedArray(key, bins) {
            var d = bins.length
            if (typeof key === "number") {
                if (key === key | 0) {
                    var b = hashInt(key)
                    bins[0] = b
                    for (var i = 0; i < d; ++i) {
                        b = hashInt(b)
                        bins[i] = b
                    }
                    return
                }
            } else if (typeof key === "string") {
                for (var i = 0; i < d; ++i) {
                    bins[i] = murmurhash3_32_gc(key, i)
                }
                return
            } else if (typeof key === "object") {
                var str
                if (key.toString) {
                    str = key.toString()
                } else {
                    str = JSON.stingify(key)
                }
                for (var i = 0; i < d; ++i) {
                    bins[i] = murmurhash3_32_gc(str, i)
                }
                return
            }
            var str = key + ""
            for (var i = 0; i < d; ++i) {
                bins[i] = murmurhash3_32_gc(str, i)
            }
        }
    }

    function CountMinSketch(accuracy, probIncorrect, hashFunc, oldTable) {
        accuracy = accuracy || 0.1
        probIncorrect = probIncorrect || 0.0001
        hashFunc = hashFunc || defaultHash

        var width = Math.ceil(Math.E / accuracy) | 0
        var depth = Math.ceil(-Math.log(probIncorrect)) | 0
        var table;
        var scratch;
        if (typeof Uint32Array === undefined) {
            if (!oldTable) {
                table = new Array(width * depth)
                for (var i = 0, n = table.length; i < n; ++i) {
                    table[i] = 0;
                }
            } else {
                table = oldTable;
            }

            scratch = new Array(depth);
            for (var i = 0; i < depth; ++i) {
                scratch[i] = 0;
            }
        } else {
            if (!oldTable) {
                table = new Uint32Array(width * depth);
            } else {
                table = oldTable;
            }
            scratch = new Uint32Array(depth);
        }

        function update(key, v) {
            var d = depth;
            var w = width;
            var ptr = 0;
            hashFunc(key, scratch);
            for (var i = 0; i < d; ++i) {
                table[ptr + (scratch[i] % w)] += v;
                ptr += w;
            }
        }

        function query(key) {
            var d = depth;
            var w = width;
            var ptr = w;
            hashFunc(key, scratch);
            var r = table[scratch[0] % w];
            for (var i = 1; i < d; ++i) {
                r = Math.min(r, table[ptr + (scratch[i] % w)]);
                ptr += w;
            }

            return r;
        }

        return {
            update: update,
            query: query,
            table: table,
            accuracy: accuracy,
            probIncorrect: probIncorrect,
            hashFunc: hashFunc,
        }
    }
}
