package nsqd

import (
	"runtime"
	"runtime/metrics"
)

const (
	goGCHeapTinyAllocsObjects               = "/gc/heap/tiny/allocs:objects"
	goGCHeapAllocsObjects                   = "/gc/heap/allocs:objects"
	goGCHeapFreesObjects                    = "/gc/heap/frees:objects"
	goGCHeapAllocsBytes                     = "/gc/heap/allocs:bytes"
	goGCHeapObjects                         = "/gc/heap/objects:objects"
	goGCHeapGoalBytes                       = "/gc/heap/goal:bytes"
	goMemoryClassesTotalBytes               = "/memory/classes/total:bytes"
	goMemoryClassesHeapObjectsBytes         = "/memory/classes/heap/objects:bytes"
	goMemoryClassesHeapUnusedBytes          = "/memory/classes/heap/unused:bytes"
	goMemoryClassesHeapReleasedBytes        = "/memory/classes/heap/released:bytes"
	goMemoryClassesHeapFreeBytes            = "/memory/classes/heap/free:bytes"
	goMemoryClassesHeapStacksBytes          = "/memory/classes/heap/stacks:bytes"
	goMemoryClassesOSStacksBytes            = "/memory/classes/os-stacks:bytes"
	goMemoryClassesMetadataMSpanInuseBytes  = "/memory/classes/metadata/mspan/inuse:bytes"
	goMemoryClassesMetadataMSPanFreeBytes   = "/memory/classes/metadata/mspan/free:bytes"
	goMemoryClassesMetadataMCacheInuseBytes = "/memory/classes/metadata/mcache/inuse:bytes"
	goMemoryClassesMetadataMCacheFreeBytes  = "/memory/classes/metadata/mcache/free:bytes"
	goMemoryClassesProfilingBucketsBytes    = "/memory/classes/profiling/buckets:bytes"
	goMemoryClassesMetadataOtherBytes       = "/memory/classes/metadata/other:bytes"
	goMemoryClassesOtherBytes               = "/memory/classes/other:bytes"
)

// use new api(runtime.metrics) read go runtime info as default
const useNewMemCollection = true

//runtimeMetricsCollection read runtime info from runtime.metrics
//support since go-1.16(new api)
func runtimeMetricsCollection() runtime.MemStats {
	var ms runtime.MemStats
	memStatsFromRM(&ms, readMetrics())
	return ms
}

// runtimeMemStatsCollection this will STW
// read go runtime info :memory ,gc etc...
func runtimeMemStatsCollection() runtime.MemStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms
}

func memStatsFromRM(ms *runtime.MemStats, rm map[string]metrics.Sample) {
	lookupOrZero := func(name string) uint64 {
		if s, ok := rm[name]; ok {
			return s.Value.Uint64()
		}
		return 0
	}
	tinyAllocs := lookupOrZero(goGCHeapTinyAllocsObjects)
	ms.Mallocs = lookupOrZero(goGCHeapAllocsObjects) + tinyAllocs
	ms.Frees = lookupOrZero(goGCHeapFreesObjects) + tinyAllocs

	ms.TotalAlloc = lookupOrZero(goGCHeapAllocsBytes)
	ms.Sys = lookupOrZero(goMemoryClassesTotalBytes)
	ms.Lookups = 0 // Already always zero.
	ms.HeapAlloc = lookupOrZero(goMemoryClassesHeapObjectsBytes)
	ms.Alloc = ms.HeapAlloc
	ms.HeapInuse = ms.HeapAlloc + lookupOrZero(goMemoryClassesHeapUnusedBytes)
	ms.HeapReleased = lookupOrZero(goMemoryClassesHeapReleasedBytes)
	ms.HeapIdle = ms.HeapReleased + lookupOrZero(goMemoryClassesHeapFreeBytes)
	ms.HeapSys = ms.HeapInuse + ms.HeapIdle
	ms.HeapObjects = lookupOrZero(goGCHeapObjects)
	ms.StackInuse = lookupOrZero(goMemoryClassesHeapStacksBytes)
	ms.StackSys = ms.StackInuse + lookupOrZero(goMemoryClassesOSStacksBytes)
	ms.MSpanInuse = lookupOrZero(goMemoryClassesMetadataMSpanInuseBytes)
	ms.MSpanSys = ms.MSpanInuse + lookupOrZero(goMemoryClassesMetadataMSPanFreeBytes)
	ms.MCacheInuse = lookupOrZero(goMemoryClassesMetadataMCacheInuseBytes)
	ms.MCacheSys = ms.MCacheInuse + lookupOrZero(goMemoryClassesMetadataMCacheFreeBytes)
	ms.BuckHashSys = lookupOrZero(goMemoryClassesProfilingBucketsBytes)
	ms.GCSys = lookupOrZero(goMemoryClassesMetadataOtherBytes)
	ms.OtherSys = lookupOrZero(goMemoryClassesOtherBytes)
	ms.NextGC = lookupOrZero(goGCHeapGoalBytes)

	// N.B. GCCPUFraction is intentionally omitted. This metric is not useful,
	// and often misleading due to the fact that it's an average over the lifetime
	// of the process.
	// See https://github.com/prometheus/client_golang/issues/842#issuecomment-861812034
	// for more details.
	ms.GCCPUFraction = 0
}

func readMetrics() (result map[string]metrics.Sample) {
	result = map[string]metrics.Sample{}
	descriptions := metrics.All()
	samples := make([]metrics.Sample, len(descriptions))
	for i := range samples {
		samples[i].Name = descriptions[i].Name
	}

	metrics.Read(samples)
	for _, v := range samples {
		result[v.Name] = v
	}
	return result
}
