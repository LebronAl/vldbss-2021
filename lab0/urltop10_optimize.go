package main

import (
	"bytes"
	"fmt"
	"strings"
)

// URLTop10 .
func OptimizeURLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    OptimizeURLTop10Map,
		ReduceFunc: OptimizeURLTop10Reduce,
		NReduce:    1,
	})
	return args
}

func OptimizeURLTop10Map(filename string, contents string) []KeyValue {
	us, cs := OptimizeTopN(FormatData(strings.Split(contents, "\n")), 10)
	kvs := make([]KeyValue, 0, len(us))
	for i := range us {
		kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %d\n", us[i], cs[i])})
	}
	return kvs
}

func OptimizeURLTop10Reduce(key string, values []string) string {
	us, cs := OptimizeTopN(FormatData(values), 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
