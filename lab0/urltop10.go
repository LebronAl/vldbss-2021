package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make(map[string]int)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs[l]++
	}
	results := make([]KeyValue, 0, len(kvs))
	for k, v := range kvs {
		results = append(results, KeyValue{k, strconv.Itoa(v)})
	}
	return results
}

func URLCountReduce(key string, values []string) string {
	sum := 0
	for _, value := range values {
		num, err := strconv.Atoi(value)
		if err != nil {
			panic(err)
		}
		sum += num
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

func URLTop10Map(filename string, contents string) []KeyValue {
	us, cs := TopN(FormatData(strings.Split(contents, "\n")), 10)
	kvs := make([]KeyValue, 0, len(us))
	for i := range us {
		kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %d\n", us[i], cs[i])})
	}
	return kvs
}

func URLTop10Reduce(key string, values []string) string {
	us, cs := TopN(FormatData(values), 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}

func FormatData(values []string) map[string]int {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}
	return cnts
}
