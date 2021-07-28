.PHONY: all

all: test_example test_homework test_optimize cleanup gendata

test_example:
	go test -v -run=TestExampleURLTop

test_homework:
	go test -v -run=TestURLTop

test_optimize:
	go test -v -run=TestOptimizeURLTop

cleanup:
	go test -v -run=TestCleanData

gendata:
	go test -v -run=TestGenData
