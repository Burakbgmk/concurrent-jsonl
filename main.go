package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/simonfrey/jsonl"
)

type Player struct {
	Name string
	Wins [][]string
}

type Product struct {
	Id          int64
	Title       string
	Price       float32
	Category    string
	Brand       string
	Url         string
	Description string
}

func main() {
	fmt.Println("Hello")
	// input, err := os.ReadFile("example.jsonl")
	input, err := os.ReadFile("products-1.jsonl")
	if err != nil {
		panic(err)
	}

	fmt.Println("----Normal----")
	start := time.Now()
	parseJsonl(input)
	fmt.Println(time.Since(start))
	fmt.Println("----Concurrent----")
	start = time.Now()
	parseJsonlConcurrent(input)
	fmt.Println(time.Since(start))
	fmt.Println("----Concurrent with jsonl----")
	start = time.Now()
	parseJsonlConcurrentWithJsonl(input)
	fmt.Println(time.Since(start))

}

func parseJsonl(body []byte) {

	r := jsonl.NewReader(strings.NewReader(string(body)))
	var ps []Product
	err := r.ReadLines(func(data []byte) error {
		p := Product{}
		err := json.Unmarshal(data, &p)
		if err != nil {
			return err
		}
		ps = append(ps, p)

		return nil
	})
	if err != nil {
		log.Printf("Failed to unmarshal, %v", err)
	}
	fmt.Println("Length: ", len(ps))

}

func parseJsonlConcurrentWithJsonl(input []byte) {
	var lineBreakMap []int

	r := jsonl.NewReader(strings.NewReader(string(input)))

	current := 0
	err := r.ReadLines(func(data []byte) error {
		new := current + len(data)
		lineBreakMap = append(lineBreakMap, new)
		current = new + 1

		return nil
	})
	if err != nil {
		panic(err)
	}

	var lineNumber int = len(lineBreakMap)

	ps := make([]Product, lineNumber)

	var wg sync.WaitGroup
	for i, pos := range lineBreakMap {
		wg.Add(1)
		go func() {
			var line []byte
			if i == 0 {
				line = input[0:pos]
			} else {
				line = input[lineBreakMap[i-1]+1 : pos]
			}
			p := Product{}
			err := json.Unmarshal(line, &p)
			if err != nil {
				log.Printf("Cannot unmarshal: %v", err)
			}
			ps[i] = p

			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("Length: ", len(ps))
}

func parseJsonlConcurrent(input []byte) {
	lineBreak := byte('\n')
	var lineBreakMap []int

	for i, b := range input {
		if b == lineBreak {
			lineBreakMap = append(lineBreakMap, i)
		} else if i == len(input)-1 {
			lineBreakMap = append(lineBreakMap, i+1)
		}
	}
	var lineNumber int = len(lineBreakMap)

	ps := make([]Product, lineNumber)

	var wg sync.WaitGroup
	for i, pos := range lineBreakMap {
		wg.Add(1)
		go func() {
			var line []byte
			if i == 0 {
				line = input[0:pos]
			} else {
				line = input[lineBreakMap[i-1]+1 : pos]
			}
			p := Product{}
			err := json.Unmarshal(line, &p)
			if err != nil {
				log.Printf("Cannot unmarshal: %v", err)
			}
			ps[i] = p

			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println("Length: ", len(ps))

}
