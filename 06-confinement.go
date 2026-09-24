package main

import (
	"fmt"
	"strconv"
)

// The type representing a confined integer - containing 2 channels
type value struct {
	// this is a send only channel requests
	requests  chan<- request
	// receive only channel responses
	responses <-chan any
}

// request is a function that receives an int* and returns any
// it knows how to operate on an integer and return some result
type request func(*int) any

////////////////////////////////////////
// Methods on value

// // Add something to the value
func (v value) add(delta int) {
	fn := func(val *int) any {
		*val = *val + delta
		return nil
	}

	v.operation(fn) // Ignores returned nil
}

// // Get the current value
func (v value) get() int {
	fn := func(val *int) any {
		return *val // This is of type int
	}

	return v.operation(fn).(int) // So this is correct
}

// Terminate the value
func (v value) done() {
	close(v.requests)
}

// Perform any operation on a value
func (v value) operation(fn request) any {
	v.requests <- fn
	return <-v.responses
}

// Create values
func makeValue() value {
	// result has requests and responses
	var result value

	// The closure needs access to the bidirectional versions
	// of the request and response channels, so that it can
	// read requests and write responses - this can be treated like a server for example
	requests := make(chan request)
	responses := make(chan any)

	// This assignment restricts the direction of requests/reponses to
	// the outside world
	// result.requests is of type chan<- request send ONLY
	result.requests = requests
	result.responses = responses

	// This implements the "value" actor - this OWNS the val 
	go func() {
		val := 0 // Confined to this goroutine!

		// aReq can be add/get/a function basically is sent to requests - ONLY way to interact with it
		for aReq := range requests {
			// The supplied function gets a reference to val
			// And we return its result as the response
			// response can either be nil or it can be a get value that's why any !
			responses <- aReq(&val) // we send the response back to responses channel
		}

		// If we get here the channel was closed
		return
	}()
	
	// caller gets value which are two channels, send only requests and receive only responses
	return result
}

// Given a value, add 1,000,000 to it, one at a time
func count(val value, label string) {

	fmt.Printf("%v init val %v\n", label, val.get())

	// each worker gets the value abstraction
	for i := 0; i < 1000000; i++ {
		val.add(1)
	}

	fmt.Printf("%v final val %v\n", label, val.get())
}

func main() {
	// Create a single integer value called val
	val := makeValue() // abstraction: a confined integer
	defer val.done()   // clean up val's closure

	d := make(chan any) // Tracks when counters are done

	// 4 goroutines each increment 1MIL times
	for i := 0; i < 4; i++ {
		go func() {
			// when a worker finishes, this defer function runs and sends a nil to d
			defer func() { d <- nil }()
			count(val, "counter"+strconv.Itoa(i))
		}()
	}

	for i := 0; i < 4; i++ {
		// this reads the nil sent when the goroutine in main the worker counter completes
		<-d // Read from all counters
	}
	fmt.Printf("Done! val is %v\n", val.get())
}
