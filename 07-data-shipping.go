package main

import (
	"fmt"
	"strconv"
)

// The type(s) representing a confined integer
// represents a request to add something to the integer
type addRequest struct {
	delta    int // how much we want to add
	response chan int // a channel to send the request back to whomever made it
}

type getRequest struct {
	// response channel again
	response chan int
}

type value struct {
	// a channel of requests should be able to carry any type of requests
	requests chan any
}

////////////////////////////////////////
// Methods on value

// // Add something to the value
func (v value) add(delta int) {
	// create a response channel - private channel 
	response := make(chan int)
	// ship the data we send a delta and a private channel to requests
	v.requests <- addRequest{delta, response}
	// wait for the response
	<-response
}

// // Get the current value
func (v value) get() int {
	// once again we make a private channel to receive response
	response := make(chan int)
	// ship the response channel to requests
	v.requests <- getRequest{response}
	// return whatever response we get back in our private channel
	return <-response
}

// Terminate the value
func (v value) done() {
	close(v.requests)
}

// Create values
func makeValue() value {
	// value has only a requests channel 
	var result value

	// Create the request channel and hand it to result; the actor
    // goroutine below captures this same channel to receive on.
	requests := make(chan any)
	// we keep it bidirectional - but technically should only send requests
	result.requests = requests

	// This implements the "value" actor
	go func() {
		// owner still owns val
		val := 0 // Confined to this goroutine!

		for req := range requests {
			// this is a type switch - what type of request did I receive?
			switch req := req.(type) {
			case addRequest:
				// if this request is an add request, add delta
				val = val + req.delta
				// send the val back to the private response channel
				req.response <- val
			case getRequest:
				req.response <- val
			}
		}
	}()

	return result
}

// Given a value, add 1,000,000 to it, one at a time
func count(val value, label string) {

	fmt.Printf("%v init val %v\n", label, val.get())

	for i := 0; i < 1000000; i++ {
		val.add(1)
	}

	fmt.Printf("%v final val %v\n", label, val.get())
}

func main() {
	val := makeValue() // abstraction: a confined integer
	defer val.done()   // clean up val's closure

	d := make(chan any) // Tracks when counters are done

	for i := 0; i < 4; i++ {
		go func() {
			defer func() { d <- nil }()
			count(val, "counter"+strconv.Itoa(i))
		}()
	}

	for i := 0; i < 4; i++ {
		<-d // Read from all counters
	}
	fmt.Printf("Done! val is %v\n", val.get())
}
