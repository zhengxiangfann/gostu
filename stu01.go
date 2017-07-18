package main

import (
	"errors"
	"fmt"
	"time"
)

func f1(arg int) (int, error) {
	if arg == 42 {
		return -1, errors.New("can't work with 42")
	}
	return arg + 3, nil
}

type argError struct {
	arg  int
	prob string
}

func (e *argError) Error() string {
	return fmt.Sprintf("%d - %s", e.arg, e.prob)
}

func f2(arg int) (int, error) {
	if arg == 42 {
		return -1, &argError{arg, "can't work with it"}
	}
	return arg + 3, nil
}

func demo1() {
	for _, i := range []int{7, 42} {
		if r, e := f1(i); e != nil {
			fmt.Println("f1 failed:", e)
		} else {
			fmt.Println("f1 worked:", r)
		}
	}
	for _, i := range []int{7, 42} {
		if r, e := f2(i); e != nil {
			fmt.Println("f2 failed:", e)
		} else {
			fmt.Println("f2 worked:", r)
		}
	}

	_, e := f2(42)
	if ae, ok := e.(*argError); ok {
		fmt.Println(ae.arg)
		fmt.Println(ae.prob)
	}
}

func f(from string) {
	for i := 0; i < 3; i++ {
		fmt.Println(from, ":", i)
	}
}

func demo2() {

	f("direct")
	go f("goroutine")

	go func(msg string) {
		fmt.Println(msg)
	}("going")

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")

}

func demo3() {

	message := make(chan string)

	go func() {
		message <- "ping"
	}()

	msg := <-message

	fmt.Println(msg)
}

func demo4() {

	messages := make(chan string, 2)
	messages <- "buffered"
	messages <- "channel"

	fmt.Println(<-messages)
	fmt.Println(<-messages)

}

func worker(done chan bool) {
	fmt.Println("working...")
	time.Sleep(time.Second)
	fmt.Println("done")

	done <- true
}

func demo5() {

	done := make(chan bool, 1)
	go worker(done)
	<-done
}

func ping(pings chan<- string, msg string) {
	pings <- msg
}

func pong(pings <-chan string, pongs chan<- string) {
	msg := <-pings
	pongs <- msg
}

func main() {

	// a := <-done
	// fmt.Println(a)
	// pings := make(chan string, 1)
	// pongs := make(chan string, 1)
	// ping(pings, "passed message")
	// pong(pings, pongs)
	// fmt.Println(<-pongs)

	// c1 := make(chan string)
	// c2 := make(chan string)

	// go func() {
	// 	time.Sleep(time.Second * 1)
	// 	c1 <- "one"
	// }()

	// go func() {
	// 	time.Sleep(time.Second * 2)
	// 	c2 <- "two"
	// }()

	// for i := 0; i < 2; i++ {
	// 	select {
	// 	case msg1 := <-c1:
	// 		fmt.Println("received", msg1)
	// 	case msg2 := <-c2:
	// 		fmt.Println("received", msg2)
	// 	}
	// }

	c1 := make(chan string, 1)
	go func() {
		time.Sleep(time.Second * 2)
		c1 <- "result 1"
	}()

	select {
	case res := <-c1:
		fmt.Println(res)
	case <-time.After(time.Second * 1):
		fmt.Println("timeout 1")
	}

	c2 := make(chan string, 1)
	go func() {
		time.Sleep(time.Second * 2)
		c2 <- "result 2"
	}()

	select {
	case res := <-c2:
		fmt.Println(res)
	case <-time.After(time.Second * 3):
		fmt.Println("timeout 2")
	}

}
