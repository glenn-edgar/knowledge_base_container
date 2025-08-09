package main

import (
    "fmt"
    "testmod/greet"
)

func main() {
    name := "User"
    message := greet.Greet(name)
    fmt.Println(message)
}

