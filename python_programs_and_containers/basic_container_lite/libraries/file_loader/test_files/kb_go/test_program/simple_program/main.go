package main

import (
    "fmt"
    "os"
)

func greet(name string) string {
    return fmt.Sprintf("Hello, %s! Running on ARM64!", name)
}

func main() {
    var name string
    if len(os.Args) > 1 {
        name = os.Args[1]
    } else {
        name = "User"
    }
    fmt.Println(greet(name))
}

