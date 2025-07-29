package greet

import "fmt"

// Greet returns a greeting message for the given name.
func Greet(name string) string {
    return fmt.Sprintf("Hello, %s! From the greet module!", name)
}

