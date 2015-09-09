package main

import (
    "github.com/powellchristoph/rsekafka"

    "fmt"
)

func main() {
    // main

    client := rsekafka.NewClient([]string{"localhost:9092"})

    /*
        Key, Value []byte
        Topic      string
        Partition  int32
        Offset     int64
    */

    client.Post("Test", "This is a test")
    messages, _ := client.Get("Test", 0)
    fmt.Println("Messages:", len(messages))
    for _, message := range messages {
        //fmt.Println("Consumed message with offset", message.Offset)
        fmt.Println("Value:", string(message.Value))
        fmt.Println("Key:", message.Key)
        fmt.Println("Topic:", message.Topic)
        fmt.Println("Partition:", message.Partition)
        fmt.Println("Offset:", message.Offset)
    }
}
