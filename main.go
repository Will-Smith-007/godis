package main

import (
	"io"
	"log"
	"net"
	"strings"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix("godis: ")

	log.Println("Started godis server on port :6379")

	server, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	defer func(server net.Listener) {
		err := server.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(server)

	aof, err := CreateAOF("database.aof")
	if err != nil {
		log.Fatal(err)
	}
	defer func(aof *AOF) {
		err := aof.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(aof)

	err = aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			log.Println("Invalid command:", command)
			return
		}
		handler(args)
	})

	// Accept multiple connections
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go handleConnection(conn, aof)
	}
}

func handleConnection(conn net.Conn, aof *AOF) {
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing connection:", err)
		}
		log.Println("Closed client connection")
	}()

	log.Println("Received client connection")

	for {
		resp := CreateResp(conn)
		val, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err)
			return
		}

		if val.typ != "array" {
			log.Println("Invalid request, expected array")
			continue
		}

		if len(val.array) == 0 {
			log.Println("Invalid request, expected array with at least one element")
			continue
		}

		command := strings.ToUpper(val.array[0].bulk)
		args := val.array[1:]

		writer := CreateWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			err := writer.Write(Value{typ: "string", str: ""})
			if command == "COMMAND" {
				continue
			}
			log.Println("Invalid command:", command)
			if err != nil {
				log.Println("Failed to write response:", err)
				return
			}
			continue
		}

		if command == "SET" || command == "HSET" || command == "DEL" {
			err := aof.Write(val)
			if err != nil {
				log.Println("ERR can not write to AOF, changes will be lost after server restart:", err)
			}
		}

		result := handler(args)
		err = writer.Write(result)
		if err != nil {
			log.Println("Failed to write response:", err)
			return
		}
	}
}
