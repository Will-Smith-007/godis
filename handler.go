package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

// hset sets a field in a hash with a given value, creating the hash if it does not exist.
// Returns an "OK" string response on success or an error Value on incorrect arguments.
func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hset' command",
		}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMutex.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMutex.Unlock()

	return Value{
		typ: "string",
		str: "OK",
	}
}

// hget retrieves the value associated with a field in a hash. Returns a null Value if the field does not exist.
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hget' command",
		}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMutex.RLock()
	value, ok := HSETs[hash][key]
	HSETsMutex.RUnlock()

	if !ok {
		return Value{
			typ: "null",
		}
	}

	return Value{
		typ:  "bulk",
		bulk: value,
	}
}

// hgetall retrieves all fields and values from a hash and returns them as an array of alternating field-value pairs.
// If the hash does not exist, it returns an empty array. Errors are returned for incorrect argument count.
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'hgetall' command",
		}
	}

	hash := args[0].bulk

	HSETsMutex.RLock()
	hashMap, ok := HSETs[hash]
	HSETsMutex.RUnlock()

	if !ok {
		// Return an empty array if the hash doesn't exist
		return Value{
			typ:   "array",
			array: []Value{},
		}
	}

	// Create an array with alternating field-value pairs
	result := make([]Value, 0, len(hashMap)*2)
	for field, value := range hashMap {
		// Add field
		result = append(result, Value{
			typ:  "bulk",
			bulk: field,
		})
		// Add value
		result = append(result, Value{
			typ:  "bulk",
			bulk: value,
		})
	}

	return Value{
		typ:   "array",
		array: result,
	}
}

// set stores a key-value pair in memory and returns an "OK" string response on success or an error Value on failure.
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'set' command",
		}
	}

	key := args[0].bulk
	value := args[1].bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return Value{
		typ: "string",
		str: "OK",
	}
}

// get retrieves the value associated with a given key from the in-memory store.
// Returns a null Value if the key does not exist or an error Value for invalid arguments.
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{
			typ: "error",
			str: "ERR wrong number of arguments for 'get' command",
		}
	}

	key := args[0].bulk

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return Value{
			typ: "null",
		}
	}

	return Value{
		typ:  "bulk",
		bulk: value,
	}
}

// ping handles a PING command by returning "PONG" or the supplied argument as a string response.
func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{
			typ: "string",
			str: "PONG",
		}
	}

	return Value{
		typ: "string",
		str: args[0].bulk,
	}
}
