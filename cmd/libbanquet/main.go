package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"unsafe"

	"github.com/darianmavgo/banquet/bridge"
)

// main is required for c-shared build mode.
func main() {}

// BanquetParse parses a raw URL string and returns a JSON string representation of the Banquet object.
// The caller is responsible for freeing the returned C string using FreeString.
//
//export BanquetParse
func BanquetParse(url *C.char) *C.char {
	goStr := C.GoString(url)

	result, err := bridge.Parse(goStr)
	if err != nil {
		// Return error as JSON object with error field
		errObj := map[string]string{"error": err.Error()}
		jsonBytes, _ := json.Marshal(errObj)
		return C.CString(string(jsonBytes))
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		errObj := map[string]string{"error": "Failed to marshal result: " + err.Error()}
		jsonBytes, _ := json.Marshal(errObj)
		return C.CString(string(jsonBytes))
	}

	return C.CString(string(jsonBytes))
}

// FreeString frees the C string returned by BanquetParse.
//
//export FreeString
func FreeString(str *C.char) {
	C.free(unsafe.Pointer(str))
}
