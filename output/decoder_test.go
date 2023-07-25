//  Fluent Bit Go!
//  ==============
//  Copyright (C) 2015-2017 Treasure Data Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package output

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

const (
	debugLevel = 11

	newTestDataFile = "./testdata/data.example"
)

func TestGetRecordNew(t *testing.T) {

	dummyRecordNew, err := os.ReadFile(newTestDataFile)
	if err != nil {
		log.Fatal(err)
	}

	arrayPtr := (*[14140]byte)(dummyRecordNew)

	dec := NewDecoder(unsafe.Pointer(arrayPtr), len(dummyRecordNew))
	if dec == nil {
		t.Fatal("dec is nil")
	}

	ret, timestamp, record := GetRecord(dec)
	if ret < 0 {
		t.Fatal("ret is negative")
	}

	// test timestamp
	ts, ok := timestamp.(time.Time)
	if !ok {
		t.Fatalf("cast error. Type is %s", reflect.TypeOf(timestamp))
	}

	if ts.Unix() != int64(0x5ea917e0) {
		t.Errorf("ts.Unix() error. given %d", ts.Unix())
	}

	// test record
	v, ok := record["schema"].(int64)
	if !ok {
		t.Fatalf("cast error. Type is %s", reflect.TypeOf(record["schema"]))
	}
	if v != 1 {
		t.Errorf(`record["schema"] is not 1 %d`, v)
	}
}

// extractTimeStamp extract the timestamp out of the MsgPack data for verification
func extractTimeStamp(data []byte) (timestamp time.Time) {

	// Manually find the time.
	//
	// ( Possibly there's a bug here https://github.com/fluent/fluent-bit-go/blob/master/output/decoder.go#L63, where it should be 1 not 0)
	//
	// The first headers of the data coming from Fluentbit looks like this
	//	das@t:~/Downloads/siden-edge-node/deployments/local/fluentbit/db$ ~/Downloads/msgpack-inspect ./data
	// ---
	// - format: "array32"
	//   header: "0xdd"
	//   length: 2
	//   children:
	//     - format: "array32"
	//       header: "0xdd"
	//       length: 2
	//       children:
	//         - format: "fixext8"
	//           header: "0xd7"
	//           exttype: 0
	//           length: 8
	//           data: "0x64a46baa019bfcc0"
	//         - format: "fixmap"
	//           header: "0x80"
	//           length: 0
	//           children: []
	// Array32 is 5 bytes long, and the fixext8 has x2 byte header, so that's where the time field starts
	// fixext8 has 8 bytes of data
	// Therefore, time bits start at: 5 + 5 + 2 = 12
	// Time bits are 8 long, so 12 + 8 = 20
	// See also: https://github.com/msgpack/msgpack/blob/master/spec.md
	//
	timeEightBytes := data[12:20] // extract bytes 12 through 20
	sec := binary.BigEndian.Uint32(timeEightBytes)
	usec := binary.BigEndian.Uint32(timeEightBytes[4:])
	timestamp = time.Unix(int64(sec), int64(usec))
	if debugLevel > 10 {
		fmt.Println("timestamp:", timestamp.Format(time.RFC850))
	}

	return timestamp
}
