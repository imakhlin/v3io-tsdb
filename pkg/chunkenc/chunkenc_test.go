// +build unit

/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package chunkenc

import (
	"fmt"
	"testing"

	"encoding/base64"
	"math/rand"
	"time"
)

const basetime = 1524690488000

type sample struct {
	t int64
	v float64
}

// [132 180 199 187 191 88 63 240 - 0 0 0 0 0 0 154 8 - 194 95 255 108 7 126 113 172 - 46 18 195 104 59 202 237 129 - 119 243 146]

func TestXor(tst *testing.T) {
	tst.Skip("Needs to be refactored - Doesn't test anything")

	samples := GenSamples(1000, 5, 1000, 100)
	var byteArray []byte

	ch := NewXORChunk()
	appender, err := ch.Appender()
	if err != nil {
		tst.Fatal(err)
	}

	for i, s := range samples {
		fmt.Println("t,v: ", s.t, s.v)
		appender.Append(s.t, s.v)
		b := ch.Bytes()
		fmt.Println(b, len(b))
		byteArray = append(byteArray, b...)
		ch.Clear()
		if i == 4 {
			fmt.Println("restarted appender")
			ch = NewXORChunk()
			appender, err = ch.Appender()
			if err != nil {
				tst.Fatal(err)
			}

		}
	}

	fmt.Println("Samples:", len(samples), "byteArray:", byteArray, len(byteArray))

	ch2, err := FromData(EncXOR, byteArray, 0)
	if err != nil {
		tst.Fatal(err)
	}

	iter := ch2.Iterator()
	i := 0
	for iter.Next() {

		if iter.Err() != nil {
			tst.Fatal(iter.Err())
		}

		t, v := iter.At()
		isMatch := t == samples[i].t && v == samples[i].v
		fmt.Println("t, v, match: ", t, v, isMatch)
		if !isMatch {
			tst.Fatalf("iterator t or v doesnt match appended index %d len %d", i, len(samples))
		}
		i++
	}
	fmt.Println()

	if i != len(samples) {
		tst.Fatalf("number of iterator samples (%d) != num of appended (%d)", i, len(samples))
	}

}

func TestBstream(t *testing.T) {
	t.Skip("Needs to be refactored - Doesn't test anything")

	src := &bstream{count: 8, stream: []byte{0x55, 0x44, 0x33}}

	bs := newBWriter(8)
	byt, _ := src.readByte()
	bs.writeByte(byt)
	fmt.Println(bs.count, bs.stream)
	for i := 1; i < 18; i++ {
		bit, _ := src.readBit()
		fmt.Println(bs.count, bs.stream, bit)
		bs.writeBit(bit)
	}

	fmt.Println("Reading:")
	bs2 := &bstream{count: 8, stream: bs.stream}
	fmt.Println(bs2.count, bs2.stream)
	for i := 1; i < 18; i++ {
		bit, _ := bs2.readBit()
		fmt.Println(bs2.count, bs2.stream, bit)
	}

}

func TestDecodeBlob(t *testing.T) {

	blobs := []string{
		"+AFlbmLa6ECynAAAAAAAAAV+QNZc2exWgNhcAUKg0VoKMIBqK4fOpgiu0VoNpFiA9/+DfQajOTzwAIPWBJRg0VoIu0BR/F+6chZgzqYNhcfQajLwQOHUwJhw0VoLaQDuK0C/cF7sXdxBlFxIWKxP2Eq4Wbpc2l5QbC95gFfEUChCEEikazZioEU9WiJqLwqAQTRqM3dAXaBqOmdwAF6JgE+yAFD5AEqTAFRoAEsgAFtnAFwXAGgntmg/YZhHjkBCE4BAWQBaKABZUoBqOg/QT7SATOiAS9aARREAQJIATa4ASkQARKEARXEAR5wAaT9kGFkVQEQaAERJAE7vAEzWAE8IAEyFAOK/INKGKZjtQOC4psBFBoDwAHyDBPJY6K0A0oyvfPf+xQ8F3x/wAPkGBL5QRegA4r8gmRkA7UDgjVuAQ0EAXsKAXwuAQcAAQKuAQNVAzqYIlHDRWghBME+FgE0egEDPAE29gEtdQERtwM6mCOsY0VoILajOpggFkNFaC8z8zqYLkpTRWghYwEBywM6mC4R40VoICEBAIEBcNADh1MC9oMDuK0CO1OBnW5fS8Pf+xQ8F3x/wAPkGBL5QRegA4r8gmRkA7UDgjVuAQ0EAXsKAXwuAQcAAQKuAQNVAzqYIlHDRWghBME+FgE0egEDPAE29gEtdQERtwM6mCOsY0VoILajOpggFkNFaC8z8zqYLkpTRWghYwEBywM6mC4R40VoICEBAIEBcNADh1MC9oMDuK0CO1OBnW5fS8OxWgLvVh+mXYI4+wOZooIGSAPf/vhUEKuEA8AEHrAQiBQA=",
		"+AFlbs/FuECjiAAAAAAAAADqYNxN5GwuiIBqL22AajOwwGs39qBqO7wQVzYAWN4AaTbwIFrPchegbx7Iajd8IEdwW45Bl0KrWeRaDFIMXzNpP7j4XrQAWXGAWWQASmXAScKAQJJARiRATN6ASSuAR2+AQoIAWH4AXYgAaUPg1FvOoGgvTIBoN2wgaUNm5M6mCtgE0VoJ5lBPZ4DOpgjVAEmBANFaCAlAThcAQicATV+ATguAzqYJ7OD3/4rQBNjg8ADUZwRY4Pf/oMkEaSDRWggXYM6mCClg0VoJ60BP8gBPmABFPgDOpgjgkNFaCAFgzqYNB+oH0VoNCGVdgEefYEWDAEfhgEHsgM6mCMEo0VoLcOhaGQBEgwDOpghgwNFaC7OAWgNARANARFeAQf2AQWAAR8yAzqYI6MjRWgtRGPf+oHAFukDwAWbjBBDY5JPg0ob6SOxWgLMOQE+BAOK/IJxUAO1A4IBIAOOpgIuuAO4rQIOBAOOpgI8tAOqBwJpmAO4rQJ0cAEJ/AEYPwEBjQEg1gEVQAEpzgEXrgEy4AFMIQFCwQGgrb1sobR4IajvxEGhDCQRb/8BGrMBA+QBC0gBBSQBCpwBAXgBHr8BHeUBGnAA=",
		"APkGBL5QRegA4r8gmRkA7UDgjVuAQ0EAXsKAXwuAQcAAQKuAQNVAzqYIlHDRWghBME+FgE0egEDPAE29gEtdQERtwM6mCOsY0VoILajOpggFkNFaC8z8zqYLkpTRWghYwEBywM6mC4R40VoICEBAIEBcNADh1MC9oMDuK0CO1OBnW5fS8OxWgLvVh+mXYI4+wOZooIGSAPf/vhUEKuEA8AEHrAQiBQA=",
		"BfuQ7UDgvvA=",
	}

	hasFailures := false
	for i, b := range blobs {
		fmt.Printf("Decoding blob [%d]:\n", i)
		if err := decodeBlob(b); err != nil {
			hasFailures = true
			t.Errorf("Unable to decode blob[%d]. Error: %v", i, err)
		}
	}

	if hasFailures{
		t.Fatalf("The test has failed with errors. See error messages above.")
	}
}

func decodeBlob(blob string) error {

	//blob := "+AFjT7+iCEBLgAAAAAAA+AFjT8A6YEBQgAAAAAAA"

	data, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return err
	}
	fmt.Println(data)

	chunk, err := FromData(EncXOR, data, 0)
	if err != nil {
		return err
	}

	iter := chunk.Iterator()
	i := 0
	for iter.Next() {

		if iter.Err() != nil {
			return iter.Err()
		}

		t, v := iter.At()
		tstr := time.Unix(int64(t/1000), 0).Format(time.RFC3339)
		fmt.Printf("unix=%d, t=%s, v=%.4f \n", t, tstr, v)
		i++
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	if i == 0 {
		fmt.Println("Empty set!")
	}

	return nil
}

func GenSamples(num, interval int, start, step float64) []sample {
	samples := []sample{}
	curTime := int64(basetime)
	v := start

	for i := 0; i <= num; i++ {
		curTime += int64(interval * 1000)
		t := curTime + int64(rand.Intn(100)) - 50
		v += float64(rand.Intn(100)-50) / 100 * step
		//fmt.Printf("t-%d,v%.2f ", t, v)
		samples = append(samples, sample{t: t, v: v})
	}

	return samples
}
