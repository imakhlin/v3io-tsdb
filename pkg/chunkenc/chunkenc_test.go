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
		"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",
		"BfuQ7UDgvvA=",
		"+AFlbtGiSEDQz4AAAAAAAADqYNpUvms7aXBPDoBrPqs4RHQAXmDAXX8AQkXAUBbAzqYK3gDRWgvjkPf/kiMGo+9OgPAAbd0GpDDxQPAAST4GlHOx4OitAL2RQO4rQIbbwEG3AEED4F3bIF1QgECW4F/xYEXAwEIJgF0dwEWAIEGRoF/ygOOpgL3WAO1A4LmEgNFaC/cw4r8gu5YA6oHAgw6A6ZdgvFiA8ABJPgXWDEePQFvBAOOpgLtFAO1A4ILYgEH5wNFaC4zkQEKgQDBAWYBARbFASqLAVCEgXQYgXurgX9ogR/jARCeARYSAQKoARamAXpXAXbjAQxSAXW8AQfGAXGVAXWhgzqYLgDTRWgg/GEpqwEj/QEXUwEWnAEUWAET5QEIjAEEZAM6mCDX40VoIKODtQOCPLgD3/74VBbXQ8AC3GwQcxvf/oMkEBaJZLsBOHoBO6oBN/4BNfoBFsoBCIoBDIgBGMgBBV4BNIABXlyBZ1iBYIqBZ16BLrwBLYABAngBOGQBAjQBPCABDdYBCP4BBewBG6gBKEQBDfABK9cBHiEBAVIBFCMBNkcBPD8BckiBdoGBAoEBBlIBGLEBN/QBFLAA=",
		"+AFlbmIEEECuDAAAAAAAAADqYNhPlGs2keBLeEqDQZBqLiqAU4BTjGo67XBpP8rIWhGATMkASmlARFxAQ++ARvIARIOAQESAQSyAR6VAQ7fAR7oAR59AXLPAW1GAQlaARW4ARY8ARTDAXP5AXN7AQAFARi6AQFwAaUNJhF/7oEagAEUIAEHHgEYsAEZfwEs2QEwMgEKJgED/gFzKwF4owEeSAEregE3RAEb/gEahAEH1AEeAQEC4wEJ3gEdkAEVIgEUZAEKaAEAwgEGAAEKCgEOcgEdywGlHHgJfhVBHCsBc4mBdw6BBTwBLYABM+IBGcYBHdgDOpgvXZNFaCwA88ABX5AQ69Pf/MuwEHezwAIPWBAeo7itAhZKAXB8gWz4gWbagQMfAXcngQd7AQIaARftARoIAXR8gzqYIP9zRWgvEwEC0AF8cIF9QYEKiAFzlIElUQEtWYF0zwFp98EGVYOOpgIX3oO1A4JgKQNFaCNhubEOp5Gdf1JLYXkTKwEPkWQBAvNAAR5LYAEaTAABG17AA8ACD1gQHqO4rQIWSgFwfIFs+IFm2oEDHwF3J4EHewECGgEX7QEaCAF0fIM6mCD/c0VoLxMBAtABfHCBfUGBCogBc5SBJVEBLVmBdM8BaffBBlWDjqYCF96DtQOCYCkDRWgjYbmxDqeRnX9SS2F5EysBD5FkAQLzQAEeS2ABGkwAARtewAPf/kiMEZf/A8ABfNwQGBwDivyCB6JgA0VoIAGOAzqYIV6GA",
	}

	hasFailures := false
	for i, b := range blobs {
		fmt.Printf("Decoding blob [%d]:\n", i)
		if err := decodeBlob(b); err != nil {
			hasFailures = true
			t.Errorf("Unable to decode blob[%d]. Error: %v", i, err)
		}
	}

	if hasFailures {
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
