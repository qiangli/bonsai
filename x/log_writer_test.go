/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	lw := &LogWriter{
		FilePath: path,
		MaxSize:  1,
		MaxAge:   1,
		Compress: false,
	}

	var err error
	lw, err = lw.Init()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lw.Close()) })
	writeToLogWriterAndVerify(t, lw, path)
}

func TestLogWriterWithCompression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	lw := &LogWriter{
		FilePath: path,
		MaxSize:  1,
		MaxAge:   1,
		Compress: true,
	}

	var err error
	lw, err = lw.Init()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lw.Close()) })
	writeToLogWriterAndVerify(t, lw, path)
}

// if this test failed and you changed anything, please check the dgraph audit decrypt command.
// The dgraph audit decrypt command uses the same decryption method
func TestLogWriterWithEncryption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log.enc")
	lw := &LogWriter{
		FilePath:      path,
		MaxSize:       1,
		MaxAge:        1,
		Compress:      false,
		EncryptionKey: []byte("1234567890123456"), // 16 bytes
	}

	var err error
	lw, err = lw.Init()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lw.Close()) })
	msg := []byte("abcd")
	msg = bytes.Repeat(msg, 256)
	msg[1023] = '\n'
	for range 10000 {
		n, err := lw.Write(msg)
		require.NoError(t, err)
		require.Equal(t, n, len(msg)+20, "write length is not equal")
	}

	require.NoError(t, lw.Close())
	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	outPath := filepath.Join(filepath.Dir(path), "audit_out.log")
	outfile, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	require.NoError(t, err)
	defer func() { require.NoError(t, outfile.Close()) }()

	block, err := aes.NewCipher(lw.EncryptionKey)
	require.NoError(t, err)
	stat, err := os.Stat(path)
	require.NoError(t, err)
	iv := make([]byte, aes.BlockSize)
	_, err = file.ReadAt(iv, 0)
	require.NoError(t, err)

	var iterator int64 = 16
	for {
		length := make([]byte, 4)
		_, err = file.ReadAt(length, iterator)
		require.Nil(t, err)
		iterator = iterator + 4
		content := make([]byte, binary.BigEndian.Uint32(length))
		_, err = file.ReadAt(content, iterator)
		require.NoError(t, err)
		iterator = iterator + int64(binary.BigEndian.Uint32(length))
		stream := cipher.NewCTR(block, iv)
		stream.XORKeyStream(content, content)
		//require.True(t, bytes.Equal(content, msg))
		_, err = outfile.Write(content)
		require.NoError(t, err)
		if iterator >= stat.Size() {
			break
		}
		iv := make([]byte, 16)
		_, err = file.ReadAt(iv, iterator)
		require.NoError(t, err)
		iterator = iterator + 16
	}
}

func writeToLogWriterAndVerify(t *testing.T, lw *LogWriter, path string) {
	msg := []byte("abcd")
	msg = bytes.Repeat(msg, 256)
	msg[1023] = '\n'
	var writers sync.WaitGroup
	errors := make(chan error, 10)
	for range 10 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for range 1000 {
				n, err := lw.Write(msg)
				if err != nil {
					errors <- err
					return
				}
				if n != len(msg) {
					errors <- fmt.Errorf("write length %d, want %d", n, len(msg))
					return
				}
			}
		}()
	}
	writers.Wait()
	close(errors)
	require.NoError(t, lw.Close())
	for err := range errors {
		require.NoError(t, err)
	}
	files, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)

	lineCount := 0
	for _, f := range files {
		func() {
			file, err := os.Open(filepath.Join(filepath.Dir(path), f.Name()))
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			var reader io.Reader = file
			if strings.HasSuffix(file.Name(), ".gz") {
				gz, err := gzip.NewReader(file)
				require.NoError(t, err)
				defer func() { require.NoError(t, gz.Close()) }()
				reader = gz
			}
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				lineCount++
			}
			require.NoError(t, scanner.Err())
		}()
	}
	require.Equal(t, 10000, lineCount)
}
