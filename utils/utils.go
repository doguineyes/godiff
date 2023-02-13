package utils

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strings"
)

var buffSize int = 128 * 1024 * 1024 // 128M

const sep = "\t"

var lineSep = []byte{'\n'}

func InitBuffSize(bs int) {
	buffSize = bs
}

func GetBuffSize() int {
	return buffSize
}

func GetSep() string {
	return sep
}

func GetDomain(line string) string {
	domain, _, found := strings.Cut(line, sep)
	if !found {
		log.Println("Not found tab sep in line: ", line)
		return ""
	}
	return domain
}

func CountLineByScan(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return -1, err
	}
	defer file.Close()
	buffSize = GetBuffSize()
	buffer := make([]byte, buffSize)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, buffSize)
	var lineCount int64 = 0
	for scanner.Scan() {
		lineCount++
	}
	return lineCount, nil
}

func CountLineByByte(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return -1, err
	}
	defer file.Close()
	buffSize = GetBuffSize()
	buffer := make([]byte, buffSize)
	var count int64 = 0
	// lineSep := []byte{'\n'}
	for {
		c, err := file.Read(buffer)
		count += int64(bytes.Count(buffer[:c], lineSep))

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
