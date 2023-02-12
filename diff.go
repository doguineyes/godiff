package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const SEP = "\t"

var BUF_SIZE int = 128 * 1024 * 1024 // 128M

var PER_PART int64 = 10 * 1000 * 1000

type part struct {
	startOffset int64
	lineCount   int64
}

func printUsage() {
	fmt.Println("Usage: godiff [options]")
	fmt.Println("Options:")
	flag.PrintDefaults()
}

func main() {
	var firstFileName string
	const (
		defaultFirstFileName = "./first_day.txt"
		firstFileNameUsage   = "the first day's file name"
	)

	var secondFileName string
	const (
		defaultSecondFileName = "./second_day.txt"
		secondFileNameUsage   = "the second day's file name"
	)

	var outputFileName string
	const (
		defaultOutputFileName = "./output.txt"
		outputFileUsage       = "output file name"
	)

	const (
		defaultBufSize = 128 * 1024 * 1024
		bufSizeUsage   = "file buffer size"
	)

	const (
		defaultPerPart int64 = 10 * 1000 * 1000
		perPartUsage         = "Lines count per part"
	)

	flag.StringVar(&firstFileName, "first", defaultFirstFileName, firstFileNameUsage)
	flag.StringVar(&firstFileName, "1", defaultFirstFileName, firstFileNameUsage+"(shorthand)")

	flag.StringVar(&secondFileName, "second", defaultSecondFileName, secondFileNameUsage)
	flag.StringVar(&secondFileName, "2", defaultSecondFileName, secondFileNameUsage+"(shorthand)")

	flag.StringVar(&outputFileName, "output", defaultOutputFileName, outputFileUsage)
	flag.StringVar(&outputFileName, "o", defaultOutputFileName, outputFileUsage)

	flag.IntVar(&BUF_SIZE, "b", defaultBufSize, bufSizeUsage)
	flag.IntVar(&BUF_SIZE, "buffer", defaultBufSize, bufSizeUsage)

	flag.Int64Var(&PER_PART, "p", defaultPerPart, perPartUsage)
	flag.Int64Var(&PER_PART, "perpart", defaultPerPart, perPartUsage)

	flag.Parse()

	fmt.Println("First day file name: ", firstFileName, ", second day file name: ", secondFileName, ", output file name: ", outputFileName)

	// ------------> Single go start
	// oldFile, err := os.Open(firstFileName)
	// if err != nil {
	// 	log.Fatal("Open old file error: ", err)
	// 	return
	// }
	// defer oldFile.Close()

	// newFile, err := os.Open(secondFileName)
	// if err != nil {
	// 	log.Fatal("Open new file error: ", err)
	// 	return
	// }
	// defer newFile.Close()

	// output, err := os.Create(outputFileName)
	// if err != nil {
	// 	log.Fatal("Create output file error: ", err)
	// 	return
	// }
	// defer output.Close()

	// var oldBuff []byte = make([]byte, BUF_SIZE)
	// var newBuff []byte = make([]byte, BUF_SIZE)

	// oldScanner := bufio.NewScanner(oldFile)
	// oldScanner.Buffer(oldBuff, BUF_SIZE)
	// newScanner := bufio.NewScanner(newFile)
	// newScanner.Buffer(newBuff, BUF_SIZE)
	// writer := bufio.NewWriterSize(output, BUF_SIZE)
	// defer writer.Flush()
	// ------------> Single go end

	startTime := time.Now()
	fmt.Println("Start: ", startTime)

	// findDiff(oldScanner, newScanner, writer)

	parts, _ := getPart(secondFileName)
	fmt.Println("Parts: ", parts)
	res := make(chan int)
	for id, part := range parts {
		go findDiff2(id, part, firstFileName, secondFileName, outputFileName, res)
	}
	for i := 0; i < len(parts); i++ {
		partId := <-res
		fmt.Println("part ", partId, "is complete")
	}

	//test get part start
	// parts, _ := getPart(secondFileName)
	// for _, p := range parts {
	// 	printFirstLine(secondFileName, p)
	// }
	//test get part end

	endTime := time.Now()
	fmt.Println("End: ", endTime, ", spend ", endTime.Sub(startTime))
}

func countLineByScan(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return -1, err
	}
	defer file.Close()
	buffer := make([]byte, BUF_SIZE)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, BUF_SIZE)
	var lineCount int64 = 0
	for scanner.Scan() {
		lineCount++
	}
	return lineCount, nil
}

func countLineByByte(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return -1, err
	}
	defer file.Close()
	buffer := make([]byte, BUF_SIZE)
	var count int64 = 0
	lineSep := []byte{'\n'}
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

func countLine(file *os.File) (int64, error) {
	file.Seek(0, 0)
	buffer := make([]byte, BUF_SIZE)
	var count int64 = 0
	lineSep := []byte{'\n'}
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

func printFirstLine(filename string, p part) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer file.Close()

	fmt.Println("Part start: ", p.startOffset, ", count: ", p.lineCount)
	buffer := make([]byte, BUF_SIZE)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, BUF_SIZE)
	file.Seek(p.startOffset, 0)
	scanner.Scan()
	fmt.Println("Firts line: ", scanner.Text())
}

func getPart(filename string) ([]part, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return nil, err
	}
	defer file.Close()

	var parts []part

	buffer := make([]byte, BUF_SIZE)
	var count int64 = 0
	var inPartsCount int64 = 0
	currentPartStart, _ := file.Seek(0, 0)
	lineSep := []byte{'\n'}
	for {
		c, err := file.Read(buffer)
		count += int64(bytes.Count(buffer[:c], lineSep))
		if count-inPartsCount > PER_PART {
			lastSepIndex := bytes.LastIndex(buffer[:c], lineSep)
			remain := c - (lastSepIndex + 1)
			parts = append(parts, part{startOffset: currentPartStart, lineCount: count - inPartsCount})
			inPartsCount = count
			nextBuffer, _ := file.Seek(0, 1)
			currentPartStart = nextBuffer - int64(remain)
		}
		switch {
		case err == io.EOF:
			//Add last part
			parts = append(parts, part{startOffset: currentPartStart, lineCount: count - inPartsCount})
			return parts, nil

		case err != nil:
			return nil, err
		}
	}
}

func getDomain(line string) string {
	domain, _, found := strings.Cut(line, SEP)
	if !found {
		log.Println("Not found tab sep in line: ", line)
		return ""
	}
	return domain
}

func findDiff2(id int, p part, oldFileName string, newFileName string, outputFileName string, res chan int) {
	// No lines to handle
	if p.lineCount <= 0 {
		res <- id
		return
	}

	oldFile, err := os.Open(oldFileName)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer oldFile.Close()

	oldBuffer := make([]byte, BUF_SIZE)
	old := bufio.NewScanner(oldFile)
	old.Buffer(oldBuffer, BUF_SIZE)

	newFile, err := os.Open(newFileName)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer newFile.Close()

	newBuffer := make([]byte, BUF_SIZE)
	new := bufio.NewScanner(newFile)
	new.Buffer(newBuffer, BUF_SIZE)

	output, err := os.Create(outputFileName + ".part_" + strconv.Itoa(id))
	if err != nil {
		log.Fatal("Create output file error: ", err)
		return
	}
	defer output.Close()

	writer := bufio.NewWriterSize(output, BUF_SIZE)
	defer writer.Flush()

	var oldHasMore bool = old.Scan()

	// No lines in first file, all lines in second should output
	if !oldHasMore {
		printLinesFrom(p.startOffset, p.lineCount, newFile, new, writer)
		res <- id
		return
	}

	var oldDomain string = getDomain(old.Text())

	// Skip the first error line
	if oldDomain == "com." {
		oldHasMore = old.Scan()
		if oldHasMore {
			oldDomain = getDomain(old.Text())
		}
	}

	newFile.Seek(p.startOffset, 0)
	fmt.Println("Part ", id, "seek offset at ", p.startOffset)
	var newHasMore bool = new.Scan()
	var newLine string = new.Text()
	fmt.Println("The first line in new: ", newLine)
	var newDomain string = getDomain(newLine)

	// Skip the first error line
	if newDomain == "com." {
		newHasMore = new.Scan()
		if newHasMore {
			newLine = new.Text()
			newDomain = getDomain(newLine)
		}
	}

	var count int64 = 0

	for count < p.lineCount && oldHasMore && newHasMore {
		if newDomain < oldDomain {
			// fmt.Println("New: ", newDomain, ", Old: ", oldDomain)
			// Find a diff
			// fmt.Println(newLine)
			writer.WriteString(newLine + "\n")
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
				count++
				newLine = new.Text()
				newDomain = getDomain(newLine)
			}
		} else if newDomain > oldDomain {
			//Old file move
			oldHasMore = old.Scan()
			if oldHasMore {
				oldDomain = getDomain(old.Text())
			}
		} else { // newDomain == oldDomain
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
				count++
				newLine = new.Text()
				newDomain = getDomain(newLine)
			}
		}
	}
	if count < p.lineCount && newHasMore {
		printSomeLines(new, writer, p.lineCount-count)
	}
	res <- id
}

func findDiff(old *bufio.Scanner, new *bufio.Scanner, writer *bufio.Writer) {
	var oldHasMore bool = old.Scan()
	var newHasMore bool = new.Scan()

	// New file is empty
	if !newHasMore {
		log.Println("No lines in new file")
		return
	}

	// Old file is empty and new file is not empty
	if !oldHasMore {
		printLines(new, writer)
		return
	}

	var oldDomain string = getDomain(old.Text())
	var newLine string = new.Text()
	var newDomain string = getDomain(newLine)

	// Jump the first error line
	if oldDomain == "com." {
		oldHasMore = old.Scan()
		if oldHasMore {
			oldDomain = getDomain(old.Text())
		}
	}

	// Jump the first error line
	if newDomain == "com." {
		newHasMore = new.Scan()
		if newHasMore {
			newLine = new.Text()
			newDomain = getDomain(newLine)
		}
	}

	for oldHasMore && newHasMore {
		if newDomain < oldDomain {
			// fmt.Println("New: ", newDomain, ", Old: ", oldDomain)
			//Find a diff
			// fmt.Println(newLine)
			writer.WriteString(newLine + "\n")
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
				newLine = new.Text()
				newDomain = getDomain(newLine)
			}
		} else if newDomain > oldDomain {
			//Old file move
			oldHasMore = old.Scan()
			if oldHasMore {
				oldDomain = getDomain(old.Text())
			}
		} else { // newDomain == oldDomain
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
				newLine = new.Text()
				newDomain = getDomain(newLine)
			}
		}
	}

	if newHasMore {
		printLines(new, writer)
	}
}

func printLines(scanner *bufio.Scanner, writer *bufio.Writer) {
	newLine := scanner.Text()
	writer.WriteString(newLine + "\n")

	for scanner.Scan() {
		newLine := scanner.Text()
		writer.WriteString(newLine + "\n")
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func printSomeLines(scanner *bufio.Scanner, writer *bufio.Writer, lineCount int64) {
	var count int64 = 0

	newLine := scanner.Text()
	count++
	writer.WriteString(newLine + "\n")

	for count < lineCount && scanner.Scan() {
		newLine := scanner.Text()
		count++
		writer.WriteString(newLine + "\n")
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func printLinesFrom(startOffset int64, lineCount int64, file *os.File, scanner *bufio.Scanner, writer *bufio.Writer) {
	file.Seek(startOffset, 0)
	var count int64 = 0
	for scanner.Scan() && count < lineCount {
		writer.WriteString(scanner.Text() + "\n")
		count++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
