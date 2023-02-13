package multifinder

import (
	"bufio"
	"bytes"
	"doguin/godiff/utils"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type part struct {
	startOffset int64
	lineCount   int64
}

var perPart int64 = 10 * 1000 * 1000

func InitPerPart(perPartLineCount int64) {
	perPart = perPartLineCount
}

func FindDiff(firstFileName string, secondFileName string, outputFileName string) {
	parts, _ := getPart(secondFileName)
	fmt.Println("Parts: ", parts)
	res := make(chan int)
	for id, part := range parts {
		go findDiff(id, part, firstFileName, secondFileName, outputFileName, res)
	}
	for i := 0; i < len(parts); i++ {
		partId := <-res
		fmt.Println("part ", partId, "is complete")
	}
}

func getPart(filename string) ([]part, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return nil, err
	}
	defer file.Close()

	var parts []part

	buffSize := utils.GetBuffSize()

	buffer := make([]byte, buffSize)
	var count int64 = 0
	var inPartsCount int64 = 0
	currentPartStart, _ := file.Seek(0, 0)
	lineSep := []byte{'\n'}
	for {
		c, err := file.Read(buffer)
		count += int64(bytes.Count(buffer[:c], lineSep))
		if count-inPartsCount > perPart {
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

func findDiff(id int, p part, oldFileName string, newFileName string, outputFileName string, res chan int) {
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

	buffSize := utils.GetBuffSize()

	oldBuffer := make([]byte, buffSize)
	old := bufio.NewScanner(oldFile)
	old.Buffer(oldBuffer, buffSize)

	newFile, err := os.Open(newFileName)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer newFile.Close()

	newBuffer := make([]byte, buffSize)
	new := bufio.NewScanner(newFile)
	new.Buffer(newBuffer, buffSize)

	output, err := os.Create(outputFileName + ".part_" + strconv.Itoa(id))
	if err != nil {
		log.Fatal("Create output file error: ", err)
		return
	}
	defer output.Close()

	writer := bufio.NewWriterSize(output, buffSize)
	defer writer.Flush()

	var oldHasMore bool = old.Scan()

	// No lines in first file, all lines in second should output
	if !oldHasMore {
		printLinesFrom(p.startOffset, p.lineCount, newFile, new, writer)
		res <- id
		return
	}

	var oldDomain string = utils.GetDomain(old.Text())

	// Skip the first error line
	if oldDomain == "com." {
		oldHasMore = old.Scan()
		if oldHasMore {
			oldDomain = utils.GetDomain(old.Text())
		}
	}

	newFile.Seek(p.startOffset, 0)
	fmt.Println("Part ", id, "seek offset at ", p.startOffset)
	var newHasMore bool = new.Scan()
	var newLine string = new.Text()
	fmt.Println("The first line in new: ", newLine)
	var newDomain string = utils.GetDomain(newLine)

	// Skip the first error line
	if newDomain == "com." {
		newHasMore = new.Scan()
		if newHasMore {
			newLine = new.Text()
			newDomain = utils.GetDomain(newLine)
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
				newDomain = utils.GetDomain(newLine)
			}
		} else if newDomain > oldDomain {
			//Old file move
			oldHasMore = old.Scan()
			if oldHasMore {
				oldDomain = utils.GetDomain(old.Text())
			}
		} else { // newDomain == oldDomain
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
				count++
				newLine = new.Text()
				newDomain = utils.GetDomain(newLine)
			}
		}
	}
	if count < p.lineCount && newHasMore {
		printSomeLines(new, writer, p.lineCount-count)
	}
	res <- id
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

func PrintParts(filename string) {
	parts, _ := getPart(filename)
	for _, p := range parts {
		printPerPartFirstLine(filename, p)
	}
}

func printPerPartFirstLine(filename string, p part) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer file.Close()

	fmt.Println("Part start: ", p.startOffset, ", count: ", p.lineCount)
	bufSize := utils.GetBuffSize()
	buffer := make([]byte, bufSize)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(buffer, bufSize)
	file.Seek(p.startOffset, 0)
	scanner.Scan()
	fmt.Println("Firts line: ", scanner.Text())
}
