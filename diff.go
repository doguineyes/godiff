package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const sep = "\t"

func main() {
	args := os.Args[1:]
	if len(args) != 2 {
		fmt.Println("Usage: diff oldfile.txt newfile.txt")
		return
	}

	fmt.Println("Find diff between ", args[0], args[1])

	oldFile, err := os.Open(args[0])
	if err != nil {
		log.Fatal("Open old file error: ", err)
		return
	}
	defer oldFile.Close()

	newFile, err := os.Open(args[1])
	if err != nil {
		log.Fatal("Open new file error: ", err)
		return
	}
	defer newFile.Close()

	output, err := os.Create("./out_diff.txt")
	if err != nil {
		log.Fatal("Create output file error: ", err)
		return
	}
	defer output.Close()

	const bufSize = 512 * 1024 * 1024

	var oldBuff []byte = make([]byte, bufSize)
	var newBuff []byte = make([]byte, bufSize)

	oldScanner := bufio.NewScanner(oldFile)
	oldScanner.Buffer(oldBuff, bufSize)
	newScanner := bufio.NewScanner(newFile)
	newScanner.Buffer(newBuff, bufSize)
	writer := bufio.NewWriterSize(output, bufSize)
	defer writer.Flush()

	fmt.Println("Start: ", time.Now())
	findDiff(oldScanner, newScanner, writer)
	fmt.Println("End: ", time.Now())
}

func getDomain(line string) string {
	domain, _, found := strings.Cut(line, sep)
	if !found {
		log.Println("Not found tab sep in line: ", line)
		return ""
	}
	return domain
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
	for scanner.Scan() {
		newLine := scanner.Text()
		writer.WriteString(newLine + "\n")
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
