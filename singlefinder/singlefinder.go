package singlefinder

import (
	"bufio"
	"doguin/godiff/utils"
	"log"
	"os"
)

func FindDiff(oldFileName string, newFileName string, outputFileName string) {
	oldFile, err := os.Open(oldFileName)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer oldFile.Close()

	newFile, err := os.Open(newFileName)
	if err != nil {
		log.Fatal("Open file error: ", err)
		return
	}
	defer newFile.Close()

	output, err := os.Create(outputFileName)
	if err != nil {
		log.Fatal("Create output file error: ", err)
		return
	}
	defer output.Close()

	bufferSize := utils.GetBuffSize()

	var oldBuff []byte = make([]byte, bufferSize)
	var newBuff []byte = make([]byte, bufferSize)
	old := bufio.NewScanner(oldFile)
	old.Buffer(oldBuff, bufferSize)
	new := bufio.NewScanner(newFile)
	new.Buffer(newBuff, bufferSize)
	writer := bufio.NewWriterSize(output, bufferSize)
	defer writer.Flush()

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

	var oldDomain string = utils.GetDomain(old.Text())
	var newLine string = new.Text()
	var newDomain string = utils.GetDomain(newLine)

	// Jump the first error line
	if oldDomain == "com." {
		oldHasMore = old.Scan()
		if oldHasMore {
			oldDomain = utils.GetDomain(old.Text())
		}
	}

	// Jump the first error line
	if newDomain == "com." {
		newHasMore = new.Scan()
		if newHasMore {
			newLine = new.Text()
			newDomain = utils.GetDomain(newLine)
		}
	}

	for oldHasMore && newHasMore {
		if newDomain < oldDomain {
			// fmt.Println("New: ", newDomain, ", Old: ", oldDomain)
			// fmt.Println(newLine)
			//Find a diff
			writer.WriteString(newLine + "\n")
			//New file move
			newHasMore = new.Scan()
			if newHasMore {
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
				newLine = new.Text()
				newDomain = utils.GetDomain(newLine)
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
