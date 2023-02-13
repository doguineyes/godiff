package main

import (
	"doguin/godiff/multifinder"
	"doguin/godiff/singlefinder"
	"doguin/godiff/utils"
	"flag"
	"fmt"
	"time"
)

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
		defaultBufSize = 128 * 1024 * 1024 // 128M
		bufSizeUsage   = "file buffer size"
	)

	const (
		defaultPerPart int64 = 100 * 1000 * 1000
		perPartUsage         = "Lines count per part"
	)

	const (
		defaultMode string = "m"
		modeUsage          = "single core=s, multi cores=m"
	)

	var bufSize int

	var perPartLineCount int64

	var mode string

	flag.StringVar(&firstFileName, "first", defaultFirstFileName, firstFileNameUsage)
	flag.StringVar(&firstFileName, "1", defaultFirstFileName, firstFileNameUsage+"(shorthand)")

	flag.StringVar(&secondFileName, "second", defaultSecondFileName, secondFileNameUsage)
	flag.StringVar(&secondFileName, "2", defaultSecondFileName, secondFileNameUsage+"(shorthand)")

	flag.StringVar(&outputFileName, "output", defaultOutputFileName, outputFileUsage)
	flag.StringVar(&outputFileName, "o", defaultOutputFileName, outputFileUsage)

	flag.IntVar(&bufSize, "b", defaultBufSize, bufSizeUsage)
	flag.IntVar(&bufSize, "buffer", defaultBufSize, bufSizeUsage)

	flag.Int64Var(&perPartLineCount, "p", defaultPerPart, perPartUsage)
	flag.Int64Var(&perPartLineCount, "perpart", defaultPerPart, perPartUsage)

	flag.StringVar(&mode, "m", defaultMode, modeUsage)
	flag.StringVar(&mode, "mode", defaultMode, modeUsage)

	flag.Parse()

	utils.InitBuffSize(bufSize)

	multifinder.InitPerPart(perPartLineCount)

	fmt.Println("First day file name: ", firstFileName, ", second day file name: ", secondFileName, ", output file name: ", outputFileName)

	startTime := time.Now()
	fmt.Println("Start: ", startTime)

	if mode == "s" {
		singlefinder.FindDiff(firstFileName, secondFileName, outputFileName)
	} else {
		multifinder.FindDiff(firstFileName, secondFileName, outputFileName)
	}

	endTime := time.Now()
	fmt.Println("End: ", endTime, ", spend ", endTime.Sub(startTime))
}
