package cmd

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/rwcarlsen/goexif/exif"

	"gopkg.in/djherbis/times.v1"
)

const timeFormat = "20060102T150405"

type (
	workerJob func()
)

var (
	extensions = map[string]interface{}{
		".jpg":  nil,
		".gif":  nil,
		".png":  nil,
		".jpeg": nil,
		".heic": nil,
		".bmp":  nil,
		".tif":  nil,
		".jpe":  nil,
		".raw":  nil,
		".mp4":  nil,
		".mov":  nil,
		".m4v":  nil,
		".3gp":  nil,
		".avi":  nil,
		".mkv":  nil,
		".webm": nil,
		".flv":  nil,
		".wmv":  nil,
		".mpg":  nil,
		".m2v":  nil,
		".mp2":  nil,
	}
	hashesMutex  = sync.Mutex{}
	hashes       map[string]string
	collisions   int64
	copyErrors   int64
	removeErrors int64

	sources     []string
	destination string
	rename      bool
	move        bool
	simulate    bool
	rootCmd     = &cobra.Command{
		Use:   "deduplicate",
		Short: "Deduplicate allows to remove duplicate images",
		Long:  `Deduplicate is an utility to remove duplicate images and rename the unique ones`,
		Run:   run,
	}

	workerJobs chan workerJob
)

func init() {
	rootCmd.PersistentFlags().StringArrayVarP(&sources, "sources", "s", nil, "Sources folder to analyze")
	rootCmd.PersistentFlags().StringVarP(&destination, "destination", "d", "", "Destination folder")
	rootCmd.PersistentFlags().BoolVarP(&rename, "rename", "r", false, "Rename with file datetime prefix")
	rootCmd.PersistentFlags().BoolVarP(&move, "move", "m", false, "Move files instead of copying")
	rootCmd.PersistentFlags().BoolVarP(&simulate, "simulate", "l", false, "Simulate process")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) {
	start := time.Now()
	var folders []string
	var dest string
	for _, item := range sources {
		if finfo, err := os.Stat(item); err == nil && finfo.IsDir() {
			item, _ = filepath.Abs(filepath.Clean(item))
			folders = append(folders, item)
		}
	}
	if finfo, err := os.Stat(destination); err == nil && finfo.IsDir() {
		dest, _ = filepath.Abs(filepath.Clean(destination))
	}

	if len(folders) == 0 {
		fmt.Println("Error! sources shouldn't be empty")
		os.Exit(1)
	}
	if dest == "" {
		fmt.Println("Error! destination shouldn't be empty")
		os.Exit(1)
	}

	// start workers
	concurrency := runtime.NumCPU() * 2
	workerJobs = make(chan workerJob, concurrency)
	for i := 0; i < concurrency; i++ {
		go worker(i + 1)
	}
	atomic.StoreInt64(&collisions, 0)
	atomic.StoreInt64(&copyErrors, 0)
	atomic.StoreInt64(&removeErrors, 0)

	fmt.Println("Source Folders:", folders)
	fmt.Println("Destination:", dest)
	fmt.Println("Rename:", rename)
	fmt.Println("Move:", move)
	fmt.Println("Concurrency Level:", concurrency)
	fmt.Println()

	fmt.Println("Calculating hashes...")
	populateHash(folders)
	fmt.Println()
	fmt.Println("Processing hashes...")
	processHashes(dest)

	close(workerJobs)

	fmt.Println()
	fmt.Println()
	fmt.Println("Total number of images with no duplicates:", len(hashes))
	fmt.Println("Total number of duplicates:", atomic.LoadInt64(&collisions))
	fmt.Println("Total number of copy errors:", atomic.LoadInt64(&copyErrors))
	if move {
		fmt.Println("Total number of remove errors:", atomic.LoadInt64(&removeErrors))
	}
	fmt.Printf("Done in %v", time.Since(start))
}

func populateHash(folders []string) {
	hashes = map[string]string{}
	wg := sync.WaitGroup{}
	for _, item := range folders {
		files, err := ioutil.ReadDir(item)
		if err != nil {
			fmt.Println("Error:", err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			if _, ok := extensions[strings.ToLower(filepath.Ext(f.Name()))]; ok {
				wg.Add(1)
				fPath := item
				fName := f.Name()
				job := func() {
					processFileHash(&wg, fPath, fName, &collisions)
				}
				workerJobs <- job
			}
		}
	}

	wg.Wait()
	fmt.Println()
	fmt.Println("Total number of images:", len(hashes))
	fmt.Println("Total number of duplicates:", collisions)
}

func processFileHash(wGroup *sync.WaitGroup, path string, fName string, collisionsCounter *int64) {
	defer wGroup.Done()
	fPath := filepath.Join(path, fName)

	data, err := ioutil.ReadFile(fPath)
	if err != nil {
		fmt.Println("Error:", fPath, err)
	}
	hash := sha512.Sum512(data)
	strHash := fmt.Sprintf("%x", hash)

	hashesMutex.Lock()
	defer hashesMutex.Unlock()
	if current, ok := hashes[strHash]; ok {
		currentCollisions := atomic.AddInt64(collisionsCounter, 1)
		fmt.Printf("(%d) File '%s' duplicate with: '%s'. Ignoring it. \n", currentCollisions, fPath, current)
	} else {
		hashes[strHash] = fPath
	}
}

func processHashes(dest string) {
	fmt.Println()
	wg := sync.WaitGroup{}

	for _, v := range hashes {
		wg.Add(1)
		sourceFile := v
		workerJobs <- func() {
			defer wg.Done()
			var destFileName string

			if rename {
				fTime, err, noRename := getFileTime(sourceFile)
				if err != nil {
					fTime = time.Now()
					fmt.Println(err)
				}
				if noRename {
					destFileName = filepath.Base(sourceFile)
				} else {
					destFileName = fmt.Sprintf("%s %s", fTime.Format(timeFormat), filepath.Base(sourceFile))
					destFileName = strings.ReplaceAll(destFileName, ":", "")
				}
			} else {
				destFileName = filepath.Base(sourceFile)
			}

			destinationFile := filepath.Clean(filepath.Join(dest, destFileName))

			if sourceFile == destinationFile {
				fmt.Printf("Skipped: source '%s' is the same as '%s'\n", sourceFile, destinationFile)
			} else {
				if simulate {
					fmt.Println(sourceFile, "->", destinationFile)
				} else {
					if !move {
						fmt.Printf("Copying '%s' to '%s'\n", sourceFile, destinationFile)
					} else {
						fmt.Printf("Moving '%s' to '%s'\n", sourceFile, destinationFile)
					}
					err := copyFile(sourceFile, destinationFile)
					if err != nil {
						fmt.Printf("Error: copying file '%s': %v\n", sourceFile, err)
						atomic.AddInt64(&copyErrors, 1)
					} else if move {
						if err := os.Remove(sourceFile); err != nil {
							fmt.Printf("Error: removing source file '%s': %v", sourceFile, err)
							atomic.AddInt64(&removeErrors, 1)
						}
					}
				}
			}
		}
	}

	wg.Wait()
}

func getFileTime(filePath string) (fileTime time.Time, err error, timeInPath bool) {
	fileName := filepath.Base(filePath)
	if len(fileName) > len(timeFormat) {
		if pTime, err := time.Parse(timeFormat, fileName[:len(timeFormat)]); err == nil {
			return pTime, nil, true
		}
	}
	file, err := os.Open(filePath)
	if err != nil {
		return time.Time{}, err, false
	}
	exifInfo, err := exif.Decode(file)
	if err == nil {
		if dt, err := exifInfo.DateTime(); err == nil {
			return dt, nil, false
		}
	}
	if stat, err := times.Stat(filePath); err == nil {
		if stat.HasBirthTime() {
			return stat.BirthTime(), nil, false
		} else {
			return stat.ModTime(), nil, false
		}
	} else {
		if st, err := file.Stat(); err == nil {
			return st.ModTime(), nil, false
		} else {
			return time.Time{},
				errors.New(fmt.Sprintf("time can't be calculated for: %v", filePath)),
				false
		}
	}
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherwise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func copyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func worker(id int) {
	for {
		select {
		case job, ok := <-workerJobs:
			if !ok {
				return
			}
			job()
		}
	}
}
