package cmd

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/rwcarlsen/goexif/exif"

	"gopkg.in/djherbis/times.v1"
)

const timeFormat = "20060102T150405"

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
	hashes map[string]string

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

	fmt.Println("Source Folders:", folders)
	fmt.Println("Destination:", dest)
	fmt.Println("Rename:", rename)
	fmt.Println()

	populateHash(folders)
	processHashes(dest)
}

func populateHash(folders []string) {
	hashes = map[string]string{}
	collisions := 0
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
				fPath := filepath.Join(item, f.Name())

				data, err := ioutil.ReadFile(fPath)
				if err != nil {
					fmt.Println("Error:", fPath, err)
				}
				hash := sha512.Sum512(data)
				strHash := fmt.Sprintf("%x", hash)

				if current, ok := hashes[strHash]; ok {
					collisions++
					fmt.Printf("(%d). File '%s' duplicate with: '%s'. Ignoring it. \n", collisions, fPath, current)
				} else {
					hashes[strHash] = fPath
				}
			}
		}
	}
	fmt.Println()
	fmt.Println("Total number of images:", len(hashes))
	fmt.Println("Total number of duplicates:", collisions)
}

func processHashes(dest string) {
	fmt.Println()
	for _, v := range hashes {
		var destFileName string

		if rename {
			fTime, err, noRename := getFileTime(v)
			if err != nil {
				fTime = time.Now()
				fmt.Println(err)
			}
			if noRename {
				destFileName = filepath.Base(v)
			} else {
				destFileName = fmt.Sprintf("%s %s", fTime.Format(timeFormat), filepath.Base(v))
				destFileName = strings.ReplaceAll(destFileName, ":", "")
			}
		} else {
			destFileName = filepath.Base(v)
		}

		nf := filepath.Clean(filepath.Join(dest, destFileName))

		if v == nf {
			fmt.Printf("Skipped: source '%s' is the same as '%s'\n", v, nf)
		} else {
			if simulate {
				fmt.Println(v, "->", nf)
			} else {
				if !move {
					fmt.Printf("Copying '%s' to '%s'\n", v, nf)
				} else {
					fmt.Printf("Moving '%s' to '%s'\n", v, nf)
				}
				err := copyFile(v, nf)
				if err != nil {
					fmt.Println("Error: copying file:", err)
					fmt.Println()
				} else if move {
					if err := os.Remove(v); err != nil {
						fmt.Println("Error: removing source file:", err)
						fmt.Println()
					}
				}
			}
		}
	}
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
