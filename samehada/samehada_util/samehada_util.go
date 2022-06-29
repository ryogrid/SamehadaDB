package samehada_util

import "os"

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
