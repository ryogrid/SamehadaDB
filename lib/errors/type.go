// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package errors

type Error string

func (e Error) Error() string { return string(e) }
