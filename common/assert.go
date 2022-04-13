package common

func SH_Assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
