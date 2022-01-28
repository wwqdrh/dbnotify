package datautil

import "time"

func ParseTime(date *time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}
