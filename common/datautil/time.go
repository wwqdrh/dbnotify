package datautil

import "time"

const TIMEF = "2006-01-02 15:04:05"

func ParseTime(date *time.Time) string {
	return date.Format(TIMEF)
}

func UnParseTime(timeStr string) (time.Time, error) {
	return time.ParseInLocation(TIMEF, timeStr, time.Local)
	// return time.Parse(TIMEF, timeStr)
}
