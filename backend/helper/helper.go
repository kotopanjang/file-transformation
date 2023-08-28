package helper

import (
	"fmt"
	"strconv"

	"github.com/shopspring/decimal"
)

// ToString is to convert any type to sring
func ToString(s any) string {
	switch s := s.(type) {
	case float64:
		return decimal.NewFromFloat(s).String()
	default:
		return fmt.Sprintf("%v", s)
	}
}

// ToInt is to convert any type to Int64
// if the convertion error, will return 0 instead
func ToInt(i any) int64 {
	switch i := i.(type) {
	case string:
		res, err := strconv.Atoi(i)
		if err != nil {
			return 0
		}
		return int64(res)
	case float64:
		return int64(i)
	}

	return 0
}
