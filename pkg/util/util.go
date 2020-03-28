package util

import (
	"fmt"
	"os"
)

func GetEnvOrDie(name string) string {
	value := os.Getenv(name)
	if len(value) == 0 {
		panic(fmt.Errorf("Env %s does not exist", name))
	}

	return value
}
