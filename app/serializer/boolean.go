package serializer

import "fmt"

func SerializeBoolean(buffer []byte, index int, value bool) (int, error) {
	if index < 0 {
		return index, fmt.Errorf("failed to serialize boolean - negative index")
	}

	if index+1 > len(buffer) {
		return index, fmt.Errorf("failed to serialize boolean - buffer too small")
	}

	if value {
		buffer[index] = 1
	} else {
		buffer[index] = 0
	}

	return index + 1, nil
}
