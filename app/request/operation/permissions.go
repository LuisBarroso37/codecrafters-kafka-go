package operation

type OperationPermission int16

// These numbers represent the bit numbers for each operation permission
// The operation permissions are defined as a 4-byte integer where each bit corresponds to a permission
// For each permission that we have, we set the corresponding bit position in the 4-byte integer to 1 (we read the bits from right to left)
const (
	UNKNOWN          = 1 << 0
	ANY              = 1 << 1
	ALL              = 1 << 2
	READ             = 1 << 3
	WRITE            = 1 << 4
	CREATE           = 1 << 5
	DELETE           = 1 << 6
	ALTER            = 1 << 7
	DESCRIBE         = 1 << 8
	CLUSTER_ACTION   = 1 << 9
	DESCRIBE_CONFIGS = 1 << 10
	ALTER_CONFIGS    = 1 << 11
	IDEMPOTENT_WRITE = 1 << 12
	CREATE_TOKENS    = 1 << 13
	DESCRIBE_TOKENS  = 1 << 14
)
