package request

type KafkaAPIKey int16

const (
	Produce                      KafkaAPIKey = 0
	Fetch                        KafkaAPIKey = 1
	ListOffsets                  KafkaAPIKey = 2
	Metadata                     KafkaAPIKey = 3
	OffsetCommit                 KafkaAPIKey = 8
	OffsetFetch                  KafkaAPIKey = 9
	FindCoordinator              KafkaAPIKey = 10
	JoinGroup                    KafkaAPIKey = 11
	Heartbeat                    KafkaAPIKey = 12
	LeaveGroup                   KafkaAPIKey = 13
	SyncGroup                    KafkaAPIKey = 14
	DescribeGroups               KafkaAPIKey = 15
	ListGroups                   KafkaAPIKey = 16
	SaslHandshake                KafkaAPIKey = 17
	ApiVersions                  KafkaAPIKey = 18
	CreateTopics                 KafkaAPIKey = 19
	DeleteTopics                 KafkaAPIKey = 20
	DeleteRecords                KafkaAPIKey = 21
	InitProducerId               KafkaAPIKey = 22
	OffsetForLeaderEpoch         KafkaAPIKey = 23
	AddPartitionsToTxn           KafkaAPIKey = 24
	AddOffsetsToTxn              KafkaAPIKey = 25
	EndTxn                       KafkaAPIKey = 26
	WriteTxnMarkers              KafkaAPIKey = 27
	TxnOffsetCommit              KafkaAPIKey = 28
	DescribeAcls                 KafkaAPIKey = 29
	CreateAcls                   KafkaAPIKey = 30
	DeleteAcls                   KafkaAPIKey = 31
	DescribeConfigs              KafkaAPIKey = 32
	AlterConfigs                 KafkaAPIKey = 33
	AlterReplicaLogDirs          KafkaAPIKey = 34
	DescribeLogDirs              KafkaAPIKey = 35
	SaslAuthenticate             KafkaAPIKey = 36
	CreatePartitions             KafkaAPIKey = 37
	CreateDelegationToken        KafkaAPIKey = 38
	RenewDelegationToken         KafkaAPIKey = 39
	ExpireDelegationToken        KafkaAPIKey = 40
	DescribeDelegationToken      KafkaAPIKey = 41
	DeleteGroups                 KafkaAPIKey = 42
	ElectLeaders                 KafkaAPIKey = 43
	IncrementalAlterConfigs      KafkaAPIKey = 44
	AlterPartitionReassignments  KafkaAPIKey = 45
	ListPartitionReassignments   KafkaAPIKey = 46
	OffsetDelete                 KafkaAPIKey = 47
	DescribeClientQuotas         KafkaAPIKey = 48
	AlterClientQuotas            KafkaAPIKey = 49
	DescribeUserScramCredentials KafkaAPIKey = 50
	AlterUserScramCredentials    KafkaAPIKey = 51
	DescribeQuorum               KafkaAPIKey = 55
	UpdateFeatures               KafkaAPIKey = 57
	DescribeCluster              KafkaAPIKey = 60
	DescribeProducers            KafkaAPIKey = 61
	UnregisterBroker             KafkaAPIKey = 64
	DescribeTransactions         KafkaAPIKey = 65
	ListTransactions             KafkaAPIKey = 66
	ConsumerGroupHeartbeat       KafkaAPIKey = 68
	ConsumerGroupDescribe        KafkaAPIKey = 69
	GetTelemetrySubscriptions    KafkaAPIKey = 71
	PushTelemetry                KafkaAPIKey = 72
	ListClientMetricsResources   KafkaAPIKey = 74
	DescribeTopicPartitions      KafkaAPIKey = 75
	AddRaftVoter                 KafkaAPIKey = 80
	RemoveRaftVoter              KafkaAPIKey = 81
)

var flexibleVersions = map[int16]int16{
	// Produce: flexible from version 7+
	0: 7,
	// Fetch: flexible from version 12+
	1: 12,
	// ListOffsets: flexible from version 6+
	2: 6,
	// Metadata: flexible from version 9+
	3: 9,
	// LeaderAndIsr: flexible from version 4+
	4: 4,
	// StopReplica: flexible from version 2+
	5: 2,
	// UpdateMetadata: flexible from version 6+
	6: 6,
	// ControlledShutdown: flexible from version 3+
	7: 3,
	// OffsetCommit: flexible from version 8+
	8: 8,
	// OffsetFetch: flexible from version 6+
	9: 6,
	// FindCoordinator: flexible from version 3+
	10: 3,
	// JoinGroup: flexible from version 6+
	11: 6,
	// Heartbeat: flexible from version 4+
	12: 4,
	// LeaveGroup: flexible from version 4+
	13: 4,
	// SyncGroup: flexible from version 4+
	14: 4,
	// DescribeGroups: flexible from version 5+
	15: 5,
	// ListGroups: flexible from version 3+
	16: 3,
	// SaslHandshake: flexible from version 2+
	17: 2,
	// ApiVersions: flexible from version 3+
	18: 3,
	// CreateTopics: flexible from version 5+
	19: 5,
	// DeleteTopics: flexible from version 4+
	20: 4,
	// DeleteRecords: flexible from version 2+
	21: 2,
	// InitProducerId: flexible from version 2+
	22: 2,
	// OffsetForLeaderEpoch: flexible from version 4+
	23: 4,
	// AddPartitionsToTxn: flexible from version 1+
	24: 1,
	// AddOffsetsToTxn: flexible from version 1+
	25: 1,
	// EndTxn: flexible from version 1+
	26: 1,
	// WriteTxnMarkers: flexible from version 0+
	27: 0,
	// TxnOffsetCommit: flexible from version 3+
	28: 3,
	// DescribeAcls: flexible from version 2+
	29: 2,
	// CreateAcls: flexible from version 2+
	30: 2,
	// DeleteAcls: flexible from version 2+
	31: 2,
	// DescribeConfigs: flexible from version 4+
	32: 4,
	// AlterConfigs: flexible from version 2+
	33: 2,
	// AlterReplicaLogDirs: flexible from version 2+
	34: 2,
	// DescribeLogDirs: flexible from version 2+
	35: 2,
	// SaslAuthenticate: flexible from version 2+
	36: 2,
	// CreatePartitions: flexible from version 2+
	37: 2,
	// CreateDelegationToken: flexible from version 2+
	38: 2,
	// RenewDelegationToken: flexible from version 2+
	39: 2,
	// ExpireDelegationToken: flexible from version 2+
	40: 2,
	// DescribeDelegationToken: flexible from version 2+
	41: 2,
	// DeleteGroups: flexible from version 2+
	42: 2,
	// ElectLeaders: flexible from version 2+
	43: 2,
	// IncrementalAlterConfigs: flexible from version 1+
	44: 1,
	// AlterPartitionReassignments: flexible from version 0+
	45: 0,
	// ListPartitionReassignments: flexible from version 0+
	46: 0,
	// OffsetDelete: flexible from version 0+
	47: 0,
	// DescribeClientQuotas: flexible from version 1+
	48: 1,
	// AlterClientQuotas: flexible from version 1+
	49: 1,
	// DescribeUserScramCredentials: flexible from version 0+
	50: 0,
	// AlterUserScramCredentials: flexible from version 0+
	51: 0,
	// Vote: flexible from version 0+
	52: 0,
	// BeginQuorumEpoch: flexible from version 0+
	53: 0,
	// EndQuorumEpoch: flexible from version 0+
	54: 0,
	// DescribeQuorum: flexible from version 0+
	55: 0,
	// AlterPartition: flexible from version 0+
	56: 0,
	// UpdateFeatures: flexible from version 0+
	57: 0,
	// Envelope: flexible from version 0+
	58: 0,
	// FetchSnapshot: flexible from version 0+
	59: 0,
	// DescribeCluster: flexible from version 0+
	60: 0,
	// DescribeProducers: flexible from version 0+
	61: 0,
	// BrokerRegistration: flexible from version 0+
	62: 0,
	// BrokerHeartbeat: flexible from version 0+
	63: 0,
	// UnregisterBroker: flexible from version 0+
	64: 0,
	// DescribeTransactions: flexible from version 0+
	65: 0,
	// ListTransactions: flexible from version 0+
	66: 0,
	// AllocateProducerIds: flexible from version 0+
	67: 0,
	// ConsumerGroupHeartbeat: flexible from version 0+
	68: 0,
	// ConsumerGroupDescribe: flexible from version 0+
	69: 0,
	// ControllerRegistration: flexible from version 0+
	70: 0,
	// GetTelemetrySubscriptions: flexible from version 0+
	71: 0,
	// PushTelemetry: flexible from version 0+
	72: 0,
	// AssignReplicasToDirs: flexible from version 0+
	73: 0,
	// ListClientMetricsResources: flexible from version 0+
	74: 0,
	// DescribeTopicPartitions: flexible from version 0+
	75: 0,
	// ShareGroupHeartbeat: flexible from version 0+
	76: 0,
	// ShareGroupDescribe: flexible from version 0+
	77: 0,
	// ShareFetch: flexible from version 0+
	78: 0,
	// ShareAcknowledge: flexible from version 0+
	79: 0,
	// AddRaftVoter: flexible from version 0+
	80: 0,
	// RemoveRaftVoter: flexible from version 0+
	81: 0,
	// UpdateRaftVoter: flexible from version 0+
	82: 0,
}

// Returns true if the given API key and version is flexible (should parse tagged fields)
func isFlexibleVersion(apiKey int16, apiVersion int16) bool {
	if version, ok := flexibleVersions[apiKey]; ok {
		return apiVersion >= version
	}

	return false
}
