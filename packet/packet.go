package packet

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

type PacketType int

const (
	UndefinedPacketType = iota
	Heartbeat
)

// packet proto: 2bytestype 4byteslength jsondata
type Packet struct {
	PackType   PacketType  `json:"packet_type"`
	PacketData interface{} `json:"packet_data"`
}

type PacketHeartbeat struct{}

func MakePacketFromJson(packetType PacketType, data []byte) (*Packet, error) {
	packet := Packet{
		PackType: packetType,
	}
	switch packetType {
	case Heartbeat:
		// heartbeat no data
		packet.PacketData = PacketHeartbeat{}
	default:
		return nil, errors.New("unsupported packet type")
	}
	return &packet, nil
}

func (p *Packet) ToProtocolBytes() ([]byte, error) {
	jsonData, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	if len(jsonData) > int(^uint32(0)) {
		return nil, fmt.Errorf("packet too large: %d", len(jsonData))
	}

	buf := make([]byte, 6+len(jsonData))
	// 2 bytes type
	binary.BigEndian.PutUint16(buf[0:2], uint16(p.PackType))
	// 4 bytes length
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(jsonData)))
	copy(buf[6:], jsonData)

	return buf, nil
}

var HeartbeatBytes []byte

func init() {
	var err error
	HeartbeatBytes, err = (&Packet{PackType: Heartbeat, PacketData: &PacketHeartbeat{}}).ToProtocolBytes()
	if err != nil {
		panic(err)
	}
}
