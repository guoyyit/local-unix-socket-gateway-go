package gateway

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// 帧格式（小端序）：
//
//	magic   uint32   固定 0x47574631，用于快速识别损坏/错位帧
//	length  uint32   payload 字节数
//	crc32   uint32   payload 的 IEEE CRC-32
//	payload [length]byte
//
// 长度边界 + 校验使接收方能够正确切分半包/粘包并识别内容损坏。
const (
	frameMagic      uint32 = 0x47574631
	frameHeaderSize        = 12
)

// 帧编解码支持的单帧 payload 硬上限，防止伪造长度头部造成巨量分配。
const hardMaxPayload = 64 * 1024 * 1024

// FrameCodec 负责单条数据帧的编码与解码。
type FrameCodec struct {
	maxPayload uint32
}

// NewFrameCodec 创建编解码器。maxPayload 为 0 时使用硬上限。
func NewFrameCodec(maxPayload uint32) *FrameCodec {
	if maxPayload == 0 || maxPayload > hardMaxPayload {
		maxPayload = hardMaxPayload
	}
	return &FrameCodec{maxPayload: maxPayload}
}

// MaxPayload 返回编解码器允许的最大 payload。
func (c *FrameCodec) MaxPayload() uint32 {
	return c.maxPayload
}

// Encode 把 payload 编码为一个完整帧。
func (c *FrameCodec) Encode(payload []byte) ([]byte, error) {
	if uint32(len(payload)) > c.maxPayload {
		return nil, ErrFrameTooLarge
	}
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], frameMagic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)
	return frame, nil
}

// writeRaw 循环处理短写，直到 frame 全部写出。
func writeRaw(w io.Writer, frame []byte) error {
	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

// WriteFrame 把 payload 作为一个完整帧写出，循环处理短写。
// 返回 nil 仅表示整帧已写入底层 Writer（对网络连接而言即已交给内核）。
func (c *FrameCodec) WriteFrame(w io.Writer, payload []byte) error {
	frame, err := c.Encode(payload)
	if err != nil {
		return err
	}
	return writeRaw(w, frame)
}

// ReadFrame 从字节流中精确读取一帧。
// headerBuf 长度必须 >= frameHeaderSize，跨调用复用以避免半包场景的额外分配。
// 返回的 payload 是新分配的切片，调用方可长期持有。
func (c *FrameCodec) ReadFrame(r io.Reader, headerBuf []byte) ([]byte, error) {
	if len(headerBuf) < frameHeaderSize {
		return nil, ErrFrameCorrupt
	}
	header := headerBuf[:frameHeaderSize]
	// io.ReadFull 内部循环直到读满 12 字节，天然覆盖半包。
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	magic := binary.LittleEndian.Uint32(header[0:4])
	length := binary.LittleEndian.Uint32(header[4:8])
	sum := binary.LittleEndian.Uint32(header[8:12])
	if magic != frameMagic || length > c.maxPayload {
		// 魔数错误通常意味着流错位或对端协议不符，无法安全重同步；
		// 长度超过本连接允许上限同样视为损坏，必须终止该连接。
		return nil, ErrFrameCorrupt
	}
	payload := make([]byte, length)
	if length > 0 {
		// 半包：ReadFull 会持续读取直到 payload 完整。
		// 粘包：本次只消费 length 字节，剩余字节留给下一次 ReadFrame。
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}
	if crc32.ChecksumIEEE(payload) != sum {
		return nil, ErrFrameCorrupt
	}
	return payload, nil
}
