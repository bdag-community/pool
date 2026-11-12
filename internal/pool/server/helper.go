package server

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ------------------- Utilities -------------------

var Diff1TargetPool = func() *big.Int {
	n := new(big.Int)
	// EXACT same diff1 as node-stratum-pool / BTC
	n.SetString("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
	return n
}()

const ShareMultiplier = 65536.0 // scrypt share multiplier (2^16)

var stratumWriteTimeout = 5 * time.Second

func fixedHexUint32(v uint32) string {
	const digits = "0123456789abcdef"
	var buf [8]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[v&0x0f]
		v >>= 4
	}
	return string(buf[:])
}

// HashToBig converts a common.Hash into a big.Int that can be used to
// perform math comparisons.
func HashToBig(hash *common.Hash) *big.Int {
	// A Hash is in little-endian, but the big package wants the bytes in
	// big-endian, so reverse them.
	buf := *hash
	blen := len(buf)
	for i := 0; i < blen/2; i++ {
		buf[i], buf[blen-1-i] = buf[blen-1-i], buf[i]
	}

	return new(big.Int).SetBytes(buf[:])
}

func bitsToTarget(compact uint32) *big.Int {
	mantissa := compact & 0x007fffff
	isNegative := compact&0x00800000 != 0
	exponent := uint(compact >> 24)
	var bn *big.Int
	if exponent <= 3 {
		mantissa >>= 8 * (3 - exponent)
		bn = big.NewInt(int64(mantissa))
	} else {
		bn = big.NewInt(int64(mantissa))
		bn.Lsh(bn, 8*(exponent-3))
	}

	// Make it negative if the sign bit is set.
	if isNegative {
		bn = bn.Neg(bn)
	}

	return bn
}

func sendResult(c *client, id interface{}, result interface{}) {
	resp := stratumResponse{ID: id, Result: result, Error: nil}
	sendJSON(c, resp)
}

func sendError(c *client, id interface{}, code int, message string) {
	resp := stratumResponse{ID: id, Result: nil, Error: []interface{}{code, message}}
	sendJSON(c, resp)
}

func sendPush(c *client, method string, params interface{}) error {
	return sendPushWithTimeout(c, method, params, stratumWriteTimeout)
}

func sendPushWithTimeout(c *client, method string, params interface{}, timeout time.Duration) error {
	notify := map[string]interface{}{
		"id":     nil,
		"method": method,
		"params": params,
	}
	b, err := json.Marshal(notify)
	if err != nil {
		log.Printf("[%s] marshal error: %v", c.addr, err)
		return err
	}
	return writeJSONLineWithTimeout(c, b, timeout)
}

func sendJSON(c *client, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("[%s] marshal error: %v", c.addr, err)
		return err
	}
	return writeJSONLine(c, b)
}

func writeJSONLine(c *client, b []byte) error {
	return writeJSONLineWithTimeout(c, b, stratumWriteTimeout)
}

func writeJSONLineWithTimeout(c *client, b []byte, timeout time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		if timeout <= 0 {
			timeout = stratumWriteTimeout
		}
		_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	if _, err := c.rw.Write(b); err != nil {
		markClientClosedAfterWriteError(c, "write", err)
		return err
	}
	if _, err := c.rw.WriteString("\n"); err != nil {
		markClientClosedAfterWriteError(c, "write", err)
		return err
	}
	if err := c.rw.Flush(); err != nil {
		markClientClosedAfterWriteError(c, "flush", err)
		return err
	}
	return nil
}

func markClientClosedAfterWriteError(c *client, action string, err error) {
	log.Printf("[%s] %s error: %v", c.addr, action, err)
	c.closed = true
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func sendCompactSubmitAccepted(c *client, id interface{}) error {
	var scratch [128]byte
	b, err := appendCompactSubmitAccepted(scratch[:0], id)
	if err != nil {
		log.Printf("[%s] marshal compact submit accepted error: %v", c.addr, err)
		return err
	}
	return writeJSONLine(c, b)
}

func appendCompactSubmitAccepted(dst []byte, id interface{}) ([]byte, error) {
	dst = append(dst, '{')
	if id != nil {
		dst = append(dst, `"id":`...)
		var err error
		dst, err = appendJSONID(dst, id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, ',')
	}
	dst = append(dst, `"result":true}`...)
	return dst, nil
}

func appendJSONID(dst []byte, id interface{}) ([]byte, error) {
	switch v := id.(type) {
	case string:
		return strconv.AppendQuote(dst, v), nil
	case float64:
		return strconv.AppendFloat(dst, v, 'f', -1, 64), nil
	case float32:
		return strconv.AppendFloat(dst, float64(v), 'f', -1, 32), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case int:
		return strconv.AppendInt(dst, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case uint64:
		return strconv.AppendUint(dst, v, 10), nil
	default:
		b, err := json.Marshal(id)
		if err != nil {
			return nil, err
		}
		return append(dst, b...), nil
	}
}

func rawParamsToStrings(raw []json.RawMessage) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := rawParamSimpleString(r); ok {
			out = append(out, s)
			continue
		}
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			out = append(out, s)
			continue
		}
		var n json.Number
		if err := json.Unmarshal(r, &n); err == nil {
			out = append(out, n.String())
			continue
		}
		out = append(out, strings.Trim(string(r), `"`))
	}
	return out
}

func rawParamSimpleString(raw []byte) (string, bool) {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", false
	}
	body := raw[1 : len(raw)-1]
	for _, b := range body {
		if b == '\\' || b < 0x20 {
			return "", false
		}
	}
	return string(body), true
}

func normalizeHex(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	s = strings.TrimPrefix(s, "0x")
	return s
}

// dummyMerkleRoot implements the "pure dummy" merkle scheme:
// coinb1 = "", coinb2 = "", merkle_branch = []
// so merkle_root = dsha256(extranonce1 || extranonce2).
func dummyMerkleRoot(extranonce1, extranonce2 []byte) [32]byte {
	total := len(extranonce1) + len(extranonce2)
	if total <= 64 {
		var buf [64]byte
		n := copy(buf[:], extranonce1)
		copy(buf[n:], extranonce2)
		h1 := sha256.Sum256(buf[:total])
		h2 := sha256.Sum256(h1[:])
		return h2
	}

	buf := make([]byte, total)
	n := copy(buf, extranonce1)
	copy(buf[n:], extranonce2)
	h1 := sha256.Sum256(buf)
	h2 := sha256.Sum256(h1[:])
	return h2
}

// sendSetExtranonce generates a new extranonce1 and sends mining.set_extranonce
func sendSetExtranonce(c *client) {
	c.mu.Lock()
	subscribed := c.extranonceSubscribed
	c.mu.Unlock()

	if !subscribed {
		return // Client didn't ask for updates
	}

	// Generate new unique extranonce1
	v := atomic.AddUint32(&en1Counter, 1)
	newEn1Bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(newEn1Bytes, v)

	c.mu.Lock()
	c.extranonce1 = [4]byte{}
	copy(c.extranonce1[:], newEn1Bytes)
	c.mu.Unlock()

	newEn1Hex := hex.EncodeToString(newEn1Bytes)
	en2Size := subscribeExtranonce2Size

	// Send notification: {"method": "mining.set_extranonce", "params": ["<en1>", <en2_size>]}
	sendPush(c, "mining.set_extranonce", []interface{}{newEn1Hex, en2Size})
	log.Printf("[%s] sent mining.set_extranonce en1=%s", c.addr, newEn1Hex)
}
