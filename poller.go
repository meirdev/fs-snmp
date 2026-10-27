package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/gosnmp/gosnmp"
)

// Sample is one polled value, sent to the output writer.
// On the wire the timestamp is sent as unix milliseconds (ts) only.
type Sample struct {
	Time   time.Time `json:"-"`
	TS     int64     `json:"ts"`
	Device string    `json:"device"`
	Host   string    `json:"host"`
	Group  string    `json:"group"`
	Name   string    `json:"name"`
	OID    string    `json:"oid"`
	Index  string    `json:"index,omitempty"`
	Type   string    `json:"type"`
	Value  any       `json:"value"`
}

// Poller polls one group of one device on a fixed interval.
type Poller struct {
	dev      Device
	group    string
	interval time.Duration
	objects  []Object
	out      chan<- Sample
	log      *slog.Logger
}

func newClient(dev Device) *gosnmp.GoSNMP {
	ver := gosnmp.Version2c
	if dev.Version == "1" {
		ver = gosnmp.Version1
	}
	return &gosnmp.GoSNMP{
		Target:             dev.Host,
		Port:               uint16(dev.Port),
		Community:          dev.Community,
		Version:            ver,
		Timeout:            dev.Timeout,
		Retries:            dev.Retries,
		ExponentialTimeout: false,
		MaxOids:            gosnmp.MaxOids,
		MaxRepetitions:     25,
	}
}

// Run polls immediately, then every interval, until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context) {
	client := newClient(p.dev)
	client.Context = ctx
	if err := client.Connect(); err != nil {
		p.log.Error("connect failed", "err", err)
		return
	}
	defer client.Conn.Close()

	start := time.Now()
	var scalars []Object
	for _, obj := range p.objects {
		if obj.IsScalar() {
			scalars = append(scalars, obj)
			continue
		}
		if err := p.walk(client, obj); err != nil {
			p.log.Warn("walk failed", "name", obj.Name, "oid", obj.OID, "err", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
	if len(scalars) > 0 {
		if err := p.get(client, scalars); err != nil {
			p.log.Warn("get failed", "err", err)
		}
	}
	p.log.Debug("poll complete", "took", time.Since(start).Round(time.Millisecond))
}

// get fetches scalar objects in batches with a single GET per batch.
func (p *Poller) get(client *gosnmp.GoSNMP, objs []Object) error {
	for len(objs) > 0 {
		n := min(len(objs), client.MaxOids)
		batch := objs[:n]
		objs = objs[n:]

		oids := make([]string, n)
		for i, o := range batch {
			oids[i] = o.OID
		}
		res, err := client.Get(oids)
		if err != nil {
			return err
		}
		for i, pdu := range res.Variables {
			if i >= len(batch) {
				break
			}
			p.emit(batch[i], pdu)
		}
	}
	return nil
}

// walk fetches a column or table with GETBULK (v2c) or GETNEXT (v1).
func (p *Poller) walk(client *gosnmp.GoSNMP, obj Object) error {
	fn := func(pdu gosnmp.SnmpPDU) error {
		p.emit(obj, pdu)
		return nil
	}
	if client.Version == gosnmp.Version1 {
		return client.Walk(obj.OID, fn)
	}
	return client.BulkWalk(obj.OID, fn)
}

func (p *Poller) emit(obj Object, pdu gosnmp.SnmpPDU) {
	typ, val := decode(pdu)
	name, idx := obj.Name, indexOf(obj, pdu.Name)
	if obj.Kind == "table" {
		// A walked table yields <entry>.<column>.<row index>. Split it so each
		// column becomes its own metric (named via "columns" when provided).
		if parts := strings.SplitN(idx, ".", 3); len(parts) == 3 {
			if alias, ok := obj.Columns[parts[1]]; ok {
				name = alias
			} else {
				name = obj.Name + "." + parts[1]
			}
			idx = parts[2]
		}
	}
	now := time.Now()
	p.out <- Sample{
		Time:   now,
		TS:     now.UnixMilli(),
		Device: p.dev.Name,
		Host:   p.dev.Host,
		Group:  p.group,
		Name:   name,
		OID:    strings.TrimPrefix(pdu.Name, "."),
		Index:  idx,
		Type:   typ,
		Value:  val,
	}
}

// indexOf strips the base OID from the returned instance OID.
// A scalar GET yields an empty index.
func indexOf(obj Object, name string) string {
	full := strings.TrimPrefix(name, ".")
	if obj.IsScalar() {
		return ""
	}
	return strings.TrimPrefix(strings.TrimPrefix(full, obj.OID), ".")
}

// decode converts a PDU into a type label and a JSON-friendly value.
func decode(pdu gosnmp.SnmpPDU) (string, any) {
	switch pdu.Type {
	case gosnmp.OctetString:
		b, _ := pdu.Value.([]byte)
		if isPrintable(b) {
			return "string", string(b)
		}
		return "hex", formatHex(b)
	case gosnmp.ObjectIdentifier:
		s, _ := pdu.Value.(string)
		return "oid", strings.TrimPrefix(s, ".")
	case gosnmp.IPAddress:
		return "ipaddr", fmt.Sprint(pdu.Value)
	case gosnmp.TimeTicks:
		return "timeticks", gosnmp.ToBigInt(pdu.Value).Uint64()
	case gosnmp.Counter64:
		return "counter64", gosnmp.ToBigInt(pdu.Value).Uint64()
	case gosnmp.Counter32:
		return "counter32", gosnmp.ToBigInt(pdu.Value).Uint64()
	case gosnmp.Gauge32:
		return "gauge32", gosnmp.ToBigInt(pdu.Value).Uint64()
	case gosnmp.Integer:
		return "integer", gosnmp.ToBigInt(pdu.Value).Int64()
	case gosnmp.Null:
		return "null", nil
	case gosnmp.NoSuchObject:
		return "noSuchObject", nil
	case gosnmp.NoSuchInstance:
		return "noSuchInstance", nil
	case gosnmp.EndOfMibView:
		return "endOfMibView", nil
	default:
		return strings.ToLower(pdu.Type.String()), fmt.Sprint(pdu.Value)
	}
}

func isPrintable(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	for _, r := range string(b) {
		if r == unicode.ReplacementChar || (!unicode.IsPrint(r) && !unicode.IsSpace(r)) {
			return false
		}
	}
	return true
}

// formatHex renders bytes as colon-separated hex (MAC-address style).
func formatHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	h := hex.EncodeToString(b)
	var sb strings.Builder
	for i := 0; i < len(h); i += 2 {
		if i > 0 {
			sb.WriteByte(':')
		}
		sb.WriteString(h[i : i+2])
	}
	return sb.String()
}
