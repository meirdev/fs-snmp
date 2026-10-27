package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Object is one OID entry from snmp.json.
type Object struct {
	Name string `json:"name"`
	OID  string `json:"oid"`
	Data string `json:"data"`
	// Kind is "table", "column" or empty (scalar / column guessed from the OID).
	Kind      string `json:"kind"`
	Component string `json:"component"`
	// Columns optionally names the columns of a "table" object by their
	// column number, e.g. {"8": "jnxOperatingCPU"}.
	Columns map[string]string `json:"columns"`
}

// IsScalar reports whether the object should be fetched with a GET
// (an instance OID ending in .0) rather than walked.
func (o Object) IsScalar() bool {
	return o.Kind == "" && strings.HasSuffix(o.OID, ".0")
}

type vendorBlock struct {
	Vendors []string `json:"vendors"`
	Objects []Object `json:"objects"`
}

// OIDFile mirrors the structure of snmp.json: every key except
// "vendor_specific" is a flat list of objects.
type OIDFile struct {
	Groups         map[string][]Object
	VendorSpecific []vendorBlock
}

// LoadOIDs parses snmp.json.
func LoadOIDs(path string) (*OIDFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	out := &OIDFile{Groups: map[string][]Object{}}
	for key, msg := range top {
		if key == "vendor_specific" {
			if err := json.Unmarshal(msg, &out.VendorSpecific); err != nil {
				return nil, fmt.Errorf("%s: vendor_specific: %w", path, err)
			}
			continue
		}
		var objs []Object
		if err := json.Unmarshal(msg, &objs); err != nil {
			return nil, fmt.Errorf("%s: group %q: %w", path, key, err)
		}
		out.Groups[key] = objs
	}
	return out, nil
}

// GroupNames lists every pollable group, including vendor_specific.
func (f *OIDFile) GroupNames() []string {
	names := make([]string, 0, len(f.Groups)+1)
	for k := range f.Groups {
		names = append(names, k)
	}
	if len(f.VendorSpecific) > 0 {
		names = append(names, "vendor_specific")
	}
	sort.Strings(names)
	return names
}

// Resolve returns the objects for a group. For vendor_specific the objects
// are filtered by the device vendor (case-insensitive match).
func (f *OIDFile) Resolve(group, vendor string) ([]Object, error) {
	if group != "vendor_specific" {
		objs, ok := f.Groups[group]
		if !ok {
			return nil, fmt.Errorf("unknown group %q (available: %s)", group, strings.Join(f.GroupNames(), ", "))
		}
		return objs, nil
	}
	if vendor == "" {
		return nil, fmt.Errorf("group vendor_specific requires the device vendor to be set")
	}
	var objs []Object
	for _, blk := range f.VendorSpecific {
		for _, v := range blk.Vendors {
			if vendorMatches(v, vendor) {
				objs = append(objs, blk.Objects...)
				break
			}
		}
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("no vendor_specific objects for vendor %q (known: %s)", vendor, strings.Join(f.KnownVendors(), ", "))
	}
	return objs, nil
}

// KnownVendors lists every vendor name appearing under vendor_specific.
func (f *OIDFile) KnownVendors() []string {
	var out []string
	for _, blk := range f.VendorSpecific {
		out = append(out, blk.Vendors...)
	}
	sort.Strings(out)
	return out
}

func vendorMatches(known, want string) bool {
	norm := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	k, w := norm(known), norm(want)
	return k == w || strings.HasPrefix(k, w) || strings.HasPrefix(w, k)
}
