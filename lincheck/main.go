// lincheck: linearizability checker for LinHistoryClient histories.
//
// Input: JSONL, one op per line (see LinHistoryClient.java):
//
//	{"proc":P,"key":K,"op":"set|get|del|incr","arg":A,"out":O,
//	 "status":"ok|fail|unknown","inv":ns,"ret":ns,"phase":"run|final"}
//
// Each key is checked on its own (a register is P-compositional) with
// Porcupine:
//   - "lin:*" keys: register, state = value | absent. SET v -> v, DEL -> absent
//     (DEL's reply count must match: 1 iff the key existed), GET returns state.
//   - "ctr:*" keys: counter, INCR returns the new value, GET the current one.
//
// Uncertain ops: status "fail" is dropped (the server definitely did not run
// it). "unknown" writes are kept with an unknown output and a return time
// after the end of the history, so the checker may place them anywhere after
// their invocation, or effectively never. "unknown" reads are dropped.
//
// Separately, the lost-write check compares each key's final read ("final"
// phase) with the writes that could legally be last: a write w can be the
// final one only if no acknowledged write was invoked after w returned. A
// final value outside that set means an acknowledged write was lost (or a
// value came back from the dead).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

type rec struct {
	Proc   int     `json:"proc"`
	Key    string  `json:"key"`
	Op     string  `json:"op"`
	Arg    *string `json:"arg"`
	Out    *string `json:"out"`
	Status string  `json:"status"`
	Inv    int64   `json:"inv"`
	Ret    int64   `json:"ret"`
	Phase  string  `json:"phase"`
}

var ignoreDelCount bool

type input struct {
	Op  string
	Arg string
}

type output struct {
	Known bool
	Val   *string // nil = absent (get) / no value
}

// ---- register model ----

type regState struct {
	Present bool
	Val     string
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return regState{} },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s := st.(regState)
		i := in.(input)
		o := out.(output)
		switch i.Op {
		case "set":
			return true, regState{Present: true, Val: i.Arg}
		case "del":
			if o.Known && o.Val != nil && !ignoreDelCount {
				want := "0"
				if s.Present {
					want = "1"
				}
				if *o.Val != want {
					return false, s
				}
			}
			return true, regState{}
		default: // get
			if o.Val == nil {
				return !s.Present, s
			}
			return s.Present && s.Val == *o.Val, s
		}
	},
	Equal: func(a, b interface{}) bool { return a.(regState) == b.(regState) },
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		o := out.(output)
		res := "?"
		if o.Known {
			if o.Val == nil {
				res = "nil"
			} else {
				res = *o.Val
			}
		}
		switch i.Op {
		case "set":
			if !o.Known {
				return fmt.Sprintf("set(%s) -> unknown", i.Arg)
			}
			return fmt.Sprintf("set(%s)", i.Arg)
		case "del":
			return "del -> " + res
		default:
			return "get -> " + res
		}
	},
	DescribeState: func(st interface{}) string {
		s := st.(regState)
		if !s.Present {
			return "absent"
		}
		return s.Val
	},
}

// ---- counter model ----

var counterModel = porcupine.Model{
	Init: func() interface{} { return int64(0) },
	Step: func(st, in, out interface{}) (bool, interface{}) {
		s := st.(int64)
		i := in.(input)
		o := out.(output)
		if i.Op == "incr" {
			if o.Known && o.Val != nil {
				n, err := strconv.ParseInt(*o.Val, 10, 64)
				if err != nil || n != s+1 {
					return false, s
				}
			}
			return true, s + 1
		}
		if o.Val == nil {
			return s == 0, s
		}
		n, err := strconv.ParseInt(*o.Val, 10, 64)
		return err == nil && n == s, s
	},
	Equal: func(a, b interface{}) bool { return a.(int64) == b.(int64) },
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		o := out.(output)
		res := "?"
		if o.Known {
			if o.Val == nil {
				res = "nil"
			} else {
				res = *o.Val
			}
		}
		return i.Op + " -> " + res
	},
	DescribeState: func(st interface{}) string { return strconv.FormatInt(st.(int64), 10) },
}

type keyResult struct {
	Key       string `json:"key"`
	Ops       int    `json:"ops"`
	Unknown   int    `json:"unknown_writes"`
	Result    string `json:"result"` // Ok | Illegal | Unknown (timeout)
	FinalRead string `json:"final_read,omitempty"`
	Lost      bool   `json:"lost_write"`
	LostWhy   string `json:"lost_write_detail,omitempty"`
	Viz       string `json:"visualization,omitempty"`
}

func main() {
	histPath := flag.String("history", "", "history JSONL")
	outDir := flag.String("out", ".", "output directory")
	timeout := flag.Duration("timeout", 60*time.Second, "per-key check timeout")
	par := flag.Int("par", 8, "keys checked in parallel")
	maxViz := flag.Int("max-viz", 20, "max visualizations to write")
	flag.BoolVar(&ignoreDelCount, "ignore-del-count", false, "diagnostic: don't check DEL's reply count")
	flag.Parse()
	if *histPath == "" {
		fmt.Fprintln(os.Stderr, "usage: lincheck -history h.jsonl [-out dir]")
		os.Exit(2)
	}

	recs, err := load(*histPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(2)
	}
	byKey := map[string][]rec{}
	var maxRet int64
	counts := map[string]int{}
	for _, r := range recs {
		counts[r.Op+"/"+r.Status]++
		if r.Ret > maxRet {
			maxRet = r.Ret
		}
		byKey[r.Key] = append(byKey[r.Key], r)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pending := maxRet + int64(time.Hour) // "never returned"

	os.MkdirAll(*outDir, 0o755)
	results := make([]keyResult, len(keys))
	infos := make([]porcupine.LinearizationInfo, len(keys))
	models := make([]porcupine.Model, len(keys))
	var wg sync.WaitGroup
	sem := make(chan struct{}, *par)
	for idx, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, k string) {
			defer wg.Done()
			defer func() { <-sem }()
			ops, unk := toOps(byKey[k], pending)
			m := registerModel
			if strings.HasPrefix(k, "ctr:") {
				m = counterModel
			}
			res, info := porcupine.CheckOperationsVerbose(m, ops, *timeout)
			kr := keyResult{Key: k, Ops: len(ops), Unknown: unk, Result: string(res)}
			if !strings.HasPrefix(k, "ctr:") {
				kr.FinalRead, kr.Lost, kr.LostWhy = lostWriteCheck(byKey[k])
			}
			results[idx] = kr
			infos[idx] = info
			models[idx] = m
		}(idx, k)
	}
	wg.Wait()

	nViz := 0
	summary := map[string]int{}
	for i := range results {
		summary[results[i].Result]++
		if results[i].Lost {
			summary["lost_write_keys"]++
		}
		if results[i].Result == string(porcupine.Illegal) && nViz < *maxViz {
			p := filepath.Join(*outDir, "viz_"+sanitize(results[i].Key)+".html")
			if err := porcupine.VisualizePath(models[i], infos[i], p); err == nil {
				results[i].Viz = p
				nViz++
			}
		}
	}

	report := map[string]interface{}{
		"history": *histPath,
		"records": len(recs),
		"keys":    len(keys),
		"counts":  counts,
		"summary": summary,
		"per_key": results,
	}
	f, _ := os.Create(filepath.Join(*outDir, "lincheck_result.json"))
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.Encode(report)
	f.Close()

	fmt.Printf("records=%d keys=%d\n", len(recs), len(keys))
	cs := make([]string, 0, len(counts))
	for c, n := range counts {
		cs = append(cs, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(cs)
	fmt.Println("ops:", strings.Join(cs, " "))
	fmt.Printf("linearizable keys=%d  VIOLATING keys=%d  timed-out keys=%d  lost-write keys=%d\n",
		summary["Ok"], summary["Illegal"], summary["Unknown"], summary["lost_write_keys"])
	shown := 0
	for _, r := range results {
		if (r.Result != "Ok" || r.Lost) && shown < 30 {
			fmt.Printf("  %s: %s lost=%v %s %s\n", r.Key, r.Result, r.Lost, r.LostWhy, r.Viz)
			shown++
		}
	}
	verdict := "PASS"
	if summary["Illegal"] > 0 || summary["lost_write_keys"] > 0 {
		verdict = "FAIL"
	} else if summary["Unknown"] > 0 {
		verdict = "INCONCLUSIVE"
	}
	fmt.Println("VERDICT:", verdict)
	if verdict == "FAIL" {
		os.Exit(1)
	}
	if verdict == "INCONCLUSIVE" {
		os.Exit(3)
	}
}

func load(path string) ([]rec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []rec
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var r rec
		if err := json.Unmarshal(b, &r); err != nil {
			// A truncated last line (client killed mid-write) is tolerated.
			fmt.Fprintf(os.Stderr, "skip line %d: %v\n", line, err)
			continue
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// toOps converts one key's records into Porcupine operations with dense
// client ids (the visualizer needs them zero-indexed).
func toOps(rs []rec, pending int64) ([]porcupine.Operation, int) {
	ids := map[int]int{}
	var ops []porcupine.Operation
	unk := 0
	for _, r := range rs {
		if r.Status == "fail" {
			continue
		}
		if r.Status == "unknown" && r.Op == "get" {
			continue
		}
		in := input{Op: r.Op}
		if r.Arg != nil {
			in.Arg = *r.Arg
		}
		out := output{Known: r.Status == "ok", Val: r.Out}
		ret := r.Ret
		if r.Status == "unknown" {
			out = output{}
			ret = pending
			unk++
		}
		id, ok := ids[r.Proc]
		if !ok {
			id = len(ids)
			ids[r.Proc] = id
		}
		ops = append(ops, porcupine.Operation{ClientId: id, Input: in, Call: r.Inv, Output: out, Return: ret})
	}
	return ops, unk
}

// lostWriteCheck: is the final read one of the values that could legally be
// last? Returns (final value, lost?, detail).
func lostWriteCheck(rs []rec) (string, bool, string) {
	var final *rec
	type w struct {
		val   *string // nil = del
		inv   int64
		ret   int64 // max int for unknown
		acked bool
	}
	var writes []w
	for i := range rs {
		r := rs[i]
		if r.Phase == "final" && r.Op == "get" && r.Status == "ok" {
			final = &rs[i]
			continue
		}
		if r.Status == "fail" || (r.Op != "set" && r.Op != "del") {
			continue
		}
		x := w{inv: r.Inv, ret: r.Ret, acked: r.Status == "ok"}
		if r.Status == "unknown" {
			x.ret = 1<<63 - 1
		}
		if r.Op == "set" {
			x.val = r.Arg
		}
		writes = append(writes, x)
	}
	if final == nil {
		return "", false, "no final read"
	}
	fv := "nil"
	if final.Out != nil {
		fv = *final.Out
	}
	var maxAckedInv int64 = -1
	for _, x := range writes {
		if x.acked && x.inv > maxAckedInv {
			maxAckedInv = x.inv
		}
	}
	if maxAckedInv < 0 && final.Out == nil {
		return fv, false, "" // no acked write: the initial "absent" may still be current
	}
	// candidates: writes not strictly followed by an acked write
	anyCand := false
	var latestAcked *w
	for i, x := range writes {
		if x.ret < maxAckedInv {
			continue
		}
		anyCand = true
		if (x.val == nil && final.Out == nil) || (x.val != nil && final.Out != nil && *x.val == *final.Out) {
			return fv, false, ""
		}
		if x.acked && (latestAcked == nil || x.inv > latestAcked.inv) {
			latestAcked = &writes[i]
		}
	}
	if !anyCand {
		if final.Out == nil {
			return fv, false, ""
		}
		return fv, true, "final=" + fv + " but key was never written"
	}
	exp := "(unknown writes only)"
	if latestAcked != nil {
		exp = "nil(del)"
		if latestAcked.val != nil {
			exp = *latestAcked.val
		}
	}
	return fv, true, "final=" + fv + " expected one of the last writes, e.g. acked " + exp
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ':' || r == '/' || r == '{' || r == '}' {
			return '_'
		}
		return r
	}, s)
}
