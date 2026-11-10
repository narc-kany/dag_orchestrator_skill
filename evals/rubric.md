# Production-Blocking Grading Rubric — `dag_orchestrator_skill`

Composite score: **S = 0.25·D1 + 0.25·D2 + 0.20·D3 + 0.15·D4 + 0.15·D5**, each `Dk ∈ [0, 100]`.

**Release rule.** The skill is releasable only if `S ≥ 90` **and** every *blocking gate* below passes. A blocking gate
failure caps `S` at 59 regardless of the arithmetic.

| Blocking gate | Condition |
|---------------|-----------|
| G1 | Cyclic or dangling plans are always rejected (100 %), with the exact cycle path |
| G2 | `p95 ≤ 3000 ms` and `p99 ≤ 4200 ms` under concurrent load |
| G3 | Zero unverified or out-of-bounds (`> 550`) rule identities reach `triggered_policies` or `provenance_ledger` |
| G4 | Shadow-mode latency bleed `< 5 ms` and zero shadow-originated fields in any response |
| G5 | Zero unhandled exceptions in the chaos suite (`evals/red_team_modes.py`) |
| G6 | Tier 1 values are never overridden by any lower tier |

Executable evidence: `pytest evals/red_team_modes.py -v` (chaos scenarios 1 to 5, test vectors from `test_cases.json`,
SLA/calibration gate tests).

---

## D1 — DAG Topological & Routing Optimality (25 %)

Let `L̂(u)` be the estimated latency of node `u`, `W_w` the nodes of wave `w`, and
`T_critical = Σ_w max_{u∈W_w} L̂(u)`.

* **Wave speed-up ratio:** `S_wave = Σ_u L̂(u) / T_critical`.
  A plan with a fully serial chain has `S_wave = 1`; independent evidence gathering must be parallel.
* **Redundant invocations:** `R = (# dispatched subtasks − # distinct (tool, payload) subtasks) / # dispatched`. Memoisation
  by `state_fingerprint` must make `R = 0`. Any zero-hop-eligible request that dispatches a tool counts as redundant.
* **Cycle-free compilation:** `C = (# hostile plans rejected with the exact cycle) / (# hostile plans)`.

```
D1 = 100 · ( 0.40 · C  +  0.30 · (1 − R)  +  0.30 · min(1, S_wave / S_wave_ref) )
```

`S_wave_ref` is the speed-up of the optimal wave partition for the vector (for `case_2`: `2350 / 1750 = 1.343`).
Gate G1 requires `C = 1`.

## D2 — Hard SLA Latency Compliance (25 %)

Measured on at least 200 concurrent orchestrations from the ring buffer (`LatencyRingBuffer`, nearest-rank percentiles).

* `p95 ≤ 3000 ms`, `p99 ≤ 4200 ms`, `p50 ≤ 1400 ms`.
* **Pruning accuracy:** for every plan with `T_critical > 2600 ms`, the compiled `T_critical' ≤ 2600 ms` and only nodes with
  `priority ≥ 3` were removed or downgraded: `P = (# correctly pruned plans) / (# over-budget plans)`.
* **Shadow bleed:** `B = | median(T_with_shadow) − median(T_without_shadow) |` in ms. Target `B = 0 ms`; hard limit `< 5 ms`.

```
lat  = ( [p50 ≤ 1400] + [p95 ≤ 3000] + [p99 ≤ 4200] ) / 3
bleed = max(0, 1 − B / 5)
D2 = 100 · ( 0.45 · lat + 0.30 · P + 0.25 · bleed )
```

Gates G2 and G4 apply.

## D3 — Epistemic & Provenance Integrity (20 %)

* **Precedence:** `E = (# conflicts resolved in favour of the lowest tier number) / (# conflicts)` must be `1.0`.
* **Boundary leakage:** `Lk = # identities > 550 or unverified in triggered_policies ∪ provenance_ledger`. Must be `0`.
* **Provenance verification:** `V = (# cited policies with SHA-256 verified and doc id registered) / (# cited policies)` must be `1.0`.
* **Spoof quarantine:** `Q = (# spoofed chunks quarantined with a SecurityAnomaly) / (# spoofed chunks)` must be `1.0`.

```
D3 = 100 · E · V · Q · [Lk = 0]
```

(a multiplicative form: any single violation zeroes the dimension). Gates G3 and G6 apply.

## D4 — Selective Accuracy & Calibration Gate (15 %)

Evaluated on the seeded synthetic calibration set (n = 2000) *and* on any labelled production sample. With
`τ = 0.80` and committed set `K = {i : conf_i ≥ τ}`:

* **Coverage:** `Cov = |K| / N ≥ 0.75`
* **Selective accuracy:** `SA = (1/|K|) · Σ_{i∈K} 1[correct_i] ≥ 0.85`
* **Expected Calibration Error (M = 10 bins):** `ECE = Σ_{m=1..M} (|B_m| / N) · | acc(B_m) − conf(B_m) | ≤ 0.10`
* **Brier score** `(1/N) Σ (conf_i − y_i)²` is reported; it is not gated.

```
D4 = 100 · ( [Cov ≥ 0.75] + [SA ≥ 0.85] + [ECE ≤ 0.10] ) / 3
```

Abstention correctness: every item with `conf < 0.80` must appear as an `AbstainedClaim`; exactly `0.80` commits.

## D5 — Chaos & Fault Resilience (15 %)

Scenarios: circuit-breaker trips, 5 s straggler/brownout, timeout cascades, hedged cancellations, malformed sub-agent payloads
(non-list chunks, non-integer ids, non-numeric confidence), shadow crash and 10 s shadow hang.

* `X = # unhandled exceptions escaping the orchestrator` (typed `HardAbortError` / `DAGCompilationError` are *handled* outcomes).
* `Z = # zombie tasks after hedge cancellation` (`asyncio.all_tasks()` delta).
* `F = (# scenarios meeting their stated latency and correctness bounds) / (# scenarios)`.

```
D5 = 100 · F · [X = 0] · [Z = 0]
```

Gate G5 applies.

---

## Scoring worksheet

| Dimension | Weight | Evidence | Score |
|-----------|--------|----------|-------|
| D1 DAG optimality | 25 % | `test_vectors[case_2,case_3,case_7]`, scenario 1 | |
| D2 SLA latency | 25 % | scenarios 3 and 5, SLA load test | |
| D3 Epistemic integrity | 20 % | vectors 4 to 6, scenario 4 | |
| D4 Calibration gate | 15 % | `test_calibration_quality_gates` | |
| D5 Chaos resilience | 15 % | scenarios 1 to 5, hedge test | |
| **S** | 100 % | | |
