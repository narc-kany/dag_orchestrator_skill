# Evaluation Report: dag_orchestrator_skill

- Generated: 2026-11-10 10:53:55Z
- Mode: offline (deterministic heuristic)
- Judge: heuristic (deterministic)
- Red Team gate: not requested

## Functional rubric matrix (scale 1-5; weights D1 0.25, D2 0.25, D3 0.20, D4 0.15, D5 0.15; pass >= 4.0)

| Case | D1 DAG | D2 SLA | D3 Epistemic | D4 Calibration | D5 Resilience | Overall | Latency (ms) | Verdict |
|---|---|---|---|---|---|---|---|---|
| `case_1_zero_hop_canonical_bypass` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_2_three_wave_diamond_families_1_2_5` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 16.0 | PASS |
| `case_3_over_budget_pruning` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_4_tier1_compute_vs_tier3_rag_contradiction` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_5_candidate_rule_low_confidence_abstention` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_6_out_of_bounds_rule_identity_612` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_7_cyclic_plan_rejected` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_8_core_compute_outage_hard_abort` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |
| `case_9_optional_node_failure_uses_ref_default` | 5.00 | 5.00 | 5.00 | 5.00 | 5.00 | **5.00** | 0.0 | PASS |

Mean overall score: **5.00** over 9 cases.

## SLA (concurrent load probe, 100 orchestrations)

| Percentile | Measured (ms) | Target (ms) | Verdict |
|---|---|---|---|
| p50 | 150.4 | 1400 | PASS |
| p95 | 184.5 | 3000 | PASS |
| p99 | 188.8 | 4200 | PASS |

Degraded runs: 0 of 100.

## Selective accuracy and calibration (seeded synthetic set, n = 2000)

| Metric | Measured | Gate | Verdict |
|---|---|---|---|
| Routing coverage | 0.831 | >= 0.75 | PASS |
| Selective accuracy | 0.899 | >= 0.85 | PASS |
| ECE (10 bins) | 0.008 | <= 0.1 | PASS |
| Brier score | 0.113 | reported | - |

## Red Team gate

Not requested (run with `--red-team`).

## Verdict

**Final verdict: PASS** (exit 0): 9 functional cases.
