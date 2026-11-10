# DAG Orchestrator Skill

![Build Status](https://img.shields.io/badge/build-not_configured-lightgrey)
![Version](https://img.shields.io/badge/version-1.0.0-blue)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![MCP Compliance](https://img.shields.io/badge/MCP-contract%201.0-green)

The DAG Orchestrator Skill is the central nervous system for an enterprise AI platform: it turns compound analytical requests into a validated, bounded Directed Acyclic Graph (DAG), dispatches independent work over Model Context Protocol (MCP) tools, and reconciles the results into a provenance-grounded response.

The reference implementation is designed around a 3,000 ms total latency budget, reserving 400 ms for synthesis after a 2,600 ms execution ceiling. It uses a strict four-tier epistemic hierarchy to resolve conflicting evidence deterministically, enforce confidence thresholds, and abstain rather than promote unsupported claims. MCP transport is pluggable; the repository includes an in-process transport for offline evaluation.

## Core Architecture

The orchestrator compiles dependencies into execution waves, runs each wave concurrently within per-tool limits, and reconciles results in fixed tier order. A higher-precedence tier wins over a conflicting lower-precedence claim.

| Tier | Evidence class | MCP tool | Resolution behavior |
| --- | --- | --- | --- |
| 1 | Deterministic compute | `tool_core_compute` | Supplies canonical facts. Tier 1 facts take precedence over policy and retrieval claims. |
| 2 | Protected policy ASTs | `tool_policy_ast` | Evaluates symbolic policy rules; candidate rules and low-confidence evaluations are not promoted. |
| 3 | Cryptographically verified RAG | `tool_hybrid_rag` | Uses retrieved claims only when their corpus identity and SHA-256 chunk digest validate; yields to Tier 1 facts and Tier 2 verdicts. |
| 4 | Abstention and shadow SLM | `tool_slm_intent_shadow` | Abstention is the fail-safe outcome for missing, low-confidence, or unresolved evidence. The SLM runs as non-blocking shadow telemetry and never supplies user-visible answer content. |

The confidence abstention threshold is $\tau = 0.80$. MCP contracts are strict Pydantic v2 models that reject extra fields, and provenance records bind rule identities to corpus tiers and SHA-256 chunk digests.

## Infrastructure and Models

| Module | Model or mechanism | Operational purpose |
| --- | --- | --- |
| [`scripts/dag_compiler.py`](scripts/dag_compiler.py) | Kahn's algorithm; layered topological sorting; critical-path estimate $T_{critical} = \sum_w \max_{u \in W_w} L(u)$ | Validates dependencies and references, forms parallel execution waves, detects cycles, and downgrades or prunes optional work when the execution budget is infeasible. |
| [`scripts/circuit_breaker.py`](scripts/circuit_breaker.py) | EWMA latency drift and full-jitter exponential backoff | Trips on consecutive failures, rolling failure rate, or sustained latency drift; transitions through CLOSED, OPEN, and HALF_OPEN states. |
| [`scripts/concurrency_controller.py`](scripts/concurrency_controller.py) | TCP Vegas queue-delay gradient with AIMD; token bucket | Adapts concurrency from observed queueing delay and utilization, applies per-tool rate limits, and hedges eligible idempotent calls while draining losing requests. |
| [`scripts/state_manager.py`](scripts/state_manager.py) | Tiered 0/1 knapsack optimization; SHA-256 state fingerprint | Preserves pinned Tier 1 context, structurally compresses and selects Tier 2 content under a 4,096-token window, evicts Tier 3 history FIFO, and fingerprints normalized request state. |
| [`scripts/telemetry_logger.py`](scripts/telemetry_logger.py) | Expected Calibration Error (ECE); preallocated single-writer latency ring buffers | Tracks calibration and Brier score, records latency without locks on the event-loop write path, and reports nearest-rank p50/p90/p95/p99 percentiles. |

The latency targets enforced by the evaluation harness are p50 <= 1,400 ms, p95 <= 3,000 ms, and p99 <= 4,200 ms. Calibration gates include deterministic routing coverage >= 0.75, selective accuracy >= 0.85, and ECE <= 0.10.

## Evaluation and Red-Team Testing

[`evals/judges.py`](evals/judges.py) is the CI/CD evaluation harness. It executes the functional vectors against the in-process orchestrator, scores five rubric dimensions (DAG optimality, SLA latency, epistemic integrity, calibration, and resilience), checks latency and calibration gates, and can invoke the release-blocking red-team suite. A case passes only when its weighted score and every dimension are at least 4.0, with no failed individual checks.

[`evals/red_team_modes.py`](evals/red_team_modes.py) exercises adversarial and resilience behavior:

| Adversarial vector | Gate objective |
| --- | --- |
| Cyclic deadlocks | Reject cycles before dispatch and terminate deterministically. |
| Context bombs | Bound execution and context growth while preserving pinned facts. |
| Latency stragglers | Exercise brownouts, hedging, timeouts, and latency-budget behavior. |
| Spoofed provenance | Quarantine forged hashes and unverified or out-of-range identities. |
| Shadow-mode crashes | Ensure shadow failures and hangs cannot block or contaminate synthesis. |

The evaluation harness fails closed with these process exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Functional, SLA, calibration, and requested red-team gates passed. |
| `1` | Functional rubric, latency SLA, or calibration gate missed. |
| `2` | Security gate failed, was skipped, or could not run; includes accepted cycles/deadlocks, identity or provenance leaks, and Tier 1 overrides. |

## Quick Start

Requirements: Python 3.11 or newer. From the repository root, create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On macOS or Linux, activate the environment with:

```bash
source .venv/bin/activate
```

Run the offline functional rubric evaluation:

```bash
python evals/judges.py --offline
```

Run the offline rubric and release-blocking red-team gate together:

```bash
python evals/judges.py --offline --red-team
```

To run the red-team pytest suite directly with detailed test output:

```bash
python -m pytest evals/red_team_modes.py -v
```

## Repository Layout

```text
dag_orchestrator_skill/
├── README.md
├── requirements.txt
├── skill.json
├── Skill.md
├── evals/
│   ├── judges.py
│   ├── red_team_modes.py
│   ├── report.md
│   ├── rubric.md
│   └── test_cases.json
├── reference/
│   ├── guidelines.md
│   └── prompts/
│       ├── conflict_synthesis.md
│       └── task_decomposition.md
├── scripts/
│   ├── __init__.py
│   ├── circuit_breaker.py
│   ├── concurrency_controller.py
│   ├── dag_compiler.py
│   ├── state_manager.py
│   └── telemetry_logger.py
└── tools/
    ├── __init__.py
    ├── mcp_router.py
    ├── mcp_schemas.py
    └── parallel_executor.py
```
