# System Evolution Roadmap

## Goal A: Reduce False Positives

We propose a staged refinement strategy that preserves the current heuristic pipeline while incrementally tightening precision.

1. **Pool Identification Hardening**
   - Introduce program-specific decoders for Raydium/Orca/Whirlpool to validate that `RelatedPoolsInfo` corresponds to actual AMM swap semantics.
   - Add a whitelist of swap program IDs and reject transactions whose `programs` set does not include a supported AMM.

2. **Amount-Consistency Calibration**
   - Learn token-specific tolerances for `relativeDiffB` using historical distributions, replacing a global threshold with a percentile-based bound.
   - Integrate fee-aware adjustment for SOL trades to distinguish rent/priority fee noise from genuine slippage.

3. **Victim Robustness Checks**
   - Require consistent price impact direction across victim swaps within a sandwich window.
   - Enforce a minimum victim size or minimum tokenA/tokenB delta to avoid small-transfer false positives.

4. **Multi-Pool Path Filtering**
   - Reject front/back transactions with `RelatedPools` cardinality above an adaptive threshold (currently bounded by signer equality).
   - Optionally classify multi-hop routes and account for path length in false-positive scoring.

[Our Contribution] We will release a false-positive benchmark derived from the `sandwiched.me` comparison script and use it for continuous threshold regression testing.

## Goal B: Offline Confidence Scoring with Machine Learning

We propose an offline scoring pipeline that assigns a probabilistic confidence score to each detected sandwich while preserving the existing deterministic output.

### Architecture

1. **Feature Store**
   - Input: `sandwiches`, `sandwich_txs`, `slot_txs`, `slot_leaders`, `jito_bundles`.
   - Features: distance metrics, `relativeDiffB`, `profitA`, `inBundle`, signer/owner relations, token class, slot leader identity, fee ratios.

2. **Labeling Strategy**
   - Silver labels derived from heuristic agreement with external sources (`sandwiched.me`) and high-confidence internal rules (e.g., `consecutive` and `perfect`).
   - Active-learning loop to sample low-confidence cases for manual inspection.

3. **Modeling**
   - Gradient-boosted trees (e.g., LightGBM/XGBoost) for tabular interpretability.
   - Calibration via isotonic regression to map scores to probabilities.

4. **Batch Inference**
   - Nightly batch job generates `confidence_score` and writes back to a new table `sandwich_scores` keyed by `sandwichId`.
   - Scores are joined during analysis and used to weight aggregate statistics.

[Our Contribution] The confidence scoring framework will be released as a reproducible pipeline with feature definitions and calibration reports, enabling transparent comparison across datasets.
