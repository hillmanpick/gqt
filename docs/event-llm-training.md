# Event contract LLM training

This workflow turns local BTC/ETH event-contract virtual tickets into local LLM
training artifacts and factor reports. Generated data stays local under
`data/`, which is ignored by Git.

## Generate the pack

PowerShell:

```powershell
$py = 'C:\Users\10194\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$db = Join-Path $env:LOCALAPPDATA 'HillmanPick\GQT Trader\data\trading\user_data\event_predictions.sqlite'
& $py 'D:\x\gqt\scripts\build_event_llm_training_pack.py' $db --output-dir 'D:\x\gqt\data\event_llm_training' --strategy all --format both
```

Outputs:

- `event_train.chat.jsonl`, `event_validation.chat.jsonl`, `event_test.chat.jsonl`
- `event_train.record.jsonl`, `event_validation.record.jsonl`, `event_test.record.jsonl`
- `manifest.json`
- `factor_report.md`
- `factor_report.json`
- `direction_baselines.json`

## Recommended usage

- Use `train` and `validation` for model training or fine-tuning.
- Keep `test` untouched for later scoring.
- Use `factor_report.md` to decide measurable strategy changes.
- Regenerate after more settled tickets arrive.

The default `--strategy all` uses all settled BTC/ETH labels because even losing
legacy strategy tickets still contain valid pre-expiry factors and realized
direction labels. Use `--strategy current` when you only want the active
strategy, or pass `--strategy direction_dataset_v2` / `--strategy
direction_dataset_v3` for a specific version comparison.
