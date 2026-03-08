# FOMO SCORING (-1..1)

## Goal
Chấm “mức độ FOMO” mạnh tay (phân cực) để agent dễ đưa ra hành động.

## Output
- `fomo_score`: float ∈ [-1, 1]
- `fomo_explain_json`: JSON object để debug & tuning.

Example:
```json
{
  "raw_score": 4.2,
  "k": 1.9,
  "final": 0.97,
  "signals": [
    {"term":"TĂNG SỐC","weight":1.5,"count":1},
    {"term":"KỶ LỤC","weight":1.0,"count":1}
  ],
  "ticker_boost": 1.3
}