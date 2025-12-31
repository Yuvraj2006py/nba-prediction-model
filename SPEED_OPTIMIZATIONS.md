# Speed Optimizations Applied

## ✅ Changes Made

### 1. Increased Default Workers (2x faster)
- **Changed**: Default workers from 5 → 10
- **Impact**: Processes 10 games simultaneously instead of 5
- **Expected Speedup**: ~2x faster

### 2. Smarter Timeout Handling
- **Changed**: Timeout retries now use shorter delays (1.5x multiplier, capped at 10s)
- **Before**: 5s, 10s, 20s delays on retries
- **After**: ~5s, ~7.5s, ~10s delays (capped)
- **Impact**: Fails faster on timeouts, doesn't waste time on dead connections

### 3. Better Error Handling
- **Added**: Separate handling for timeout vs other errors
- **Impact**: Timeout games are skipped faster, allowing other games to proceed
- **Benefit**: Doesn't block other workers waiting for timeouts

### 4. Graceful Timeout Skipping
- **Added**: Games that timeout are marked as 'timeout' status (not error)
- **Impact**: Can retry timeout games later without blocking current collection
- **Benefit**: Collection continues even when some games timeout

---

## 📊 Expected Performance

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Default Workers** | 5 | 10 | 2x parallelization |
| **Timeout Retry Delays** | 5s, 10s, 20s | ~5s, ~7.5s, ~10s | 50% faster on timeouts |
| **Games per Second** | ~1.08 | ~2.0-2.5 | 2x faster |
| **Time per Season** | ~17 min (1,088 games) | ~8-10 min | ~50% faster |

---

## 🚀 How to Use

### Default (10 workers - recommended)
```bash
python scripts/collect_historical_data_parallel.py
```

### More Aggressive (20 workers - if stable)
```bash
python scripts/collect_historical_data_parallel.py --workers 20
```

### Conservative (5 workers - if hitting rate limits)
```bash
python scripts/collect_historical_data_parallel.py --workers 5
```

---

## ⚙️ Additional Optimizations You Can Try

### Option 1: Reduce Rate Limit Delay
Edit `.env` file:
```env
RATE_LIMIT_DELAY=0.5  # Reduce from 1.0 to 0.5 seconds
```
**Warning**: May hit rate limits if too aggressive

### Option 2: Reduce Retry Delays
Edit `.env` file:
```env
RETRY_DELAY=3.0  # Reduce from 5.0 to 3.0 seconds
MAX_RETRIES=2    # Reduce from 3 to 2 retries
```

### Option 3: Increase Workers Further
```bash
python scripts/collect_historical_data_parallel.py --workers 20
```
**Warning**: May hit rate limits or database locks

---

## 📝 Notes

1. **Timeouts**: Games that timeout will be skipped but can be retried later by running the script again
2. **Rate Limits**: If you see many rate limit errors, reduce workers or increase `RATE_LIMIT_DELAY`
3. **Database Locks**: If you see SQLite lock errors, reduce workers (SQLite doesn't handle many concurrent writes well)
4. **Resumability**: The script is fully resumable - failed/timeout games will be retried on next run

---

## 🎯 Recommendation

**Start with default (10 workers)** - this should give you:
- ~8-10 minutes per season (vs 17 minutes before)
- Better handling of timeouts
- More reliable collection

If stable, you can try 20 workers for even faster collection!
