# Basketball Reference Data Requirements

## What We Need from Basketball Reference

We need to scrape **boxscore pages** from Basketball Reference. Each boxscore page contains team and player statistics for a single game.

### URL Format
```
https://www.basketball-reference.com/boxscores/YYYYMMDD0TTM.html
```
- `YYYYMMDD` = Game date (e.g., 20221018)
- `0` = Literal zero
- `TTM` = 3-letter home team abbreviation (e.g., BOS for Boston)

Example: `https://www.basketball-reference.com/boxscores/202210180BOS.html`

---

## Required Data Fields

### 0. Game Details (per game)

We need **game outcome information** for each game:

- ✅ **Game ID** (we already have this)
- ✅ **Season** (e.g., '2022-23')
- ✅ **Season Type** (Regular Season, Playoffs, etc.)
- ✅ **Game Date**
- ✅ **Home Team ID**
- ✅ **Away Team ID**
- ✅ **Home Score** - **CRITICAL - Need to extract from page**
- ✅ **Away Score** - **CRITICAL - Need to extract from page**
- ✅ **Winner** (calculated from scores)
- ✅ **Point Differential** (calculated: home_score - away_score)
- ✅ **Game Status** (finished, since we can access boxscore)

**Where to find on Basketball Reference:**
- Scores are typically in the page header/scorebox area
- Can also extract from team stats tables (points column)

---

### 1. Team Statistics (per team, per game)

We need **2 team stat records per game** (one for home team, one for away team).

#### Basic Scoring Stats:
- ✅ **Points** (PTS)
- ✅ **Field Goals Made** (FG)
- ✅ **Field Goals Attempted** (FGA)
- ✅ **Field Goal Percentage** (FG%)
- ✅ **3-Pointers Made** (3P)
- ✅ **3-Pointers Attempted** (3PA)
- ✅ **3-Point Percentage** (3P%)
- ✅ **Free Throws Made** (FT)
- ✅ **Free Throws Attempted** (FTA)
- ✅ **Free Throw Percentage** (FT%)

#### Rebounding Stats:
- ✅ **Offensive Rebounds** (ORB) - **CRITICAL - Currently missing!**
- ✅ **Defensive Rebounds** (DRB) - **CRITICAL - Currently missing!**
- ✅ **Total Rebounds** (TRB) - **CRITICAL - Currently missing!**

#### Other Stats:
- ✅ **Assists** (AST)
- ✅ **Steals** (STL)
- ✅ **Blocks** (BLK)
- ✅ **Turnovers** (TOV)
- ✅ **Personal Fouls** (PF)

#### Advanced Metrics (calculated, not scraped):
- True Shooting Percentage (TS%)
- Effective Field Goal Percentage (eFG%)

---

### 2. Player Statistics (per player, per game)

We need **all players who played** in the game (typically 8-15 players per team).

#### Player Info:
- ✅ **Player Name** (from link text)
- ✅ **Player ID** (extracted from URL: `/players/j/jamesle01.html` → `jamesle01`)
- ✅ **Team ID** (we already have this from game record)
- ✅ **Minutes Played** (MP) - Format: "MM:SS" (e.g., "35:42")

#### Basic Stats:
- ✅ **Points** (PTS)
- ✅ **Total Rebounds** (TRB)
- ✅ **Assists** (AST)
- ✅ **Field Goals Made** (FG)
- ✅ **Field Goals Attempted** (FGA)
- ✅ **3-Pointers Made** (3P)
- ✅ **3-Pointers Attempted** (3PA)
- ✅ **Free Throws Made** (FT)
- ✅ **Free Throws Attempted** (FTA)

#### Optional Stats (if available):
- ✅ **Plus/Minus** (+/-) - May be in advanced stats table

---

## Page Structure on Basketball Reference

### What the Boxscore Page Contains:

1. **Game Header**
   - Date, teams, final score
   - (We already have this from NBA API)

2. **Team Basic Stats Tables** (2 tables)
   - Table 1: Away team basic stats
   - Table 2: Home team basic stats
   - Each has a "Team Totals" row we need to extract

3. **Player Basic Stats Tables** (2 tables)
   - Table 3: Away team player stats
   - Table 4: Home team player stats
   - Each row is a player

4. **Advanced Stats Tables** (optional)
   - May contain plus/minus and other advanced metrics

### HTML Structure:
- Tables have class: `sortable stats_table`
- Team totals row usually contains "Team Totals" or "Totals" text
- Player rows are regular data rows (not totals)

---

## Current Status

### ✅ What We Can Extract (from existing code):
- All team basic stats (points, FG, 3P, FT, assists, steals, blocks, turnovers, fouls)
- All player basic stats
- Percentages (stored as decimals 0-1, converted to 0-100)

### ❌ What's Currently Missing:
- **Rebounds** (ORB, DRB, TRB) - The main issue!
  - Code looks for these fields but they're not being extracted properly
  - Need to verify the HTML structure on Basketball Reference

---

## Selenium Setup Requirements

To use Selenium, you need:

1. **Chrome Browser** (or Firefox)
   - Must be installed on your system

2. **ChromeDriver**
   - Download from: https://chromedriver.chromium.org/
   - Must match your Chrome version
   - Add to PATH or place in project directory

3. **Python Selenium Package**
   - Already in `requirements.txt`: `selenium>=4.11.0`

### Installation Steps:

```bash
# 1. Check Chrome version
# Open Chrome → Settings → About Chrome

# 2. Download matching ChromeDriver
# Visit: https://chromedriver.chromium.org/downloads
# Download version matching your Chrome

# 3. Extract ChromeDriver
# Place chromedriver.exe in project root or add to PATH

# 4. Verify Selenium is installed
pip install selenium
```

---

## Testing Checklist

Once Selenium is set up, we should test:

1. ✅ Can we access Basketball Reference boxscore pages?
2. ✅ Can we extract team rebounds (ORB, DRB, TRB)?
3. ✅ Can we extract all other team stats?
4. ✅ Can we extract player stats?
5. ✅ Does it work for games from 2022-23 season?
6. ✅ Does it work for recent games?

---

## Next Steps

1. **You:** Set up ChromeDriver
2. **Me:** Update Selenium collector to properly extract rebounds
3. **Together:** Test on a few sample games
4. **Then:** Run backfill for all games

Let me know when ChromeDriver is ready and I'll help test it!
