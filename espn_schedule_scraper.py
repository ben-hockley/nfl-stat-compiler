import requests
from bs4 import BeautifulSoup
import json
from dataclasses import dataclass
from typing import Optional, List, Any
from database_connector import insert_all_player_stats, wipe_all_stats_tables

# URL of the page
url = "https://www.espn.com/nfl/schedule/_/week/1/year/2025/seasontype/2"

# Add headers to look like a normal browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

# Fetch the page
response = requests.get(url, headers=headers)
response.raise_for_status()  # will throw an error if the request failed

# Parse HTML
soup = BeautifulSoup(response.text, "html.parser")


def get_game_ids(season: int, week: int, season_type: int) -> list[str]:
    """
    This method scrapes the ESPN NFL schedule page to get all the game IDs for a given season, week, and season type.

    :param season: The NFL season year (e.g., 2025)
    :param week: The week number of the NFL season (e.g., 1)
    :param season_type: The type of season (1 for preseason, 2 for regular season, 3 for playoffs)

    :return: A list of game IDs as strings
    """
    game_ids = []
    url = f"https://www.espn.com/nfl/schedule/_/week/{week}/year/{season}/seasontype/{season_type}"

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    td_elements = soup.find_all("td", class_="teams__col Table__TD")
    for td in td_elements:
        links = td.find_all("a", href=True)
        for link in links:
            href = link["href"]
            if "gameId" in href:
                game_id = href.split("gameId/")[1].split("/")[0]
                game_ids.append(game_id)
    return game_ids

def get_game_json(game_id: str) -> dict:
    """
    This method fetches the JSON data for a given game ID from ESPN's API.

    :param game_id: The game ID as a string

    :return: A dictionary containing the game's JSON data
    """
    api_url = f"https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary?region=us&lang=en&contentorigin=espn&event={game_id}"
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    return json.loads(response.text)


# ----- PLAYER STATS -----
print("\n=== 🏈 PLAYER STATS ===\n")

@dataclass
class PassingStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    completions_attempts: Optional[str]            # composite "C/A" kept as string
    passing_yards: Optional[int]
    passing_touchdowns: Optional[int]
    interceptions: Optional[int]
    sacks: Optional[int]

@dataclass
class RushingStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    rushing_attempts: Optional[int]
    rushing_yards: Optional[int]
    rushing_touchdowns: Optional[int]
    longest_run: Optional[int]

@dataclass
class ReceivingStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    receptions: Optional[int]
    receiving_yards: Optional[int]
    receiving_touchdowns: Optional[int]
    longest_reception: Optional[int]
    targets: Optional[int]

@dataclass
class FumblesStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    fumbles: Optional[int]
    fumbles_lost: Optional[int]
    fumbles_recovered: Optional[int]

@dataclass
class DefensiveStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    total_tackles: Optional[int]
    solo_tackles: Optional[int]
    sacks: Optional[int]
    tackles_for_loss: Optional[int]
    passes_defended: Optional[int]
    qb_hits: Optional[int]
    defensive_touchdowns: Optional[int]

@dataclass
class InterceptionsStats:
    team_id: Optional[int]
    team_name: Optional[str]
    player_id: Optional[int]
    player_name: Optional[str]
    player_headshot_url: Optional[str]
    interceptions: Optional[int]
    interception_yards: Optional[int]
    interception_touchdowns: Optional[int]


def _parse_int(value: Any) -> Optional[int]:
    """
    Convert a raw stat value to int when it represents a single numeric value.
    - Handles strings like "123", "1,234", "12.0" -> returns int
    - Truncates fractional values (int(float(...)))
    - Returns None for composite values like "12-18" or non-numeric text
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        v = value.strip()
        # reject common composite formats (keep as None)
        if "-" in v or "/" in v:
            return None
        # remove commas
        v = v.replace(",", "")
        try:
            # allow floats but convert to int by truncation
            return int(float(v))
        except ValueError:
            return None
    return None


def _safe_stat(stats: list, idx: int) -> Optional[int]:
    try:
        raw = stats[idx]
    except (IndexError, TypeError):
        return None
    return _parse_int(raw)


def get_player_stats(game_id: int) -> List[List[Any]]:
    data = get_game_json(str(game_id))
    boxscore = data.get("boxscore", {})


    passing_stats_list: List[PassingStats] = []
    rushing_stats_list: List[RushingStats] = []
    receiving_stats_list: List[ReceivingStats] = []
    fumbles_stats_list: List[FumblesStats] = []
    defensive_stats_list: List[DefensiveStats] = []
    interceptions_stats_list: List[InterceptionsStats] = []

    for player in boxscore.get("players", []):
        team = player.get("team", {})

        team_id = team.get("id")
        team_name = team.get("displayName")
        for stat_group in player.get("statistics", []):

            # Passing Stats
            if stat_group.get("name") == "passing":

                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    completions_attempts_raw = _safe_stat(stats, 0)  # will be None for "C-A" style
                    # keep original string when composite, otherwise keep string form for completions_attempts
                    completions_attempts = athlete.get("stats", [None])[0]
                    if isinstance(completions_attempts, str) and "-" in completions_attempts:
                        completions_attempts_str = completions_attempts
                    else:
                        # if a single numeric value appears (rare), keep its string form
                        completions_attempts_str = None if completions_attempts is None else str(completions_attempts)

                    passing_yards = _safe_stat(stats, 1)
                    passing_touchdowns = _safe_stat(stats, 3)
                    interceptions = _safe_stat(stats, 4)
                    sacks = _safe_stat(stats, 5)

                    passing_stats = PassingStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        completions_attempts=completions_attempts_str,
                        passing_yards=passing_yards,
                        passing_touchdowns=passing_touchdowns,
                        interceptions=interceptions,
                        sacks=sacks,
                    )
                    passing_stats_list.append(passing_stats)
            
            # Rushing Stats
            elif stat_group.get("name") == "rushing":
                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    rushing_attempts = _safe_stat(stats, 0)
                    rushing_yards = _safe_stat(stats, 1)
                    # yards_per_carry is dropped (not stored) or could be converted similarly
                    rushing_touchdowns = _safe_stat(stats, 3)
                    longest_run = _safe_stat(stats, 4)

                    rushing_stats = RushingStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        rushing_attempts=rushing_attempts,
                        rushing_yards=rushing_yards,
                        rushing_touchdowns=rushing_touchdowns,
                        longest_run=longest_run,
                    )
                    rushing_stats_list.append(rushing_stats)
                    
            # Receiving Stats
            elif stat_group.get("name") == "receiving":
                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    receptions = _safe_stat(stats, 0)
                    receiving_yards = _safe_stat(stats, 1)
                    # yards_per_reception ignored for numeric storage
                    receiving_touchdowns = _safe_stat(stats, 3)
                    longest_reception = _safe_stat(stats, 4)
                    targets = _safe_stat(stats, 5)

                    receiving_stats = ReceivingStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        receptions=receptions,
                        receiving_yards=receiving_yards,
                        receiving_touchdowns=receiving_touchdowns,
                        longest_reception=longest_reception,
                        targets=targets,
                    )
                    receiving_stats_list.append(receiving_stats)

            # Fumbles Stats
            elif stat_group.get("name") == "fumbles":
                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    fumbles = _safe_stat(stats, 0)
                    fumbles_lost = _safe_stat(stats, 1)
                    fumbles_recovered = _safe_stat(stats, 2)

                    fumbles_stats = FumblesStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        fumbles=fumbles,
                        fumbles_lost=fumbles_lost,
                        fumbles_recovered=fumbles_recovered,
                    )
                    fumbles_stats_list.append(fumbles_stats)

            # Defensive Stats
            elif stat_group.get("name") == "defensive":
                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    total_tackles = _safe_stat(stats, 0)
                    solo_tackles = _safe_stat(stats, 1)
                    sacks = _safe_stat(stats, 2)
                    tackles_for_loss = _safe_stat(stats, 3)
                    passes_defended = _safe_stat(stats, 4)
                    qb_hits = _safe_stat(stats, 5)
                    defensive_touchdowns = _safe_stat(stats, 6)

                    defensive_stats = DefensiveStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        total_tackles=total_tackles,
                        solo_tackles=solo_tackles,
                        sacks=sacks,
                        tackles_for_loss=tackles_for_loss,
                        passes_defended=passes_defended,
                        qb_hits=qb_hits,
                        defensive_touchdowns=defensive_touchdowns,
                    )
                    defensive_stats_list.append(defensive_stats)
            # Interceptions Stats
            elif stat_group.get("name") == "interceptions":
                for athlete in stat_group.get("athletes", []):
                    bio = athlete.get("athlete", {})

                    athlete_id = bio.get("id")
                    athlete_name = bio.get("displayName")
                    headshot_url = bio.get("headshot", {}).get("href")

                    stats = athlete.get("stats", [])

                    interceptions = _safe_stat(stats, 0)
                    interception_yards = _safe_stat(stats, 1)
                    interception_touchdowns = _safe_stat(stats, 2)

                    interceptions_stats = InterceptionsStats(
                        team_id=team_id,
                        team_name=team_name,
                        player_id=athlete_id,
                        player_name=athlete_name,
                        player_headshot_url=headshot_url,
                        interceptions=interceptions,
                        interception_yards=interception_yards,
                        interception_touchdowns=interception_touchdowns,
                    )
                    interceptions_stats_list.append(interceptions_stats)
            # Add Special Teams (Kicking, Punting, Kick and Punt Returns) as needed)

    player_stats = [passing_stats_list, rushing_stats_list, receiving_stats_list,
                    fumbles_stats_list, defensive_stats_list, interceptions_stats_list]
    return player_stats

def compile_season_stats(season: int, end_week: int, season_type: int):
    """
    Compiles player stats for all games in gameweeks 1..end_week (inclusive) for the given season and season_type.

    :param season: The NFL season year (e.g., 2025)
    :param end_week: The last week to process (processing will start at week 1)
    :param season_type: The type of season (1 for preseason, 2 for regular season, 3 for playoffs)
    """
    if not isinstance(end_week, int) or end_week < 1:
        raise ValueError("end_week must be an integer >= 1")
    
    if season_type not in [1, 2, 3]:
        raise ValueError("season_type must be 1 (preseason), 2 (regular season), or 3 (playoffs)")
    elif season_type == 1 and end_week > 4:
        raise ValueError("Preseason (season_type=1) only has weeks 1-4")
    elif season_type == 2 and end_week > 18:
        raise ValueError("Regular Season (season_type=2) only has weeks 1-18")
    elif season_type == 3 and end_week > 4:
        raise ValueError("Playoffs (season_type=3) only has weeks 1-4")
    
    if season_type == 1:
        season_type_str = "Preseason"
    elif season_type == 2:
        season_type_str = "Regular Season"
    else:
        season_type_str = "Playoffs"

    print(f"\n=== Compiling stats for Season {season}, Weeks 1..{end_week}, Season Type {season_type_str} ===\n")

    # Wipe existing stats tables once before importing the range of weeks
    wipe_all_stats_tables()

    for week in range(1, end_week + 1):
        print(f"\n*** Processing Week {week} of {end_week} ***\n")
        game_ids = get_game_ids(season, week, season_type)
        print(f"Found {len(game_ids)} games for Season {season}, Week {week}, Season Type {season_type_str}.")

        for game_id in game_ids:
            print(f"\n--- Processing Game ID: {game_id} (Week {week}) ---")
            try:
                stats = get_player_stats(int(game_id))
                inserted = insert_all_player_stats(stats)
                print(f"Inserted/updated counts: {inserted}")
            except Exception as e:
                # keep minimal output; caller can inspect logs or re-run
                print(f"Warning: failed processing game {game_id} (week {week}): {e}")
    
    print(f"\n=== Completed compiling stats for Season {season}, Weeks 1..{end_week}, Season Type {season_type_str} ===\n")


# Example usage:
#compile_season_stats(season=2025, end_week=2, season_type=2)  # Regular Season
