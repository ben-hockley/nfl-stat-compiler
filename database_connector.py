from config.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from typing import Any, Iterable, List, Optional, Tuple
import mariadb


def get_connection(
    user = DB_USER,
    password = DB_PASSWORD,
    host: str = DB_HOST,
    port: int = DB_PORT,
    database: str = DB_NAME,
) -> mariadb.Connection:
    """
    Return a mariadb.Connection connected to the specified database.
    Credentials default to environment variables: DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME.
    Raises mariadb.Error on failure.
    """
    return mariadb.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        autocommit=False,
    )

database_connection = get_connection()

def execute_query(
    conn: mariadb.Connection,
    query: str,
    params: Optional[Iterable[Any]] = None,
    fetch: bool = True,
) -> List[Tuple]:
    """
    Execute a query using provided connection.
    If fetch==True returns rows as list of tuples, otherwise returns empty list.
    Caller is responsible for committing/rolling back where needed.
    """
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        if fetch:
            return cur.fetchall()
        return []
    finally:
        cur.close()


def execute_many(
    conn: mariadb.Connection,
    query: str,
    params_seq: Iterable[Iterable[Any]],
) -> None:
    """
    Execute many (e.g., bulk inserts). Caller should commit after success.
    """
    cur = conn.cursor()
    try:
        cur.executemany(query, params_seq)
    finally:
        cur.close()


# -----------------------
# Insert helpers for dataclasses produced by espn_schedule_scraper.get_player_stats
# Each insert_* function will perform upsert-like behavior:
# - If player_id exists in the table, numeric fields are added to stored values.
# - If player_id does not exist, a new row is inserted.
# Accepts either dataclass instances or dicts (uses getattr then dict access).
# -----------------------

def _field(obj: Any, name: str) -> Any:
    """Get attribute from object or key from dict, returning None if missing."""
    if obj is None:
        return None
    if hasattr(obj, name):
        return getattr(obj, name)
    if isinstance(obj, dict):
        return obj.get(name)
    return None

def _to_int(v: Any) -> Optional[int]:
    """Convert a value to int, returning None if it can't be converted."""
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).replace(",", "")))
        except Exception:
            return None

def _int_or_zero(v: Any) -> int:
    n = _to_int(v)
    return 0 if n is None else n

def _merge_completions(existing: Optional[str], incoming: Optional[str]) -> Optional[str]:
    """
    Merge two completions-attempts strings by summing corresponding sides of the slash.
    - Examples:
        existing="10/15", incoming="5/8" -> "15/23"
        existing="10/15", incoming="7"   -> "17/15"
        existing=None, incoming="7/9"    -> "7/9"
        existing="7", incoming="3"       -> "10"
    """
    def to_pair(s: Optional[str]) -> tuple[Optional[int], Optional[int]]:
        if s is None:
            return (None, None)
        s = str(s).strip()
        if "/" in s:
            parts = s.split("/", 1)
            left = _to_int(parts[0])
            right = _to_int(parts[1])
            return (left, right)
        # single number (treat as left/completions)
        n = _to_int(s)
        return (n, None)

    e_left, e_right = to_pair(existing)
    i_left, i_right = to_pair(incoming)

    # If both entirely missing -> None
    if e_left is None and e_right is None and i_left is None and i_right is None:
        return None

    # Sum left sides (treat missing as 0)
    left_sum = (e_left or 0) + (i_left or 0)

    # Determine if we should produce a "left-right" form:
    # produce right side if any right exists in either value
    # otherwise return single value (left only)
    if e_right is not None or i_right is not None:
        right_sum = (e_right or 0) + (i_right or 0)
        return f"{left_sum}/{right_sum}"
    else:
        return str(left_sum)
    

def insert_passing_stats(passing_stats: Iterable[Any]) -> int:
    """
    Insert or add-to existing passing_stats rows.
    If a row with the same player_id exists, numeric fields are summed and
    completions_attempts strings are merged by summing each side of the dash.
    Returns number of rows processed (inserted + updated).
    """
    if not passing_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in passing_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")
            completions_attempts = _field(s, "completions_attempts")  # keep as string if present

            p_yards = _int_or_zero(_field(s, "passing_yards"))
            p_tds = _int_or_zero(_field(s, "passing_touchdowns"))
            ints = _int_or_zero(_field(s, "interceptions"))
            sacks = _int_or_zero(_field(s, "sacks"))

            # check existing row by player_id
            cur.execute(
                "SELECT id, passing_yards, passing_touchdowns, interceptions, sacks, completions_attempts FROM passing_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                # add numeric values
                existing_p_yards = _int_or_zero(row[1])
                existing_p_tds = _int_or_zero(row[2])
                existing_ints = _int_or_zero(row[3])
                existing_sacks = _int_or_zero(row[4])
                existing_comps = row[5]

                new_p_yards = existing_p_yards + p_yards
                new_p_tds = existing_p_tds + p_tds
                new_ints = existing_ints + ints
                new_sacks = existing_sacks + sacks

                # merge completions_attempts: sum corresponding sides of dash
                comps_to_set = _merge_completions(existing_comps, completions_attempts)

                cur.execute(
                    """
                    UPDATE passing_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        completions_attempts=%s, passing_yards=%s, passing_touchdowns=%s,
                        interceptions=%s, sacks=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (
                        team_id, team_name, player_name, headshot,
                        comps_to_set, new_p_yards, new_p_tds, new_ints, new_sacks,
                        pid,
                    ),
                )
            else:
                # insert new row
                cur.execute(
                    """
                    INSERT INTO passing_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       completions_attempts, passing_yards, passing_touchdowns, interceptions, sacks, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (
                        team_id, team_name, pid, player_name, headshot,
                        completions_attempts, p_yards, p_tds, ints, sacks,
                    ),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()

def insert_rushing_stats(rushing_stats: Iterable[Any]) -> int:
    if not rushing_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in rushing_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")

            attempts = _int_or_zero(_field(s, "rushing_attempts"))
            yards = _int_or_zero(_field(s, "rushing_yards"))
            tds = _int_or_zero(_field(s, "rushing_touchdowns"))
            longest = _to_int(_field(s, "longest_run"))

            cur.execute(
                "SELECT id, rushing_attempts, rushing_yards, rushing_touchdowns, longest_run FROM rushing_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                existing_attempts = _int_or_zero(row[1])
                existing_yards = _int_or_zero(row[2])
                existing_tds = _int_or_zero(row[3])
                existing_longest = _to_int(row[4]) or 0

                new_attempts = existing_attempts + attempts
                new_yards = existing_yards + yards
                new_tds = existing_tds + tds
                # for "longest_run" keep max
                new_longest = max(existing_longest, longest or 0)

                cur.execute(
                    """
                    UPDATE rushing_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        rushing_attempts=%s, rushing_yards=%s, rushing_touchdowns=%s, longest_run=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (team_id, team_name, player_name, headshot, new_attempts, new_yards, new_tds, new_longest, pid),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO rushing_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       rushing_attempts, rushing_yards, rushing_touchdowns, longest_run, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (team_id, team_name, pid, player_name, headshot, attempts, yards, tds, longest),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()

def insert_receiving_stats(receiving_stats: Iterable[Any]) -> int:
    if not receiving_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in receiving_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")

            receptions = _int_or_zero(_field(s, "receptions"))
            yards = _int_or_zero(_field(s, "receiving_yards"))
            tds = _int_or_zero(_field(s, "receiving_touchdowns"))
            longest = _to_int(_field(s, "longest_reception"))
            targets = _int_or_zero(_field(s, "targets"))

            cur.execute(
                "SELECT id, receptions, receiving_yards, receiving_touchdowns, longest_reception, targets FROM receiving_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                existing_recs = _int_or_zero(row[1])
                existing_yards = _int_or_zero(row[2])
                existing_tds = _int_or_zero(row[3])
                existing_longest = _to_int(row[4]) or 0
                existing_targets = _int_or_zero(row[5])

                new_recs = existing_recs + receptions
                new_yards = existing_yards + yards
                new_tds = existing_tds + tds
                new_longest = max(existing_longest, longest or 0)
                new_targets = existing_targets + targets

                cur.execute(
                    """
                    UPDATE receiving_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        receptions=%s, receiving_yards=%s, receiving_touchdowns=%s, longest_reception=%s, targets=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (team_id, team_name, player_name, headshot, new_recs, new_yards, new_tds, new_longest, new_targets, pid),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO receiving_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       receptions, receiving_yards, receiving_touchdowns, longest_reception, targets, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (team_id, team_name, pid, player_name, headshot, receptions, yards, tds, longest, targets),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()

def insert_fumbles_stats(fumbles_stats: Iterable[Any]) -> int:
    if not fumbles_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in fumbles_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")

            fumbles = _int_or_zero(_field(s, "fumbles"))
            lost = _int_or_zero(_field(s, "fumbles_lost"))
            recovered = _int_or_zero(_field(s, "fumbles_recovered"))

            cur.execute(
                "SELECT id, fumbles, fumbles_lost, fumbles_recovered FROM fumbles_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                existing_fumbles = _int_or_zero(row[1])
                existing_lost = _int_or_zero(row[2])
                existing_recovered = _int_or_zero(row[3])

                new_fumbles = existing_fumbles + fumbles
                new_lost = existing_lost + lost
                new_recovered = existing_recovered + recovered

                cur.execute(
                    """
                    UPDATE fumbles_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        fumbles=%s, fumbles_lost=%s, fumbles_recovered=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (team_id, team_name, player_name, headshot, new_fumbles, new_lost, new_recovered, pid),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO fumbles_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       fumbles, fumbles_lost, fumbles_recovered, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (team_id, team_name, pid, player_name, headshot, fumbles, lost, recovered),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()

def insert_defensive_stats(defensive_stats: Iterable[Any]) -> int:
    if not defensive_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in defensive_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")

            total_tackles = _int_or_zero(_field(s, "total_tackles"))
            solo_tackles = _int_or_zero(_field(s, "solo_tackles"))
            sacks = _int_or_zero(_field(s, "sacks"))
            tfl = _int_or_zero(_field(s, "tackles_for_loss"))
            passes_def = _int_or_zero(_field(s, "passes_defended"))
            qb_hits = _int_or_zero(_field(s, "qb_hits"))
            def_tds = _int_or_zero(_field(s, "defensive_touchdowns"))

            cur.execute(
                "SELECT id, total_tackles, solo_tackles, sacks, tackles_for_loss, passes_defended, qb_hits, defensive_touchdowns FROM defensive_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                existing_total = _int_or_zero(row[1])
                existing_solo = _int_or_zero(row[2])
                existing_sacks = _int_or_zero(row[3])
                existing_tfl = _int_or_zero(row[4])
                existing_passes = _int_or_zero(row[5])
                existing_qb_hits = _int_or_zero(row[6])
                existing_def_tds = _int_or_zero(row[7])

                new_total = existing_total + total_tackles
                new_solo = existing_solo + solo_tackles
                new_sacks = existing_sacks + sacks
                new_tfl = existing_tfl + tfl
                new_passes = existing_passes + passes_def
                new_qb_hits = existing_qb_hits + qb_hits
                new_def_tds = existing_def_tds + def_tds

                cur.execute(
                    """
                    UPDATE defensive_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        total_tackles=%s, solo_tackles=%s, sacks=%s, tackles_for_loss=%s,
                        passes_defended=%s, qb_hits=%s, defensive_touchdowns=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (team_id, team_name, player_name, headshot,
                     new_total, new_solo, new_sacks, new_tfl, new_passes, new_qb_hits, new_def_tds, pid),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO defensive_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       total_tackles, solo_tackles, sacks, tackles_for_loss, passes_defended, qb_hits, defensive_touchdowns, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (team_id, team_name, pid, player_name, headshot,
                     total_tackles, solo_tackles, sacks, tfl, passes_def, qb_hits, def_tds),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()

def insert_interceptions_stats(interceptions_stats: Iterable[Any]) -> int:
    if not interceptions_stats:
        return 0
    cur = database_connection.cursor()
    processed = 0
    try:
        for s in interceptions_stats:
            pid = _field(s, "player_id")
            if pid is None:
                continue
            team_id = _field(s, "team_id")
            team_name = _field(s, "team_name")
            player_name = _field(s, "player_name")
            headshot = _field(s, "player_headshot_url")

            ints = _int_or_zero(_field(s, "interceptions"))
            yards = _int_or_zero(_field(s, "interception_yards"))
            tds = _int_or_zero(_field(s, "interception_touchdowns"))

            cur.execute(
                "SELECT id, interceptions, interception_yards, interception_touchdowns FROM interceptions_stats WHERE player_id = %s",
                (pid,),
            )
            row = cur.fetchone()
            if row:
                existing_ints = _int_or_zero(row[1])
                existing_yards = _int_or_zero(row[2])
                existing_tds = _int_or_zero(row[3])

                new_ints = existing_ints + ints
                new_yards = existing_yards + yards
                new_tds = existing_tds + tds

                cur.execute(
                    """
                    UPDATE interceptions_stats
                    SET team_id=%s, team_name=%s, player_name=%s, player_headshot_url=%s,
                        interceptions=%s, interception_yards=%s, interception_touchdowns=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE player_id=%s
                    """,
                    (team_id, team_name, player_name, headshot, new_ints, new_yards, new_tds, pid),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO interceptions_stats
                      (team_id, team_name, player_id, player_name, player_headshot_url,
                       interceptions, interception_yards, interception_touchdowns, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                    """,
                    (team_id, team_name, pid, player_name, headshot, ints, yards, tds),
                )
            processed += 1
        database_connection.commit()
        return processed
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()


def insert_all_player_stats(player_stats: List[List[Any]]) -> dict:
    """
    Accepts player_stats as returned by get_player_stats:
      [passing_stats_list, rushing_stats_list, receiving_stats_list,
       fumbles_stats_list, defensive_stats_list, interceptions_stats_list]
    Returns a dict with counts inserted per table.
    Commits after each table insert.
    """
    results = {
        "passing": 0,
        "rushing": 0,
        "receiving": 0,
        "fumbles": 0,
        "defensive": 0,
        "interceptions": 0,
    }
    if not player_stats or not isinstance(player_stats, list):
        return results

    try:
        results["passing"] = insert_passing_stats(player_stats[0] or [])
        results["rushing"] = insert_rushing_stats(player_stats[1] or [])
        results["receiving"] = insert_receiving_stats(player_stats[2] or [])
        results["fumbles"] = insert_fumbles_stats(player_stats[3] or [])
        results["defensive"] = insert_defensive_stats(player_stats[4] or [])
        results["interceptions"] = insert_interceptions_stats(player_stats[5] or [])
    except Exception:
        # rollback on unexpected error and re-raise
        database_connection.rollback()
        raise
    return results

def wipe_all_stats_tables() -> None:
    """
    Wipe all stats tables. USE WITH CAUTION.
    Commits after all deletions.
    """
    cur = database_connection.cursor()
    try:
        tables = [
            "passing_stats",
            "rushing_stats",
            "receiving_stats",
            "fumbles_stats",
            "defensive_stats",
            "interceptions_stats",
        ]
        for table in tables:
            cur.execute(f"DELETE FROM {table}")
        database_connection.commit()
    except Exception:
        database_connection.rollback()
        raise
    finally:
        cur.close()