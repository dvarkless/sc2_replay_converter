SELECT games_played, zerg_played, protoss_played, terran_played, wins, loses, most_played_race, highest_league
FROM player_info
WHERE player_id = %(player_id)s;

