UPDATE player_info
SET
nickname = %(nickname)s,
games_played = %(games_played)s,
zerg_played = %(zerg_played)s,
protoss_played = %(protoss_played)s,
terran_played = %(terran_played)s,
wins = %(wins)s,
loses = %(loses)s,
most_played_race = %(most_played_race)s,
highest_league = %(highest_league)s
WHERE
player_id = %(player_id)s
