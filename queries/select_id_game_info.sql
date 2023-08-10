SELECT game_id FROM game_info
WHERE players_hash = %(players_hash)s
  AND timestamp_played = %(timestamp_played)s
