SELECT map_hash, map_name, matchup_type, first_game_date
FROM map_info
WHERE map_hash = %(map_hash)s;

