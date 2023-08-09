UPDATE map_info
SET 
map_name = %(map_name)s,
matchup_type = %(matchup_type)s,
first_game_date = %(first_game_date)s
WHERE map_hash = %(map_hash)s;
