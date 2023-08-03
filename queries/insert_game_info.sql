INSERT INTO game_info (timestamp_played, timestamp_processed, players_hash, end_time, player_1_id, player_1_race, player_1_league, player_2_id, player_2_race, player_2_league, map_hash, matchup, is_ladder, replay_path)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING game_id;
