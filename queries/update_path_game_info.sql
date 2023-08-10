UPDATE game_info
SET 
replay_path = %(replay_path)s
WHERE game_id = %(game_id)s;
