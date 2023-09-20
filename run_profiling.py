import cProfile
import pstats
from replay_process import ReplayProcess, ReplayFilter
from datetime import datetime


def get_profile(replays_dir):
    with cProfile.Profile as pr:
        replay_filter = ReplayFilter()
        replay_filter.is_1v1 = True
        replay_filter.game_len = [1920, 28800]
        replay_filter.time_played = datetime(2021, 1, 1)
        processor = ReplayProcess(
            "./configs/secrets.yml",
            "configs/database.yml",
            "./starcraft2_replay_parse/data/game_info.csv",
            ticks_per_pos=32,
        )
        processor.process_replays(replays_dir, filt=replay_filter)

    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.dump_stats('profile.prof')
