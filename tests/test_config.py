import pytest

from config import get_config

# create test data
TEST_CONFIG = \
    '''
db_host: localhost
db_name: sc2replays
db_user: user
db_password: password
'''


@pytest.fixture()
def create_temp_config_file(tmpdir):
    temp_dir = tmpdir.mkdir("config_test")
    temp_config_file = temp_dir.join("test_config.yml")
    temp_config_file.write(TEST_CONFIG)
    return temp_config_file


# Test case 1
def test_get_config_positive_case(create_temp_config_file):
    config_path = create_temp_config_file
    config = get_config(config_path)
    assert isinstance(config, dict)
    assert config['db_host'] == 'localhost'
    assert config['db_name'] == 'sc2replays'
    assert config['db_user'] == 'user'
    assert config['db_password'] == 'password'


# Test case 2
def test_get_config_with_wrong_path():
    with pytest.raises(AssertionError):
        get_config('./configs/wrong_config23.yml')


# Test case 3
def test_get_config_with_wrong_suffix():
    with pytest.raises(AssertionError):
        get_config('./configs/database.json')
