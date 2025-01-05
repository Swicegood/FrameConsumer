import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, time
import pytz
import sys
import os
import json

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scheduled_checks import check_curtains, check_presence, schedule_checks

def test_sync():
    assert True

@pytest.fixture
def redis_client():
    client = AsyncMock()
    client.rpush = AsyncMock()
    return client

@pytest.mark.asyncio
async def test_check_curtains(redis_client):
    with patch('scheduled_checks.get_latest_frame', new_callable=AsyncMock) as mock_get_frame, \
         patch('scheduled_checks.process_image_for_curtains', new_callable=AsyncMock) as mock_process:
        
        # Setup
        mock_get_frame.return_value = b'fake_image_data'
        mock_process.return_value = 'yes'

        # Test
        await check_curtains(redis_client, 'AXIS_ID', '12:33pm')

        # Assert
        redis_client.rpush.assert_called_once()
        args = redis_client.rpush.call_args[0]
        assert args[0] == 'alert_queue'
        alert_data = json.loads(args[1])
        assert 'Curtains are closed' in alert_data['message']

@pytest.mark.asyncio
async def test_check_presence(redis_client):
    with patch('scheduled_checks.fetch_descriptions_for_timerange', new_callable=AsyncMock) as mock_fetch, \
         patch('scheduled_checks.process_descriptions_for_presence', new_callable=AsyncMock) as mock_process, \
         patch('scheduled_checks.get_latest_frame', new_callable=AsyncMock) as mock_get_frame:
        
        # Setup
        mock_fetch.return_value = 'Sample descriptions'
        mock_process.return_value = 'no'
        mock_get_frame.return_value = b'fake_image_data'

        # Test
        await check_presence(redis_client, 'g8rHNVCflWO1ptKN', time(10,30), time(11,10))

        # Assert
        redis_client.rpush.assert_called_once()
        args = redis_client.rpush.call_args[0]
        assert args[0] == 'alert_queue'
        alert_data = json.loads(args[1])
        assert 'No single person detected' in alert_data['message']

@pytest.mark.asyncio
async def test_schedule_checks(redis_client):
    with patch('aiocron.crontab') as mock_crontab, \
         patch('scheduled_checks.connect_redis', new_callable=AsyncMock) as mock_connect:
        
        # Setup
        mock_connect.return_value = redis_client

        # Test
        await schedule_checks()

        # Assert
        assert mock_crontab.call_count == 10  # 3 for curtains + 7 for presence

if __name__ == "__main__":
    pytest.main([__file__, "-v"])