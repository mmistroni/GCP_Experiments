from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4
@dataclass(frozen=True, order=True)
class Tweets():
    """ Class to store tweets of users """

    tweet_body: str = None
    tweet_time: datetime = datetime.utcnow()
    tweet_id: UUID = uuid4()
    tweet_lang: str = 'en-IN'
    tweet_place: str = 'IN'
    tweet_retweet_count: int = 0
    tweet_user_id: str = None
    tweet_user_name: str = None