import polars as pl
import re
import datetime
from typing import (
    List,
)


class KakaoTalk2DataFrame:
    """


        Parameters
        ----------
        path : str
            저장된 데이터의 주소값을 입력합니다.

        not_user : List[str], default None
            분석에서 제외할 유저 이름을 입력합니다.

        bot_used : bool, optional, default True
            채팅방에서 봇을 사용하는가에 대한 bool값 입니다.

        encoding : str, optional, default 'utf-8'
            Encoding to use for UTF when reading/writing

        date_format : str, optional, default "%Y년 %m월 %d일 %p %I:%M"
            The strftime to parse time
        """
    def __init__(
            self,
            path: str,
            *,
            not_user: List[str] = None,
            bot_used: bool = True,
            encoding: str = 'utf-8',
            date_format: str = '%Y년 %m월 %d일 %p %I:%M'
    ):
        self.date_format = date_format

        with open(path, 'r', encoding=encoding) as f:
            top = f.readline().strip()
            save_point = f.readline().strip()
            chat_raw = f.read()
            self.check = chat_raw

        # 타이틀 및 참여인원 파싱
        self.title, self.participants_num = top.replace(' 님과 카카오톡 대화', '').rsplit(' ', 1)
        self.participants_num = int(self.participants_num) - bot_used

        # 채팅 저장일 파싱
        _, value = save_point.split(' : ')
        value = value.replace('오전', 'am').replace('오후', 'pm')
        self.save_point = datetime.datetime.strptime(value, date_format)

        data = self._text_split(chat_raw)

        date_ser = pl.Series(data[1::2])
        date_ser = self._time_parsing_ko(date_ser)

        chat_ser = pl.Series(data[2::2])
        chat_df = self._chat_parsing(chat_ser)

        # 데이터 병합
        self.data = pl.DataFrame({
            'date': date_ser.dt.date(),
            'time': date_ser.dt.time(),
            'name': chat_df.get_column('name'),
            'event': chat_df.get_column('event'),
            'chat': chat_df.get_column('chat'),
        }, )

        # 활동 유저
        self.get_users(not_user=not_user)

    def _text_split(
            self,
            text : str = None,

    ) -> List[str]:
        """
        시간, 발화 분리

        Parameters
        ----------
        text : str, default None

        Return
        ------
        List[str]
        """
        date_pattern = r'(\d{4}년 \d{1,2}월 \d{1,2}일 오[전후] \d{1,2}:\d{1,2}),?'
        date_pattern = re.compile(date_pattern)
        data = date_pattern.split(text)
        return data

    def _time_parsing_ko(
            self,
            date_ser : pl.Series,
    ) -> pl.Series:
        """
        str to Date

        Parameters
        ----------


        Return
        ------
        pl.Series
        If parsing succeeded.

        """
        date_ser = date_ser.str.replace('오전', 'am')
        date_ser = date_ser.str.replace('오후', 'pm')
        date_ser = date_ser.str.replace(',', '')
        date_ser = date_ser.str.to_datetime(format=self.date_format)
        return date_ser

    def _chat_parsing(
            self,
            chat_ser : pl.Series,
    ) -> pl.DataFrame:
        """
        str to Date

        Parameters
        ----------


        Return
        ------
        pl.DataFrame
        If parsing succeeded.
        """
        chat_ser = chat_ser.str.replace(
            r'(.+?)이 .+?님에서 (.+?)님으로 (.|\n)+',
            r'$2님이 $1이 되었습니다.',
        )
        chat_ser = chat_ser.str.replace(
            '(채팅방 관리자)가 (메시지를 가렸습니다.)',
            r'$1님이 $2 : ',
        )
        chat_ser = chat_ser.str.splitn(' : ', 2)
        chat_df = chat_ser.struct.rename_fields(['name', 'chat']).struct.unnest()

        # 발화자, 이벤트 파싱
        name_event = chat_df.select(
            pl.col('name')
            .str.extract_groups(r'((.+)님[이을](.+?습니다.)|(.+))')
        )
        name1 = name_event.select(
            pl.col('name')
            .struct[1].alias('name')
        )
        name2 = name_event.select(
            pl.col('name')
            .struct[3].alias('name')
        )
        name = name1.select(
            pl.col('name')
            .fill_null(name2.get_column('name'))
        )
        event = name_event.select(
            pl.col('name')
            .struct[2].alias('name')
        )
        chat_df = pl.DataFrame({
            'name': name.get_column('name').str.strip_chars(),
            'event': event.get_column('name').str.strip_chars(),
            'chat': chat_df.get_column('chat').str.strip_chars(),
        })
        return chat_df

    def get_users(self, not_user=[]):
        df = self.data
        user_all = df.get_column('name').unique()
        user_io = df.filter(
            pl.col('event')
            .is_in(['들어왔습니다.', '나갔습니다.', '내보냈습니다.'])
        ).sort(['name', 'date', 'time'])
        user_io = user_io.unique(
            subset=['name'],
            keep='last',
            maintain_order=True
        )
        user_out = user_io.filter(
            pl.col('event')
            .is_in(['나갔습니다.', '내보냈습니다.'])
        ).get_column('name')
        result = user_all.filter(~user_all.is_in(user_out))
        self.users = result.filter(~result.is_in(not_user))


if __name__ == '__main__':
    data = KakaoTalk2DataFrame(
        path=r"C:\Users\kgh07\_project\KakaoTalk To DataFrame\data\testdata_small.txt",
        bot_used=True,
        not_user=['', '방장봇', '채팅방 관리자']
    )
    print(data.data.shape)
    print(data.data.head())