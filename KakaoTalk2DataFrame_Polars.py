import polars as pl
import re
import datetime

from time import time


class KakaoTalk2DataFrame:
    def __init__(self, path, not_user, bot_used=True, encoding='utf-8', date_format='%Y년 %m월 %d일 %p %I:%M'):
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

        # 시간 및 발화 분리
        date_pattern = re.compile(r'(\d{4}년 \d{1,2}월 \d{1,2}일 오[전후] \d{1,2}:\d{1,2}),?')
        data = date_pattern.split(chat_raw)

        # 시간 파싱
        chat_date = pl.Series(data[1::2])
        chat_date = chat_date.str.replace('오전', 'am')
        chat_date = chat_date.str.replace('오후', 'pm')
        chat_date = chat_date.str.replace(',', '')
        chat_date = chat_date.str.to_datetime(format=date_format)

        # 발화자, 발화 파싱
        chat_data = pl.Series(data[2::2])
        chat_data = chat_data.str.replace(
            r'(.+?)이 .+?님에서 (.+?)님으로 (.|\n)+',
            r'$2님이 $1이 되었습니다.',
        )
        chat_data = chat_data.str.replace(
            '(채팅방 관리자)가 (메시지를 가렸습니다.)',
            r'$1님이 $2 : ',
        )
        chat_data = chat_data.str.splitn(' : ', 2)
        chat_data = chat_data.struct.rename_fields(['name', 'chat']).struct.unnest()

        # 발화자, 이벤트 파싱
        name_event = chat_data.select(
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

        # 데이터 병합
        self.data = pl.DataFrame({
            'date': chat_date.dt.date(),
            'time': chat_date.dt.time(),
            'name': name.get_column('name').str.strip_chars(),
            'event': event.get_column('name').str.strip_chars(),
            'chat': chat_data.get_column('chat').str.strip_chars(),
        }, )

        # 활동 유저
        self.get_users(not_user=not_user)

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