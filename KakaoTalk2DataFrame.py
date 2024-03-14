import re
import numpy as np
import pandas as pd
import datetime

from time import time

class KakaoTalk2DataFrame:

    def __init__(
            self,
            path,
            not_user,
            bot_used=True,
            encoding='utf-8',
            date_format='%Y년 %m월 %d일 %p %I:%M'
    ):
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
        chat_date = pd.Series(data[1::2])
        chat_date = chat_date.str.replace('오전', 'am')
        chat_date = chat_date.str.replace('오후', 'pm')
        chat_date = chat_date.str.replace(',', '')
        chat_date = pd.to_datetime(chat_date, format=date_format)

        # 발화자, 발화 파싱
        chat_data = pd.Series(data[2::2])
        chat_data = chat_data.str.replace(
            r'(.+?)이 .+?님에서 (.+?)님으로 (.|\n)+',
            r'\2님이 \1이 되었습니다.',
            regex=True
        )
        chat_data = chat_data.str.replace(
            '(채팅방 관리자)가 (메시지를 가렸습니다.)',
            r'\1님이 \2 : ',
            regex=True
        )
        chat_data = pd.DataFrame(chat_data.str.split(' : ', n=1).values.tolist())
        chat_data[0] = chat_data[0].str.strip()
        chat_data[1] = chat_data[1].str.strip()

        # 발화자, 이벤트 파싱
        name_event = pd.DataFrame(chat_data[0].str.split('님[이을] (.+?습니다.)', regex=True).tolist())

        # 데이터 병합
        self.data = pd.DataFrame(
            data={
                'date': chat_date.dt.date,
                'time': chat_date.dt.time,
                'name': name_event[0],
                'event': name_event[1],
                'chat': chat_data[1],
            },

        )

        # get_users
        self.get_users(not_user=not_user)

    def get_users(self, not_user=[]):
        df = self.data
        user_all = df.name.unique()
        user_io = df[df.event.isin(['들어왔습니다.', '나갔습니다.', '내보냈습니다.'])].sort_values(['name', 'date', 'time'])
        user_out = user_io.loc[
            (~user_io.name.duplicated('last')) & (user_io.event.isin(['나갔습니다.', '내보냈습니다.'])), 'name'].values
        result = user_all[np.isin(user_all, user_out, invert=True)]
        self.users = result[np.isin(result, not_user, invert=True)]

if __name__ == '__main__':
    data = KakaoTalk2DataFrame(
        path=r"C:\Users\kgh07\_project\KakaoTalk To DataFrame\data\testdata_small.txt",
        bot_used=True,
        not_user=['', '방장봇', '채팅방 관리자']
    )
    print(data.data.shape)