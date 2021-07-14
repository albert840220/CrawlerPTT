import time
import requests
import bs4
import re
import pandas as pd
import datetime
import pymysql
from flask import Flask, render_template, request, url_for
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, NVARCHAR
from flask_paginate import Pagination, get_page_parameter
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# flask_sqlalchemy
db = SQLAlchemy()

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://root:0000@localhost:3306/PTT"
db.init_app(app)

# sqlalchemy
engine = create_engine('mysql+pymysql://root:0000@localhost:3306/PTT')

search_sql = None


class Craw_data(db.Model):
    author = db.Column(db.String(50))
    title = db.Column(db.String(50))
    date = db.Column(db.DateTime)
    content = db.Column(db.Text)
    commenter = db.Column(db.String(50))
    comment = db.Column(db.Text)
    comment_time = db.Column(db.Text)
    id = db.Column(db.Integer, primary_key=True)  # 一定要ID
    def __repr__(self):
        return 'author={}, title={}, date={}, content={}, commenter={}, comment={}, comment_time={}'.format(
            self.author, self.title, self.date, self.content, self.commenter, self.comment, self.comment_time)


class PttCrawler(object):

    def __init__(self):
        self.df = None
        self.ptt_URL = 'https://www.ptt.cc'
        self.board = None
        self.pages_url = {}
        self.posts_url = []
        self.all_item = []

    def get_pages_urls(self, start_page, end_page):
        my_headers = {'cookie': 'over18=1;'}
        response = requests.get(f'https://www.ptt.cc/bbs/{self.board}/index.html', headers=my_headers)
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        # print(soup)
        # 找最後一頁的index
        target = soup.find_all('a', class_="btn wide")
        previous_page_href = target[1]['href']
        max_index = int(re.sub("\D", "", previous_page_href)) + 1

        if int(end_page) > max_index:
            print(f'The total number of pages is {max_index} pages!')
        else:
            self.pages_url = {i: f'https://www.ptt.cc/bbs/{self.board}/index{i}.html' for i in
                              range(start_page, end_page + 1)}

    def get_posts_urls(self, page_url):
        my_headers = {'cookie': 'over18=1;'}
        response = requests.get(page_url, headers=my_headers)
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        # 找看板標題
        target = soup.find_all("a", href=re.compile(f"^/bbs/{self.board}/M"))
        for posts_url in target:
            self.posts_url.append(self.ptt_URL + posts_url['href'])

    def parse_post(self, post_url):
        # 設定Header與Cookie
        my_headers = {'cookie': 'over18=1;'}
        # 發送get 請求 到 ptt 八卦版
        response = requests.get(post_url, headers=my_headers)
        #  把網頁程式碼(HTML) 丟入 bs4模組分析
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        # PTT 上方4個欄位
        # print(soup)
        try:
            header = soup.find_all('span', 'article-meta-value')
            # 作者
            author = header[0].text
            # 看版
            board = header[1].text
            # 標題
            title = header[2].text
            # 日期
            raw_date = header[3].text
            split_date = raw_date.split(' ')

            # 'Fri Jun  1 19:33:00 2001'
            # 'Wed May 23 15:53:49 2001'
            if len(split_date) == 5:
                pass
            else:
                split_date.remove('')

            date = f"{split_date[2]}{split_date[1]}{split_date[4]} {split_date[3]}"
            date = datetime.datetime.strptime(date, '%d%b%Y %H:%M:%S')

            # 查找所有html 元素 抓出內容
            main_container = soup.find(id='main-container')
            # 把所有文字都抓出來
            all_text = main_container.text
            # 把整個內容切割透過 "-- " 切割成2個陣列
            pre_text = all_text.split('--')[0]
            # 把每段文字 根據 '\n' 切開
            texts = pre_text.split('\n')
            contents = texts[2:]
            # 內容
            content = '\n'.join(contents)
            # 查詢所有留言者
            all_commenter = soup.find_all('span', class_='push-userid')
            # 查詢所有留言內容
            all_comment = soup.find_all('span', class_='push-content')
            # 查詢所有留言時間
            all_comment_time = soup.find_all('span', class_='push-ipdatetime')
            # 留言者相關資訊
            for commenter, comment, comment_time in zip(all_commenter, all_comment, all_comment_time):
                self.all_item.append({
                    'author': author,
                    'title': title,
                    'date': date,
                    'content': content,
                    'commenter': commenter.text,
                    'comment': comment.text.replace(": ", ""),
                    'comment_time': comment_time.text.replace("\n", '')
                })
        except:
            pass

    def merge_posts(self, start_page, end_page):  # 合併貼文
        for i in self.posts_url:
            self.parse_post(i)

    def run(self, board: str, start_page: str, end_page: str):
        self.board = board
        start_page = int(start_page)
        end_page = int(end_page)
        self.get_pages_urls(start_page, end_page)
        # 抓取所有posts的網址
        for i in range(start_page, end_page + 1):
            # print(self.pages_url)
            self.get_posts_urls(self.pages_url[i])
        # 合併所有page的資料
        self.merge_posts(start_page, end_page)
        # return self.all_item
        df = pd.DataFrame(self.all_item)
        return df

    def export(self, file_path: str, file_type: str = 'csv', data: pd.DataFrame = None):
        item_dict = {
            'author': [],
            "title": [],
            "date": [],
            "content": [],
            "commenter": [],
            "comment": [],
            "comment_time": []
        }
        if data is None:
            data = pd.DataFrame(item_dict)
        if file_type == 'csv':
            data.to_csv(file_path, index=False, encoding='utf_8_sig')
        elif file_type == 'text':
            data.to_csv(file_path, sep='\t', index=False)
        elif file_type == 'html':
            data = data.replace("\n", '', regex=True)  # 正則去\n
            html_data = data.to_html(file_path, index=False)
            return html_data
        return None


@app.route('/', methods=['GET', 'POST'])
def add_ptt():
    if request.method == 'POST':
        board = request.values['Board']
        start = request.values['Start']
        end = request.values['End']
        crawler = PttCrawler()
        data = crawler.run(board=board, start_page=start, end_page=end)
        data.to_sql('craw_data', engine, if_exists='replace', index=False,
                    dtype={"author": NVARCHAR(length=50), "title": NVARCHAR(length=50),
                           'commenter': NVARCHAR(length=50)})
        #新增ID primary key
        with engine.connect() as con:
            con.execute('ALTER TABLE craw_data ADD column id int(10) unsigned primary key AUTO_INCREMENT;')

        return render_template('result.html')
    return render_template('result.html')


@app.route('/search', methods=['GET', 'POST'])
def search():
    global search_sql
    page_num = int(request.args.get('page') or 1)
    if request.method == 'POST':
        author = '%'+request.values['author']+'%'
        title = '%'+request.values['title']+'%'
        # max_count = request.values['max']
        if author != '%%':
            # sql_data = Craw_data.query.filter_by(author=f'%%{author}%%')filter(Table.name.like('%BOB%')
            search_sql = Craw_data.query.filter(Craw_data.author.like(author))#.order_by(Craw_data.comment_time)# .limit(max_count)
            data_paginate = search_sql.paginate(page=page_num, per_page=5, error_out=False)
            result = data_paginate.items
            next_url = url_for('search', page=data_paginate.next_num) if data_paginate.has_next else None
            prev_url = url_for('search', page=data_paginate.prev_num) if data_paginate.has_prev else None
            return render_template('result.html', result=result, next_url=next_url, prev_url=prev_url)
            # sql_data = Craw_data.query.filter_by(author=f'%%{author}%%')
            # f"""SELECT * FROM craw_data Where author like '%%{author}%%' order by comment_time DESC limit 0,{max_count}"""
        else:
            search_sql = Craw_data.query.filter(Craw_data.title.like(title))
            data_paginate = search_sql.paginate(page=page_num, per_page=5, error_out=False)
            result = data_paginate.items
            next_url = url_for('search', page=data_paginate.next_num) if data_paginate.has_next else None
            prev_url = url_for('search', page=data_paginate.prev_num) if data_paginate.has_prev else None
            return render_template('result.html', result=result, next_url=next_url, prev_url=prev_url)
            # sql_data = Craw_data.query.filter_by(author=f'%%{author}%%')
            # sql_data = Craw_data.query.filter_by(title=f'%%{title}%%').order_by(Craw_data.comment_time).limit(f'{max_count}')
            # f"""SELECT * FROM craw_data Where title like '%%{title}%%' order by comment_time DESC limit 0,{max_count}"""
    data_paginate = search_sql.paginate(page=page_num, per_page=5, error_out=False)
    result = data_paginate.items
    next_url = url_for('search', page=data_paginate.next_num) if data_paginate.has_next else None
    prev_url = url_for('search', page=data_paginate.prev_num) if data_paginate.has_prev else None
    return render_template('result.html', result=result, next_url=next_url, prev_url=prev_url)


if __name__ == '__main__':
    # start = time.time()
    # main()
    # end = time.time()
    # print(f"Spend {int(end - start)} seconds")
    app.run(host='0.0.0.0', port=8787, debug=False)
