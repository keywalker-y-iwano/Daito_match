# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import paramiko
import mojimoji as moji
import string
import re
import csv
import logging
import requests
from decimal import Decimal, ROUND_HALF_UP
import psutil

logging.basicConfig(level=logging.INFO)
logging.info('=========================start========================')
def mail_gun(text):
    files = {
        'from': (None, 'y_iwano <mailgun@mg.keywalker.co.jp>'),
        'to': (None, 'y_iwano@keywalker.co.jp,a_okada@keywalker.co.jp,k_yamashita@keywalker.co.jp'),
        'subject': (None, '大都技研：名寄せ処理エラー'),
        'text': (None, text),
        }
    response = requests.post('https://api.mailgun.net/v3/mg.keywalker.co.jp/messages', files=files, auth=('api', 'key-c81c485158951df1e152505f1b82e674'))

def sprit_dai_p(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[0]

def sprit_dai_s(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[1]

def sprit_dai_sum(s):
    str_s = str(s)
    if not str_s:
        return ''
    else:
        str_s_list = str_s.split(',')
        return str_s_list[2]

def sprit_store_ps(s):
    str_s = str(s)
    pattern_p = r'ﾊﾟﾁﾝｺ(.+?)台'
    pattern_s = r'ｽﾛｯﾄ(.+?)台'
    pattern_ps = r'ﾊﾟﾁｽﾛ(.+?)台'
    if not str_s:
        return ''
    else:
        p = re.findall(pattern_p, str_s)
        s = re.findall(pattern_s, str_s)
        ps = re.findall(pattern_ps, str_s)
    try:
        if not p:
            output_p = 0
        else:
            output_p = int(p[0])
        if not s:
            output_s = 0
        else:
            output_s = int(s[0])
        if not ps:
            output_ps = 0
        else:
            output_ps = int(ps[0])
        output_sum = output_p + output_s + output_ps
        if output_sum == 0:
            return ',,'
        else:
            return str(output_p) + ',' + str(output_s + output_ps) + ',' + str(output_sum)
    except ValueError:
        return ''

def calc_rate(s):
    if(len(s) == 1):
        try:
            if str(s[0]) != '':
                s[0] = float(s[0])
                return Decimal(str(s[0])).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            else:
                return ''
        except ValueError:
            return 0
    else:
        try:
            output = float(s[0])/float(s[1])
            output =  Decimal(str(output)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            return output
        except ValueError:
            return 0

def calc_han(s):
    output = s[0:int(len(s)/2)]
    return output

def add_zero(s):
    if '/' in s:
        date_temp = s.split('/')
        if len(date_temp) != 3:
            return ''
        if len(date_temp[1]) == 1:
            month = '0' + date_temp[1]
        else:
            month = date_temp[1]

        if len(date_temp[2]) == 1:
            day = '0' + date_temp[2]
        else:
            day = date_temp[2]
        date = str(date_temp[0]) + '/' + month + '/' + day
        return date
    else:
        return ''

def check_date(s):
    if '/' in s:
        return s
    else:
        return ''

def delete_brackets(s):
    """
    括弧と括弧内文字列を削除
    """
    """ brackets to zenkaku """
    table = {
        "(": "（",
        ")": "）",
        "<": "＜",
        ">": "＞",
        "{": "｛",
        "}": "｝",
        "[": "［",
        "]": "］"
    }
    for key in table.keys():
        s = s.replace(key, table[key])
    """ delete zenkaku_brackets """
    l = ['（[^（|^）]*）', '【[^【|^】]*】', '＜[^＜|^＞]*＞', '［[^［|^］]*］',
         '「[^「|^」]*」', '｛[^｛|^｝]*｝', '〔[^〔|^〕]*〕', '〈[^〈|^〉]*〉']
    for l_ in l:
        s = re.sub(l_, "", s)
    """ recursive processing """
    return delete_brackets(s) if sum([1 if re.search(l_, s) else 0 for l_ in l]) > 0 else s

def read_csv(self, filepath, index_col=None):
    sftp = self.client.open_sftp()
    with sftp.open(filepath, "r") as f:
         df = pd.read_csv(f, encoding="utf-8", index_col=index_col)
    return df


dat = datetime.datetime.now()
date = dat.strftime('%Y/%m/%d')
dir_date = dat.strftime('%Y%m%d')
dir_date = dir_date[2:8]

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('=====================Connect:GCP======================')

# SFTP接続先の設定
HOST = '104.198.89.41'
PORT = 'ssh'
USER = 'y_iwano'
KEY_FILE  = './id_rsa'
# 秘密鍵ファイルからキーを取得
rsa_key = paramiko.RSAKey.from_private_key_file(KEY_FILE)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    #SFTPセッション開始
    client.connect(HOST, PORT, USER, pkey=rsa_key) # キーを指定することでパスワードは必要なし
    sftp_connection = client.open_sftp()
    with sftp_connection.open("/home/crawler/file/p-world_kisyu.csv", "r") as f:
        PW_model_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/p-world_dai.csv", "r") as f:
        PW_table_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/p-world_tenpo.csv", "r") as f:
        PW_store_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_kisyu.csv", "r") as f:
        DM_model_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_dai.csv", "r") as f:
        DM_table_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/crawler/file/dmm_tenpo.csv", "r") as f:
        DM_store_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/y_iwano/Daito/Pair_data/pair_tenpo.csv", "r") as f:
        store_pair_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
    with sftp_connection.open("/home/y_iwano/Daito/Pair_data/pair_kisyu.csv", "r") as f:
        model_pair_array = pd.read_csv(f, encoding="utf-8_sig",dtype=object)
finally:
    client.close()

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('=======================機種データ=======================')
logging.info('===============clensing : DM_model_array==============')

DM_model_array_clensing = DM_model_array.copy()
DM_model_array_clensing['clensing_title'] = ''
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].str.replace('の掲載機種一覧','')
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].str.replace(' ','')
DM_model_array_clensing['maker'] = DM_model_array_clensing['maker'].apply(calc_han)
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['title'].apply(moji.zen_to_han)
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.replace(' ','').str.replace('ⅰ','i').str.replace('ⅱ','ii').str.replace('ⅲ','iii').str.replace('ⅳ','iv').str.replace('ⅴ','v').str.replace('ⅵ','vi').str.replace('ⅶ','vii').str.replace('ⅷ','viii').str.replace('ⅸ','ix').str.replace('ⅹ','x')
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.lower()
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
DM_model_array_clensing['clensing_title'] = DM_model_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('･','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('‐','')
DM_model_array_clensing['d_date'] = DM_model_array_clensing['info5'].apply(delete_brackets)
DM_model_array_clensing['s_date'] = DM_model_array_clensing['d_date'].apply(moji.zen_to_han)
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].fillna('')
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].apply(delete_brackets)
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].str.replace('上旬予定','').str.replace('予定','')
DM_model_array_clensing['s_date'] = DM_model_array_clensing['s_date'].apply(add_zero)
DM_model_array_clensing['info1'] = DM_model_array_clensing['info1'].str.replace('件','').str.replace('（','').str.replace('）','').str.replace('(','').str.replace(')','')
DM_model_array_clensing['site'] = 1
DM_model_array_clensing = DM_model_array_clensing.drop_duplicates(subset=['clensing_title','P_S'])

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('===============clensing : PW_model_array==============')

PW_model_array_clensing = PW_model_array.copy()
PW_model_array_clensing['clensing_title'] = ''
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['title'].apply(moji.zen_to_han)
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.replace(' ','').str.replace('ⅰ','i').str.replace('ⅱ','ii').str.replace('ⅲ','iii').str.replace('ⅳ','iv').str.replace('ⅴ','v').str.replace('ⅵ','vi').str.replace('ⅶ','vii').str.replace('ⅷ','viii').str.replace('ⅸ','ix').str.replace('ⅹ','x')
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.lower()
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
PW_model_array_clensing['clensing_title'] = PW_model_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('･','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('‐','')
PW_model_array_clensing['d_date'] = PW_model_array_clensing['d_date'].str.replace('導入開始：','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].fillna('')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].str.replace('調査日：','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].str.replace('上旬','').str.replace('下旬','').str.replace('予定','')
PW_model_array_clensing['s_date'] = PW_model_array_clensing['s_date'].apply(add_zero)
PW_model_array_clensing['site'] = 0

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('===================check: model_list==================')

DM_model_array_merge = pd.merge(DM_model_array_clensing, model_pair_array, on='dmm_pcode', how='left')
DM_model_array_merge = DM_model_array_merge.drop_duplicates(subset=['title','P_S'])
PW_model_array_merge = pd.merge(PW_model_array_clensing, model_pair_array, on='pw_pcode', how='left')
PW_model_array_merge = PW_model_array_merge.drop_duplicates(subset=['title','P_S'])
model_array_merge = pd.merge(PW_model_array_merge, DM_model_array_merge, on=['clensing_title','P_S'], how='left')
model_array_merge_for_not_match = model_array_merge.copy()
model_array_merge = model_array_merge.rename(columns={'title_x':'機種名'})
model_array_merge = model_array_merge.rename(columns={'maker_x':'maker'})
model_array_merge = model_array_merge.rename(columns={'k_kind_x':'k_kind'})
model_array_merge = model_array_merge.rename(columns={'info1_y':'レビュー件数'})
model_array_merge = model_array_merge.rename(columns={'info2_y':'レビュースコア'})
model_array_merge = model_array_merge.rename(columns={'info4_x':'機種種別'})
model_array_merge = model_array_merge.rename(columns={'pw_pcode':'pcode'})
model_array_merge = model_array_merge.rename(columns={'d_date_x':'導入年月'})
model_array_merge = model_array_merge.rename(columns={'s_date_x':'更新日付'})
model_array_merge['レビュー件数'] = model_array_merge['レビュー件数'].fillna(0)
model_array_merge['レビュースコア'] = model_array_merge['レビュースコア'].fillna('0.00')
model_array_merge = model_array_merge.drop(columns=['clensing_title', 'type_y', 'info10_y', 'title_y', 'title6_x', 'title10_y', 'k_kind_y', 'd_date_y', 'pw_pcode_x', 'title11_y', 'info11_y', 'title5_y', 'info5_y', 'info8_x', 'title6_y', 'info6_y', 'title7_y', 'info7_y', 'title8_y', 'info8_y', 'title9_y', 'info9_y', 'title11_x', 'info11_x', 'title8_x', 'info9_x', 'title9_x', 'info10_x', 'title10_x', 'info11_x', 'title11_x', 'info6_x', 'k_kind', 'title5_x', 'info5_x', 'title7_x', 'info7_x', 'title1_y', 'title2_y', 'title3_y', 'title4_y', 'info4_y', 'update_date', 'k_comment_y', 'title0_y', 'info0_y', 'info3_y', 'dmm_pcode_x', 'url', 'pw_pcode_y', 's_date_y', 'type_x', 'site_x', 'site_y', 'maker_y', 'k_comment_x', 'title0_x', 'info0_x', 'title1_x', 'info1_x', 'title2_x', 'info2_x', 'title3_x', 'info3_x', 'title4_x'])
model_array_merge['dmm_pcode_y'] = model_array_merge['dmm_pcode_y'].fillna(0)
model_array_merge['dmm_pcode'] = model_array_merge['dmm_pcode'].fillna(0)
model_array_merge['dmm_pcode'] = model_array_merge['dmm_pcode_y'].where((model_array_merge['dmm_pcode'] == 0) & (model_array_merge['dmm_pcode_y'] != 0 ), model_array_merge['dmm_pcode'])
model_array_merge['dmm_pcode'] = model_array_merge['dmm_pcode'].str.replace('0','')
model_array_merge = model_array_merge.drop(columns='dmm_pcode_y')

output_kisyu_to_csv = pd.DataFrame(columns=['pcode', '機種名', 'Ｐ／Ｓ区分', '機種種別', 'タイプ', 'メーカー名', '機種コメント', '情報0', '内容0', '情報1', '内容1', '情報2', '内容2', '情報3', '内容3', '情報4', '内容4', '情報5', '内容5', '情報6', '内容6', '情報7', '内容7', '情報8', '内容8', '情報9', '内容9', '情報10', '内容10', '情報11', '内容11', '導入年月', '更新日付', 'dmm_pcode'])

output_kisyu_to_csv['pcode'] = 'p' + model_array_merge['pcode']
output_kisyu_to_csv['機種名'] = model_array_merge['機種名']
output_kisyu_to_csv['Ｐ／Ｓ区分'] = model_array_merge['P_S']
output_kisyu_to_csv['機種種別'] = model_array_merge['機種種別']
output_kisyu_to_csv['メーカー名'] = model_array_merge['maker']
output_kisyu_to_csv['情報0'] = '型式名'
output_kisyu_to_csv['内容0'] = model_array_merge['機種名']
output_kisyu_to_csv['情報1'] = 'レビュー件数'
output_kisyu_to_csv['内容1'] = model_array_merge['レビュー件数']
output_kisyu_to_csv['情報2'] = 'レビュースコア'
output_kisyu_to_csv['内容2'] = model_array_merge['レビュースコア']
output_kisyu_to_csv['導入年月'] = model_array_merge['導入年月']
output_kisyu_to_csv['更新日付'] = model_array_merge['更新日付']
output_kisyu_to_csv['dmm_pcode'] = 'dmm_' + model_array_merge['dmm_pcode']

output_kisyu_to_csv_temp = output_kisyu_to_csv
output_kisyu_to_csv = output_kisyu_to_csv.drop_duplicates(subset=['機種名','pcode'])

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('==========duplicate : output_kisyu_not_match==========')

model_output_array_inner = pd.merge(PW_model_array_merge, DM_model_array_merge, on=['clensing_title','P_S'], how='inner')
model_output_array_outer = pd.merge(PW_model_array_merge, DM_model_array_merge, on=['clensing_title','P_S'], how='outer')

model_output_array_inner['比較用の列'] = model_output_array_inner.apply(lambda x: '{}_{}'.format(x[2], x[35]), axis=1)
model_output_array_outer['比較用の列'] = model_output_array_outer.apply(lambda x: '{}_{}'.format(x[2], x[35]), axis=1)
model_array_merge_for_not_match['比較用の列'] = model_output_array_outer.apply(lambda x: '{}_{}'.format(x[2], x[35]), axis=1)

PW_model_not_match = model_array_merge_for_not_match[~model_array_merge_for_not_match['比較用の列'].isin(model_output_array_inner['比較用の列'])]
DM_model_not_match = model_output_array_outer[~model_output_array_outer['比較用の列'].isin(model_array_merge_for_not_match['比較用の列'])]

output_kisyu_not_match_PW = pd.DataFrame(columns=['pcode', '機種名', 'Ｐ／Ｓ区分'])
output_kisyu_not_match_PW['pcode'] = 'p' + PW_model_not_match['pw_pcode']
output_kisyu_not_match_PW['機種名'] = PW_model_not_match['title_x']
output_kisyu_not_match_PW['Ｐ／Ｓ区分'] = PW_model_not_match['P_S']

output_kisyu_not_match_DM = pd.DataFrame(columns=['dmm_pcode', '機種名', 'Ｐ／Ｓ区分'])
output_kisyu_not_match_DM['dmm_pcode'] = 'dmm_' + DM_model_not_match['dmm_pcode']
output_kisyu_not_match_DM['機種名'] = DM_model_not_match['title_y']
output_kisyu_not_match_DM['Ｐ／Ｓ区分'] = DM_model_not_match['P_S']

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('=========================台データ=======================')
logging.info('================clensing : DM_table_array==============')


DM_table_array_clensing = DM_table_array.copy()
DM_table_array_clensing['clensing_title'] = ''
DM_table_array_clensing['co_date'] = ''

DM_table_array_clensing['p_code'] = 'dmm_' + DM_table_array_clensing['p_code']
DM_table_array_clensing['num'] = DM_table_array_clensing['num'].str.replace('台','')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].apply(moji.zen_to_han)
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].str.replace(' ','').str.replace('台','').str.replace('S','').str.replace('[','').str.replace('金額','').str.replace(']','').str.replace('【','').str.replace('】','').str.replace('円','').str.replace('し','').str.replace('貸','').str.replace('玉','').str.replace('枚','').str.replace('ﾊﾟﾁ','').str.replace('ﾝｺ','').str.replace('ｽﾛ','').str.replace('ｯﾄ','').str.replace('貸しS','').str.replace('しS','').str.replace('¥','').str.replace('ﾗﾌｪ⑤','').str.replace('ﾗﾌｪ','').str.replace('②','').str.replace('⓻','').str.replace(',','').str.replace('ｲﾁ','').str.replace('甘ﾃﾞｼﾞ･羽根物','').str.replace('ぱち','').str.replace('ﾐﾄﾞﾙ･ﾗｲﾄ','')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate'].str.replace('=','/').str.replace('‐','/').str.replace(':','/').str.replace('階','/')
DM_table_array_clensing['rate_list'] = DM_table_array_clensing['rate'].str.split('/')
DM_table_array_clensing['rate'] = DM_table_array_clensing['rate_list'].apply(calc_rate)
DM_table_array_clensing['co_date'] = date
DM_table_array_clensing = DM_table_array_clensing.drop('rate_list', axis=1)

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)
logging.info('============================1==========================')

DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['p_title'].fillna('')
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].apply(moji.zen_to_han)
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.replace(' ','').str.replace('Ⅰ','I').str.replace('Ⅱ','II').str.replace('Ⅲ','III').str.replace('Ⅳ','IV').str.replace('Ⅴ','V').str.replace('Ⅵ','VI').str.replace('Ⅶ','VII').str.replace('Ⅷ','VIII').str.replace('Ⅸ','IX').str.replace('Ⅹ','X')
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.lower()
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
DM_table_array_clensing['clensing_title'] = DM_table_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('☆','').str.replace('†','').str.replace('…','')
DM_table_array_clensing['site'] = 1

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('===============clensing : PW_table_array==============')

PW_table_array_clensing = PW_table_array.copy()
PW_table_array_clensing['clensing_title'] = ''
PW_table_array_clensing['co_date'] = ''

PW_table_array_clensing['p_code'] = 'p' + PW_table_array_clensing['p_code']
PW_table_array_clensing['num'] = PW_table_array_clensing['num'].str.replace('台','')
PW_table_array_clensing['pw_t_code'] = PW_table_array_clensing['pw_t_code'].str.replace('.htm','')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].fillna('')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].apply(moji.zen_to_han)
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate'].str.replace(' ','').str.replace('[','').str.replace(']','').str.replace('【','').str.replace('】','').str.replace('円','').str.replace('貸','').str.replace('玉','').str.replace('枚','').str.replace('ﾊﾟﾁ','').str.replace('ﾝｺ','').str.replace('ｽﾛ','').str.replace('ｯﾄ','').str.replace('貸しS','').str.replace('しS','').str.replace('¥','').str.replace('ﾗﾌｪ⑤','').str.replace('ﾗﾌｪ','').str.replace('②','').str.replace('⓻','').str.replace(',','')
PW_table_array_clensing['rate_list'] = PW_table_array_clensing['rate'].str.split('/')
PW_table_array_clensing['rate'] = PW_table_array_clensing['rate_list'].apply(calc_rate)
PW_table_array_clensing['co_date'] = date
PW_table_array_clensing = PW_table_array_clensing.drop('rate_list', axis=1)

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)
logging.info('============================2==========================')

PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['p_title'].fillna('')
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].apply(moji.zen_to_han)
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.replace(' ','').str.replace('Ⅰ','I').str.replace('Ⅱ','II').str.replace('Ⅲ','III').str.replace('Ⅳ','IV').str.replace('Ⅴ','V').str.replace('Ⅵ','VI').str.replace('Ⅶ','VII').str.replace('Ⅷ','VIII').str.replace('Ⅸ','IX').str.replace('Ⅹ','X')
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.lower()
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.translate(str.maketrans( '', '',string.punctuation))
PW_table_array_clensing['clensing_title'] = PW_table_array_clensing['clensing_title'].str.replace('‐','').str.replace('｢','').str.replace('｣','').str.replace('〜','').str.replace('【','').str.replace('】','').str.replace('☆','').str.replace('†','').str.replace('…','')
PW_table_array_clensing['site'] = 0

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('==================marge : table_array=================')

output_dai_to_csv = pd.DataFrame(columns=['state_cd', 't_code', 'pcode', '正式機種名', '機種名(店舗入力名)', '設置台数', '貸玉量', '更新日付'])
table_array_outer = pd.merge(PW_table_array_clensing, DM_table_array_clensing, on=['clensing_title', 'rate', 'state_cd'], how='outer')

table_array_outer['num_x'] = table_array_outer['num_x'].str.replace('#NAME?','0')
table_array_outer['num_y'] = table_array_outer['num_y'].str.replace('#NAME?','0')
table_array_outer['num_x'] = table_array_outer['num_x'].str.replace('?','')
table_array_outer['num_y'] = table_array_outer['num_y'].str.replace('?','')
table_array_outer['num_x'] = table_array_outer['num_x'].str.replace('-','0')
table_array_outer['num_y'] = table_array_outer['num_y'].str.replace('-','0')
table_array_outer['num_x'] = table_array_outer['num_x'].fillna(0)
table_array_outer['num_y'] = table_array_outer['num_y'].fillna(0)
table_array_outer['num_x'] = table_array_outer['num_x'].astype('int64')
table_array_outer['num_y'] = table_array_outer['num_y'].astype('int64')

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)
logging.info('============================3==========================')

table_array_outer['p_title_x'] = table_array_outer['p_title_x'].fillna('')
table_array_outer['p_title_y'] = table_array_outer['p_title_y'].fillna('')
table_array_outer['pw_t_code'] = table_array_outer['pw_t_code'].fillna('')
table_array_outer['dmm_t_code'] = table_array_outer['dmm_t_code'].fillna('')
table_array_outer['co_date_x'] = table_array_outer['co_date_x'].fillna('')
table_array_outer['co_date_y'] = table_array_outer['co_date_y'].fillna('')
table_array_outer['p_code_x'] = table_array_outer['p_code_x'].fillna('')
table_array_outer['p_code_y'] = table_array_outer['p_code_y'].fillna('')

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)
logging.info('============================4==========================')

table_array_outer['dmm_t_code'] = table_array_outer['dmm_t_code'].where((table_array_outer['dmm_t_code'] == ''), ('dmm_' + table_array_outer['dmm_t_code']))
table_array_outer['num_x'] = table_array_outer['num_y'].where((table_array_outer['num_x'] == 0) & (table_array_outer['num_y'] != 0 ), table_array_outer['num_x'])
table_array_outer['p_title_y'] = table_array_outer['p_title_x'].where((table_array_outer['p_title_y'] == '' ) & (table_array_outer['p_title_x'] != ''), table_array_outer['p_title_y'])
table_array_outer['pw_t_code'] = table_array_outer['dmm_t_code'].where((table_array_outer['pw_t_code'] == '') & (table_array_outer['dmm_t_code'] != ''), table_array_outer['pw_t_code'])
table_array_outer['co_date_x'] = table_array_outer['co_date_y'].where((table_array_outer['co_date_x'] == '' ) & (table_array_outer['co_date_y'] != ''), table_array_outer['co_date_x'])

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)
logging.info('============================5==========================')

output_dai_to_csv['state_cd'] = table_array_outer['state_cd']
output_dai_to_csv['t_code'] = table_array_outer['pw_t_code']
output_dai_to_csv['pcode'] = table_array_outer['p_code_x']
output_dai_to_csv['正式機種名'] = table_array_outer['p_title_x']
output_dai_to_csv['機種名(店舗入力名)'] = table_array_outer['p_title_y']
output_dai_to_csv['設置台数'] = table_array_outer['num_x']
output_dai_to_csv['貸玉量'] = table_array_outer['rate']
output_dai_to_csv['更新日付'] = table_array_outer['co_date_x']
output_dai_to_csv = output_dai_to_csv.loc[:, ['state_cd', 't_code', 'pcode', '正式機種名', '機種名(店舗入力名)', '設置台数', '貸玉量', '更新日付']]
output_dai_to_csv = output_dai_to_csv.drop_duplicates(subset=['t_code', 'pcode', '貸玉量'])

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('=======================店舗データ=======================')
logging.info('================clensing : DM_store_array=============')

DM_store_array_clensing = DM_store_array.copy()
DM_store_array_clensing['clensing_title'] = ''
DM_store_array_clensing['address'] = ''
DM_store_array_clensing['address'] = DM_store_array_clensing['adress']
DM_store_array_clensing['address'] = DM_store_array_clensing['address'].str.replace(' ','').str.replace('\n','').str.replace('大きな地図で見る','')
DM_store_array_clensing['clensing_title'] = DM_store_array_clensing['name'].apply(moji.zen_to_han)
DM_store_array_clensing['clensing_title'] = DM_store_array_clensing['clensing_title'].str.replace(' ','')
DM_store_array_clensing['clensing_title'] = DM_store_array_clensing['clensing_title'].apply(delete_brackets)
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].fillna('')
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].apply(delete_brackets)
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].str.replace('更新日:','')
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].str.replace(' ','')
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].fillna('')
DM_store_array_clensing['tenpo_update'] = DM_store_array_clensing['tenpo_update'].where((DM_store_array_clensing['tenpo_update'] == ''), ('2020/' + DM_store_array_clensing['tenpo_update']))
DM_store_array_clensing['parking'] = DM_store_array_clensing['parking'].apply(moji.zen_to_han)
DM_store_array_clensing['parking'] = DM_store_array_clensing['parking'].str.replace('　','').str.replace('\\','').str.replace('n','').str.replace('台','')
DM_store_array_clensing['dmm_t_code'] = 'dmm_' + DM_store_array_clensing['dmm_t_code']
DM_store_array_clensing['t_code'] = DM_store_array_clensing['dmm_t_code']
DM_store_array_clensing['co_date'] = date
DM_store_array_clensing = DM_store_array_clensing.drop_duplicates(subset='t_code')
DM_store_array_clensing = DM_store_array_clensing.drop_duplicates(subset=['clensing_title','state_cd'])
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai'].fillna('')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(moji.zen_to_han)
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].str.replace(' ','').str.replace(':','')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].str.replace('/','').str.replace('-','0台')
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(delete_brackets)
DM_store_array_clensing['dai_sprit'] = DM_store_array_clensing['dai_sprit'].apply(sprit_store_ps)
DM_store_array_clensing['dai_p'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_p)
DM_store_array_clensing['dai_s'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_s)
DM_store_array_clensing['dai_sum'] = DM_store_array_clensing['dai_sprit'].apply(sprit_dai_sum)
DM_store_array_clensing['site'] = 1

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('================clensing : PW_store_array=============')

PW_store_array_clensing = PW_store_array.copy()
PW_store_array_clensing['clensing_title'] = ''
PW_store_array_clensing['address'] = ''
PW_store_array_clensing['address'] = PW_store_array_clensing['adress']
PW_store_array_clensing['address'] = PW_store_array_clensing['address'].str.replace(' ','').str.replace('\\n','').str.replace('大きな地図で見る','')
PW_store_array_clensing['clensing_title'] = PW_store_array_clensing['name'].apply(moji.zen_to_han)
PW_store_array_clensing['clensing_title'] = PW_store_array_clensing['clensing_title'].str.replace(' ','')
PW_store_array_clensing['clensing_title'] = PW_store_array_clensing['clensing_title'].apply(delete_brackets)
PW_store_array_clensing['parking'] = PW_store_array_clensing['parking'].str.replace('　','').str.replace('\\','').str.replace('n','').str.replace('台','')
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].fillna('')
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].apply(moji.zen_to_han)
PW_store_array_clensing['tenpo_update'] = PW_store_array_clensing['tenpo_update'].str.replace('皆様のご来店をお待ちしております｡','').str.replace('最終更新日','').str.replace(' ','').str.replace('<','').str.replace('>','')
PW_store_array_clensing['pw_t_code'] = PW_store_array_clensing['pw_t_code'].str.replace('.htm','')
PW_store_array_clensing['t_code'] = PW_store_array_clensing['pw_t_code'].str.replace('.htm','')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai'].fillna('')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(moji.zen_to_han)
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].str.replace(' ','').str.replace(':','')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].str.replace('/','').str.replace('-','0台')
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(delete_brackets)
PW_store_array_clensing['dai_sprit'] = PW_store_array_clensing['dai_sprit'].apply(sprit_store_ps)
PW_store_array_clensing['dai_p'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_p)
PW_store_array_clensing['dai_s'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_s)
PW_store_array_clensing['dai_sum'] = PW_store_array_clensing['dai_sprit'].apply(sprit_dai_sum)
PW_store_array_clensing['co_date'] = date
PW_store_array_clensing['site'] = 0

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('===================check: Store_list==================')

store_pair_array = store_pair_array.rename(columns={'t_code':'pw_t_code'})
store_pair_array['dmm_t_code'] = 'dmm_' + store_pair_array['dmm_t_code']

DM_store_array_merge = pd.merge(DM_store_array_clensing, store_pair_array, on=['state_cd','dmm_t_code'], how='inner')
DM_store_array_merge_drop_PW = DM_store_array_merge.dropna(subset=['pw_t_code_y'])
DM_store_array_merge_drop_PW['t_code'] = DM_store_array_merge_drop_PW['pw_t_code_y']

PW_store_array_merge = pd.merge(PW_store_array_clensing, store_pair_array, on=['state_cd','pw_t_code'], how='inner')
PW_store_array_merge_drop_DM = PW_store_array_merge.dropna(subset=['dmm_t_code_y'])

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('===============duplicate : output_tenpo===============')

output_tenpo_pp = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_pp['state_cd'] = PW_store_array_merge_drop_DM['state_cd']
output_tenpo_pp['t_code'] =  PW_store_array_merge_drop_DM['pw_t_code']
output_tenpo_pp['ホール名'] =   PW_store_array_merge_drop_DM['name']
output_tenpo_pp['address'] =PW_store_array_merge_drop_DM['address']
output_tenpo_pp['access'] = PW_store_array_merge_drop_DM['access']
output_tenpo_pp['closeday'] =PW_store_array_merge_drop_DM['closeday']
output_tenpo_pp['opentime'] = PW_store_array_merge_drop_DM['opentime']
output_tenpo_pp['service'] = PW_store_array_merge_drop_DM['service']
output_tenpo_pp['rate'] = PW_store_array_merge_drop_DM['rate']
output_tenpo_pp['dai'] = PW_store_array_merge_drop_DM['dai']
output_tenpo_pp['dai_p'] = PW_store_array_merge_drop_DM['dai_p']
output_tenpo_pp['dai_s'] = PW_store_array_merge_drop_DM['dai_s']
output_tenpo_pp['dai_sum'] = PW_store_array_merge_drop_DM['dai_sum']
output_tenpo_pp['parking'] = PW_store_array_merge_drop_DM['parking']
output_tenpo_pp['tel'] = PW_store_array_merge_drop_DM['tel']
output_tenpo_pp['tenpo_update'] = PW_store_array_merge_drop_DM['tenpo_update']
output_tenpo_pp['co_date'] = PW_store_array_merge_drop_DM['co_date']
output_tenpo_pp['pw_t_code'] = PW_store_array_merge_drop_DM['pw_t_code']
output_tenpo_pp['dmm_t_code'] =  PW_store_array_merge_drop_DM['dmm_t_code_y']
output_tenpo_pp['merge_url'] = PW_store_array_merge_drop_DM['url']
output_tenpo_pp['clensing_title'] = PW_store_array_merge_drop_DM['clensing_title']
output_tenpo_pp = output_tenpo_pp.drop_duplicates(subset=['clensing_title','state_cd'])

output_tenpo_pd = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_pd['state_cd'] = DM_store_array_merge_drop_PW['state_cd']
output_tenpo_pd['t_code'] = DM_store_array_merge_drop_PW['dmm_t_code']
output_tenpo_pd['ホール名'] = DM_store_array_merge_drop_PW['name']
output_tenpo_pd['address'] = DM_store_array_merge_drop_PW['address']
output_tenpo_pd['access'] = DM_store_array_merge_drop_PW['access']
output_tenpo_pd['closeday'] = DM_store_array_merge_drop_PW['closeday']
output_tenpo_pd['opentime'] = DM_store_array_merge_drop_PW['opentime']
output_tenpo_pd['service'] = DM_store_array_merge_drop_PW['service']
output_tenpo_pd['rate'] = DM_store_array_merge_drop_PW['rate']
output_tenpo_pd['dai'] = DM_store_array_merge_drop_PW['dai']
output_tenpo_pd['dai_p'] = DM_store_array_merge_drop_PW['dai_p']
output_tenpo_pd['dai_s'] = DM_store_array_merge_drop_PW['dai_s']
output_tenpo_pd['dai_sum'] = DM_store_array_merge_drop_PW['dai_sum']
output_tenpo_pd['parking'] = DM_store_array_merge_drop_PW['parking']
output_tenpo_pd['tel'] = DM_store_array_merge_drop_PW['tel']
output_tenpo_pd['tenpo_update'] = DM_store_array_merge_drop_PW['tenpo_update']
output_tenpo_pd['co_date'] = DM_store_array_merge_drop_PW['co_date']
output_tenpo_pd['merge_url'] = DM_store_array_merge_drop_PW['url']
output_tenpo_pd['pw_t_code'] = DM_store_array_merge_drop_PW['pw_t_code_y']
output_tenpo_pd['dmm_t_code'] = DM_store_array_merge_drop_PW['dmm_t_code']
output_tenpo_pd['clensing_title'] = DM_store_array_merge_drop_PW['clensing_title']

output_tenpo_pc = pd.merge(output_tenpo_pp, output_tenpo_pd, on=['pw_t_code','dmm_t_code'], how='outer')
output_tenpo_pc['dmm_t_code'] = output_tenpo_pc['dmm_t_code'].str.replace('dmm_','')
output_tenpo_pc['dmm_t_code'] = 'dmm_' + output_tenpo_pc['dmm_t_code']

store_list_innerp = pd.merge(PW_store_array_clensing, output_tenpo_pc, on='pw_t_code', how='inner')
store_list_innerp['比較用の列'] = store_list_innerp.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)

PW_store_array_clensing['比較用の列'] = PW_store_array_clensing.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
PW_store_list_match = PW_store_array_clensing[~PW_store_array_clensing['比較用の列'].isin(store_list_innerp['比較用の列'])]
PW_store_duplicate = PW_store_list_match

store_list_inner = pd.merge(DM_store_array_clensing, output_tenpo_pc, on='dmm_t_code', how='inner')
store_list_inner['比較用の列'] = store_list_inner.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)

DM_store_array_clensing['比較用の列'] = DM_store_array_clensing.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
DM_store_list_match = DM_store_array_clensing[~DM_store_array_clensing['比較用の列'].isin(store_list_inner['比較用の列'])]
DM_store_duplicate = DM_store_list_match

store_array_output = pd.merge(PW_store_duplicate, DM_store_duplicate, on=['clensing_title','state_cd'], how='inner')

#名寄せによりマッチした例
output_tenpo_match = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_match['state_cd'] = store_array_output['state_cd']
output_tenpo_match['t_code'] = store_array_output['t_code_x']
output_tenpo_match['ホール名'] = store_array_output['name_x']
output_tenpo_match['address'] = store_array_output['address_x']
output_tenpo_match['access'] = store_array_output['access_x']
output_tenpo_match['closeday'] = store_array_output['closeday_x']
output_tenpo_match['opentime'] = store_array_output['opentime_x']
output_tenpo_match['service'] = store_array_output['service_x']
output_tenpo_match['rate'] = store_array_output['rate_x']
output_tenpo_match['dai'] = store_array_output['dai_x']
output_tenpo_match['dai_p'] = store_array_output['dai_p_x']
output_tenpo_match['dai_s'] = store_array_output['dai_s_x']
output_tenpo_match['dai_sum'] = store_array_output['dai_sum_x']
output_tenpo_match['parking'] = store_array_output['parking_x']
output_tenpo_match['tel'] = store_array_output['tel_x']
output_tenpo_match['tenpo_update'] = store_array_output['tenpo_update_x']
output_tenpo_match['co_date'] = store_array_output['co_date_x']
output_tenpo_match['merge_url'] = store_array_output['url_x']
output_tenpo_match['pw_t_code'] = store_array_output['pw_t_code_x']
output_tenpo_match['dmm_t_code'] = store_array_output['dmm_t_code_y']

store_array_output_N_outer = pd.merge(PW_store_duplicate, DM_store_duplicate, on=['clensing_title','state_cd'], how='outer')
store_array_output_N_inner = pd.merge(PW_store_duplicate, DM_store_duplicate, on=['clensing_title','state_cd'], how='inner')

store_array_output_N_outer['比較用の列'] = store_array_output_N_outer.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
store_array_output_N_inner['比較用の列'] = store_array_output_N_inner.apply(lambda x: '{}_{}'.format(x[1], x[27]), axis=1)
store_array_output_N = store_array_output_N_outer[~store_array_output_N_outer['比較用の列'].isin(store_array_output_N_inner['比較用の列'])]

PW_store_array_output = store_array_output_N.dropna(subset=['site_x'])
DM_store_array_output = store_array_output_N.dropna(subset=['site_y'])

output_tenpo_p = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_d = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])

#DMだけ
output_tenpo_p['state_cd'] = PW_store_array_output['state_cd']
output_tenpo_p['t_code'] = PW_store_array_output['pw_t_code_x']
output_tenpo_p['ホール名'] = PW_store_array_output['name_x']
output_tenpo_p['address'] = PW_store_array_output['address_x']
output_tenpo_p['access'] = PW_store_array_output['access_x']
output_tenpo_p['closeday'] = PW_store_array_output['closeday_x']
output_tenpo_p['opentime'] = PW_store_array_output['opentime_x']
output_tenpo_p['service'] = PW_store_array_output['service_x']
output_tenpo_p['rate'] = PW_store_array_output['rate_x']
output_tenpo_p['dai'] = PW_store_array_output['dai_x']
output_tenpo_p['dai_p'] = PW_store_array_output['dai_p_x']
output_tenpo_p['dai_s'] = PW_store_array_output['dai_s_x']
output_tenpo_p['dai_sum'] = PW_store_array_output['dai_sum_x']
output_tenpo_p['parking'] = PW_store_array_output['parking_x']
output_tenpo_p['tel'] = PW_store_array_output['tel_x']
output_tenpo_p['tenpo_update'] = PW_store_array_output['tenpo_update_x']
output_tenpo_p['co_date'] = PW_store_array_output['co_date_x']
output_tenpo_p['merge_url'] = PW_store_array_output['url_x']
output_tenpo_p['pw_t_code'] = PW_store_array_output['pw_t_code_x']

#PWだけ
output_tenpo_d['state_cd'] = DM_store_array_output['state_cd']
output_tenpo_d['t_code'] =  DM_store_array_output['dmm_t_code_y']
output_tenpo_d['ホール名'] = DM_store_array_output['name_y']
output_tenpo_d['address'] = DM_store_array_output['address_y']
output_tenpo_d['access'] = DM_store_array_output['access_y']
output_tenpo_d['closeday'] = DM_store_array_output['closeday_y']
output_tenpo_d['opentime'] = DM_store_array_output['opentime_y']
output_tenpo_d['service'] = DM_store_array_output['service_y']
output_tenpo_d['rate'] = DM_store_array_output['rate_y']
output_tenpo_d['dai'] = DM_store_array_output['dai_y']
output_tenpo_d['dai_p'] = DM_store_array_output['dai_p_y']
output_tenpo_d['dai_s'] = DM_store_array_output['dai_s_y']
output_tenpo_d['dai_sum'] = DM_store_array_output['dai_sum_y']
output_tenpo_d['parking'] = DM_store_array_output['parking_y']
output_tenpo_d['tel'] = DM_store_array_output['tel_y']
output_tenpo_d['merge_url'] = DM_store_array_output['url_y']
output_tenpo_d['tenpo_update'] = DM_store_array_output['tenpo_update_x']
output_tenpo_d['co_date'] = DM_store_array_output['co_date_y']
output_tenpo_d['dmm_t_code'] = DM_store_array_output['dmm_t_code_y']

output_tenpo_pc_drop_PW = output_tenpo_pc.dropna(subset=['state_cd_x'])
output_tenpo_pc_drop_DM = output_tenpo_pc[output_tenpo_pc['state_cd_x'].isnull()]

output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'総務省コード_x' : '総務省コード'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'ホール名_x' : 'ホール名'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'address_x' : 'address'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'access_x' : 'access'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'state_cd_x' : 'state_cd'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'t_code_x' : 't_code'})
output_tenpo_pc_drop_PW['pw_t_code'] = output_tenpo_pc_drop_PW['t_code']
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'closeday_x' : 'closeday'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'opentime_x' : 'opentime'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'service_x' : 'service'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'rate_x' : 'rate'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'dai_x' : 'dai'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'dai_p_x' : 'dai_p'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'dai_s_x' : 'dai_s'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'dai_sum_x' : 'dai_sum'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'parking_x' : 'parking'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'tel_x' : 'tel'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'url_y' : 'url'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'tenpo_update_x' : 'tenpo_update'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'merge_url_x' : 'merge_url'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'tel_x' : 'tel'})
output_tenpo_pc_drop_PW = output_tenpo_pc_drop_PW.rename(columns={'co_date_x' : 'co_date'})

output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'総務省コード_y' : '総務省コード'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'ホール名_y' : 'ホール名'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'address_y' : 'address'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'access_y' : 'access'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'state_cd_y' : 'state_cd'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'pw_t_code' : 't_code'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'closeday_y' : 'closeday'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'opentime_y' : 'opentime'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'service_y' : 'service'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'rate_y' : 'rate'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'dai_y' : 'dai'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'dai_p_y' : 'dai_p'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'dai_s_y' : 'dai_s'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'dai_sum_y' : 'dai_sum'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'parking_y' : 'parking'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'tel_y' : 'tel'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'url_y' : 'url'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'tenpo_update_y' : 'tenpo_update'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'merge_url_y' : 'merge_url'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'tel_y' : 'tel'})
output_tenpo_pc_drop_DM = output_tenpo_pc_drop_DM.rename(columns={'co_date_y' : 'co_date'})
output_tenpo_pc_drop_DM['pw_t_code'] = output_tenpo_pc_drop_DM['t_code']

output_tenpo_to_csv = pd.concat([output_tenpo_pc_drop_PW ,output_tenpo_pc_drop_DM, output_tenpo_match, output_tenpo_p, output_tenpo_d],sort=True)

output_tenpo_to_csv = output_tenpo_to_csv.loc[:, ['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code']]

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('==========duplicate : Output_Tenpo_Not_Match==========')

output_tenpo_not_match_PW = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])
output_tenpo_not_match_DM = pd.DataFrame(columns=['総務省コード', 'state_cd', 't_code', 'ホール名', 'sv_level', 'sv_mail', 'sv_bbs', 'sv_ssc', 'sv_dedama', 'address', 'access', 'closeday', 'opentime', 'service', 'rate', 'dai', 'dai_p', 'dai_s', 'dai_sum', 'parking', 'tel', 'url', 'tenpo_update', 'co_date', 'merge_url', 'pw_t_code', 'dmm_t_code'])

output_tenpo_not_match_PW['state_cd'] = PW_store_list_match['state_cd']
output_tenpo_not_match_PW['t_code'] = PW_store_list_match['pw_t_code']
output_tenpo_not_match_PW['ホール名'] = PW_store_list_match['name']
output_tenpo_not_match_PW['address'] = PW_store_list_match['address']
output_tenpo_not_match_PW['access'] = PW_store_list_match['access']
output_tenpo_not_match_PW['closeday'] = PW_store_list_match['closeday']
output_tenpo_not_match_PW['opentime'] = PW_store_list_match['opentime']
output_tenpo_not_match_PW['service'] = PW_store_list_match['service']
output_tenpo_not_match_PW['rate'] = PW_store_list_match['rate']
output_tenpo_not_match_PW['dai'] = PW_store_list_match['dai']
output_tenpo_not_match_PW['dai_p'] = PW_store_list_match['dai_p']
output_tenpo_not_match_PW['dai_s'] = PW_store_list_match['dai_s']
output_tenpo_not_match_PW['dai_sum'] = PW_store_list_match['dai_sum']
output_tenpo_not_match_PW['parking'] = PW_store_list_match['parking']
output_tenpo_not_match_PW['tel'] = PW_store_list_match['tel']
output_tenpo_not_match_PW['tenpo_update'] = PW_store_list_match['tenpo_update']
output_tenpo_not_match_PW['co_date'] = PW_store_list_match['co_date']
output_tenpo_not_match_PW['merge_url'] = PW_store_list_match['url']
output_tenpo_not_match_PW['pw_t_code'] = PW_store_list_match['pw_t_code']

output_tenpo_not_match_DM['state_cd'] = DM_store_list_match['state_cd']
output_tenpo_not_match_DM['t_code'] = DM_store_list_match['t_code']
output_tenpo_not_match_DM['ホール名'] = DM_store_list_match['name']
output_tenpo_not_match_DM['address'] = DM_store_list_match['address']
output_tenpo_not_match_DM['access'] = DM_store_list_match['access']
output_tenpo_not_match_DM['closeday'] = DM_store_list_match['closeday']
output_tenpo_not_match_DM['opentime'] = DM_store_list_match['opentime']
output_tenpo_not_match_DM['service'] = DM_store_list_match['service']
output_tenpo_not_match_DM['rate'] = DM_store_list_match['rate']
output_tenpo_not_match_DM['dai'] = DM_store_list_match['dai']
output_tenpo_not_match_DM['dai_p'] = DM_store_list_match['dai_p']
output_tenpo_not_match_DM['dai_s'] = DM_store_list_match['dai_s']
output_tenpo_not_match_DM['dai_sum'] = DM_store_list_match['dai_sum']
output_tenpo_not_match_DM['parking'] = DM_store_list_match['parking']
output_tenpo_not_match_DM['tel'] = DM_store_list_match['tel']
output_tenpo_not_match_DM['co_date'] = DM_store_list_match['co_date']
output_tenpo_not_match_DM['merge_url'] = DM_store_list_match['url']
output_tenpo_not_match_DM['tenpo_update'] = DM_store_list_match['tenpo_update']
output_tenpo_not_match_DM['dmm_t_code'] = DM_store_list_match['dmm_t_code']

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('=================Check: Data-Check====================')

KISYU_THRESHOLD = 9208
DAI_THRESHOLD = 1727817
TENPO_THRESHOLD = 12573

kisyu_th_text = ''
dai_th_text = ''
tenpo_th_text = ''
kisyu_pcode_text = ''
kisyu_kisyu_text = ''
kisyu_PS_text = ''
dai_state_text = ''
dai_kisyu_text = ''
dai_pcode_text = ''
tenpo_tcode_text = ''
tenpo_hole_text = ''
tenpo_address_texts = ''

kisyu_rate = len(output_kisyu_to_csv) / KISYU_THRESHOLD
dai_rate = len(output_dai_to_csv) / DAI_THRESHOLD
tenpo_rate = len(output_tenpo_to_csv) / TENPO_THRESHOLD

if kisyu_rate < 0.9:
    kisyu_th_text = '機種データが' + str(100 *(1 - kisyu_rate)) + '%減少しました\n'
if dai_rate < 0.9:
    dai_th_text = '台データが' + str(100 *(1 - dai_rate)) + '%減少しました\n'
if tenpo_rate < 0.9:
    tenpo_th_text = '店舗データが' + str(100 *(1 - tenpo_rate)) + '%減少しました\n'

kisyu_len = len(output_kisyu_to_csv)
kisyu_check_temp = output_kisyu_to_csv.dropna(subset=['pcode'])
kisyu_len_temp = len(kisyu_check_temp)
if kisyu_len != kisyu_len_temp:
    kisyu_pcode_text = '機種データ：pcodeに' + str(kisyu_len - kisyu_len_temp) + '個の欠損''があります\n'
    kisyu_len = kisyu_len_temp

kisyu_check_temp = kisyu_check_temp.dropna(subset=['機種名'])
kisyu_len_temp = len(kisyu_check_temp)
if kisyu_len != kisyu_len_temp:
    kisyu_kisyu_text = '機種データ：機種名に' + str(kisyu_len - kisyu_len_temp) + '個の欠損があります\n'
    kisyu_len = kisyu_len_temp

kisyu_check_temp = kisyu_check_temp.dropna(subset=['Ｐ／Ｓ区分'])
kisyu_len_temp = len(kisyu_check_temp)
if kisyu_len != kisyu_len_temp:
    kisyu_PS_text = '機種データ：Ｐ／Ｓ区分に' + str(kisyu_len - kisyu_len_temp) + '個の欠損があります\n'
    kisyu_len = kisyu_len_temp


dai_len = len(output_dai_to_csv)
dai_check_temp = output_dai_to_csv.dropna(subset=['state_cd'])
dai_len_temp = len(dai_check_temp)
if dai_len != dai_len_temp:
    dai_state_text = '台データ：state_cdに'+ str(dai_len - dai_len_temp) +'個の欠損があります\n'
    dai_len = dai_len_temp

dai_check_temp = dai_check_temp.dropna(subset=['機種名(店舗入力名)'])
dai_len_temp = len(dai_check_temp)
if dai_len != dai_len_temp:
    dai_kisyu_text = '台データ：機種名(店舗入力名)に'+ str(dai_len - dai_len_temp) +'個の欠損があります\n'
    dai_len = dai_len_temp

dai_check_temp = dai_check_temp.dropna(subset=['pcode'])
dai_len_temp = len(dai_check_temp)
if dai_len != dai_len_temp:
    dai_pcode_text = '台データ：pcodeに'+ str(dai_len - dai_len_temp) +'個の欠損があります\n'
    dai_len = dai_len_temp


tenpo_len = len(output_tenpo_to_csv)
tenpo_check_temp = output_tenpo_to_csv.dropna(subset=['t_code'])
tenpo_len_temp = len(tenpo_check_temp)
if tenpo_len != tenpo_len_temp:
    tenpo_tcode_text = '店舗データ：t_codeに'+ str(tenpo_len - tenpo_len_temp) +'個の欠損があります\n'
    tenpo_len = tenpo_len_temp

tenpo_check_temp = tenpo_check_temp.dropna(subset=['ホール名'])
tenpo_len_temp = len(tenpo_check_temp)
if tenpo_len != tenpo_len_temp:
    tenpo_hole_text = '店舗データ：ホール名に'+ str(tenpo_len - tenpo_len_temp) +'個の欠損があります\n'
    tenpo_len = tenpo_len_temp

tenpo_check_temp = tenpo_check_temp.dropna(subset=['address'])
tenpo_len_temp = len(tenpo_check_temp)
if tenpo_len != tenpo_len_temp:
    tenpo_address_text = '店舗データ：addressに'+ str(tenpo_len - tenpo_len_temp) +'個の欠損があります\n'
    tenpo_len = tenpo_len_temp

output_text = str(kisyu_th_text + dai_th_text + tenpo_th_text + kisyu_pcode_text + kisyu_kisyu_text + kisyu_PS_text + dai_state_text + dai_kisyu_text + dai_pcode_text + tenpo_tcode_text + tenpo_hole_text + tenpo_address_texts)
if output_text != '':
    mail_gun(output_text)

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('==============Connect: Daito-Network==================')

# SFTP接続先の設定
HOST = 'sftp.daitogiken.com'
PORT = 2022
SFTP_USER = 'kwk-001'
SFTP_PASSWORD = 'EtrWJPf5'

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
client.connect(HOST, port=PORT, username=SFTP_USER, password=SFTP_PASSWORD)
try:
    # SFTPセッション開始
    sftp_connection = client.open_sftp()
    # ファイル出力
    file_kisyu = sftp_connection.file('/incoming/p_kisyu_'+ dir_date +'.csv', "a", -1)
    file_kisyu.write("\uFEFF")
    file_kisyu.write(output_kisyu_to_csv.to_csv(index=False, encoding="utf-8"))
    file_dai = sftp_connection.file('/incoming/p_dai_'+ dir_date +'.csv', "a", -1)
    file_dai.write("\uFEFF")
    file_dai.write(output_dai_to_csv.to_csv(index=False, encoding="utf-8"))
    file_tempo = sftp_connection.file('/incoming/p_tenpo_'+ dir_date +'.csv', "a", -1)
    file_tempo.write("\uFEFF")
    file_tempo.write(output_tenpo_to_csv.to_csv(index=False, encoding="utf-8"))
finally:
    client.close()

# SFTP接続先の設定
HOST = '104.198.89.41'
PORT = 'ssh'
USER = 'y_iwano'
KEY_FILE  = './id_rsa'
# 秘密鍵ファイルからキーを取得
rsa_key = paramiko.RSAKey.from_private_key_file(KEY_FILE)
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    #SFTPセッション開始
    client.connect(HOST, PORT, USER, pkey=rsa_key) # キーを指定することでパスワードは必要なし
    sftp_connection = client.open_sftp()
    # ファイル出力
    file_kisyu1 = sftp_connection.file('/home/y_iwano/Daito/NMatch/kisyu_PW_'+ dir_date +'.csv', "a", -1)
    file_kisyu1.write("\uFEFF")
    file_kisyu1.write(output_kisyu_not_match_PW.to_csv(index=False, encoding="utf-8"))
    file_kisyu2 = sftp_connection.file('/home/y_iwano/Daito/NMatch/kisyu_DM_'+ dir_date +'.csv', "a", -1)
    file_kisyu2.write("\uFEFF")
    file_kisyu2.write(output_kisyu_not_match_DM.to_csv(index=False, encoding="utf-8"))
    file_tempo1 = sftp_connection.file('/home/y_iwano/Daito/NMatch/tenpo_DM_'+ dir_date +'.csv', "a", -1)
    file_tempo1.write("\uFEFF")
    file_tempo1.write(output_tenpo_not_match_PW.to_csv(index=False, encoding="utf-8"))
    file_tempo2 = sftp_connection.file('/home/y_iwano/Daito/NMatch/tenpo_PW_'+ dir_date +'.csv', "a", -1)
    file_tempo2.write("\uFEFF")
    file_tempo2.write(output_tenpo_not_match_DM.to_csv(index=False, encoding="utf-8"))
finally:
    client.close()

mem = psutil.virtual_memory()
logging.info('Used_memory:' + mem.used)

logging.info('==========================end=========================')